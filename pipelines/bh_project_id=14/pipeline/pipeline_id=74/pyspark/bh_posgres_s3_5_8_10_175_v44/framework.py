"""Portable spark-plain runtime helpers (emitted as ``framework.py``).

Cloud-specific behaviour (Secrets Manager, EMR/YARN detect, s3a credentials,
managed jar filtering, executor ``addFile``) lives in the sibling cloud adapter
(``emr.py`` / future ``azure.py``). Kept free of migrate-provider imports.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

try:
    import glue as _cloud  # sibling (generated project / spark-submit --py-files)
except ImportError:
    from bh_bhengine_pyspark.runtime import glue as _cloud


# Import name -> pip requirement for the driver process. JDBC/Kafka drivers are
# JVM jars handled by spark.jars.packages, so this list is connector-independent.
# Specs mirror requirements.txt so a relocated main.py installs the same versions
# (notably PySpark 3.x: the emitted code is not validated against PySpark 4).
# Cloud SDKs live in the cloud adapter (see ``_cloud.REQUIRED_PACKAGES``) and are
# merged into bootstrap below.
_REQUIRED_PACKAGES = {
    "pyspark": "pyspark>=3.5,<4",
    "yaml": "pyyaml>=6.0",
}


def _all_required_packages():
    """Framework + cloud-adapter pip specs for bootstrap / missing probes."""
    packages = dict(_REQUIRED_PACKAGES)
    extra = getattr(_cloud, "REQUIRED_PACKAGES", None)
    if isinstance(extra, dict):
        packages.update(extra)
    return packages

_BOOTSTRAPPED = "_BH_SPARK_PLAIN_BOOTSTRAPPED"
# Set by TTU (and similar hosts) when main.py is exec()'d in-process — never
# create a sibling .venv or os.execve (that would replace the host process).
_MANAGED = "BH_SPARK_PLAIN_MANAGED"


def _is_managed_spark_runtime():
    """True on EMR/YARN (or when BH_SPARK_RUNTIME marks a managed cluster).

    Managed runtimes already ship Hadoop/S3A jars and must not pin
    ``SPARK_LOCAL_IP`` to loopback or auto-pip-install beside main.py.
    """
    return _cloud.is_managed_runtime()


def _in_virtualenv():
    return bool(os.environ.get("VIRTUAL_ENV")) or sys.prefix != getattr(
        sys, "base_prefix", sys.prefix
    )


def _project_venv_python(base):
    """Create (once) a virtualenv beside main.py and return its interpreter."""
    venv_dir = base / ".venv"
    python = venv_dir / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    if python.is_file():
        return python
    print(f"Creating virtualenv: {venv_dir}")
    try:
        subprocess.check_call([sys.executable, "-m", "venv", str(venv_dir)])
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(
            f"Could not create {venv_dir}: {exc}\n"
            "Install the venv module (Debian/Ubuntu: sudo apt install python3-venv), "
            "or run this pipeline from an activated virtualenv."
        ) from exc
    return python


def _packages_importable(python):
    """True when ``python`` can already import every required package."""
    pkgs = _all_required_packages()
    probe = "import " + ", ".join(sorted(pkgs))
    try:
        return (
            subprocess.call(
                [str(python), "-c", probe],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            == 0
        )
    except OSError:
        return False


def _pip_install(python, base, modules):
    """Install requirements.txt when shipped, else the given modules' pinned specs."""
    packages = _all_required_packages()
    requirements = base / "requirements.txt"
    if requirements.is_file():
        args = ["-r", str(requirements)]
    else:
        args = [packages[mod] for mod in modules]
    cmd = [str(python), "-m", "pip", "install", "--disable-pip-version-check", *args]
    print(f"Installing: {' '.join(args)}")
    if subprocess.call(cmd) != 0:
        raise RuntimeError("Dependency install failed: " + " ".join(cmd))


def _ensure_dependencies(base_dir=None):
    """Make the driver's Python packages importable, installing them if needed.

    Generated pipelines are meant to run as ``python3 main.py`` with no manual
    setup. Distro interpreters are externally managed (PEP 668) and reject
    installs, so when the current interpreter is not a virtualenv a project-local
    ``.venv`` is created beside main.py and the pipeline re-executes from it.

    On EMR/YARN never ``pip install --user`` (YARN home is often non-writable).
    Preinstall on the AMI/bootstrap or ship a venv; fail fast if packages are missing.
    TTU / in-process hosts set ``BH_SPARK_PLAIN_MANAGED=1`` and must preinstall.
    """
    packages = _all_required_packages()
    missing = [mod for mod in packages if importlib.util.find_spec(mod) is None]
    if not missing:
        return

    if base_dir is not None:
        base = Path(base_dir)
    else:
        # When main.py is exec()'d in tests, ``__file__`` may be absent.
        file_hint = globals().get("__file__")
        base = Path(file_hint).resolve().parent if file_hint else Path.cwd()
    if os.environ.get(_MANAGED) == "1":
        # Host runner owns the environment (TTU live/fixture). Do not bootstrap.
        raise RuntimeError(
            "Missing packages in managed environment: {}. Install on the host: {} -m pip install {}".format(
                ", ".join(missing),
                sys.executable,
                " ".join(packages[mod] for mod in missing),
            )
        )
    if _is_managed_spark_runtime():
        # EMR/YARN/Glue: pip --user fails (non-writable home / system Python).
        custom = getattr(_cloud, "missing_managed_packages_message", None)
        if callable(custom):
            raise RuntimeError(custom(missing, packages, sys.executable))
        raise RuntimeError(
            "Missing packages on managed EMR/YARN runtime: {}. "
            "Preinstall on the AMI/bootstrap (do not pip install --user): {} -m pip install {}. "
            "Alternatively ship them on spark-submit --py-files "
            "(e.g. a zip of the cloud adapter deps and pyyaml).".format(
                ", ".join(missing),
                sys.executable,
                " ".join(packages[mod] for mod in missing),
            )
        )
    if os.environ.get(_BOOTSTRAPPED) == "1":
        # Already reinstalled and restarted once; a second pass means the install
        # did not take, so stop instead of re-executing forever.
        raise RuntimeError(
            "Still missing after install: {}\nInstall manually: {} -m pip install {}".format(
                ", ".join(missing),
                sys.executable,
                " ".join(packages[mod] for mod in missing),
            )
        )

    print(f"Missing required packages: {', '.join(missing)}")
    if _in_virtualenv():
        _pip_install(sys.executable, base, missing)
        importlib.invalidate_caches()
        return

    python = _project_venv_python(base)
    if not _packages_importable(python):
        # A virtualenv is isolated from the current interpreter, so install the
        # full set: a package present here (and thus not in `missing`) would
        # otherwise be skipped and still be absent after the restart.
        _pip_install(python, base, list(packages))

    script = Path(sys.argv[0]).resolve()
    print(f"Restarting pipeline with {python}")
    sys.stdout.flush()
    os.execve(
        str(python),
        [str(python), str(script), *sys.argv[1:]],
        dict(os.environ, **{_BOOTSTRAPPED: "1"}),
    )


def _env(name, default=None):
    if not name:
        return default
    return os.environ.get(str(name), default)


def _normalize_aws_region(raw):
    """Normalize SCH enum regions (``US_EAST_1``) to SDK form (``us-east-1``)."""
    import re as _re

    text = str(raw or "").strip()
    if not text:
        return ""
    if "-" in text:
        return text.lower()
    if _re.fullmatch(r"[A-Za-z]+(_[A-Za-z0-9]+)+", text):
        return text.lower().replace("_", "-")
    return text.lower()


def _sanitize_param_value(key, val):
    """Normalize region-like parameter values before EL substitution / submit."""
    text = str(val)
    k = str(key or "").strip()
    if k in {
        "S3_REGION",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "region",
        "region_name",
        "aws_region",
    } or k.endswith("_REGION") or k.endswith("_region"):
        return _normalize_aws_region(text) or text
    return text


def _configure_local_spark_env():
    """Pin the driver to loopback when the host name resolves to 127.x.

    On WSL2 and many containers the host name maps to 127.0.1.1, so Spark
    advertises an unreachable interface and BlockManager registration fails with
    a NullPointerException. Delegates to the cloud adapter (no-op on EMR/YARN).
    """
    _cloud.configure_spark_env()


def _spark_extra_jars(project_dir=None):
    """Resolve custom JARs for ``spark.jars`` (customer libraries on the classpath).

    Collects paths from ``BH_SPARK_JARS`` / ``SPARK_JARS`` (comma, colon, or
    semicolon separated) and ``<project_dir>/jars/*.jar``. Missing paths are
    skipped. Returns a comma-separated list suitable for ``spark.jars``.
    """
    import re as _re

    paths = []
    raw = os.environ.get("BH_SPARK_JARS") or os.environ.get("SPARK_JARS") or ""
    for part in _re.split(r"[,:;]", str(raw)):
        text = part.strip()
        if text:
            paths.append(Path(text).expanduser())
    root = Path(project_dir) if project_dir is not None else Path.cwd()
    jars_dir = root / "jars"
    if jars_dir.is_dir():
        paths.extend(sorted(jars_dir.glob("*.jar")))
    seen = set()
    out = []
    for path in paths:
        try:
            resolved = str(path.resolve())
        except Exception:
            continue
        if resolved in seen or not Path(resolved).is_file():
            continue
        seen.add(resolved)
        out.append(resolved)
    return ",".join(out)




def _spark_jar_packages(coords_csv):
    """Reconcile Maven coords with the jars already shipped inside PySpark.

    hadoop-aws must match the Hadoop runtime exactly (a 3.4.x jar on a Hadoop
    3.3.x build fails when s3a initialises), and Hadoop 3.3.x needs AWS SDK v1
    rather than v2. Coords whose artifact is already present are dropped so Ivy
    does not put a second, conflicting copy on the classpath.

    On EMR/YARN, hadoop-aws and AWS SDK bundles are always dropped — the cluster
    already provides matching S3A support; Ivy-pulling another copy breaks s3a.
    """
    coords = [c.strip() for c in str(coords_csv or "").split(",") if c.strip()]
    if not coords:
        return ""
    if _is_managed_spark_runtime():
        coords = _cloud.filter_jar_packages(coords)
    try:
        import pyspark

        jars_dir = Path(pyspark.__file__).parent / "jars"
        names = [p.name for p in jars_dir.glob("*.jar")] if jars_dir.is_dir() else []
    except Exception:
        names = []

    hadoop_ver = ""
    for name in names:
        if name.startswith("hadoop-client-api-"):
            hadoop_ver = name[len("hadoop-client-api-") : -len(".jar")]
            break

    resolved = []
    for coord in coords:
        parts = coord.split(":")
        if len(parts) != 3:
            resolved.append(coord)
            continue
        group, artifact, version = parts
        if artifact == "hadoop-aws" and hadoop_ver:
            version = hadoop_ver
        if (
            group == "software.amazon.awssdk"
            and artifact == "bundle"
            and hadoop_ver.startswith("3.3.")
        ):
            group, artifact, version = "com.amazonaws", "aws-java-sdk-bundle", "1.12.262"
        prefix = artifact + "-"
        if any(n.startswith(prefix) and n[len(prefix) : len(prefix) + 1].isdigit() for n in names):
            continue
        resolved.append(f"{group}:{artifact}:{version}")
    return ",".join(resolved)


# Execution evidence: (kind, stage, rows, target, verified_objects)
_PIPELINE_EVENTS = []


def _log(msg):
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


# Targets written as files/objects, whose destination is a listable Hadoop path.
# A Kafka topic or JDBC table is not, so it can never be verified this way.
_VERIFIABLE_FORMATS = ("csv", "json", "parquet", "delta", "orc", "avro", "text")


def _hadoop_list_uri(path):
    """URI Hadoop should list after an S3 write.

    Glue's EMRFS talks ``s3://``. Spark writers use ``s3a://``. ``exists()`` on an
    S3 prefix is often false even when part files exist, so callers must list.
    """
    uri = str(path or "").strip()
    if uri.startswith("s3a://") and _is_managed_spark_runtime():
        return "s3://" + uri[len("s3a://") :]
    return uri


def _verify_output_path(spark, path):
    """Count non-metadata files actually present at ``path`` via Hadoop FS.

    Returns None when the path cannot be inspected at all.
    """
    try:
        jvm = spark.sparkContext._jvm
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        uri = _hadoop_list_uri(path)
        hpath = jvm.org.apache.hadoop.fs.Path(uri)
        fs = hpath.getFileSystem(hconf)
        # Do not trust exists() on S3 prefixes — false here used to fail the
        # whole Glue job with SystemExit(1) after a successful json/parquet write.
        count = 0
        walker = fs.listFiles(hpath, True)
        while walker.hasNext():
            name = walker.next().getPath().getName()
            if not (name.startswith("_") or name.startswith(".")):
                count += 1
        return count
    except Exception:
        return None


def _sink_capture_df(df):
    """Return a dataframe suitable for test/compare sink capture.

    Upstream PySpark stages may add ``PY_KAFKA_PAYLOAD`` as an internal helper
    column for Kafka serialization. StreamSets does not expose that column on the
    destination record, so drop it before ``sinks[...]`` assignment.
    """
    if "PY_KAFKA_PAYLOAD" in df.columns:
        return df.drop("PY_KAFKA_PAYLOAD")
    return df


def _record_read(stage, df):
    """Log how many rows a reader produced so an empty source is obvious."""
    try:
        rows = df.count()
    except Exception as exc:
        _log(f"[READ ] {stage}: row count unavailable ({type(exc).__name__})")
        return df
    _PIPELINE_EVENTS.append(("read", stage, rows, "", None))
    _log(f"[READ ] {stage}: {rows} row(s)")
    return df


def _record_write(spark, stage, df, target, fmt="", mode=""):
    """Log rows written; for file targets also verify objects really landed."""
    try:
        rows = df.count()
    except Exception:
        rows = -1
    verifiable = str(fmt).strip().lower() in _VERIFIABLE_FORMATS
    verified = (
        _verify_output_path(spark, target)
        if verifiable and str(target).strip()
        else None
    )
    _PIPELINE_EVENTS.append(("write", stage, rows, str(target), verified))
    detail = f"[{fmt}/{mode}]" if fmt or mode else ""
    suffix = "" if verified is None else f", verified {verified} file(s) at target"
    _log(f"[WRITE] {stage}: {rows} row(s) -> {target} {detail}{suffix}".rstrip())


def _pipeline_summary(pipeline_name=""):
    """Print an unambiguous execution verdict. Returns True when data landed.

    Exists because a spark-plain run that reads/writes nothing still exits 0,
    which is indistinguishable from success without an explicit statement.
    """
    writes = [e for e in _PIPELINE_EVENTS if e[0] == "write"]
    reads = [e for e in _PIPELINE_EVENTS if e[0] == "read"]
    bar = "=" * 68
    _log("\n" + bar)
    _log(f"PIPELINE EXECUTION SUMMARY{(': ' + pipeline_name) if pipeline_name else ''}")
    _log(bar)
    for _, stage, rows, _t, _v in reads:
        _log(f"  READ   {stage}: {rows} row(s)")
    if not writes:
        _log("  WRITE  (none)")
    for _, stage, rows, target, verified in writes:
        vtxt = "n/a" if verified is None else f"{verified} file(s)"
        _log(f"  WRITE  {stage}: {rows} row(s) -> {target} (verified: {vtxt})")
    _log(bar)

    problems = []
    if not writes:
        problems.append("no writer executed (nothing was written)")
    for _, stage, rows, target, verified in writes:
        if str(target).lower() == "discard":
            continue  # intentional Trash / Discard destination
        if rows == 0:
            problems.append(f"{stage} wrote 0 rows (source empty or fully filtered)")
        if verified == 0:
            tgt = str(target)
            # S3A prefix exists()/listFiles is eventually consistent and often
            # reports 0 right after a successful Spark write; do not fail the run.
            if rows > 0 and tgt.lower().startswith(("s3a://", "s3://", "s3n://")):
                _log(
                    f"  NOTE   {stage}: Hadoop listing saw 0 files at {tgt} "
                    "after Spark wrote rows (S3 prefix exists() is unreliable); "
                    "not treating as failure"
                )
            else:
                problems.append(f"{stage} target has no files: {target}")

    if problems:
        _log("RESULT: FAILED - pipeline did NOT deliver data")
        for p in problems:
            _log(f"  - {p}")
        _log(bar)
        return False
    delivered = [e for e in writes if str(e[3]).lower() != "discard"]
    if delivered:
        total = sum(e[2] for e in delivered if e[2] > 0)
        _log(
            f"RESULT: SUCCESS - pipeline executed and delivered data "
            f"({len(delivered)} writer(s), {total} row(s) written)"
        )
    else:
        _log(
            "RESULT: SUCCESS - pipeline executed "
            "(all writers are Discard/Trash; no external target)"
        )
    _log(bar)
    return True


def _rename_column_ci(df, src, dst):
    """Rename src→dst with case-insensitive source match; no-op if src missing.

    When both names are present the destination is already populated (readers
    that project ``src AS dst`` make this rename a no-op), so the redundant
    source is dropped rather than allowed to overwrite the destination.
    """
    if src == dst:
        return df
    cols = list(df.columns)
    resolved = src if src in cols else next((c for c in cols if str(c).lower() == str(src).lower()), None)
    if resolved is None:
        return df
    if dst in cols and dst != resolved:
        return df.drop(resolved)
    return df.withColumnRenamed(resolved, dst)


def _normalize_secret_payload(data):
    """Flatten config blocks and normalize common Kafka / JDBC secret key aliases.

    Nested ``config`` is flattened away. Non-empty top-level keys override
    ``config`` (same precedence as ``{**cfg, **data}`` for real values). Empty
    or null top-level values do not wipe a nested ``config`` value.
    """
    if not isinstance(data, dict):
        return {}
    cfg = data.get("config")
    merged = dict(cfg) if isinstance(cfg, dict) else {}
    for key, value in data.items():
        if key == "config":
            continue
        if value not in (None, ""):
            merged[key] = value
    alias_groups = {
        "ssl_truststore_password": (
            "truststorePassword",
            "truststore_password",
            "sslTruststorePassword",
        ),
        "ssl_keystore_password": (
            "keystorePassword",
            "keystore_password",
            "sslKeystorePassword",
        ),
        "ssl_key_password": ("keyPassword", "key_password", "sslKeyPassword"),
        "bootstrap_servers": ("bootstrap.servers", "metadataBrokerList"),
        "security_protocol": ("securityProtocol", "security_protocol"),
        "sasl_mechanism": ("saslMechanism", "sasl_mechanism"),
        "sasl_username": ("username", "user"),
        "sasl_password": ("password", "pass"),
        "metastore_uris": (
            "metastoreUris",
            "hive_metastore_uris",
            "hiveMetastoreUris",
        ),
        "warehouse": (
            "s3_warehouse",
            "table_location",
            "tableLocation",
        ),
    }
    for canonical, aliases in alias_groups.items():
        if merged.get(canonical) not in (None, ""):
            continue
        for alt in aliases:
            if merged.get(alt) not in (None, ""):
                merged[canonical] = merged[alt]
                break
    return merged


_SECRET_CACHE = {}


def _secret_payload(value):
    if not isinstance(value, str) or not value.strip():
        return {}
    text = value.strip()
    if text.startswith("{"):
        try:
            data = json.loads(text)
        except Exception:
            data = None
        if isinstance(data, dict):
            return _normalize_secret_payload(data)
    return {"password": text, "aws_secret_access_key": text}


def _secret_name_candidates(secret_name, conn=None):
    """Vault lookup names for a secret ref.

    Catalog stores scoped refs such as ``<kv-scope>/<secret-name>`` while the
    vault only holds the trailing segment, so the tail is tried first.
    """
    sources = [secret_name]
    if isinstance(conn, dict):
        sources.extend([conn.get("secretName"), conn.get("name")])
    candidates = []
    for raw in sources:
        text = str(raw or "").strip()
        if not text:
            continue
        for cand in (text.rsplit("/", 1)[-1].strip(), text, text.replace("/", "-")):
            if cand and cand not in candidates:
                candidates.append(cand)
    return candidates


def _azure_secret_client():
    """Deprecated shim — Azure KV lives in the cloud adapter."""
    return _cloud._azure_secret_client()






def _secret_payload_looks_resolved(resolved):
    """True when the payload has any connector-usable credential/endpoint field."""
    if not isinstance(resolved, dict) or not resolved:
        return False
    # JDBC / Oracle / Postgres, S3, Kafka (and common aliases after normalize).
    markers = (
        "host",
        "password",
        "username",
        "user",
        "aws_secret_access_key",
        "aws_access_key_id",
        "bucket",
        "bootstrap_servers",
        "sasl_username",
        "sasl_password",
        "topic",
        "subscribe",
        "ssl_ca_pem",
        "ssl_truststore_location",
        "jdbc_url",
        "url",
        "metastore_uris",
        "warehouse",
        "s3_warehouse",
    )
    return any(str(resolved.get(k) or "").strip() for k in markers)


def _require_secret_host(conn, secrets, *, kind="JDBC"):
    """Fail fast when a secret-backed connection has no host (avoid localhost)."""
    host = None
    if isinstance(secrets, dict):
        host = secrets.get("host")
    if not host and isinstance(conn, dict):
        host = conn.get("host")
    host = str(host or "").strip()
    if host:
        return host
    label = ""
    if isinstance(conn, dict):
        label = conn.get("secret_name") or conn.get("name") or conn.get("connection_type") or ""
    raise ValueError(
        f"{kind} host unresolved for connection {label!r}. "
        "Secret lookup failed or the secret payload is missing host. "
        "Check Secrets Manager / Key Vault permissions, region, and JSON keys."
    )


def _fetch_secret(conn):
    """Retrieve secret credentials dynamically using secret_name from the cloud adapter."""
    if not isinstance(conn, dict):
        return {}
    secret_name = str(
        conn.get("secret_name") or conn.get("secretName") or conn.get("name") or ""
    ).strip()
    if not secret_name:
        return {}
    if secret_name in _SECRET_CACHE and _SECRET_CACHE[secret_name]:
        return _SECRET_CACHE[secret_name]

    candidates = _secret_name_candidates(secret_name, conn)
    resolved, last_err = _cloud.fetch_secret(candidates, conn=conn)
    # Cloud adapters return raw JSON; normalize aliases / nested config here.
    if resolved:
        resolved = _normalize_secret_payload(resolved)

    # Inline non-secret endpoint fields from the connection as a last resort
    # (catalog enrich may leave host/bucket on the in-memory conn).
    fallback = _normalize_secret_payload(conn)
    for k, v in fallback.items():
        if k not in resolved or not resolved[k]:
            resolved[k] = v

    if secret_name and resolved:
        _SECRET_CACHE[secret_name] = resolved

    if not _secret_payload_looks_resolved(resolved):
        sys.stderr.write(
            f"ERROR: Could not resolve secret_name={secret_name!r}. Last error: {last_err}\n"
        )
    return resolved


def _ensure_jdbc_jars(spark):
    """Ensure common JDBC drivers are on the active Spark JVM classpath.

    Probes Oracle + Postgres (+ peers). Prefers jars already present via
    ``spark.jars`` / ``--jars`` / ``./jars``. On EMR/YARN never Maven-downloads
    (outbound often blocked); local/dev may download a missing driver once.
    """
    drivers = (
        ("oracle.jdbc.OracleDriver", "ojdbc", "ojdbc8-21.11.0.0.jar",
         "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.11.0.0/ojdbc8-21.11.0.0.jar"),
        ("org.postgresql.Driver", "postgresql", "postgresql-42.7.12.jar",
         "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.12/postgresql-42.7.12.jar"),
        ("com.mysql.cj.jdbc.Driver", "mysql-connector", "mysql-connector-j-8.2.0.jar",
         "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar"),
    )

    search_dirs = [
        Path.cwd() / "jars",
        Path.cwd(),
        Path("/tmp/jars"),
    ]
    file_hint = globals().get("__file__")
    if file_hint:
        search_dirs.insert(0, Path(file_hint).resolve().parent / "jars")
        search_dirs.insert(1, Path(file_hint).resolve().parent)
    try:
        conf_jars = spark.sparkContext.getConf().get("spark.jars") or ""
        for part in str(conf_jars).split(","):
            text = part.strip()
            if text:
                search_dirs.append(Path(text).expanduser())
    except Exception:
        pass

    jvm = spark.sparkContext._jvm
    missing = []
    for class_name, _needle, _jar, _url in drivers:
        try:
            jvm.java.lang.Class.forName(class_name)
        except Exception:
            missing.append((class_name, _needle, _jar, _url))

    if not missing:
        return

    def _add_jar(jar_path):
        print(f"Adding JDBC JAR to Spark Context: {jar_path}")
        try:
            if hasattr(spark.sparkContext, "addJar"):
                spark.sparkContext.addJar(jar_path)
            elif hasattr(spark.sparkContext, "_jsc") and spark.sparkContext._jsc:
                spark.sparkContext._jsc.sc().addJar(jar_path)
        except Exception:
            try:
                if hasattr(spark.sparkContext, "_jsc") and spark.sparkContext._jsc:
                    spark.sparkContext._jsc.sc().addJar(jar_path)
            except Exception as exc:
                print(f"Warning: Could not add Jar to SparkContext: {exc}")

    for class_name, needle, jar_name, url in missing:
        target_jar = None
        for candidate in search_dirs:
            try:
                path = candidate if candidate.suffix.lower() == ".jar" else candidate / jar_name
                if path.is_file() and needle in path.name.lower():
                    target_jar = path
                    break
                if candidate.is_file() and needle in candidate.name.lower():
                    target_jar = candidate
                    break
                # Also accept any jar in jars/ whose name matches the needle.
                if candidate.is_dir():
                    for hit in sorted(candidate.glob("*.jar")):
                        if needle in hit.name.lower():
                            target_jar = hit
                            break
                if target_jar:
                    break
            except Exception:
                continue

        if not target_jar and not _is_managed_spark_runtime():
            target_dir = Path.cwd() / "jars"
            try:
                target_dir.mkdir(parents=True, exist_ok=True)
                target_jar = target_dir / jar_name
                print(f"Downloading JDBC driver ({jar_name})...")
                import urllib.request

                urllib.request.urlretrieve(url, str(target_jar))
            except Exception as exc:
                print(f"Could not auto-download JDBC driver {jar_name}: {exc}")
                target_jar = None

        if not target_jar or not target_jar.is_file():
            print(
                f"WARNING: {class_name} missing and jar matching {needle!r} not found; "
                "pass --jars / --packages on EMR, or Glue --extra-jars."
            )
            continue
        _add_jar(str(target_jar.resolve()))

def _jdbc_driver(connection_type):
    return {
        "PostgreSQL": "org.postgresql.Driver",
        "Oracle": "oracle.jdbc.OracleDriver",
        "MySQL": "com.mysql.cj.jdbc.Driver",
        "MsSQL": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "Snowflake": "net.snowflake.client.jdbc.SnowflakeDriver",
    }.get(str(connection_type or ""), "org.postgresql.Driver")


def _jdbc_split_table_ref(table):
    """Return (schema_or_None, bare_table) for JDBC metadata lookups."""
    text = str(table or "").strip()
    if not text or " " in text or "(" in text:
        return None, text
    if "." in text:
        schema, name = text.rsplit(".", 1)
        return schema.strip() or None, name.strip()
    return None, text


def _jdbc_is_numeric_partition_type(data_type) -> bool:
    """Spark JDBC parallel read on AWS Glue expects numeric partition bounds.

    Date/timestamp MIN/MAX (e.g. ``2026-04-13``) cannot be cast to integer bounds
    and fail at runtime — only numeric columns are safe for ``partitionColumn``.
    """
    from pyspark.sql.types import (
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
    )

    return isinstance(
        data_type,
        (ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType),
    )


def _jdbc_schema_field_for_column(spark, url, table, props, partition_column):
    """Return Spark ``StructField`` for ``partition_column`` on ``table``, if known."""
    col = str(partition_column or "").strip()
    if not col:
        return None
    try:
        sample = spark.read.jdbc(url=url, table=table, properties=dict(props or {})).limit(0)
        by_name = {f.name: f for f in sample.schema.fields}
        if col in by_name:
            return by_name[col]
        lower = col.lower()
        for name, field in by_name.items():
            if name.lower() == lower:
                return field
    except Exception:
        return None
    return None


def _jdbc_discover_partition_column(spark, url, table, props):
    """Resolve a partition column when StreamSets left offsetColumn empty.

    Mirrors Transformer JDBC Table selection when skip-offset is on:
    primary-key column first, else first indexed **numeric** column.
    Falls back to first integral Spark column if JDBC metadata is unavailable.
    Date/timestamp columns are excluded — Glue JDBC parallel read needs numeric bounds.
    """
    schema_name, bare = _jdbc_split_table_ref(table)
    props = dict(props or {})
    # 1) JDBC DatabaseMetaData primary keys / indexes
    try:
        jvm = spark._jvm
        driver = props.get("driver")
        if driver:
            jvm.java.lang.Class.forName(driver)
        conn = jvm.java.sql.DriverManager.getConnection(
            url,
            props.get("user") or props.get("username") or "",
            props.get("password") or "",
        )
        try:
            meta = conn.getMetaData()
            # Primary key
            rs = meta.getPrimaryKeys(None, schema_name, bare)
            pk_cols = []
            while rs.next():
                pk_cols.append((int(rs.getShort("KEY_SEQ")), rs.getString("COLUMN_NAME")))
            rs.close()
            if pk_cols:
                pk_cols.sort(key=lambda x: x[0])
                return str(pk_cols[0][1])
            # Indexed columns (unique indexes preferred)
            rs = meta.getIndexInfo(None, schema_name, bare, False, True)
            indexed = []
            while rs.next():
                col = rs.getString("COLUMN_NAME")
                if col:
                    indexed.append(str(col))
            rs.close()
            if indexed:
                # Prefer numeric/datetime among indexed via schema probe below.
                candidates = indexed
            else:
                candidates = []
        finally:
            conn.close()
    except Exception:
        candidates = []

    # 2) Spark schema probe — prefer indexed names, else first integral/timestamp.
    try:
        sample = spark.read.jdbc(url=url, table=table, properties=props).limit(0)
        fields = list(sample.schema.fields)
    except Exception:
        return None

    def _is_split_friendly(field):
        from pyspark.sql.types import ByteType

        if isinstance(field.dataType, ByteType):
            return True
        return _jdbc_is_numeric_partition_type(field.dataType)

    by_name = {f.name: f for f in fields}
    for name in candidates:
        field = by_name.get(name)
        if field is not None and _is_split_friendly(field):
            return name
    for field in fields:
        if _is_split_friendly(field):
            return field.name
    return None


def _jdbc_partition_bounds(spark, url, table, props, partition_column):
    """Compute MIN/MAX for Spark JDBC partitionColumn bounds."""
    if not partition_column:
        return None, None
    col = str(partition_column).replace('"', "")
    # Quote identifiers lightly for common engines; bounds query is a subquery table.
    q = f'(SELECT MIN("{col}") AS lo, MAX("{col}") AS hi FROM {table}) _bh_bounds'
    try:
        row = spark.read.jdbc(url=url, table=q, properties=props).collect()[0]
        lo, hi = row[0], row[1]
        if lo is None or hi is None:
            return None, None
        return str(lo), str(hi)
    except Exception:
        # Unquoted fallback (Postgres lowercases unquoted identifiers).
        q2 = f"(SELECT MIN({col}) AS lo, MAX({col}) AS hi FROM {table}) _bh_bounds"
        try:
            row = spark.read.jdbc(url=url, table=q2, properties=props).collect()[0]
            lo, hi = row[0], row[1]
            if lo is None or hi is None:
                return None, None
            return str(lo), str(hi)
        except Exception:
            return None, None


def _jdbc_read_partitioned(
    spark,
    url,
    table,
    props,
    *,
    partition_column=None,
    lower_bound=None,
    upper_bound=None,
    num_partitions=1,
):
    """Spark JDBC read with optional parallel partitions (Transformer parity)."""
    props = dict(props or {})
    try:
        n = int(num_partitions or 1)
    except (TypeError, ValueError):
        n = 1
    use_parallel = (
        n > 1
        and partition_column
        and lower_bound is not None
        and upper_bound is not None
        and str(lower_bound) != ""
        and str(upper_bound) != ""
    )
    if use_parallel:
        field = _jdbc_schema_field_for_column(
            spark, url, table, props, str(partition_column)
        )
        if field is not None and not _jdbc_is_numeric_partition_type(field.dataType):
            use_parallel = False
    if use_parallel:
        return spark.read.jdbc(
            url=url,
            table=table,
            column=str(partition_column),
            lowerBound=str(lower_bound),
            upperBound=str(upper_bound),
            numPartitions=n,
            properties=props,
        )
    return spark.read.jdbc(url=url, table=table, properties=props)


def _jdbc_align_to_table(spark, df, url, table, props):
    """Keep only columns that exist on the JDBC target table.

    Name-based JDBC writers ignore extra columns; Spark truncate/append
    requires every DataFrame column to exist on the table, so leftovers
    (e.g. validation flags) fail the write. If the table is missing
    (first create), return the original DataFrame.
    """
    read_props = {k: v for k, v in dict(props or {}).items() if k != "truncate"}
    try:
        table_cols = list(spark.read.jdbc(url=url, table=table, properties=read_props).columns)
    except Exception:
        return df
    keep = [c for c in table_cols if c in df.columns]
    if not keep:
        return df
    return df.select(*keep)





def _local_path(prefix, file_name):
    prefix = (prefix or "").rstrip("/")
    file_name = (file_name or "").lstrip("/")
    if not prefix:
        return file_name or "."
    if not file_name:
        return prefix
    return f"{prefix}/{file_name}"


def _s3_bucket(*candidates):
    """Prefer a concrete bucket; skip StreamSets/catalog stubs like ``bucket-name``."""
    placeholders = {
        "",
        "bucket",
        "bucket-name",
        "your-bucket",
        "<bucket>",
        "<bucket-name>",
        "${bucket}",
    }
    for cand in candidates:
        text = str(cand or "").strip()
        if text.startswith("s3a://"):
            text = text[6:]
        elif text.startswith("s3://"):
            text = text[5:]
        elif text.startswith("s3a:/"):
            text = text[5:]
        if "/" in text:
            text = text.split("/")[0]
        text = text.strip()
        if text and text.lower() not in placeholders and not text.startswith("<"):
            return text
    return ""


def _s3_path(conn, file_name, prefix=""):
    conn = conn if isinstance(conn, dict) else {}
    sec = _fetch_secret(conn)
    # Envelope/conn first: live enrich inlines the real bucket; secrets often still
    # carry StreamSets placeholder metadata (bucket-name).
    bucket = _s3_bucket(
        conn.get("bucket"),
        conn.get("bucket_name"),
        sec.get("bucket"),
        sec.get("bucket_name"),
        os.environ.get("S3_BUCKET_NAME"),
    )
    base = (sec.get("file_path_prefix") or sec.get("prefix") or conn.get("file_path_prefix") or prefix or "").strip("/")
    if base.startswith("s3a://"):
        base = base[6:]
    elif base.startswith("s3://"):
        base = base[5:]
    elif base.startswith("s3a:/"):
        base = base[5:]
    if bucket and base.startswith(bucket):
        base = base[len(bucket):].lstrip("/")
    name = (file_name or "").lstrip("/")
    parts = [p for p in (base, name) if p]
    key = "/".join(parts)
    scheme = str(getattr(_cloud, "OBJECT_STORE_SCHEME", None) or "s3a").strip() or "s3a"
    if not bucket:
        label = conn.get("secret_name") or conn.get("name") or conn.get("connection_type") or ""
        raise ValueError(
            f"S3 bucket unresolved for connection {label!r}. "
            "Refusing a local fallback (Glue would write to ProxyLocalFileSystem). "
            "The S3 secret still has a StreamSets stub like bucket-name; this is not an IAM failure."
        )
    uri = f"{scheme}://{bucket}/{key}" if key else f"{scheme}://{bucket}"
    _log(f"[S3   ] path {uri}")
    return uri


def _configure_s3(spark, conn):
    secrets = _fetch_secret(conn)
    # Catalog live enrich inlines aws_* on conn; prefer those when SM/KV is unreachable.
    key = (
        _env(conn.get("aws_access_key_id_env"))
        or secrets.get("aws_access_key_id")
        or secrets.get("access_key")
        or conn.get("aws_access_key_id")
        or conn.get("access_key")
        or os.environ.get("AWS_ACCESS_KEY_ID")
    )
    secret = (
        _env(conn.get("aws_secret_access_key_env"))
        or secrets.get("aws_secret_access_key")
        or secrets.get("secret_key")
        or conn.get("aws_secret_access_key")
        or conn.get("secret_key")
        or os.environ.get("AWS_SECRET_ACCESS_KEY")
    )
    token = (
        os.environ.get("AWS_SESSION_TOKEN")
        or secrets.get("aws_session_token")
        or conn.get("aws_session_token")
    )
    region = _normalize_aws_region(
        secrets.get("region_name")
        or secrets.get("region")
        or secrets.get("aws_region")
        or conn.get("region_name")
        or conn.get("region")
        or os.environ.get("AWS_DEFAULT_REGION")
        or os.environ.get("S3_REGION")
        or "us-east-1"
    ) or "us-east-1"
    endpoint = conn.get("endpoint_url") or secrets.get("endpoint_url") or secrets.get("endpoint")
    _cloud.configure_object_store(
        spark,
        access_key=key,
        secret_key=secret,
        session_token=token,
        region=region,
        endpoint=endpoint,
    )






def _find_connections_file(filename="connections.yml"):
    """Locate connections.yml next to main.py or in the process cwd."""
    file_hint = globals().get("__file__")
    candidates = []
    if file_hint:
        candidates.append(Path(file_hint).resolve().parent / filename)
    candidates.append(Path.cwd() / filename)
    # Glue extra-files are often materialized under /tmp, not beside the script.
    candidates.append(Path("/tmp") / filename)
    for path in candidates:
        if path.is_file():
            return path
    return candidates[0]


def _load_connections(path=None):
    target = Path(path) if path else _find_connections_file("connections.yml")
    if not target.is_file():
        target = _find_connections_file("connections.yml")
    text = target.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore
        data = yaml.safe_load(text) or {}
    except Exception:
        # Minimal YAML subset fallback (key: / nested scalars) via json if converted.
        data = {}
        current = None
        for raw in text.splitlines():
            line = raw.split("#", 1)[0].rstrip()
            if not line.strip():
                continue
            if not line.startswith(" ") and line.endswith(":"):
                current = line[:-1].strip().strip('"').strip("'")
                data[current] = {}
                continue
            if current is None or ":" not in line:
                continue
            k, v = line.strip().split(":", 1)
            v = v.strip()
            if v in {"true", "false"}:
                parsed = v == "true"
            else:
                try:
                    parsed = json.loads(v)
                except Exception:
                    parsed = v.strip('"').strip("'")
            data[current][k.strip()] = parsed
    if not isinstance(data, dict):
        return {}
    return data


def _parse_simple_param_yml(text):
    """Minimal flat KEY: value YAML when PyYAML is unavailable."""
    out = {}
    for raw in text.splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#") or ":" not in stripped:
            continue
        key, val = stripped.split(":", 1)
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]
        if key:
            out[key] = val
    return out


def _load_parameters(path=None):
    """Load flat ``parameters.yml`` (StreamSets constants / BH defaults)."""
    target = Path(path) if path else _find_connections_file("parameters.yml")
    if not target.is_file():
        target = _find_connections_file("parameters.yml")
    if not target.is_file():
        return {}
    text = target.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        data = yaml.safe_load(text) or {}
    except Exception:
        data = _parse_simple_param_yml(text)
    if not isinstance(data, dict):
        return {}
    return {
        str(k): _sanitize_param_value(str(k), v)
        for k, v in data.items()
        if k is not None and v is not None
    }


def _params_from_env(env=None):
    """Parse ``BH_PIPELINE_PARAMS`` JSON object from the environment."""
    import os as _os

    raw = str((env or _os.environ).get("BH_PIPELINE_PARAMS") or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    return {
        str(k): _sanitize_param_value(str(k), v)
        for k, v in data.items()
        if k is not None and v is not None
    }


def _parse_param_cli(argv=None):
    """Parse ``--param KEY=VAL`` / ``--param=KEY=VAL`` from argv."""
    import sys as _sys

    out = {}
    args = list(argv if argv is not None else _sys.argv[1:])
    i = 0
    while i < len(args):
        arg = args[i]
        raw = None
        if arg == "--param" and i + 1 < len(args):
            raw = args[i + 1]
            i += 2
        elif isinstance(arg, str) and arg.startswith("--param="):
            raw = arg.split("=", 1)[1]
            i += 1
        else:
            i += 1
            continue
        if not raw or "=" not in raw:
            continue
        key, val = raw.split("=", 1)
        key = key.strip()
        if key:
            out[key] = _sanitize_param_value(key, val)
    return out


def _merge_param_maps(*maps):
    """Merge parameter maps; later maps win."""
    merged = {}
    for m in maps:
        if not m:
            continue
        for key, val in m.items():
            k = str(key or "").strip()
            if not k or val is None:
                continue
            merged[k] = _sanitize_param_value(k, val)
    return merged


def _resolve_submit_parameters(defaults=None, *, argv=None, env=None):
    """Merge parameters.yml defaults < BH_PIPELINE_PARAMS < CLI ``--param``."""
    return _merge_param_maps(
        defaults,
        _params_from_env(env),
        _parse_param_cli(argv),
    )


