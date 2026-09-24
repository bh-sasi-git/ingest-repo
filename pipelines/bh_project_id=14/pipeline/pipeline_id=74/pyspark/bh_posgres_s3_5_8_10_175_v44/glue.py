"""AWS Glue cloud adapter for spark-plain generated projects.

Shipped beside ``framework.py`` as ``glue.py``. Same public surface as ``emr.py``
so ``framework.py`` can ``import glue as _cloud``. Glue Spark already runs
DataFrames — this module is runtime detect, secrets, and s3a wiring only.
Job bootstrap (GlueContext / Job.init) lives in generated ``main()``.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def is_managed_runtime() -> bool:
    """True on AWS Glue (or when BH_SPARK_RUNTIME marks a managed cluster)."""
    forced = str(os.environ.get("BH_SPARK_RUNTIME") or "").strip().lower()
    if forced in ("glue", "glueetl", "gluestreaming", "managed", "cluster"):
        return True
    if forced in ("local", "dev", "laptop"):
        return False
    if os.environ.get("GLUE_VERSION") or os.environ.get("AWS_GLUE_WORKFLOW_NAME"):
        return True
    return False


def configure_spark_env() -> None:
    """Pin the driver to loopback when the host name resolves to 127.x.

    Skipped on EMR/YARN and when a non-local master is configured — forcing
    ``127.0.0.1`` there breaks executor registration.
    """
    if is_managed_runtime():
        return
    master = os.environ.get("SPARK_MASTER") or os.environ.get("MASTER") or ""
    if master and not master.startswith("local"):
        return
    if os.environ.get("SPARK_LOCAL_IP"):
        return
    try:
        import socket

        if socket.gethostbyname(socket.gethostname()).startswith("127."):
            os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    except Exception:
        # Do not force loopback on unexpected DNS failures (unsafe on clusters).
        pass


def _secret_payload(raw):
    """Parse a Secrets Manager / Key Vault string into a plain dict.

    Nested ``config`` flattening and connector alias normalization happen in
    the portable framework after fetch (``_normalize_secret_payload``).
    """
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {"password": text, "aws_secret_access_key": text}


# Pip packages required by this cloud adapter (installed via framework bootstrap
# when requirements.txt is absent). Glue already ships boto3; PyYAML does not.
REQUIRED_PACKAGES = {
    "boto3": "boto3>=1.34",
}

# Glue job argument --additional-python-modules (comma-separated pip specs).
# Do not include pyspark or boto3 — Glue provides those and reinstalling conflicts.
ADDITIONAL_PYTHON_MODULES = ("pyyaml>=6.0",)

# Glue Spark uses EMRFS. Writers must use s3://. A missing bucket used to
# return a relative path (ProxyLocalFileSystem) so the job "succeeded" with
# no objects in S3.
OBJECT_STORE_SCHEME = "s3"


def missing_managed_packages_message(missing, packages, python):
    """Fail-fast hint when Glue is missing driver packages (no runtime pip)."""
    specs = ",".join(packages[mod] for mod in missing if mod in packages)
    return (
        "Missing packages on AWS Glue: {}. Glue Spark images do not include "
        "these (EMR AMIs often do). Set job argument --additional-python-modules "
        "to '{}' (catalog Glue submit does this). Do not pip install at runtime "
        "on {}: Glue Python is a managed runtime.".format(
            ", ".join(missing),
            specs or "pyyaml>=6.0",
            python,
        )
    )


def _azure_secret_client():
    vault_url = (
        os.environ.get("AZURE_VAULT_URL")
        or os.environ.get("AZURE_KEYVAULT_URL")
        or os.environ.get("KEY_VAULT_URL")
    )
    if not vault_url:
        return None
    from azure.keyvault.secrets import SecretClient

    tenant = os.environ.get("AZURE_TENANT_ID")
    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    if tenant and client_id and client_secret:
        from azure.identity import ClientSecretCredential

        credential = ClientSecretCredential(tenant, client_id, client_secret)
    else:
        from azure.identity import DefaultAzureCredential

        credential = DefaultAzureCredential()
    return SecretClient(vault_url=vault_url, credential=credential)


def fetch_secret(candidates, conn=None):
    """Resolve secret payload via AWS Secrets Manager (then Azure KV fallback).

    Returns ``(resolved_dict, last_error_or_None)``. Does not apply connection
    inline fallbacks — that stays in the portable framework.
    """
    resolved = {}
    last_err = None
    conn = conn if isinstance(conn, dict) else {}
    candidates = [c for c in (candidates or []) if c]

    if str(os.environ.get("SECRET_MANAGER_PROVIDER") or "").lower() != "azure":
        try:
            import boto3

            regions = [
                conn.get("region_name") or conn.get("region"),
                os.environ.get("AWS_REGION"),
                os.environ.get("AWS_DEFAULT_REGION"),
                "us-east-1",
                "us-east-2",
                "us-west-2",
            ]
            seen = set()
            valid_regions = [r for r in regions if r and not (r in seen or seen.add(r))]
            for reg in valid_regions:
                if resolved:
                    break
                try:
                    client = boto3.client("secretsmanager", region_name=reg)
                except Exception as ex:
                    last_err = f"Region {reg} ClientError: {type(ex).__name__} - {ex}"
                    continue
                for candidate in candidates:
                    try:
                        resp = client.get_secret_value(SecretId=candidate)
                    except Exception as ex:
                        last_err = (
                            f"Region {reg} (candidate {candidate}): "
                            f"{type(ex).__name__} - {ex}"
                        )
                        continue
                    if "SecretString" in resp:
                        resolved = _secret_payload(resp["SecretString"])
                        if resolved:
                            break
        except Exception as ex:
            last_err = f"boto3 Error: {type(ex).__name__} - {ex}"

    if not resolved:
        try:
            client = _azure_secret_client()
            if client is not None:
                for candidate in candidates:
                    try:
                        sec = client.get_secret(candidate)
                    except Exception as ex:
                        last_err = f"Azure (candidate {candidate}): {type(ex).__name__} - {ex}"
                        continue
                    if sec and sec.value:
                        resolved = _secret_payload(sec.value)
                        if resolved:
                            break
        except Exception as ex:
            last_err = f"Azure Error: {type(ex).__name__} - {ex}"

    return resolved, last_err


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


def _s3a_endpoint_is_broken(endpoint):
    """True when ``fs.s3a.endpoint`` would make AWS SDK build ``https:`` / reject URI."""
    text = str(endpoint or "").strip()
    if not text:
        return True
    if "${" in text:
        return True
    # SCH enum leaked into hostname: s3.US_EAST_1.amazonaws.com
    if "_" in text:
        return True
    if text in ("https:", "http:", "https://", "http://"):
        return True
    return False


def configure_object_store(
    spark,
    *,
    access_key=None,
    secret_key=None,
    session_token=None,
    region=None,
    endpoint=None,
):
    """Wire Hadoop ``fs.s3a.*`` for Glue / local s3a access."""
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    key = access_key
    secret = secret_key
    token = session_token
    if key and secret:
        conf.set("fs.s3a.access.key", key)
        conf.set("fs.s3a.secret.key", secret)
        if token:
            conf.set(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
            )
            conf.set("fs.s3a.session.token", token)
        else:
            conf.set(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
    elif key:
        conf.set("fs.s3a.access.key", key)
    elif secret:
        conf.set("fs.s3a.secret.key", secret)
    else:
        # Prefer the instance-profile / default chain on Glue (no static keys).
        conf.set(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider,"
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
    region_norm = _normalize_aws_region(region) if region and "${" not in str(region) else ""
    if region_norm:
        conf.set("fs.s3a.endpoint.region", region_norm)
    endpoint_text = str(endpoint).strip() if endpoint not in (None, "") else ""
    if endpoint_text and not _s3a_endpoint_is_broken(endpoint_text):
        conf.set("fs.s3a.endpoint", endpoint_text)
    else:
        # Clear poisoned values left by Scala (``s3.${S3_REGION}.…`` or
        # ``s3.US_EAST_1.amazonaws.com``). Prefer region-only resolution.
        try:
            existing = conf.get("fs.s3a.endpoint")
            if existing and _s3a_endpoint_is_broken(existing):
                conf.unset("fs.s3a.endpoint")
        except Exception:
            pass


def filter_jar_packages(coords):
    """Drop Maven coords that Glue already provides (hadoop-aws / AWS SDK)."""
    out = []
    for coord in coords or []:
        parts = str(coord).split(":")
        if len(parts) != 3:
            out.append(coord)
            continue
        group, artifact, _version = parts
        if (
            artifact == "hadoop-aws"
            or (group == "software.amazon.awssdk" and artifact == "bundle")
            or (group == "com.amazonaws" and artifact == "aws-java-sdk-bundle")
        ):
            continue
        out.append(coord)
    return out


def ship_file(spark, src_path, *, name=None):
    """Copy a local file to Python cwd and Java user.dir, ``addFile``, return basename.

    Glue Kafka AdminClient resolves relative ``ssl.*.location`` against Java
    ``user.dir``, which is often not Python ``Path.cwd()``. Missing that copy
    loads an empty PKCS12 and fails with ``trustAnchors parameter must be
    non-empty``. Basename stays executor-safe once SparkFiles localizes it.
    """
    src = Path(str(src_path)).expanduser()
    if not src.is_file():
        return str(src_path)
    dest_name = name or src.name
    try:
        payload = src.read_bytes()
    except Exception:
        return str(src_path)
    dests = [Path.cwd() / dest_name]
    try:
        user_dir = spark.sparkContext._jvm.java.lang.System.getProperty("user.dir")
        if user_dir:
            dests.append(Path(str(user_dir)) / dest_name)
    except Exception:
        pass
    written = src
    seen: set[str] = set()
    for dest in dests:
        try:
            key = str(dest.resolve())
        except Exception:
            key = str(dest)
        if key in seen:
            continue
        seen.add(key)
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            if dest.is_file():
                try:
                    if dest.resolve() == src.resolve():
                        written = dest
                        continue
                except Exception:
                    pass
            dest.write_bytes(payload)
            written = dest
        except Exception:
            continue
    try:
        spark.sparkContext.addFile(str(Path(written).resolve()))
    except Exception as ex:
        sys.stderr.write(f"WARNING: addFile({dest_name}) failed: {ex}\n")
        sys.stderr.flush()
    return dest_name
