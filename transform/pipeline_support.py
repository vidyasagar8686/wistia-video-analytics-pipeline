"""Pure contracts and versioned publication helpers shared by Glue jobs and tests."""
import hashlib
import ipaddress
import json
from datetime import datetime
from pathlib import Path


def mask_ip(value):
    """Return a coarse network, or null; never pass malformed identifiers through."""
    if not value:
        return None


    try:
        address = ipaddress.ip_address(value)
        bits = 24 if address.version == 4 else 48
        return str(ipaddress.ip_network(f"{address}/{bits}", strict=False))
    except ValueError:
        return None


def suppression_keys(payload):
    keys = payload.get("visitor_keys") if isinstance(payload, dict) else None
    if not isinstance(keys, list) or any(not isinstance(key, str) or not key for key in keys):
        raise ValueError("suppression registry must contain nonempty string visitor_keys")
    return set(keys)


def catalog_name(run_id):
    return "wistia_gold_v_" + hashlib.sha256(run_id.encode()).hexdigest()[:24]


def read_pointer(s3, bucket, key, started_at=None):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        if started_at:
            current = json.loads(response["Body"].read())
            if current.get("started_at") and datetime.fromisoformat(current["started_at"].replace("Z", "+00:00")) >= datetime.fromisoformat(started_at.replace("Z", "+00:00")):
                raise RuntimeError("A newer or equal pipeline execution has already published")
        return response["ETag"]
    except Exception as error:
        if getattr(error, "response", {}).get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise


def publish_catalog(glue, s3, bucket, base_prefix, run_id, tables, expected_etag, started_at=None):
    """Publish an entire immutable database, then switch one conditional pointer.

    Consumers select the database named by the pointer for the duration of a
    report. No existing table location is changed. Failed catalog writes leave
    the previous pointer intact. Incomplete candidate databases are not releases.
    """
    database = catalog_name(run_id)
    glue.create_database(DatabaseInput={"Name": database,
                                       "Description": "Validated Wistia release " + run_id})
    for name, definition in tables.items():
        glue.create_table(DatabaseName=database, TableInput={
            "Name": name, "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
            "StorageDescriptor": {
                "Columns": definition["columns"], "Location": definition["location"],
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
            },
        })
    manifest = {"run_id": run_id, "database": database, "tables": tables, "started_at": started_at,
                "metric_definition": "wistia_project_v1", "status": "validated"}
    condition = {"IfMatch": expected_etag} if expected_etag else {"IfNoneMatch": "*"}
    s3.put_object(Bucket=bucket, Key=base_prefix + "releases/current.json",
                  Body=json.dumps(manifest).encode(), ContentType="application/json", **condition)
    return manifest


def publish_local(base_path, run_id, tables):
    manifest = {"run_id": run_id, "tables": tables, "status": "validated",
                "metric_definition": "wistia_project_v1"}
    path = Path(base_path) / "releases"
    path.mkdir(parents=True, exist_ok=True)
    temporary = path / (run_id + ".json.tmp")
    temporary.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    temporary.replace(path / "current.json")
    return manifest
