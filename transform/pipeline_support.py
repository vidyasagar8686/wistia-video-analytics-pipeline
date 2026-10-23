"""Pure contracts and versioned publication helpers shared by Glue jobs and tests."""
import hashlib
import ipaddress
import json
from datetime import datetime
from pathlib import Path


class DataContractError(ValueError):
    """A source batch cannot safely be used for analytical publication."""


def validate_business_keys(df, keys, source, string_keys=None):
    """Reject missing/blank/mistyped opaque identifiers before grouping rows.

    Do not trim, invent, hash or substitute a different ID for a missing source
    key. Messages identify the contract, never the offending personal values.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    string_keys = keys if string_keys is None else string_keys
    missing = set(keys) - set(df.columns)
    if missing:
        raise DataContractError(f"{source}: missing business-key columns {sorted(missing)}")
    invalid = F.lit(False)
    for key in keys:
        condition = F.col(key).isNull()
        if key in string_keys:
            if not isinstance(df.schema[key].dataType, StringType):
                raise DataContractError(f"{source}: business key {key} must be a string")
            condition = condition | (F.length(F.col(key)) == 0) | F.col(key).rlike(r"^\s|\s$")
        invalid = invalid | condition
    if df.filter(invalid).limit(1).count():
        raise DataContractError(f"{source}: missing, blank or whitespace-padded business key {', '.join(keys)}")
    return df


def deduplicate_records(df, keys, source, order_col=None, string_keys=None):
    """Deduplicate canonical published fields, never arbitrary raw-file metadata.

    Events have no source revision field: conflicting payloads for one event key
    fail even across load dates. Snapshots select the maximum explicit ordering
    timestamp; different payloads tied at that timestamp also fail. Exact copies
    collapse after comparing every canonical field, with no hash winner.
    """
    from pyspark.sql import Window, functions as F
    validate_business_keys(df, keys, source, string_keys)
    if order_col:
        from pyspark.sql.types import DateType, TimestampType
        if order_col not in df.columns:
            raise DataContractError(f"{source}: missing ordering column {order_col}")
        if not isinstance(df.schema[order_col].dataType, (DateType, TimestampType)):
            raise DataContractError(f"{source}: ordering column {order_col} must be a parsed date or timestamp")
        if df.filter(F.col(order_col).isNull()).limit(1).count():
            raise DataContractError(f"{source}: null or invalid ordering value {order_col}")
    candidates = df.dropDuplicates()
    if order_col:
        latest = F.max(F.col(order_col)).over(Window.partitionBy(*keys))
        candidates = (candidates.withColumn("_dedup_latest", latest)
                      .filter(F.col(order_col) == F.col("_dedup_latest"))
                      .drop("_dedup_latest"))
    if candidates.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
        policy = f" at latest {order_col}" if order_col else " without a source revision"
        raise DataContractError(f"{source}: conflicting copies for business key{policy}")
    return candidates


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
