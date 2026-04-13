"""
Iceberg catalog administration CLI.

Manages table lifecycle — creation, schema evolution, compaction.
Run as a K8s Job or locally for catalog operations.
"""

import argparse
import logging
import os
import sys

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("catalog-admin")


# ── Table definitions ──────────────────────────────────────────────

TABLES = {
    "analytics.watch_history": {
        "schema": Schema(
            NestedField(1, "event_type", StringType(), required=True),
            NestedField(2, "video_id", StringType(), required=True),
            NestedField(3, "user_id", StringType(), required=True),
            NestedField(4, "session_id", StringType(), required=True),
            NestedField(5, "bytes_read", LongType(), required=True),
            NestedField(6, "event_ts", TimestamptzType(), required=True),
            NestedField(7, "ingested_at", TimestamptzType(), required=True),
        ),
        "partition_spec": PartitionSpec(
            PartitionField(
                source_id=6,  # event_ts
                field_id=1000,
                transform=DayTransform(),
                name="event_day",
            ),
        ),
        "properties": {
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "zstd",
            "write.target-file-size-bytes": str(128 * 1024 * 1024),  # 128MB
            "history.expire.max-snapshot-age-ms": str(5 * 24 * 3600 * 1000),  # 5 days
        },
    },
}


def get_catalog() -> object:
    """Load Iceberg catalog from environment variables.

    Local dev: uses LocalStack Glue (GLUE_ENDPOINT=http://localhost:4566)
    AWS:       uses real AWS Glue (no GLUE_ENDPOINT, uses IAM credentials)
    """
    catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "glue")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://iceberg-warehouse/")
    glue_endpoint = os.getenv("GLUE_ENDPOINT", "")
    s3_endpoint = os.getenv("S3_ENDPOINT", "")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    catalog_props = {
        "type": "glue",
        "warehouse": warehouse,
        "glue.region": aws_region,
    }

    # LocalStack overrides for local development
    if glue_endpoint:
        catalog_props["glue.endpoint"] = glue_endpoint
        catalog_props["glue.access-key-id"] = os.getenv("AWS_ACCESS_KEY_ID", "test")
        catalog_props["glue.secret-access-key"] = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

    if s3_endpoint:
        catalog_props["s3.endpoint"] = s3_endpoint
        catalog_props["s3.access-key-id"] = os.getenv("AWS_ACCESS_KEY_ID", "test")
        catalog_props["s3.secret-access-key"] = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
        catalog_props["s3.path-style-access"] = "true"

    catalog = load_catalog(catalog_name, **catalog_props)
    return catalog


def cmd_create_tables(args):
    """Create all defined Iceberg tables if they don't exist."""
    catalog = get_catalog()

    for table_name, defn in TABLES.items():
        namespace = table_name.split(".")[0]
        try:
            catalog.create_namespace(namespace)
            logger.info(f"Created namespace: {namespace}")
        except Exception:
            logger.debug(f"Namespace {namespace} already exists")

        try:
            catalog.load_table(table_name)
            logger.info(f"Table {table_name} already exists — skipping")
        except Exception:
            catalog.create_table(
                table_name,
                schema=defn["schema"],
                partition_spec=defn["partition_spec"],
                properties=defn["properties"],
            )
            logger.info(f"Created table: {table_name}")


def cmd_compact(args):
    """Rewrite small files into larger ones for query performance."""
    catalog = get_catalog()
    table_name = args.table

    table = catalog.load_table(table_name)
    result = table.compact()
    logger.info(f"Compaction result for {table_name}: {result}")


def cmd_expire_snapshots(args):
    """Remove old snapshots to reclaim storage."""
    catalog = get_catalog()
    table_name = args.table

    table = catalog.load_table(table_name)
    table.manage_snapshots().expire_snapshots()
    logger.info(f"Expired old snapshots for {table_name}")


def cmd_list_tables(args):
    """List all tables in the catalog."""
    catalog = get_catalog()
    for ns in catalog.list_namespaces():
        ns_name = ns[0] if isinstance(ns, tuple) else ns
        logger.info(f"Namespace: {ns_name}")
        for tbl in catalog.list_tables(ns_name):
            table = catalog.load_table(f"{ns_name}.{tbl[1]}" if isinstance(tbl, tuple) else tbl)
            logger.info(f"  {tbl} — {len(table.schema().fields)} columns, snapshots: {len(table.metadata.snapshots)}")


def cmd_describe(args):
    """Show schema and metadata for a table."""
    catalog = get_catalog()
    table = catalog.load_table(args.table)
    logger.info(f"Table: {args.table}")
    logger.info(f"Location: {table.metadata.location}")
    logger.info(f"Schema:\n{table.schema()}")
    logger.info(f"Partition spec: {table.spec()}")
    logger.info(f"Properties: {table.metadata.properties}")
    logger.info(f"Snapshots: {len(table.metadata.snapshots)}")


def main():
    parser = argparse.ArgumentParser(description="Iceberg catalog administration")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("create-tables", help="Create all defined tables")
    subparsers.add_parser("list", help="List all tables")

    describe_parser = subparsers.add_parser("describe", help="Describe a table")
    describe_parser.add_argument("table", help="Table name (e.g. analytics.watch_history)")

    compact_parser = subparsers.add_parser("compact", help="Compact small files")
    compact_parser.add_argument("table", help="Table name")

    expire_parser = subparsers.add_parser("expire-snapshots", help="Expire old snapshots")
    expire_parser.add_argument("table", help="Table name")

    args = parser.parse_args()

    commands = {
        "create-tables": cmd_create_tables,
        "list": cmd_list_tables,
        "describe": cmd_describe,
        "compact": cmd_compact,
        "expire-snapshots": cmd_expire_snapshots,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
