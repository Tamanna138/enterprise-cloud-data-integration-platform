"""Run a local PySpark batch pipeline over generated order CSV files.

The runner is intended to execute inside the official Spark Docker image, which
provides both Java and PySpark without changing the user's Anaconda environment.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_COLUMNS = [
    "order_id",
    "order_timestamp",
    "customer_id",
    "store_id",
    "category",
    "quantity",
    "unit_price",
]
RAW_SCHEMA = StructType([StructField(name, StringType(), True) for name in SOURCE_COLUMNS])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate and transform synthetic orders with PySpark.")
    parser.add_argument("--input-dir", type=Path, required=True, help="Directory containing orders_*.csv and optional manifest.json.")
    parser.add_argument("--data-root", type=Path, default=Path("data"), help="Root directory for bronze, silver, quarantine, gold, and metrics outputs.")
    parser.add_argument("--run-id", help="Unique run label. Defaults to the current UTC timestamp.")
    parser.add_argument("--shuffle-partitions", type=int, default=4, help="Spark shuffle partitions for this local run (default: 4).")
    return parser.parse_args()


def resolve_path(path: Path) -> Path:
    return path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()


def create_run_id(requested: str | None) -> str:
    run_id = requested or datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%S_%fZ")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", run_id):
        raise ValueError("--run-id may contain only letters, numbers, dots, underscores, and hyphens.")
    return run_id


def add_rejection_reasons(df: DataFrame) -> DataFrame:
    """Parse fields and attach explicit rejection reasons without dropping rows."""
    parsed = (
        df.withColumn("_order_id", F.trim(F.col("order_id")).cast(LongType()))
        .withColumn("_event_timestamp", F.to_timestamp(F.trim(F.col("order_timestamp"))))
        .withColumn("_customer_id", F.when(F.length(F.trim(F.coalesce(F.col("customer_id"), F.lit("")))) == 0, F.lit(None)).otherwise(F.trim(F.col("customer_id"))))
        .withColumn("_store_id", F.lower(F.trim(F.col("store_id"))))
        .withColumn("_category", F.lower(F.trim(F.col("category"))))
        .withColumn("_quantity", F.trim(F.col("quantity")).cast("int"))
        .withColumn("_unit_price", F.trim(F.col("unit_price")).cast(DecimalType(12, 2)))
        .withColumn("_payload", F.to_json(F.struct(*[F.col(name) for name in SOURCE_COLUMNS])))
    )

    # Identical re-deliveries keep one copy. If the same ID arrives with different
    # payloads, every conflicting record is rejected instead of choosing a winner.
    id_stats = (
        parsed.where(F.col("_order_id").isNotNull())
        .groupBy("_order_id")
        .agg(
            F.count(F.lit(1)).alias("_id_count"),
            F.countDistinct(F.col("_payload")).alias("_payload_count"),
        )
    )
    ranked = (
        parsed.join(id_stats, on="_order_id", how="left")
        .withColumn(
            "_duplicate_rank",
            F.row_number().over(
                Window.partitionBy("_order_id").orderBy(F.col("_source_file"), F.col("_row_id"))
            ),
        )
    )

    checks = [
        (F.col("_order_id").isNull(), "invalid_order_id"),
        (F.col("_event_timestamp").isNull(), "invalid_order_timestamp"),
        (F.col("_customer_id").isNull(), "missing_customer_id"),
        (F.length(F.trim(F.coalesce(F.col("_store_id"), F.lit("")))) == 0, "missing_store_id"),
        (F.length(F.trim(F.coalesce(F.col("_category"), F.lit("")))) == 0, "missing_category"),
        (F.col("_quantity").isNull() | (F.col("_quantity") <= 0), "non_positive_or_invalid_quantity"),
        (F.col("_unit_price").isNull() | (F.col("_unit_price") < 0), "negative_or_invalid_unit_price"),
        ((F.col("_id_count") > 1) & (F.col("_payload_count") > 1), "conflicting_duplicate_order_id"),
        ((F.col("_id_count") > 1) & (F.col("_payload_count") == 1) & (F.col("_duplicate_rank") > 1), "duplicate_delivery"),
    ]
    return ranked.withColumn(
        "reject_reason",
        F.concat_ws("|", *[F.when(condition, F.lit(reason)) for condition, reason in checks]),
    )


def main() -> None:
    args = parse_args()
    if args.shuffle_partitions < 1:
        raise ValueError("--shuffle-partitions must be at least 1")

    input_dir = resolve_path(args.input_dir)
    data_root = resolve_path(args.data_root)
    source_files = sorted(input_dir.glob("orders_*.csv"))
    if not source_files:
        raise FileNotFoundError(f"No orders_*.csv files found in {input_dir}")

    run_id = create_run_id(args.run_id)
    destinations = {
        name: data_root / name / run_id
        for name in ("bronze", "silver", "quarantine", "gold")
    }
    metrics_path = data_root / "metrics" / f"{run_id}.json"
    occupied = [path for path in [*destinations.values(), metrics_path] if path.exists()]
    if occupied:
        raise FileExistsError(f"Run outputs already exist: {occupied}. Choose a new --run-id.")

    # Bronze is a file-level copy: preserve source CSV bytes for replay and audit.
    destinations["bronze"].mkdir(parents=True)
    for source_file in source_files:
        shutil.copy2(source_file, destinations["bronze"] / source_file.name)
    source_manifest = input_dir / "manifest.json"
    if source_manifest.exists():
        shutil.copy2(source_manifest, destinations["bronze"] / source_manifest.name)

    input_bytes = sum(path.stat().st_size for path in source_files)
    started = time.perf_counter()
    spark = (
        SparkSession.builder.appName("enterprise-cloud-data-integration-platform")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze_files = sorted(destinations["bronze"].glob("orders_*.csv"))
        raw = (
            spark.read.schema(RAW_SCHEMA)
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .csv([str(path) for path in bronze_files])
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_row_id", F.monotonically_increasing_id())
        )
        classified = add_rejection_reasons(raw).persist(StorageLevel.MEMORY_AND_DISK)
        reason_array = F.split(F.col("reject_reason"), r"\|")
        reason_names = [reason for _, reason in [
            (None, "invalid_order_id"),
            (None, "invalid_order_timestamp"),
            (None, "missing_customer_id"),
            (None, "missing_store_id"),
            (None, "missing_category"),
            (None, "non_positive_or_invalid_quantity"),
            (None, "negative_or_invalid_unit_price"),
            (None, "conflicting_duplicate_order_id"),
            (None, "duplicate_delivery"),
        ]]

        row_counts = classified.agg(
            F.count(F.lit(1)).alias("input_rows"),
            F.sum(F.when(F.col("reject_reason") == "", 1).otherwise(0)).alias("valid_rows"),
            F.sum(F.when(F.col("reject_reason") != "", 1).otherwise(0)).alias("quarantined_rows"),
        ).first().asDict()
        row_counts = {key: int(value or 0) for key, value in row_counts.items()}
        if row_counts["input_rows"] != row_counts["valid_rows"] + row_counts["quarantined_rows"]:
            raise RuntimeError("Reconciliation failed: input rows do not equal valid plus quarantined rows.")

        reason_exprs = [
            F.sum(F.when(F.array_contains(reason_array, reason), 1).otherwise(0)).alias(reason)
            for reason in reason_names
        ]
        reason_result = classified.agg(*reason_exprs).first().asDict()
        reason_counts = {name: int(count or 0) for name, count in reason_result.items() if count}

        quarantine = classified.where(F.col("reject_reason") != "").select(
            *SOURCE_COLUMNS, "_source_file", "reject_reason"
        )
        quarantine.coalesce(1).write.mode("errorifexists").parquet(str(destinations["quarantine"]))

        valid = classified.where(F.col("reject_reason") == "")
        silver = valid.select(
            F.col("_order_id").alias("order_id"),
            F.col("_event_timestamp").alias("order_timestamp"),
            F.col("_customer_id").alias("customer_id"),
            F.col("_store_id").alias("store_id"),
            F.col("_category").alias("category"),
            F.col("_quantity").alias("quantity"),
            F.col("_unit_price").alias("unit_price"),
            F.to_date(F.col("_event_timestamp")).alias("order_date"),
            (F.col("_unit_price") * F.col("_quantity")).cast(DecimalType(14, 2)).alias("order_total"),
        )
        silver.repartition(args.shuffle_partitions, "order_date").write.mode("errorifexists").partitionBy("order_date").parquet(str(destinations["silver"]))

        gold = silver.groupBy("order_date", "store_id", "category").agg(
            F.count(F.lit(1)).alias("order_line_count"),
            F.sum("quantity").alias("unit_count"),
            F.sum("order_total").cast(DecimalType(18, 2)).alias("gross_sales"),
        )
        gold.repartition(args.shuffle_partitions, "order_date").write.mode("errorifexists").partitionBy("order_date").parquet(str(destinations["gold"]))
        classified.unpersist()
    finally:
        spark.stop()

    elapsed_seconds = time.perf_counter() - started
    output_bytes = sum(path.stat().st_size for name in ("silver", "quarantine", "gold") for path in destinations[name].rglob("*") if path.is_file())
    source_rows_expected = None
    if source_manifest.exists():
        source_rows_expected = json.loads(source_manifest.read_text(encoding="utf-8")).get("rows_written")
    if source_rows_expected is not None and row_counts["input_rows"] != source_rows_expected:
        raise RuntimeError(f"Input count mismatch: manifest says {source_rows_expected}, Spark read {row_counts['input_rows']}.")

    metrics = {
        "run_id": run_id,
        "synthetic_data": True,
        "input_directory": str(input_dir),
        "source_files": len(source_files),
        "input_bytes": input_bytes,
        "output_bytes_silver_quarantine_gold": output_bytes,
        **row_counts,
        "rejection_rows_by_reason": reason_counts,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "rows_per_second": round(row_counts["input_rows"] / elapsed_seconds, 2) if elapsed_seconds else None,
        "spark_version": spark.version,
        "shuffle_partitions": args.shuffle_partitions,
        "execution_mode": "local Docker container",
        "bronze_path": str(destinations["bronze"]),
        "silver_path": str(destinations["silver"]),
        "quarantine_path": str(destinations["quarantine"]),
        "gold_path": str(destinations["gold"]),
        "measured_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")

    print(f"Run ID: {run_id}")
    print(f"Input rows: {row_counts['input_rows']:,}")
    print(f"Valid rows: {row_counts['valid_rows']:,}")
    print(f"Quarantined rows: {row_counts['quarantined_rows']:,}")
    print(f"Issue counts: {json.dumps(reason_counts, sort_keys=True)}")
    print(f"Elapsed seconds: {metrics['elapsed_seconds']}")
    print(f"Rows per second: {metrics['rows_per_second']}")
    print(f"Metrics: {metrics_path}")


if __name__ == "__main__":
    main()
