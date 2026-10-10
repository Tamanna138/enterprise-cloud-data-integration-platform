"""Print the schema and a small sample from one run's gold Parquet output."""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, required=True, help="Gold output directory for one pipeline run.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.path.exists():
        raise FileNotFoundError(f"Gold output directory does not exist: {args.path}")

    spark = SparkSession.builder.appName("inspect-gold-output").getOrCreate()
    try:
        gold = spark.read.parquet(str(args.path))
        silver_path = args.path.parent.parent / "silver" / args.path.name
        silver = spark.read.parquet(str(silver_path))

        gold_totals = gold.agg(
            F.sum("order_line_count").alias("order_line_count"),
            F.sum("unit_count").alias("unit_count"),
            F.sum("gross_sales").alias("gross_sales"),
        ).first().asDict()
        silver_totals = silver.agg(
            F.count(F.lit(1)).alias("order_line_count"),
            F.sum("quantity").alias("unit_count"),
            F.sum("order_total").alias("gross_sales"),
        ).first().asDict()

        print("Silver-to-gold reconciliation:")
        print(f"Silver order lines: {silver_totals['order_line_count']:,}")
        print(f"Gold order lines:   {gold_totals['order_line_count']:,}")
        print(f"Silver units:       {silver_totals['unit_count']:,}")
        print(f"Gold units:         {gold_totals['unit_count']:,}")
        print(f"Silver gross sales: {silver_totals['gross_sales']}")
        print(f"Gold gross sales:   {gold_totals['gross_sales']}")

        if gold_totals != silver_totals:
            raise RuntimeError("Gold totals do not reconcile with silver totals.")
        print("Reconciliation passed: gold preserves all silver order lines, units, and sales.")

        print("Gold output schema:")
        gold.printSchema()
        print("First 10 daily aggregates:")
        gold.orderBy("order_date", "store_id", "category").show(10, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
