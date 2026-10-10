"""Generate deterministic synthetic order CSV files for pipeline learning and benchmarking.

The generator streams rows to bounded-size files rather than holding the full dataset
in memory. It uses only Python's standard library.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
COLUMNS = [
    "order_id",
    "order_timestamp",
    "customer_id",
    "store_id",
    "category",
    "quantity",
    "unit_price",
]
STORES = ["store_north", "store_south", "store_east", "store_west"]
CATEGORIES = ["books", "stationery", "home", "electronics", "toys"]
BASE_TIMESTAMP = datetime(2026, 9, 1, tzinfo=timezone.utc)


def build_valid_row(index: int, rng: random.Random) -> dict[str, str | int]:
    """Create one valid synthetic order line using the seeded random generator."""
    order_time = BASE_TIMESTAMP + timedelta(seconds=rng.randrange(90 * 24 * 60 * 60))
    cents = rng.randint(100, 50_000)
    return {
        "order_id": 1_000_000_000 + index,
        "order_timestamp": order_time.isoformat(timespec="seconds"),
        "customer_id": rng.randint(1, 100_000),
        "store_id": rng.choice(STORES),
        "category": rng.choice(CATEGORIES),
        "quantity": rng.randint(1, 8),
        "unit_price": f"{cents / 100:.2f}",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write reproducible synthetic order CSV files in manageable chunks."
    )
    parser.add_argument("--rows", type=int, default=1_000_000, help="Total output rows (default: 1,000,000).")
    parser.add_argument("--rows-per-file", type=int, default=100_000, help="Maximum data rows per CSV file (default: 100,000).")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for repeatable contents (default: 42).")
    parser.add_argument(
        "--issue-frequency",
        type=int,
        default=1_000,
        help="Inject up to four issue rows per this many records (default: 1,000).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/source/generated"),
        help="Output directory, relative to the project root unless absolute.",
    )
    return parser.parse_args()


def generate(args: argparse.Namespace) -> Path:
    if args.rows <= 0:
        raise ValueError("--rows must be greater than zero")
    if args.rows_per_file <= 0:
        raise ValueError("--rows-per-file must be greater than zero")
    if args.issue_frequency < 5:
        raise ValueError("--issue-frequency must be at least 5")

    output_dir = args.output_dir
    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    existing_csvs = list(output_dir.glob("orders_*.csv"))
    if existing_csvs:
        raise FileExistsError(
            f"Found existing generated CSV files in {output_dir}. "
            "Choose a new --output-dir so runs cannot accidentally mix."
        )

    rng = random.Random(args.seed)
    issue_counts: Counter[str] = Counter()
    previous_valid_row: dict[str, str | int] | None = None
    file_count = 0

    for file_start in range(0, args.rows, args.rows_per_file):
        file_count += 1
        file_path = output_dir / f"orders_{file_count:05d}.csv"
        file_end = min(file_start + args.rows_per_file, args.rows)

        with file_path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=COLUMNS)
            writer.writeheader()

            for index in range(file_start, file_end):
                slot = index % args.issue_frequency
                if index > 0 and slot == 0 and previous_valid_row is not None:
                    # Repeat the preceding record exactly to model a duplicate delivery.
                    row = previous_valid_row.copy()
                    issue_counts["duplicate_order"] += 1
                else:
                    row = build_valid_row(index, rng)
                    if slot == 1:
                        row["customer_id"] = ""
                        issue_counts["missing_customer_id"] += 1
                    elif slot == 2:
                        row["quantity"] = 0
                        issue_counts["non_positive_quantity"] += 1
                    elif slot == 3:
                        row["unit_price"] = "-5.00"
                        issue_counts["negative_unit_price"] += 1
                    else:
                        previous_valid_row = row.copy()
                writer.writerow(row)

    manifest = {
        "rows_requested": args.rows,
        "rows_written": args.rows,
        "rows_per_file": args.rows_per_file,
        "file_count": file_count,
        "seed": args.seed,
        "issue_frequency": args.issue_frequency,
        "injected_issue_rows_by_reason": dict(sorted(issue_counts.items())),
        "injected_issue_rows": sum(issue_counts.values()),
        "valid_by_generator_rules": args.rows - sum(issue_counts.values()),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "synthetic_data": True,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest_path


def main() -> None:
    args = parse_args()
    manifest_path = generate(args)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    print(f"Generated {manifest['rows_written']:,} rows across {manifest['file_count']} CSV files.")
    print(f"Output directory: {manifest_path.parent}")
    print(f"Injected issue rows: {manifest['injected_issue_rows']:,}")
    for reason, count in manifest["injected_issue_rows_by_reason"].items():
        print(f"  {reason}: {count:,}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
