# Data contract

Each row is a synthetic order line. The scale generator follows these rules for valid records:

| Field | Meaning | Validity rule |
|---|---|---|
| `order_id` | Identifier for the order line | Required and unique |
| `order_timestamp` | UTC event time | Required and parseable as an ISO 8601 timestamp |
| `customer_id` | Customer identifier | Required |
| `store_id` | Store identifier | Required |
| `category` | Product category | Required |
| `quantity` | Units in the order line | Integer greater than zero |
| `unit_price` | Price per unit in synthetic currency units | Numeric and zero or greater |

The generator deterministically injects four types of issues at regular intervals: exact duplicate delivery, missing customer identifier, zero quantity, and negative unit price. This makes data quality behavior reproducible. The small hand-written learning sample has a higher issue density so the first notebook can reveal problems quickly.

The currency unit is intentionally not assigned to a real country or currency. A production data contract would need a currency field and explicit precision/rounding rules before calculating cross-market revenue.

## Example generator runs

Start with a small generated batch to learn the file shape before processing large volumes:

```text
python scripts/generate_orders.py --rows 10000 --rows-per-file 2000 --output-dir data/source/generated_10000
```

The planned medium benchmark is one million rows:

```text
python scripts/generate_orders.py --rows 1000000 --rows-per-file 100000 --output-dir data/source/generated_1000000
```

Each run produces multiple CSV files and a `manifest.json` with the seed, counts, and injected issue types. Generated data is local-only and should not be committed to GitHub.
