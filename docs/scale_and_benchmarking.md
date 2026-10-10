# Scale and benchmark plan

## Purpose

Show that the design can process large datasets while keeping the demonstration repeatable and honest. Synthetic scale is a way to exercise the pipeline; it is not a claim that this laptop has a production cluster or that the sample represents real business data.

## Profiles

| Profile | Rows | Purpose |
|---|---:|---|
| learning | 20 | Understand the schema and data quality issues in Jupyter |
| small | 10,000 | Validate the end-to-end path quickly; completed |
| medium | 1,000,000 | Demonstrate million-row processing; completed |
| large | 5,000,000 | Optional stress profile, subject to available memory and disk |

Profiles will be adjustable. A deterministic seed will make generated data reproducible. The generator will stream rows in chunks so it does not build the full source dataset in memory.

## Pipeline behavior

1. Generate CSV source data with a controlled proportion of quality issues.
2. Preserve the input in bronze.
3. Read and transform with PySpark DataFrames.
4. Route invalid rows to quarantine with explicit reason codes.
5. Write valid silver data as Parquet and produce grouped gold aggregates.
6. Compare input count to valid plus rejected count and report any mismatch.

## Metrics to report

For each actual run, record:

- configured and observed input row count;
- generated CSV size and output Parquet size;
- valid and rejected row counts by reason;
- wall-clock run time and throughput;
- Python, Spark, and Java versions;
- execution mode, CPU count, and memory limits.

Do not compare timings across different machines without describing the environment. Do not put a result into the README until that run has completed and the figures have been checked.

## Measured local runs

These results are from synthetic data on one HP EliteBook 840 G3 using Docker Desktop with WSL2. The Spark container was limited to 2 CPUs and 2.5 GB memory and ran in local mode with two worker threads and four shuffle partitions. Timings include Spark startup, reading, transformations, and output writes; they are end-to-end local measurements, not cluster benchmarks.

| Profile | Input rows | Source CSV | Valid | Quarantined | Silver + quarantine + gold | Elapsed | Throughput |
|---|---:|---:|---:|---:|---:|---:|---:|
| small | 10,000 | 720,064 bytes | 9,961 | 39 | 728,816 bytes | 109.923 s | 90.97 rows/s |
| medium | 1,000,000 | 71,966,566 bytes | 996,001 | 3,999 | 29,369,556 bytes | 221.174 s | 4,521.32 rows/s |

For both runs, `input_rows = valid_rows + quarantined_rows`, matching the generator manifest. The input is synthetic and uses a fixed seed. The million-row result demonstrates this local pipeline's behavior at that volume; it does not establish production-scale or cloud performance. Machine-level memory and CPU utilization were not captured, so those are still open measurement items.

The million-row gold output was also reconciled against silver: both contain 996,001 order lines and 4,480,218 units, and both sum to 1,122,803,816.77 in synthetic gross-sales units. The inspection command confirmed all three totals match. These amounts belong to generated sample data and are not real revenue.

Metrics are preserved as JSON under `data/metrics/`. Generated source and transformed datasets are excluded from version control; commit the small metrics reports and this documentation, not the large data files.

## Resource-aware execution

The project will keep a small profile for the learning workflow and an optional large profile for scale demonstrations. Run one heavy service at a time at first. Docker-based object storage and Airflow will be optional additions; the base Spark batch path should be understandable before those services are introduced.
