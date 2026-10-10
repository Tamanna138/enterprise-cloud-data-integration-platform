# Architecture notes

## Data flow

The project uses a medallion-style layout so original inputs remain available and transformations have clear responsibilities.

- **Generator:** creates deterministic synthetic order events using a configurable row count and seed. It writes incrementally so source generation does not require one in-memory table containing every row.
- **Source:** CSV files at small and large scales.
- **Bronze:** immutable source copies retained to support replay and audit.
- **Silver:** standardized Parquet records that pass the documented contract. Invalid rows go to quarantine with a reason.
- **Gold:** analytics outputs such as daily sales by store and category.
- **Warehouse/query layer:** a local serving option for the aggregates, added after the core files are correct.

## Why keep these layers?

- Bronze lets us replay processing if transformation logic changes.
- Silver separates data cleaning and validation from business calculations.
- Gold makes common analytics easier and gives consumers a stable, documented shape.
- Quarantine makes data problems visible instead of silently dropping records.
- Parquet and partitioning demonstrate columnar storage and partition pruning. We will choose partition keys based on the access patterns and measured file sizes.

## Scaling strategy

The 20-row CSV is for first-time learning and interactive profiling only. The scale demonstration will use a seeded generator with volume profiles up to millions of rows, chunked file writes, and PySpark in local mode. The Spark pipeline will use DataFrame transformations and avoid collecting the full dataset into Python memory.

A benchmark report will include row count, generated input bytes, valid and quarantined counts, elapsed time, rows per second, software versions, and an anonymized machine resource profile. Numbers will be added only after measured runs.

## Local learning version and cloud mapping

The project runs locally first. Planned mappings are object storage to Amazon S3, Spark transformations to AWS Glue, orchestration to Airflow, and the serving layer to Redshift. Docker-hosted substitutes are for local practice; they are not AWS deployments. Cloud deployment will be a separate optional phase so the core project can be completed without cloud charges.

## Resource management

Large-data profiles will be configurable. Start small, monitor memory and disk use, and increase volume gradually. Airflow and storage services will be optional and started separately from the core Spark run to avoid overloading a limited-memory laptop.
