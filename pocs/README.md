# PySpark Production POCs (Databricks)

This folder contains production-style POCs aligned to reliability, incremental processing, and performance tuning.

## POCs
- `POC1_BATCH_QUALITY.md` — Batch ingestion with quality gates, quarantine, and Delta optimization.
- `POC2_INCREMENTAL_CDC.md` — Incremental loads + CDC merge + SCD2 dimension.
- `POC3_PERF_PROFILING.md` — Join/aggregation performance profiling and tuning.

## Notebooks
- `notebooks/00a_ingest_datasets.py`
- `notebooks/00_setup_pocs.py`
- `notebooks/01_poc1_batch_quality.py`
- `notebooks/02_poc2_incremental_cdc.py`
- `notebooks/03_poc3_performance_profiling.py`

## Dataset Expectations
This repo is wired for two public datasets:
- Retail: UCI Online Retail (CSV export from the original dataset)
- Banking: Cifer Fraud Detection (PaySim-structured transactions)

Required columns used in the POCs:
- Retail: `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`
- Banking: `step`, `type`, `amount`, `nameOrig`, `oldbalanceOrg`, `newbalanceOrig`, `nameDest`, `oldbalanceDest`, `newbalanceDest`, `isFraud`, `isFlaggedFraud`

Datasets are staged under `/Volumes/workspace/default/datasets/` by `notebooks/00a_ingest_datasets.py`.
