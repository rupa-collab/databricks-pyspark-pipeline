# POC 1: Batch Ingestion + Data Quality + Delta Lake

## Goal
Build a robust raw -> bronze -> silver pipeline with data quality gates and metrics.

## Inputs
UCI Online Retail CSV export with columns:
- `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`

## Outputs
- `bronze_orders` (Delta)
- `silver_orders` (Delta)
- `gold_sales_by_product` (Delta)
- `quarantine_orders` (Delta)
- `metrics_job` and `metrics_quality` (Delta)

## Run Steps
1. Open `notebooks/00_setup_pocs.py` and set dataset paths.
2. Run `notebooks/01_poc1_batch_quality.py`.
3. Validate tables and metrics.

## Expected Results
- Invalid or negative records are quarantined.
- `silver_orders` contains cleaned data.
- Metrics tables show row counts and durations.
