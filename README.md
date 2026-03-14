# Databricks PySpark Medallion Pipeline + Production POCs

Production-style data pipelines using PySpark and Delta Lake, plus three production-ready POCs for batch quality, incremental CDC, and performance profiling.

## Architecture
Raw Data -> Bronze -> Silver -> Gold

## Tech Stack
- PySpark
- Databricks
- Delta Lake

## POCs
- Batch ingestion + data quality + Delta optimization
- Incremental loads + CDC merge + SCD2 dimension
- Performance profiling and tuning

POC notebooks and docs live in:
- `notebooks/00_setup_pocs.py`
- `notebooks/01_poc1_batch_quality.py`
- `notebooks/02_poc2_incremental_cdc.py`
- `notebooks/03_poc3_performance_profiling.py`
- `pocs/README.md`

## Datasets
- Retail: UCI Online Retail (CSV export)
- Banking: Cifer Fraud Detection (PaySim-structured)

## Run Pipeline (local)
python main_pipeline.py

## Run POCs (Databricks)
1. Open `notebooks/00_setup_pocs.py` and set dataset paths.
2. Run each POC notebook in order.
