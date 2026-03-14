# POC 3: Performance + Cost Profiling

## Goal
Measure performance impact of broadcast joins, partition tuning, and caching.

## Inputs
- Orders from UCI Online Retail (or `silver_orders` from POC1)
- Products derived from `StockCode` if no product dataset is provided

## Outputs
- `metrics_job` (Delta) with runtime metrics

## Run Steps
1. Open `notebooks/00_setup_pocs.py` and set dataset paths.
2. Run `notebooks/03_poc3_performance_profiling.py`.
3. Compare runtime metrics for tuning steps.

## Expected Results
- Broadcast join should reduce shuffle time for small dimension tables.
- Tuning `spark.sql.shuffle.partitions` should change runtime.
- Caching should reduce repeated aggregation time.
