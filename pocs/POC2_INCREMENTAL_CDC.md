# POC 2: Incremental Loads + CDC Merge + SCD2

## Goal
Simulate daily drops and apply CDC upserts to silver while maintaining SCD2 history in gold.

## Inputs
Cifer Fraud Detection (PaySim-structured) CSV with columns:
- `step`, `type`, `amount`, `nameOrig`, `oldbalanceOrg`, `newbalanceOrig`, `nameDest`, `oldbalanceDest`, `newbalanceDest`, `isFraud`, `isFlaggedFraud`

## Outputs
- `silver_bank_txn` (Delta)
- `gold_account_dim` (Delta, SCD2)
- `metrics_job` (Delta)

## Run Steps
1. Open `notebooks/00_setup_pocs.py` and set dataset paths.
2. Run `notebooks/02_poc2_incremental_cdc.py` for multiple batch dates.
3. Inspect `gold_account_dim` to confirm history tracking.

## Expected Results
- CDC merges are idempotent for the same batch.
- SCD2 table tracks changes with `effective_start`, `effective_end`, `is_current`.
