# Pipeline Overview

This pipeline ingests transaction data, transforms it, and loads it into bronze, silver, and gold tables. It runs to ensure data is available for downstream analytics and reporting. If it stops, critical business metrics and reports will be unavailable.

## Pipeline Steps

1. Connect to the DuckDB database using `get_connection()`.
2. Set up required tables using `setup_tables(con)`.
3. Load merchant data into the `merchants` table using `load_merchants(con)`.
4. Load transactions into the `bronze_transactions` table using `load_bronze(con, transactions)`.
5. Transform bronze transactions to silver using `transform_bronze_to_silver(transactions, merchants)`.
6. Load transformed data into the `silver_transactions` table using `load_silver(con, silver_rows)`.
7. Compute merchant performance metrics using `compute_merchant_performance(silver_rows)`.
8. Compute daily summary metrics using `compute_daily_summary(silver_rows)`.
9. Load performance and summary data into gold tables using `load_gold(con, merchant_perf, daily_summary)`.

## Schedule / Trigger

The pipeline runs every night at 2 AM UTC via a cron job.

## Failure Modes

1. **Database Connection Failure**
   - **Root Cause**: DuckDB service is down.
   - **Symptom**: `get_connection()` fails.
2. **Table Creation Failure**
   - **Root Cause**: Syntax error in SQL.
   - **Symptom**: `setup_tables(con)` raises an exception.
3. **Merchant Data Load Failure**
   - **Root Cause**: Corrupt merchant data.
   - **Symptom**: `load_merchants(con)` raises an exception.
4. **Bronze Load Failure**
   - **Root Cause**: Malformed transaction data.
   - **Symptom**: `load_bronze(con, transactions)` raises an exception.
5. **Silver Transformation Failure**
   - **Root Cause**: Missing merchant IDs in transactions.
   - **Symptom**: `transform_bronze_to_silver(transactions, merchants)` raises an exception.

## Recovery Actions

1. **Database Connection Failure**
   - Check DuckDB service status.
   - Restart the service if necessary.
   - Retry the pipeline.
2. **Table Creation Failure**
   - Review SQL syntax in `setup_tables(con)`.
   - Correct the syntax and rerun the pipeline.
3. **Merchant Data Load Failure**
   - Inspect `MERCHANTS` data for corruption.
   - Clean the data and rerun `load_merchants(con)`.
4. **Bronze Load Failure**
   - Validate transaction data format.
   - Correct the data and rerun `load_bronze(con, transactions)`.
5. **Silver Transformation Failure**
   - Ensure all transactions have valid merchant IDs.
   - Correct the data and rerun `transform_bronze_to_silver(transactions, merchants)`.

## Known Bugs

- Hardcoded AWS credentials in the source code.
- Lack of null handling in `transform_bronze_to_silver()`.

## Escalation Contacts

1. **On-call DE**: Priya Nair (priya.nair@sigmadatatech.in, +91-98400-11111)
2. **Tech Lead**: Arjun Mehta (arjun.mehta@sigmadatatech.in)
3. **Platform Manager**: Kavya Reddy (kavya.reddy@sigmadatatech.in)

## Data Quality Checks

- Verify the count of records in `bronze_transactions`, `silver_transactions`, `gold_merchant_performance`, and `gold_daily_summary`.
- Ensure `quality_flag` is set correctly in `silver_transactions`.
- Check for missing merchant names and categories in `silver_transactions`.
- Validate the total revenue and transaction counts in `gold_merchant_performance` and `gold_daily_summary`.