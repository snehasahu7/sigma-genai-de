# Data Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data, enriches it with merchant information, and transforms it into clean, enriched, and aggregated formats for analytical purposes.

## Data Flow Diagram

```
+----------------+      +--------------------+      +--------------------+      +--------------------+
|  Source        | ---> |  Bronze Layer       | ---> |  Silver Layer       | ---> |  Gold Layer         |
|  (Dirty & Clean)|      |  (bronze_transactions) |      |  (silver_transactions) |      |  (gold_merchant_performance, |
|                |      |  (merchants)         |      |                       |      |  gold_daily_summary)   |
+----------------+      +--------------------+      +--------------------+      +--------------------+
```

## Key Design Decisions
- **Layered Approach**: The pipeline uses a three-tier architecture (Bronze, Silver, Gold) to separate raw data ingestion, data cleaning, and aggregation.
- **Data Quality Flags**: Introduced quality flags in the Silver layer to distinguish between clean and dirty data.
- **Aggregation at Gold Layer**: Aggregations and summaries are computed at the Gold layer to facilitate complex analytical queries.
- **Timestamps**: Ingestion timestamps are added at each layer to track data processing timelines.

## Known Limitations
- **Single Source**: The pipeline currently only processes data from a single source. Adding more sources would require modifications.
- **Data Quality**: The pipeline does not handle all possible data quality issues, only flags negative amounts and duplicates.
- **No Error Handling**: The pipeline lacks robust error handling, which could lead to data loss in case of failures.
- **Static Merchant Data**: Merchant data is loaded once and not updated, which could lead to stale information.

## Dependencies
- **DuckDB**: The pipeline relies on DuckDB for data storage and processing.
- **MERCHANTS Data**: A predefined list of merchants is used to enrich transaction data.
- **TRANSACTIONS_CLEAN & TRANSACTIONS_DIRTY**: The pipeline processes both clean and dirty transaction data.