import shutil
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, count, max, broadcast, expr, coalesce, mode
from pyspark.sql.types import StringType, FloatType, DateType

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("[Stage: Ingest Bronze] Starting ingestion")
        
        # Read raw CSV files with all columns as strings
        transactions_df = (spark.read.format("csv")
                           .option("header", "true")
                          .option("inferSchema", "false")
                          .load(input_path))
        
        # Add metadata columns
        transactions_df = transactions_df.withColumn("ingestion_timestamp", lit(run_date))
                                         .withColumn("source_file", lit("transactions.csv"))
                                        .withColumn("pipeline_run_id", lit(run_id))
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/ingestion_timestamp={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write as Parquet partitioned by date
        transactions_df.write.mode("overwrite").partitionBy("ingestion_timestamp").parquet(output_path)
        
        logging.info(f"[Stage: Ingest Bronze] Ingested {transactions_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Ingest Bronze] Error: {e}, Row count: {transactions_df.count()}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("[Stage: Transform Silver] Starting transformation")
        
        # Read Bronze Parquet with partition pruning on run_date
        transactions_df = (spark.read.format("parquet")
                           .load(bronze_path)
                          .filter(col("ingestion_timestamp") == run_date)
                          .where(col("transaction_id").isNotNull() & (col("amount") >= 0)))
        
        logging.info(f"[Stage: Transform Silver] Input count: {transactions_df.count():,} rows")
        
        # Cast columns to correct types
        transactions_df = transactions_df.withColumn("amount", col("amount").cast(FloatType()))
                                        .withColumn("transaction_date", col("transaction_date").cast(DateType()))
                                        .withColumn("transaction_id", col("transaction_id").cast(StringType()))
                                        .withColumn("merchant_id", col("merchant_id").cast(StringType()))
        
        # Read merchants CSV
        merchants_df = (spark.read.format("csv")
                        .option("header", "true")
                       .option("inferSchema", "false")
                        .load(merchants_path))
        
        # Cache merchants DataFrame (small, referenced multiple times)
        merchants_df = merchants_df.cache()
        
        # Deduplicate on transaction_id keeping latest ingestion_timestamp
        transactions_dedup_df = (transactions_df.withColumn("row_number", 
                              when(col("transaction_id").isNotNull(), 
                                   col("ingestion_timestamp").cast("long")).otherwise(0))
                              .withColumn("row_number", 
                                 when(col("transaction_id").isNotNull(), 
                                      col("row_number").cast("long")).otherwise(0))
                              .dropDuplicates(["transaction_id"])
                              .orderBy(col("row_number").desc()))
        
        logging.info(f"[Stage: Transform Silver] After dedup count: {transactions_dedup_df.count():,} rows")
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/ingestion_timestamp={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write as Parquet partitioned by date
        transactions_dedup_df.write.mode("overwrite").partitionBy("ingestion_timestamp").parquet(output_path)
        
        logging.info(f"[Stage: Transform Silver] Transformed {transactions_dedup_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Transform Silver] Error: {e}, Row count: {transactions_dedup_df.count()}")
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Merchant Performance] Starting aggregation")
        
        # Read Silver layer data with partition pruning
        silver_df = spark.read.parquet(silver_path).filter(col("ingestion_timestamp") == run_date)
        
        logging.info(f"[Stage: Build Merchant Performance] Input count: {silver_df.count():,} rows")
        
        # Calculate metrics
        merchant_performance_df = silver_df.groupBy("merchant_id", "merchant_name", "category", "city", "ingestion_timestamp") \
          .agg(
                sum(coalesce(col("amount").cast(FloatType()), 0.0)).alias("total_revenue"),
                count("*").alias("txn_count"),
                (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            ).where(col("status") == "COMPLETED")
        
        logging.info(f"[Stage: Build Merchant Performance] Output count: {merchant_performance_df.count():,} rows")
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/ingestion_timestamp={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write Gold layer data
        merchant_performance_df.repartition("ingestion_timestamp").write.mode("overwrite").parquet(output_path)
        
        logging.info(f"[Stage: Build Merchant Performance] Aggregated {merchant_performance_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Build Merchant Performance] Error: {e}, Row count: {merchant_performance_df.count()}")
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        logging.info("[Stage: Build Customer LTV] Starting aggregation")
        
        # Read Silver layer data
        silver_df = spark.read.parquet(silver_path)
        
        logging.info(f"[Stage: Build Customer LTV] Input count: {silver_df.count():,} rows")
        
        # Calculate metrics
        customer_ltv_df = silver_df.groupBy("customer_id") \
           .agg(
                sum(coalesce(col("amount").cast(FloatType()), 0.0)).alias("total_spent"),
                count("*").alias("total_txns"),
                expr("avg(amount)").alias("avg_txn_value"),
                min("transaction_date").alias("first_txn_date"),
                max("transaction_date").alias("last_txn_date"),
                mode("payment_method").alias("preferred_payment_method")
            ).where(col("status") == "COMPLETED")
        
        logging.info(f"[Stage: Build Customer LTV] Output count: {customer_ltv_df.count():,} rows")
        
        # Delete existing partition before writing
        partition_path = output_path
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write Gold layer data
        customer_ltv_df.write.mode("overwrite").parquet(output_path)
        
        logging.info(f"[Stage: Build Customer LTV] Aggregated {customer_ltv_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Build Customer LTV] Error: {e}, Row count: {customer_ltv_df.count()}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Daily Summary] Starting aggregation")
        
        # Read Silver layer data with partition pruning
        silver_df = spark.read.parquet(silver_path).filter(col("ingestion_timestamp") == run_date)
        
        logging.info(f"[Stage: Build Daily Summary] Input count: {silver_df.count():,} rows")
        
        # Calculate metrics
        daily_summary_df = silver_df.groupBy("ingestion_timestamp") \
            .agg(
                sum(coalesce(col("amount").cast(FloatType()), 0.0)).alias("total_revenue"),
                count("*").alias("total_txns"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("merchant_id").alias("unique_merchants"),
                (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            )
        
        logging.info(f"[Stage: Build Daily Summary] Output count: {daily_summary_df.count():,} rows")
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/ingestion_timestamp={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write Gold layer data
        daily_summary_df.repartition("ingestion_timestamp").write.mode("overwrite").parquet(output_path)
        
        logging.info(f"[Stage: Build Daily Summary] Aggregated {daily_summary_df.count():,} rows")
    except Exception as e:
        logging.error(f"[Stage: Build Daily Summary] Error: {e}, Row count: {daily_summary_df.count()}")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    try:
        logging.info("[Stage: Run Gold] Starting gold layer aggregations")
        
        # Define output paths for Gold tables
        merchant_performance_path = f"{gold_output_dir}/merchant_performance"
        customer_ltv_path = f"{gold_output_dir}/customer_ltv"
        daily_summary_path = f"{gold_output_dir}/daily_summary"
        
        # Build Gold layer tables
        build_merchant_performance(spark, silver_path, merchant_performance_path, run_date)
        build_customer_ltv(spark, silver_path, customer_ltv_path)
        build_daily_summary(spark, silver_path, daily_summary_path, run_date)
        
        # Collect row counts
        run_metadata = {
            "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
            "run_date": run_date,
            "run_id": datetime.now().isoformat(),
            "run_status": "SUCCESS",
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat(),
            "merchant_performance_path": merchant_performance_path,
            "customer_ltv_path": customer_ltv_path,
            "daily_summary_path": daily_summary_path
        }
        
        # Write run metadata summary
        spark.sparkContext.parallelize([run_metadata]).write.json(f"{gold_output_dir}/run_metadata")
        
        logging.info("[Stage: Run Gold] Gold layer aggregations completed successfully")
    except Exception as e:
        logging.error(f"[Stage: Run Gold] Error: {e}")
        run_metadata["run_status"] = "FAILED"
        run_metadata["error_message"] = str(e)
        spark.sparkContext.parallelize([run_metadata]).write.json(f"{gold_output_dir}/run_metadata")
        raise

# Initialize Spark session
spark = SparkSession.builder.appName("Sigma DataTech Transaction Analytics Pipeline").getOrCreate()
