"""
Batch Processing with Spark on EMR
Handles historical data analysis and aggregations
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("E-Commerce Batch Processing") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    
    def process_daily_metrics(self, input_path, output_path):
        """Process daily aggregated metrics"""
        logger.info(f"Reading data from {input_path}")
        
        # Read from S3 Parquet
        df = self.spark.read.parquet(input_path)
        
        # Calculate daily metrics by store
        daily_metrics = df.groupBy(
            to_date("timestamp").alias("date"),
            "store_id"
        ).agg(
            sum("amount").alias("daily_revenue"),
            count("*").alias("transaction_count"),
            avg("amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers"),
            max("amount").alias("max_transaction"),
            min("amount").alias("min_transaction")
        )
        
        # Add derived metrics
        daily_metrics = daily_metrics.withColumn(
            "revenue_per_customer",
            col("daily_revenue") / col("unique_customers")
        )
        
        # Write to Redshift
        logger.info(f"Writing results to {output_path}")
        daily_metrics.write \
            .format("jdbc") \
            .option("url", "jdbc:redshift://cluster.region.redshift.amazonaws.com:5439/analytics") \
            .option("dbtable", "daily_store_metrics") \
            .option("user", "admin") \
            .option("password", "password") \
            .mode("append") \
            .save()
        
        logger.info("Batch processing completed successfully")
    
    def calculate_customer_ltv(self, transactions_path):
        """Calculate Customer Lifetime Value"""
        df = self.spark.read.parquet(transactions_path)
        
        # Calculate LTV per customer
        ltv = df.groupBy("customer_id").agg(
            sum("amount").alias("total_spent"),
            count("*").alias("total_orders"),
            datediff(max("timestamp"), min("timestamp")).alias("customer_age_days"),
            avg("amount").alias("avg_order_value")
        )
        
        # Segment customers
        ltv = ltv.withColumn(
            "segment",
            when(col("total_spent") > 10000, "VIP")
            .when(col("total_spent") > 5000, "High Value")
            .when(col("total_spent") > 1000, "Medium Value")
            .otherwise("Low Value")
        )
        
        return ltv

if __name__ == "__main__":
    processor = BatchProcessor()
    processor.process_daily_metrics(
        "s3://ecommerce-data/transactions/",
        "s3://ecommerce-data/metrics/"
    )
