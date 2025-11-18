"""Spark Structured Streaming for Real-Time Analytics"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main():
    spark = SparkSession.builder.appName("E-Commerce Analytics").getOrCreate()
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ecommerce-transactions") \
        .load()
    
    query = df.writeStream.format("console").start()
    query.awaitTermination()

if __name__ == "__main__":
    main()
