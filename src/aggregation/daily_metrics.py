from pyspark.sql import SparkSession
from pyspark.sql.functions import (sum, avg, max, min, count, countDistinct, round, 
                                   col, when, to_date, date_trunc)

spark = SparkSession.builder \
    .appName("DailyMetricsCalculator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read in the data
df = spark.read.parquet("/app/data/transactions")
df = df.withColumn("transaction_date", to_date(col("timestamp")))

# Aggregated DF
aggregated = df.groupBy("transaction_date", "region", "merchant_category", "merchant_size", "payment_method", "card_network").agg(
    countDistinct("transaction_id").alias("transaction_count"),
    round(avg("processing_latency_ms"),2).alias("average_latency_ms"),
    sum(when(col("status") == "success", 1).otherwise(0)).alias("successful_transactions"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("average_amount"),
    sum(when(col("status") == "declined", 1).otherwise(0)).alias("declined_transactions"),
)

# Month and Year
aggregated = aggregated.withColumn("transaction_year", date_trunc("year", col("transaction_date")))
aggregated = aggregated.withColumn("transaction_month", date_trunc("month", col("transaction_date")))

# Success and Decline rate
aggregated = aggregated.withColumn("success_rate", round(col("successful_transactions") / col("transaction_count"),4))
aggregated = aggregated.withColumn("decline_rate", round(col("declined_transactions") / col("transaction_count"),4))

aggregated.write.mode("overwrite").parquet("/app/data/daily_metrics")

