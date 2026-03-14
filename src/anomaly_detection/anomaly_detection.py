from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, avg, stddev, col, lit, concat_ws


spark = SparkSession.builder \
    .appName("AnomalyChecker") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("/app/data/daily_metrics")

def detect_anomalies(df, group_cols, level_name, metrics):
    # Aggregate
    all_cols = ["transaction_date"] + group_cols
    agg_df = df.groupBy(all_cols).agg(*[expr.alias(name) for name, expr in metrics])
    
    # Window
    if group_cols:
        window = Window.partitionBy(group_cols).orderBy("transaction_date").rowsBetween(-30, -1)
    else:
        window = Window.orderBy("transaction_date").rowsBetween(-30, -1)
    
    # Calculate Z-scores
    anomalies = []
    for metric_name, _ in metrics:
        agg_df = agg_df.withColumn(f"{metric_name}_mean", avg(metric_name).over(window))
        agg_df = agg_df.withColumn(f"{metric_name}_std", stddev(metric_name).over(window))
        agg_df = agg_df.withColumn(
            f"{metric_name}_zscore",
            (col(metric_name) - col(f"{metric_name}_mean")) / col(f"{metric_name}_std")
        )
        
        # Create dimension_value column
        if group_cols:
            dim_value = concat_ws(", ", *[col(c) for c in group_cols])
        else:
            dim_value = lit("all")
        
        metric_anomalies = agg_df.filter(
            (col(f"{metric_name}_zscore") > 3) | (col(f"{metric_name}_zscore") < -3)
        ).select(
            col("transaction_date"),
            lit(level_name).alias("level"),
            lit(", ".join(group_cols) if group_cols else "none").alias("dimension"),
            dim_value.alias("dimension_value"),
            lit(metric_name).alias("metric"),
            col(metric_name).alias("metric_value"),
            col(f"{metric_name}_mean").alias("expected_value"),
            col(f"{metric_name}_zscore").alias("zscore")
        )
        anomalies.append(metric_anomalies)
    
    result = anomalies[0]
    for a in anomalies[1:]:
        result = result.union(a)
    
    return result


# Define metrics to track
metrics = [
    ("avg_decline_rate", avg("decline_rate")),
    ("avg_latency", avg("average_latency_ms")),
    ("total_transactions", sum("transaction_count")),
    ("total_volume", sum("total_amount"))
]

# Load raw data
df = spark.read.parquet("/app/data/daily_metrics")

# Daily level (no dimensions)
daily_anomalies = detect_anomalies(df, [], "daily", metrics)

# By region
region_anomalies = detect_anomalies(df, ["region"], "daily_region", metrics)

# By payment method
payment_anomalies = detect_anomalies(df, ["payment_method"], "daily_payment_method", metrics)

# By merchant category
merchant_anomalies = detect_anomalies(df, ["merchant_category"], "daily_merchant_category", metrics)

# By merchant size
size_anomalies = detect_anomalies(df, ["merchant_size"], "daily_merchant_size", metrics)

# By card network
card_anomalies = detect_anomalies(df, ["card_network"], "daily_card_network", metrics)

# Combine all
all_anomalies = daily_anomalies.union(region_anomalies).union(payment_anomalies).union(merchant_anomalies).union(size_anomalies).union(card_anomalies)

all_anomalies.write.mode("overwrite").parquet("/app/data/anomalies")