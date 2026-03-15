from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, when

spark = SparkSession.builder \
    .appName("DrillDown") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

anomalies = spark.read.parquet("/app/data/anomalies")

daily_anomalies = anomalies.filter(col("level") == "daily_total")
dimension_anomalies = anomalies.filter(col("level") != "daily_total")


drilldown = daily_anomalies.alias("d").join(
    dimension_anomalies.alias("dim"),
    (col("d.transaction_date") == col("dim.transaction_date")) & 
    (col("d.metric") == col("dim.metric")),
    "inner"
).select(
    col("d.transaction_date"),
    col("d.metric"),
    col("d.zscore").alias("daily_zscore"),
    col("dim.level"),
    col("dim.dimension_value"),
    col("dim.zscore").alias("dimension_zscore")
)

drilldown_clean = drilldown.withColumn(
    "dimension_value",
    when(col("dimension_value") == "", lit("non-card"))
    .otherwise(col("dimension_value"))
).orderBy(col("dimension_zscore").desc())


drilldown_clean.write.mode("overwrite").parquet("/app/data/drilldown")