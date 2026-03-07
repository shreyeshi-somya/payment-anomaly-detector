from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    rand, floor, lit, concat, when, col, udf,
    date_add, to_timestamp, month, year, dayofmonth
)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import StringType
import string
import random

spark = SparkSession.builder \
    .appName("PaymentDataGenerator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def random_id(seed):
    chars = string.ascii_letters + string.digits
    random.seed(seed)
    return ''.join(random.choices(chars, k=5))

random_id_udf = udf(random_id, StringType())

# Generate base dataframe with 5M rows
df = spark.range(0, 5_000_000)

# Transaction ID
df = df.withColumn("transaction_id", random_id_udf(col("id")))

# Timestamp
start_date = "2024-01-01"
days_range = 730

df = df.withColumn(
    "timestamp",
    to_timestamp(
        concat(
            date_add(lit(start_date), floor(rand(1) * days_range).cast("int")),
            lit(" "),
            floor(rand(2) * 24).cast("int"),  # hour
            lit(":"),
            floor(rand(3) * 60).cast("int"),  # minute
            lit(":"),
            floor(rand(4) * 60).cast("int")   # second
        )
    )
)

categories = ["retail", "saas", "food_bev", "electronics", "services"]

# Merchant Category
# Weights: retail 30%, food_bev 30%, services 20%, electronics 10%, saas 10%
df = df.withColumn("_rand", rand(5))
df = df.withColumn(
    "merchant_category",
    when(col("_rand") < 0.30, lit("retail"))
    .when(col("_rand") < 0.60, lit("food_bev"))
    .when(col("_rand") < 0.80, lit("services"))
    .when(col("_rand") < 0.90, lit("electronics"))
    .otherwise(lit("saas"))
).drop("_rand")

# Amount
df = df.withColumn(
    "amount",
    spark_round(
        when(col("merchant_category") == "retail", rand(6) * 10000 + 1)
        .when(col("merchant_category") == "saas", rand(6) * rand(6) * 5000 + 2)
        .when(col("merchant_category") == "food_bev", rand(6) * rand(6) * 500 + 2)
        .when(col("merchant_category") == "electronics", (1 - rand(6) * rand(6)) * 2000 + 50)
        .when(col("merchant_category") == "services", rand(6) * 9990 + 10),
        2
    )
)

# Payment Method
df = df.withColumn("_rand", rand(7))
df = df.withColumn(
    "payment_method",
    when(col("_rand") < 0.70, lit("card"))
    .when(col("_rand") < 0.95, lit("ach"))
    .otherwise(lit("wire"))
).drop("_rand")

# Card Network
df = df.withColumn("_rand", rand(8))
df = df.withColumn(
    "card_network",
    when((col("payment_method") == "card") & (col("_rand") < 0.08), "discover")
    .when((col("payment_method") == "card") & (col("_rand") < 0.2), "amex")
    .when((col("payment_method") == "card") & (col("_rand") < 0.5), "mastercard")
    .when((col("payment_method") == "card"), "visa")
).drop("_rand")

# Region
df = df.withColumn("_rand", rand(9))
df = df.withColumn(
    "region",
    when(col("_rand") < 0.17, lit("midwest"))
    .when(col("_rand") < 0.42, lit("west"))
    .when(col("_rand") < 0.70, lit("south"))
    .otherwise(lit("northeast"))
).drop("_rand")

# Merchant Size
df = df.withColumn("_rand", rand(10))
df = df.withColumn(
    "merchant_size",
    when(col("_rand") < 0.1, lit("enterprise"))
    .when(col("_rand") < 0.4, lit("mid-market"))
    .otherwise(lit("smb"))
).drop("_rand")

# Processing Latency ms
df = df.withColumn(
    "processing_latency_ms",
    when(col("payment_method") == "card", floor(rand(11) * 400 + 100))
    .when(col("payment_method") == "ach", floor(rand(11) * 1500 + 500))
    .when(col("payment_method") == "wire", floor(rand(11) * 4000 + 1000))
    .cast("int")
)

# Status
df = df.withColumn("_rand", rand(12))
df = df.withColumn(
    "status",
    when(col("_rand") < 0.01, lit("failed"))
    .when(col("_rand") < 0.06, lit("declined"))
    .otherwise(lit("success"))
).drop("_rand")

# Decline Reason
df = df.withColumn("_rand", rand(13))
df = df.withColumn(
    "decline_reason",
    when((col("status") == "declined") & (col("_rand") < 0.1), "network_error")
    .when((col("status") == "declined") & (col("_rand") < 0.3), "card_expired")
    .when((col("status") == "declined") & (col("_rand") < 0.5), "fraud_suspected")
    .when((col("status") == "declined"), "insufficient_funds")
).drop("_rand")

df = df.drop("id")


######################## Anomalies ########################

# Gateway Outage in first few days of Aug 2024 for Visa in the West Region
df = df.withColumn(
    "status",
    when(
        (year(col("timestamp")) == 2024) & 
        (month(col("timestamp")) == 8) & 
        (dayofmonth(col("timestamp")) <= 3) &
        (col("card_network") == "visa") &
        (col("region") == "west") &
        (rand(14) < 0.80),
        lit("declined")
    ).otherwise(col("status"))
)

# Fraud Spike - February 2025, 1 week, 3x volume in electronics
fraud_spike = df.filter(
    (year(col("timestamp")) == 2025) & 
    (month(col("timestamp")) == 2) & 
    (dayofmonth(col("timestamp")) <= 7) &
    (col("merchant_category") == "electronics")
)
df = df.union(fraud_spike).union(fraud_spike)

# Latency degradation - June 2025, 2 weeks, ACH processing time
df = df.withColumn(
    "processing_latency_ms",
    when(
        (year(col("timestamp")) == 2025) & 
        (month(col("timestamp")) == 6) & 
        (dayofmonth(col("timestamp")) <= 14) &
        (col("payment_method") == "ach"),
        col("processing_latency_ms") * 2
    ).otherwise(col("processing_latency_ms"))
)

# Holiday spike - December 2024 and 2025, 2 weeks each, 2x overall volume
holiday_spike = df.filter(
    ((year(col("timestamp")) == 2024) | (year(col("timestamp")) == 2025)) & 
    (month(col("timestamp")) == 12) & 
    (dayofmonth(col("timestamp")) <= 14)
)
df = df.union(holiday_spike)

print(df.count())
df.write.mode("overwrite").parquet("/app/data/transactions")