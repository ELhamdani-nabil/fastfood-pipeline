from pyspark.sql.types import *

order_schema = StructType([
    StructField("order_id", StringType()), StructField("restaurant_id", StringType()),
    StructField("customer_id", StringType()), StructField("city", StringType()),
    StructField("status", StringType()), StructField("amount_mad", DoubleType()),
    StructField("items_count", IntegerType()), StructField("distance_km", DoubleType()),
    StructField("courier_id", StringType()), StructField("event_time", TimestampType())])

resto_schema = StructType([
    StructField("restaurant_id", StringType()), StructField("name", StringType()),
    StructField("city", StringType()), StructField("category", StringType()),
    StructField("latitude", DoubleType()), StructField("longitude", DoubleType())])
