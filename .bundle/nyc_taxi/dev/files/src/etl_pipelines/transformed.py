from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

catalog = spark.conf.get("bundle_catalog")
schema = spark.conf.get("bundle_schema")

# PATHS 
bronze_path = "abfss://bronze@nyctaxidata28.dfs.core.windows.net/taxi_data/"

#  BRONZE TABLE 
@dp.table(name="bronze_taxi_trips", table_properties={"delta.feature.timestampNtz": "supported"})
@dp.expect_all_or_drop({"rule_1": "VendorID IS NOT NULL"})
def bronze_taxi_trips():
    df = spark.readStream \
        .format("delta") \
        .load(bronze_path)
    return df

# SILVER TABLE 
silver_rules = {
    "rule_1": "VendorID IS NOT NULL",
    "rule_2": "trip_distance > 0",
    "rule_3": "total_amount > 0",
    "rule_4": "passenger_count > 0",
    "rule_5": "pickup_datetime IS NOT NULL"
}

@dp.table(name="silver_taxi_trips", table_properties={"delta.feature.timestampNtz": "supported"})
@dp.expect_all_or_drop(silver_rules)
def silver_taxi_trips():
    df = dp.read_stream("bronze_taxi_trips")

    df = df.select(
        col("VendorID"),
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        col("passenger_count"),
        col("trip_distance"),
        col("RatecodeID"),
        col("payment_type"),
        col("fare_amount"),
        col("tip_amount"),
        col("tolls_amount"),
        col("total_amount"),
        col("congestion_surcharge"),
        col("Airport_fee"),
        col("PULocationID"),
        col("DOLocationID")
    )
    return df

# GOLD TABLE 
gold_rules = {
    "rule_1": "VendorID IS NOT NULL",
    "rule_2": "total_amount > 0"
}

@dp.table(name="gold_taxi_trips", table_properties={"delta.feature.timestampNtz": "supported"})
@dp.expect_all_or_drop(gold_rules)
def gold_taxi_trips():
    df = dp.read("silver_taxi_trips")

    # Trip duration in minutes
    df = df.withColumn("trip_duration_mins",
        round(
            (unix_timestamp(col("dropoff_datetime")) -
             unix_timestamp(col("pickup_datetime"))) / 60, 2
        )
    )

    # Cost per mile
    df = df.withColumn("cost_per_mile",
        when(col("trip_distance") > 0,
            round(col("total_amount") / col("trip_distance"), 2))
        .otherwise(lit(0.0))
    )

    # Tip percentage
    df = df.withColumn("tip_percentage",
        when(col("fare_amount") > 0,
            round(col("tip_amount") / col("fare_amount") * 100, 2))
        .otherwise(lit(0.0))
    )

    # Payment category
    df = df.withColumn("payment_category",
        when(col("payment_type") == 1, lit("Credit Card"))
        .when(col("payment_type") == 2, lit("Cash"))
        .when(col("payment_type") == 3, lit("No Charge"))
        .when(col("payment_type") == 4, lit("Dispute"))
        .otherwise(lit("Unknown"))
    )

    # Trip distance category
    df = df.withColumn("trip_distance_category",
        when(col("trip_distance") <= 1,   lit("Short"))
        .when(col("trip_distance") <= 5,  lit("Medium"))
        .when(col("trip_distance") <= 10, lit("Long"))
        .otherwise(lit("Very Long"))
    )

    # Pickup hour
    df = df.withColumn("pickup_hour",
        hour(col("pickup_datetime"))
    )

    # Pickup hour category
    df = df.withColumn("pickup_hour_category",
        when(col("pickup_hour").between(6, 11),  lit("Morning"))
        .when(col("pickup_hour").between(12, 17), lit("Afternoon"))
        .when(col("pickup_hour").between(18, 21), lit("Evening"))
        .otherwise(lit("Night"))
    )

    # Is airport trip
    df = df.withColumn("is_airport_trip",
        when(
            (col("PULocationID").isin([1, 132, 138])) |
            (col("DOLocationID").isin([1, 132, 138])),
            lit("Yes")
        ).otherwise(lit("No"))
    )

    # Dedup
    df = df.dropDuplicates([
        "pickup_datetime",
        "dropoff_datetime",
        "PULocationID",
        "DOLocationID"
    ])

    return df
