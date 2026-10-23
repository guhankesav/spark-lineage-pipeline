"""Silver and gold transformations."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def to_silver(valid: DataFrame) -> DataFrame:
    """Typed, derived columns used by every downstream table. Partition column: pickup_date."""
    pickup = F.col("tpep_pickup_datetime")
    return valid.select(
        pickup.alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").alias("dropoff_ts"),
        F.to_date(pickup).alias("pickup_date"),
        F.hour(pickup).alias("pickup_hour"),
        F.col("PULocationID").cast("int").alias("pu_location_id"),
        F.col("DOLocationID").cast("int").alias("do_location_id"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
        ((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp(pickup)) / 60.0).alias("duration_min"),
    )


def daily_zone_stats(silver: DataFrame, zones: DataFrame, broadcast_zones: bool = True) -> DataFrame:
    """Trips and fare/duration statistics per pickup day and pickup zone.

    The zone lookup has ~265 rows, so it is broadcast to every executor instead of shuffling
    the trip table for a sort-merge join.
    """
    lookup = zones.select(
        F.col("LocationID").cast("int").alias("pu_location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
    )
    if broadcast_zones:
        lookup = F.broadcast(lookup)
    return (
        silver.join(lookup, "pu_location_id", "left")
        .groupBy("pickup_date", "borough", "zone")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            F.round(F.percentile_approx("duration_min", 0.5), 1).alias("median_duration_min"),
        )
    )


def hourly_demand(silver: DataFrame) -> DataFrame:
    return silver.groupBy("pickup_date", "pickup_hour").agg(
        F.count(F.lit(1)).alias("trips"),
        F.round(F.sum("total_amount"), 2).alias("revenue"),
    )
