from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType, TimestampNTZType

from pipeline.spark import use_current_python

# Mirrors the TLC files: timestamp_ntz holds local NYC wall-clock time, so no time zone conversion applies.
RAW_SCHEMA = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType()),
    StructField("tpep_dropoff_datetime", TimestampNTZType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("passenger_count", LongType()),
    StructField("trip_distance", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("total_amount", DoubleType()),
])


def trip(pickup=datetime(2024, 1, 15, 8, 0), minutes=10, pu=1, do=2, distance=2.0, fare=10.0, total=12.0):
    """One raw trip row; every default is valid so each test only states what it breaks."""
    dropoff = None if pickup is None else pickup + timedelta(minutes=minutes)
    return (pickup, dropoff, pu, do, 1, distance, fare, total)


@pytest.fixture(scope="session")
def spark():
    use_current_python()
    session = (SparkSession.builder.master("local[2]").appName("tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()


@pytest.fixture
def raw_df(spark):
    def build(rows):
        return spark.createDataFrame(rows, RAW_SCHEMA)
    return build


@pytest.fixture
def zones_df(spark):
    return spark.createDataFrame(
        [("1", "EWR", "Newark Airport"), ("2", "Queens", "Jamaica Bay")],
        ["LocationID", "Borough", "Zone"],
    )
