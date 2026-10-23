from datetime import date, datetime

from conftest import trip
from pipeline import transform


def plan(df):
    return df._jdf.queryExecution().executedPlan().toString()


def test_silver_derives_date_hour_and_duration(raw_df):
    row = transform.to_silver(raw_df([trip(pickup=datetime(2024, 1, 15, 23, 50), minutes=25)])).first()
    assert row["pickup_date"] == date(2024, 1, 15)
    assert row["pickup_hour"] == 23
    assert row["duration_min"] == 25.0


def test_daily_zone_stats_aggregates_per_day_and_zone(raw_df, zones_df):
    silver = transform.to_silver(raw_df([
        trip(pu=1, fare=10.0, distance=1.0, minutes=10),
        trip(pu=1, fare=20.0, distance=3.0, minutes=20),
        trip(pu=2, fare=7.0),
        trip(pu=99, fare=5.0),  # zone missing from the lookup: kept, with null borough
    ]))
    stats = {(r["borough"], r["zone"]): r for r in transform.daily_zone_stats(silver, zones_df).collect()}
    newark = stats[("EWR", "Newark Airport")]
    assert (newark["trips"], newark["avg_fare"], newark["avg_distance"]) == (2, 15.0, 2.0)
    assert stats[("Queens", "Jamaica Bay")]["trips"] == 1
    assert stats[(None, None)]["trips"] == 1


def test_zone_lookup_is_broadcast(raw_df, zones_df):
    silver = transform.to_silver(raw_df([trip()]))
    assert "BroadcastHashJoin" in plan(transform.daily_zone_stats(silver, zones_df))


def test_without_broadcast_spark_falls_back_to_sort_merge(spark, raw_df, zones_df):
    silver = transform.to_silver(raw_df([trip()]))
    previous = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        assert "SortMergeJoin" in plan(transform.daily_zone_stats(silver, zones_df, broadcast_zones=False))
    finally:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", previous)


def test_hourly_demand_sums_revenue(raw_df):
    silver = transform.to_silver(raw_df([
        trip(pickup=datetime(2024, 1, 15, 8, 5), total=12.5),
        trip(pickup=datetime(2024, 1, 15, 8, 40), total=7.5),
        trip(pickup=datetime(2024, 1, 15, 9, 0), total=3.0),
    ]))
    rows = {r["pickup_hour"]: (r["trips"], r["revenue"]) for r in transform.hourly_demand(silver).collect()}
    assert rows == {8: (2, 20.0), 9: (1, 3.0)}
