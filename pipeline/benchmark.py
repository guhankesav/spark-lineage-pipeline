"""Measures three Spark optimizations on real data. Each case: 1 warm-up run, then the median of N timed runs.

    python -m pipeline.benchmark --start 2024-01 --end 2024-06

Runs execute through the ``noop`` sink so the full plan is computed without writing output.
"""
import argparse
import json
import platform
import statistics
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline import quality, transform
from pipeline.run import month_bounds, months_between
from pipeline.spark import build_session


def _execute(df: DataFrame) -> None:
    df.write.format("noop").mode("overwrite").save()


def _median_seconds(action, runs: int) -> float:
    action()  # warm-up: JIT, file metadata, OS page cache
    timings = []
    for _ in range(runs):
        started = time.perf_counter()
        action()
        timings.append(time.perf_counter() - started)
    return round(statistics.median(timings), 3)


def _plan(df: DataFrame) -> str:
    return df._jdf.queryExecution().executedPlan().toString()


def partition_pruning(spark: SparkSession, silver_path: str, scratch: Path, runs: int) -> dict:
    """One-day query on date-partitioned vs. unpartitioned copies of silver.

    The unpartitioned copy is written twice: in its natural (roughly time-ordered) order, where Parquet
    row-group min/max statistics can still skip most data, and randomly shuffled, where they cannot.
    """
    silver = spark.read.parquet(silver_path)
    ordered_path, shuffled_path = str(scratch / "silver_flat_ordered"), str(scratch / "silver_flat_shuffled")
    silver.write.mode("overwrite").parquet(ordered_path)
    silver.orderBy(F.rand(seed=7)).write.mode("overwrite").parquet(shuffled_path)

    def one_day(table: DataFrame) -> DataFrame:
        return (table.filter(F.col("pickup_date") == "2024-01-15")
                .agg(F.count(F.lit(1)).alias("trips"), F.avg("fare_amount").alias("avg_fare")))

    # Tables are opened once, as a catalog would, so the timings measure the scan rather than
    # directory listing. Partition discovery is measured separately because it dominates small queries.
    tables = {name: spark.read.parquet(path) for name, path in
              (("shuffled", shuffled_path), ("ordered", ordered_path), ("partitioned", silver_path))}
    queries = {name: one_day(table) for name, table in tables.items()}
    shuffled = _median_seconds(lambda: _execute(queries["shuffled"]), runs)
    ordered = _median_seconds(lambda: _execute(queries["ordered"]), runs)
    pruned = _median_seconds(lambda: _execute(queries["partitioned"]), runs)
    discovery = _median_seconds(lambda: spark.read.parquet(silver_path).schema, runs)
    return {
        "unpartitioned_shuffled_s": shuffled,
        "unpartitioned_time_ordered_s": ordered,
        "partitioned_s": pruned,
        "speedup_vs_shuffled": round(shuffled / pruned, 1),
        "speedup_vs_time_ordered": round(ordered / pruned, 1),
        "partition_discovery_s": discovery,
    }


def join_strategy(spark: SparkSession, silver_path: str, zones: DataFrame, runs: int) -> dict:
    silver = spark.read.parquet(silver_path)
    default_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    broadcast_df = transform.daily_zone_stats(silver, zones, broadcast_zones=True)
    broadcast = _median_seconds(lambda: _execute(broadcast_df), runs)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        sort_merge_df = transform.daily_zone_stats(silver, zones, broadcast_zones=False)
        assert "SortMergeJoin" in _plan(sort_merge_df)
        sort_merge = _median_seconds(lambda: _execute(sort_merge_df), runs)
    finally:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", default_threshold)
    assert "BroadcastHashJoin" in _plan(broadcast_df)
    return {"sort_merge_s": sort_merge, "broadcast_s": broadcast, "speedup": round(sort_merge / broadcast, 1)}


def caching(spark: SparkSession, raw_paths: list[str], zones: DataFrame, first: str, last: str, runs: int) -> dict:
    start, end = month_bounds(first, last)

    def silver_from_raw() -> DataFrame:
        valid, _ = quality.split(spark.read.parquet(*raw_paths), start, end)
        return transform.to_silver(valid)

    def both_gold(silver: DataFrame) -> None:
        _execute(transform.daily_zone_stats(silver, zones))
        _execute(transform.hourly_demand(silver))

    uncached = _median_seconds(lambda: both_gold(silver_from_raw()), runs)

    def cached_run() -> None:
        silver = silver_from_raw().cache()
        silver.count()
        both_gold(silver)
        silver.unpersist(blocking=True)

    cached = _median_seconds(cached_run, runs)
    return {"recompute_s": uncached, "cached_s": cached, "speedup": round(uncached / cached, 1)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", default="2024-01")
    parser.add_argument("--end")
    parser.add_argument("--out", default="data/out")
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()
    last = args.end or args.start
    raw_paths = [f"data/raw/yellow_tripdata_{m}.parquet" for m in months_between(args.start, last)]
    silver_path = f"{args.out}/silver/trips"
    scratch = Path(args.out) / "bench"
    scratch.mkdir(parents=True, exist_ok=True)

    spark = build_session("spark-lineage-benchmark")
    try:
        zones = spark.read.option("header", True).csv("data/raw/taxi_zone_lookup.csv")
        results = {
            "machine": f"{platform.system()} {platform.machine()}, Spark {spark.version}, local[*]",
            "months": f"{args.start}..{last}",
            "silver_rows": spark.read.parquet(silver_path).count(),
            "runs_per_case": args.runs,
            "partition_pruning": partition_pruning(spark, silver_path, scratch, args.runs),
            "join_strategy": join_strategy(spark, silver_path, zones, args.runs),
            "caching": caching(spark, raw_paths, zones, args.start, last, args.runs),
        }
    finally:
        spark.stop()
    Path("bench").mkdir(exist_ok=True)
    Path("bench/results.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
