"""End-to-end batch run: raw trips -> silver (+ quarantine) -> two gold tables, one OpenLineage job per step.

    python -m pipeline.run --start 2024-01 --end 2024-06
"""
import argparse
import json
import time
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from pipeline import quality, transform
from pipeline.lineage import Lineage
from pipeline.spark import build_session


def _first_of(month: str) -> date:
    year, mon = (int(p) for p in month.split("-"))
    return date(year, mon, 1)


def month_bounds(first: str, last: str | None = None) -> tuple[date, date]:
    """[first day of ``first``, first day after ``last``)."""
    end = _first_of(last or first)
    return _first_of(first), date(end.year + (end.month == 12), 1 if end.month == 12 else end.month + 1, 1)


def months_between(first: str, last: str) -> list[str]:
    start, end = month_bounds(first, last)
    months, cur = [], start
    while cur < end:
        months.append(f"{cur.year}-{cur.month:02d}")
        cur = date(cur.year + (cur.month == 12), 1 if cur.month == 12 else cur.month + 1, 1)
    return months


def run(spark: SparkSession, raw_paths: list[str], zones_path: str, out_dir: str, first: str, last: str,
        lineage_path: str) -> dict:
    out = Path(out_dir)
    silver_path, quarantine_path = str(out / "silver" / "trips"), str(out / "quarantine" / "trips")
    zone_stats_path, hourly_path = str(out / "gold" / "daily_zone_stats"), str(out / "gold" / "hourly_demand")
    month_start, month_end = month_bounds(first, last)
    lineage = Lineage(lineage_path)
    summary: dict = {"months": f"{first}..{last}"}

    raw = spark.read.parquet(*raw_paths)
    zones = spark.read.option("header", True).csv(zones_path)

    started = time.perf_counter()
    with lineage.job("taxi.bronze_to_silver", [(p, raw.schema) for p in raw_paths]) as job:
        valid, rejected = quality.split(raw, month_start, month_end)
        silver = transform.to_silver(valid)
        silver.write.mode("overwrite").partitionBy("pickup_date").parquet(silver_path)
        rejected.write.mode("overwrite").parquet(quarantine_path)
        written_silver, written_quarantine = spark.read.parquet(silver_path), spark.read.parquet(quarantine_path)
        summary["raw_rows"] = raw.count()
        summary["silver_rows"] = written_silver.count()
        summary["rejected_by_rule"] = quality.reason_counts(written_quarantine)
        job.output(silver_path, written_silver.schema, summary["silver_rows"])
        job.output(quarantine_path, written_quarantine.schema, sum(summary["rejected_by_rule"].values()))
    summary["silver_seconds"] = round(time.perf_counter() - started, 1)

    silver = spark.read.parquet(silver_path)
    for name, path, build, inputs in (
        ("taxi.gold_daily_zone_stats", zone_stats_path, lambda: transform.daily_zone_stats(silver, zones),
         [(silver_path, silver.schema), (zones_path, zones.schema)]),
        ("taxi.gold_hourly_demand", hourly_path, lambda: transform.hourly_demand(silver),
         [(silver_path, silver.schema)]),
    ):
        with lineage.job(name, inputs) as job:
            build().write.mode("overwrite").parquet(path)
            written = spark.read.parquet(path)
            summary[name.split(".")[-1] + "_rows"] = written.count()
            job.output(path, written.schema, summary[name.split(".")[-1] + "_rows"])

    (out / "run_summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", default="2024-01", help="first month, YYYY-MM")
    parser.add_argument("--end", help="last month, YYYY-MM (default: same as --start)")
    parser.add_argument("--zones", default="data/raw/taxi_zone_lookup.csv")
    parser.add_argument("--out", default="data/out")
    parser.add_argument("--lineage", default="data/out/lineage.jsonl")
    args = parser.parse_args()
    last = args.end or args.start
    raw = [f"data/raw/yellow_tripdata_{m}.parquet" for m in months_between(args.start, last)]
    Path(args.out).mkdir(parents=True, exist_ok=True)
    spark = build_session()
    try:
        print(json.dumps(run(spark, raw, args.zones, args.out, args.start, last, args.lineage), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
