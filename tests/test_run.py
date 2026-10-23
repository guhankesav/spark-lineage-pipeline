"""End-to-end run on synthetic files, checking outputs, the quarantine, and the OpenLineage events."""
import json
from datetime import date, datetime

import pytest
from conftest import trip
from pipeline.lineage import Lineage
from pipeline.run import month_bounds, months_between, run


def events(path):
    return [json.loads(line) for line in open(path)]


@pytest.fixture
def pipeline_output(spark, raw_df, tmp_path):
    jan, feb = tmp_path / "raw_jan.parquet", tmp_path / "raw_feb.parquet"
    raw_df([trip(), trip(pu=2), trip(fare=-3.0), trip(pickup=datetime(2023, 12, 1))]).write.parquet(str(jan))
    raw_df([trip(pickup=datetime(2024, 2, 3, 9, 0)), trip(distance=0.0)]).write.parquet(str(feb))
    zones = tmp_path / "zones.csv"
    zones.write_text("LocationID,Borough,Zone\n1,EWR,Newark Airport\n2,Queens,Jamaica Bay\n")
    lineage_file = tmp_path / "lineage.jsonl"
    summary = run(spark, [str(jan), str(feb)], str(zones), str(tmp_path / "out"), "2024-01", "2024-02", str(lineage_file))
    return summary, tmp_path / "out", lineage_file


def test_every_raw_row_lands_in_silver_or_quarantine(pipeline_output):
    summary, _, _ = pipeline_output
    assert summary["raw_rows"] == 6
    assert summary["silver_rows"] == 3
    assert summary["rejected_by_rule"] == {"negative_amount": 1, "outside_month": 1, "invalid_distance": 1}
    assert summary["raw_rows"] == summary["silver_rows"] + sum(summary["rejected_by_rule"].values())


def test_silver_is_partitioned_by_pickup_date(pipeline_output):
    _, out, _ = pipeline_output
    partitions = sorted(p.name for p in (out / "silver" / "trips").iterdir() if p.is_dir())
    assert partitions == ["pickup_date=2024-01-15", "pickup_date=2024-02-03"]


def test_lineage_has_start_and_complete_for_every_job(pipeline_output):
    _, _, lineage_file = pipeline_output
    seen = [(e["job"]["name"], e["eventType"]) for e in events(lineage_file)]
    for job in ("taxi.bronze_to_silver", "taxi.gold_daily_zone_stats", "taxi.gold_hourly_demand"):
        assert (job, "START") in seen and (job, "COMPLETE") in seen
    assert len(seen) == 6


def test_lineage_links_datasets_with_schema_and_row_counts(pipeline_output):
    summary, out, lineage_file = pipeline_output
    complete = {e["job"]["name"]: e for e in events(lineage_file) if e["eventType"] == "COMPLETE"}

    silver_job = complete["taxi.bronze_to_silver"]
    assert [i["name"].split("/")[-1] for i in silver_job["inputs"]] == ["raw_jan.parquet", "raw_feb.parquet"]
    outputs = {o["name"]: o for o in silver_job["outputs"]}
    silver = outputs[str(out / "silver" / "trips")]
    assert silver["outputFacets"]["outputStatistics"]["rowCount"] == summary["silver_rows"]
    assert {"pickup_date", "duration_min"} <= {f["name"] for f in silver["facets"]["schema"]["fields"]}
    assert outputs[str(out / "quarantine" / "trips")]["outputFacets"]["outputStatistics"]["rowCount"] == 3

    # Gold reads exactly what bronze_to_silver wrote: the edge that makes this a lineage graph.
    zone_inputs = [i["name"] for i in complete["taxi.gold_daily_zone_stats"]["inputs"]]
    assert str(out / "silver" / "trips") in zone_inputs and any(n.endswith("zones.csv") for n in zone_inputs)


def test_failed_job_emits_fail_event(tmp_path):
    lineage_file = tmp_path / "lineage.jsonl"
    with pytest.raises(RuntimeError):
        with Lineage(str(lineage_file)).job("taxi.broken", [("in.parquet", None)]):
            raise RuntimeError("boom")
    assert [e["eventType"] for e in events(lineage_file)] == ["START", "FAIL"]


def test_month_helpers_cross_year_boundary():
    assert month_bounds("2024-12") == (date(2024, 12, 1), date(2025, 1, 1))
    assert months_between("2024-11", "2025-02") == ["2024-11", "2024-12", "2025-01", "2025-02"]
