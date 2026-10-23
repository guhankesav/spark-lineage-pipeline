# Spark Lineage Pipeline

A PySpark batch pipeline over six months of NYC TLC yellow-taxi trips (**20.3 million rows**) that
separates bad records into a quarantine, writes date-partitioned clean data, builds two analytics
tables, and records **OpenLineage** events for every step so each table can be traced back to its sources.

```
raw monthly Parquet ──► taxi.bronze_to_silver ──► silver/trips (partitioned by pickup_date)
                                │                         │
                                └──► quarantine/trips     ├──► taxi.gold_daily_zone_stats ◄── taxi_zone_lookup.csv
                                     (reject_reason)      └──► taxi.gold_hourly_demand
```

## Run

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt   # needs Java 17+
scripts/download_data.sh 2024-01 2024-06                             # public TLC data, ~330 MB
.venv/bin/python -m pipeline.run --start 2024-01 --end 2024-06
.venv/bin/python -m pipeline.benchmark --start 2024-01 --end 2024-06 --runs 7
.venv/bin/python -m pytest
```

## Results (January to June 2024)

From [`docs/run_summary.json`](docs/run_summary.json):

| | Rows |
|---|---|
| Raw trips | 20,332,093 |
| Silver (valid) | 19,673,717 |
| Quarantined | 658,376 (3.2%) |
| Gold: daily zone stats | 41,351 |
| Gold: hourly demand | 4,367 |

Quarantine reasons: invalid distance 359,311, negative amount 292,849, non-positive duration 6,177,
pickup outside the requested months 39. Every raw row lands in exactly one of silver or quarantine;
the tests assert `raw = silver + quarantined`.

## Data quality

Rules run in order and each rejected row records the **first** rule it failed (`reject_reason`):
missing timestamp, missing zone, dropoff not after pickup, pickup outside the requested months,
distance outside (0, 200] miles, negative fare or total. Nothing is silently dropped.

## Lineage

`pipeline/lineage.py` wraps each job in OpenLineage `START` and `COMPLETE` (or `FAIL`) run events.
Each event names the input and output datasets with a **schema facet**, and outputs carry an
**output statistics facet** with the row count actually written. The gold jobs list `silver/trips`
as an input, which is the edge that links the jobs into one lineage graph.

Events are appended to a JSON-lines file; [`docs/lineage_sample.jsonl`](docs/lineage_sample.jsonl)
is the output of the six-month run. Sending them to Marquez or another OpenLineage backend is a
transport configuration change.

## Benchmarks

Local mode on one laptop (Apple Silicon, Spark 4.0.1, `local[*]`), 19.7M silver rows, median of
7 runs after a warm-up, executed through Spark's `noop` sink. Raw numbers: [`bench/results.json`](bench/results.json).

| Optimization | Result | What it showed |
|---|---|---|
| Partition pruning, one-day query | **3.4x faster** than scanning shuffled unpartitioned data; 1.5x vs. time-ordered data | Parquet row-group min/max statistics already skip most of a time-ordered file, so pruning matters most when data is not naturally clustered |
| Partition discovery | 0.17 s per table open, about 7x the pruned query itself | Listing 182 partition directories dominates small queries; a catalog (Hive, Glue, Iceberg) avoids repeating it |
| Broadcast vs. sort-merge join | **1.7x faster** with the 265-row zone lookup broadcast (7.0 s to 4.1 s; 1.4x in an earlier run) | Broadcasting the small side avoids shuffling 19.7M trips |
| Caching silver for two gold tables | **0.7x (slower)** | Materializing the cache cost more than re-reading compressed Parquet twice; caching pays off only with more consumers or expensive upstream steps |

The first benchmark version opened the table inside each timed run and showed partitioning as
slower; separating partition discovery from the scan exposed the real cost.

## Testing

15 pytest tests on synthetic data, run in CI on every push: every quality rule and its precedence,
derived columns, aggregates, the broadcast and sort-merge physical plans, an end-to-end run across
two monthly files, lineage events and facets, the `FAIL` event, and month-range edge cases.
Test schemas mirror the TLC files (`timestamp_ntz`), so hour extraction uses the local NYC clock.
