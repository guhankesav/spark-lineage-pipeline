from datetime import date, datetime

from conftest import trip
from pipeline import quality

JAN = (date(2024, 1, 1), date(2024, 2, 1))


def reasons(raw_df, rows):
    tagged = quality.tag_rejects(raw_df(rows), *JAN)
    return [r["reject_reason"] for r in tagged.collect()]


def test_valid_trip_passes(raw_df):
    assert reasons(raw_df, [trip()]) == [None]


def test_each_rule_catches_its_case(raw_df):
    rows = [
        trip(pickup=None),
        trip(pu=None),
        trip(minutes=0),
        trip(pickup=datetime(2023, 12, 31, 23, 50)),
        trip(distance=0.0),
        trip(distance=250.0),
        trip(fare=-5.0),
        trip(total=-1.0),
    ]
    assert reasons(raw_df, rows) == [
        "missing_timestamp", "missing_zone", "non_positive_duration", "outside_month",
        "invalid_distance", "invalid_distance", "negative_amount", "negative_amount",
    ]


def test_first_failing_rule_wins(raw_df):
    # Fails both duration and distance; duration is checked first.
    assert reasons(raw_df, [trip(minutes=-5, distance=0.0)]) == ["non_positive_duration"]


def test_split_partitions_every_row_exactly_once(raw_df):
    rows = [trip(), trip(), trip(fare=-1.0), trip(distance=0.0)]
    valid, rejected = quality.split(raw_df(rows), *JAN)
    assert valid.count() == 2
    assert "reject_reason" not in valid.columns
    assert quality.reason_counts(rejected) == {"negative_amount": 1, "invalid_distance": 1}
