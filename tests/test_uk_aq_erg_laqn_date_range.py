from datetime import datetime, timedelta, timezone

from scripts.erg_laqn.source import hourly_points, hourly_url


def test_erg_laqn_date_range_converts_output_hours_to_source_days() -> None:
    start = datetime(2026, 1, 26, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=12)
    url = hourly_url("MY1", "NO2", start, end, "beginning")
    assert url.endswith("/StartDate=2026-01-25/EndDate=2026-01-27/Json")


def test_erg_laqn_blank_and_zero_are_distinct_at_the_gmt_midnight_boundary() -> None:
    start = datetime(2026, 10, 25, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=4)
    payload = {"RawAQData": {"@SiteCode": "MY1", "@SpeciesCode": "NO2", "Data": [
        {"@MeasurementDateGMT": "2026-10-24 23:00:00", "@Value": " "},
        {"@MeasurementDateGMT": "2026-10-25 00:00:00", "@Value": "0"},
        {"@MeasurementDateGMT": "2026-10-25 01:00:00", "@Value": "invalid"},
    ]}}
    points, stats = hourly_points(payload, "MY1", "NO2", start, end, "beginning")
    assert points == [(start + timedelta(hours=1), 0.0, None)]
    assert stats == {"hours_blank": 1, "hours_invalid": 1, "hours_unknown_quality": 1}
