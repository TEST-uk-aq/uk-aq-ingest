import os
from datetime import datetime, timedelta, timezone

import pytest

from scripts.sos.sos_ingest import UkAirClient, _parse_datapoints

LIVE_ENV = os.getenv("UKAIR_LIVE")


pytestmark = pytest.mark.live


def _require_live():
    if LIVE_ENV != "1":
        pytest.skip("UKAIR_LIVE=1 not set; skipping live SOS tests")


def test_live_services():
    _require_live()
    client = UkAirClient()
    services = client.services()
    assert services, "Expected at least one service from SOS"


def test_live_stations_and_timeseries_and_data():
    _require_live()
    client = UkAirClient()
    services = client.services()
    assert services, "No services returned"
    service_ref = str(services[0].get("id"))

    stations = client.stations(service_ref, bbox=None, region=None)
    assert stations, "No stations returned"

    # Fetch timeseries without station filter to avoid API 400s
    ts_payload = client.timeseries(service_ref, station_ids=None, batch_size=10)
    assert ts_payload, "No timeseries returned"

    ts_id = ts_payload[0].get("id")
    assert ts_id, "Timeseries missing id"

    window_end = datetime.now(timezone.utc)
    window_start = window_end - timedelta(hours=6)
    timespan = f"{window_start.isoformat()}/{window_end.isoformat()}"

    data = client.timeseries_data(str(ts_id), timespan)
    values = data.get("values", [])
    points = _parse_datapoints(values)
    assert points, "No datapoints parsed from live getData"
