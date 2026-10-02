from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from .profiles import OfficialNetworkProfile, pollutant_code_for_graph_series
from .ricardo_time import (
    ricardo_wall_clock_epoch_ms_to_utc,
    uk_local_wall_clock_to_utc,
)


SUPPORTED_GRAPH_DAYS = (7, 14, 21, 30, 60, 90)


class NiDataError(RuntimeError):
    pass


@dataclass(frozen=True)
class NiGraphPoint:
    observed_at: str
    value: float


@dataclass(frozen=True)
class NiGraphSeries:
    source_name: str
    pollutant_code: str
    points: List[NiGraphPoint]


@dataclass(frozen=True)
class NiGraphPayload:
    series: List[NiGraphSeries]
    unknown_series_names: List[str]
    warnings: List[str]


@dataclass(frozen=True)
class NiLatestDataRow:
    pollutant_label: str
    concentration: float
    period: str
    observed_at: str


@dataclass(frozen=True)
class NiLatestDataPayload:
    rows: List[NiLatestDataRow]
    warnings: List[str]


def choose_graph_period(start: datetime, end: datetime) -> Tuple[int, bool]:
    requested_seconds = max(0.0, (end - start).total_seconds())
    for days in SUPPORTED_GRAPH_DAYS:
        if requested_seconds <= days * 24 * 60 * 60:
            return days, False
    return SUPPORTED_GRAPH_DAYS[-1], True


def parse_graph_payload(payload: object) -> NiGraphPayload:
    if not isinstance(payload, list):
        raise NiDataError("NI graph response is not a list")

    points_by_pollutant: Dict[str, Dict[str, NiGraphPoint]] = {}
    source_names: Dict[str, str] = {}
    unknown_series_names = set()
    warnings: List[str] = []
    valid_charts = 0

    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            warnings.append(f"graph item {index} is not an object")
            continue
        raw_chart = item.get("json")
        if not isinstance(raw_chart, str) or not raw_chart.strip():
            warnings.append(f"graph item {index} has no JSON chart")
            continue
        try:
            chart = json.loads(raw_chart)
        except json.JSONDecodeError as exc:
            warnings.append(f"graph item {index} chart JSON invalid: {exc}")
            continue
        if not isinstance(chart, dict):
            warnings.append(f"graph item {index} chart is not an object")
            continue
        raw_series = chart.get("series")
        if not isinstance(raw_series, list):
            warnings.append(f"graph item {index} has no series array")
            continue

        valid_charts += 1
        for raw in raw_series:
            if not isinstance(raw, dict):
                continue
            source_name = str(raw.get("name") or "").strip()
            if not source_name:
                continue
            pollutant_code = pollutant_code_for_graph_series(source_name)
            if pollutant_code is None:
                unknown_series_names.add(source_name[:100])
                continue
            data = raw.get("data")
            if not isinstance(data, list):
                continue

            source_names.setdefault(pollutant_code, source_name)
            pollutant_points = points_by_pollutant.setdefault(pollutant_code, {})
            for point in data:
                if not isinstance(point, list) or len(point) < 2:
                    continue
                timestamp_ms, raw_value = point[0], point[1]
                if raw_value is None:
                    continue
                if isinstance(timestamp_ms, bool) or not isinstance(
                    timestamp_ms, (int, float)
                ):
                    continue
                if isinstance(raw_value, bool) or not isinstance(
                    raw_value, (int, float)
                ):
                    continue
                if not math.isfinite(float(timestamp_ms)) or not math.isfinite(
                    float(raw_value)
                ):
                    continue
                try:
                    observed = ricardo_wall_clock_epoch_ms_to_utc(
                        float(timestamp_ms)
                    )
                except (OSError, OverflowError, ValueError):
                    continue
                observed_at = observed.isoformat().replace("+00:00", "Z")
                pollutant_points[observed_at] = NiGraphPoint(
                    observed_at=observed_at,
                    value=float(raw_value),
                )

    if valid_charts == 0:
        detail = warnings[0] if warnings else "response contained no chart objects"
        raise NiDataError(f"NI graph response has no valid chart: {detail}")

    parsed_series = [
        NiGraphSeries(
            source_name=source_names[pollutant_code],
            pollutant_code=pollutant_code,
            points=[points[key] for key in sorted(points)],
        )
        for pollutant_code, points in sorted(points_by_pollutant.items())
    ]
    return NiGraphPayload(
        series=parsed_series,
        unknown_series_names=sorted(unknown_series_names),
        warnings=warnings,
    )


class _LatestDataTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.table_found = False
        self._in_target_table = False
        self._table_depth = 0
        self._current_row: Optional[List[str]] = None
        self._current_cell: Optional[List[str]] = None
        self.rows: List[List[str]] = []

    def handle_starttag(
        self, tag: str, attrs: List[Tuple[str, Optional[str]]]
    ) -> None:
        tag = tag.lower()
        if not self._in_target_table:
            if tag != "table":
                return
            attributes = {key.lower(): value or "" for key, value in attrs}
            classes = set(attributes.get("class", "").split())
            if "latest-data__table" not in classes:
                return
            self.table_found = True
            self._in_target_table = True
            self._table_depth = 1
            return

        if tag == "table":
            self._table_depth += 1
        elif tag == "tr":
            self._current_row = []
        elif tag == "td" and self._current_row is not None:
            self._current_cell = []
        elif tag == "br" and self._current_cell is not None:
            self._current_cell.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if not self._in_target_table:
            return
        tag = tag.lower()
        if tag == "td" and self._current_cell is not None:
            assert self._current_row is not None
            self._current_row.append("".join(self._current_cell))
            self._current_cell = None
        elif tag == "tr" and self._current_row is not None:
            if self._current_row:
                self.rows.append(self._current_row)
            self._current_row = None
            self._current_cell = None
        elif tag == "table":
            self._table_depth -= 1
            if self._table_depth <= 0:
                self._in_target_table = False

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._current_cell.append(data)


_CONCENTRATION_RE = re.compile(
    r"[-+]?(?:\d{1,3}(?:,\d{3})+(?:\.\d*)?|\d+(?:\.\d*)?|\.\d+)"
)
_LATEST_TIMESTAMP_RE = re.compile(
    r"^(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})$"
)


def _normalise_cell(value: str) -> str:
    return " ".join(value.split())


def _parse_concentration(value: str) -> Optional[float]:
    match = _CONCENTRATION_RE.search(value)
    if match is None:
        return None
    try:
        numeric = float(match.group(0).replace(",", ""))
    except ValueError:
        return None
    return numeric if math.isfinite(numeric) else None


def _parse_latest_timestamp(value: str) -> Optional[str]:
    match = _LATEST_TIMESTAMP_RE.fullmatch(value.strip())
    if match is None:
        return None
    day, month, year, hour, minute = (int(part) for part in match.groups())
    if hour == 24:
        if minute != 0:
            return None
        hour = 0
        day_rollover = True
    elif 0 <= hour <= 23:
        day_rollover = False
    else:
        return None
    try:
        source_wall = datetime(year, month, day, hour, minute)
    except ValueError:
        return None
    if day_rollover:
        source_wall += timedelta(days=1)
    observed = uk_local_wall_clock_to_utc(source_wall)
    return observed.isoformat().replace("+00:00", "Z")


def parse_latest_data_html(html: str) -> NiLatestDataPayload:
    parser = _LatestDataTableParser()
    try:
        parser.feed(html)
        parser.close()
    except Exception as exc:
        raise NiDataError(f"NI latest-data HTML invalid: {exc}") from exc
    if not parser.table_found:
        raise NiDataError("NI latest-data table not found")

    rows: List[NiLatestDataRow] = []
    warnings: List[str] = []
    for index, raw_cells in enumerate(parser.rows, start=1):
        cells = [_normalise_cell(cell) for cell in raw_cells]
        if len(cells) < 5:
            warnings.append(f"latest-data row {index} has fewer than five cells")
            continue
        pollutant_label, concentration_text, period, updated_text = (
            cells[0],
            cells[2],
            cells[3],
            cells[4],
        )
        concentration = _parse_concentration(concentration_text)
        if concentration is None:
            warnings.append(f"latest-data row {index} has no numeric concentration")
            continue
        observed_at = _parse_latest_timestamp(updated_text)
        if observed_at is None:
            warnings.append(f"latest-data row {index} has an invalid timestamp")
            continue
        rows.append(
            NiLatestDataRow(
                pollutant_label=pollutant_label,
                concentration=concentration,
                period=period,
                observed_at=observed_at,
            )
        )

    return NiLatestDataPayload(rows=rows, warnings=warnings)


def is_hourly_mean(period: str) -> bool:
    return " ".join(period.lower().split()) == "hourly mean"


def _parse_iso_utc(value: object) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def acquire_ni_observations(
    profile: OfficialNetworkProfile,
    http: Any,
    stations: Sequence[Dict[str, Any]],
    timeseries_by_station: Mapping[int, Mapping[str, Dict[str, Any]]],
    start: datetime,
    end: datetime,
    pollutant_mapper: Callable[[object], Optional[str]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
    graph_days, graph_period_limited = choose_graph_period(start, end)
    observations: List[Dict[str, Any]] = []
    stats = {
        "stations_attempted": 0,
        "stations_no_recent_graph": 0,
        "stations_failed": 0,
        "series_polled": 0,
        "series_failed": 0,
        "source_failures": 0,
        "graph_period_days": graph_days,
    }
    warnings: List[str] = []
    if graph_period_limited:
        warnings.append(
            "requested window exceeds the NI site's 90-day graph limit; "
            "only the available 90-day source window was requested"
        )

    for station in stations:
        station_id = int(station["id"])
        site_code = str(station["station_ref"]).upper()
        destination = timeseries_by_station.get(station_id) or {}
        stats["stations_attempted"] += 1
        graph_parsed = False
        graph_usable = False
        html_usable = False
        graph_rows: List[Dict[str, Any]] = []
        html_rows: List[Dict[str, Any]] = []
        polled_timeseries = set()

        try:
            graph_response = http.get_json(
                profile.site_graph_url(site_code, graph_days)
            )
            graph_payload = parse_graph_payload(graph_response)
            graph_parsed = True
        except Exception as exc:
            stats["source_failures"] += 1
            warnings.append(
                f"{site_code} graph unavailable: {type(exc).__name__}: {exc}"
            )
        else:
            for warning in graph_payload.warnings:
                warnings.append(f"{site_code} graph: {warning}")
            if graph_payload.unknown_series_names:
                warnings.append(
                    f"{site_code} graph: unknown series "
                    f"{graph_payload.unknown_series_names[:10]}"
                )
            for series in graph_payload.series:
                target = destination.get(series.pollutant_code)
                if target is None:
                    continue
                timeseries_id = int(target["id"])
                polled_timeseries.add(timeseries_id)
                for point in series.points:
                    observed = _parse_iso_utc(point.observed_at)
                    if observed is None or observed < start or observed > end:
                        continue
                    graph_usable = True
                    graph_rows.append(
                        {
                            "connector_id": profile.connector_id,
                            "timeseries_id": timeseries_id,
                            "observed_at": _utc_iso(observed),
                            "value": point.value,
                            "status": None,
                        }
                    )
            if not graph_usable:
                warnings.append(
                    f"{site_code} graph: no recognised mapped pollutant data"
                )

        if graph_parsed and not graph_rows:
            stats["stations_no_recent_graph"] += 1
        graph_keys = {
            (int(row["timeseries_id"]), str(row["observed_at"]))
            for row in graph_rows
        }

        try:
            latest_payload = parse_latest_data_html(
                http.get_text(profile.site_url(site_code))
            )
        except Exception as exc:
            stats["source_failures"] += 1
            warnings.append(
                f"{site_code} HTML unavailable: {type(exc).__name__}: {exc}"
            )
        else:
            for warning in latest_payload.warnings:
                warnings.append(f"{site_code} HTML: {warning}")
            for latest in latest_payload.rows:
                if not is_hourly_mean(latest.period):
                    continue
                pollutant_code = pollutant_mapper(latest.pollutant_label)
                if pollutant_code is None:
                    warnings.append(
                        f"{site_code} HTML: unknown pollutant "
                        f"{latest.pollutant_label[:100]}"
                    )
                    continue
                target = destination.get(pollutant_code)
                if target is None:
                    continue
                timeseries_id = int(target["id"])
                polled_timeseries.add(timeseries_id)
                observed = _parse_iso_utc(latest.observed_at)
                if observed is None or observed < start or observed > end:
                    continue
                html_usable = True
                observed_at = _utc_iso(observed)
                if (timeseries_id, observed_at) in graph_keys:
                    continue
                html_rows.append(
                    {
                        "connector_id": profile.connector_id,
                        "timeseries_id": timeseries_id,
                        "observed_at": observed_at,
                        "value": latest.concentration,
                        "status": None,
                    }
                )
            if not html_usable:
                warnings.append(
                    f"{site_code} HTML: no recognised mapped hourly-mean data"
                )

        if not graph_usable and not html_usable:
            stats["stations_failed"] += 1
        stats["series_polled"] += len(polled_timeseries)
        observations.extend(graph_rows)
        observations.extend(html_rows)

    return observations, stats, warnings
