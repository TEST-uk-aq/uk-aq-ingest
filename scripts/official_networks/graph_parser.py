from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from .profiles import pollutant_code_for_graph_series


class GraphDataError(RuntimeError):
    pass


@dataclass(frozen=True)
class GraphPoint:
    observed_at: str
    value: float


@dataclass(frozen=True)
class GraphSeries:
    source_name: str
    pollutant_code: str
    points: List[GraphPoint]


@dataclass(frozen=True)
class GraphPayload:
    site_name: Optional[str]
    period_label: Optional[str]
    series: List[GraphSeries]
    unknown_series_names: List[str]
    no_recent_graph_data: bool


def parse_embedded_graph_data(html: str) -> GraphPayload:
    marker = "var graphData="
    start = html.find(marker)
    if start < 0:
        raise GraphDataError("graphData marker not found")
    start += len(marker)

    end = html.find(";</script>", start)
    if end < 0:
        end = html.find(";</script", start)
    if end < 0:
        raise GraphDataError("graphData end marker not found")

    try:
        outer = json.loads(html[start:end].strip())
    except json.JSONDecodeError as exc:
        raise GraphDataError(f"graphData outer JSON invalid: {exc}") from exc

    if not isinstance(outer, dict):
        raise GraphDataError("graphData outer value is not an object")

    graph = outer.get("graph")
    if not isinstance(graph, dict):
        return GraphPayload(
            site_name=None,
            period_label=None,
            series=[],
            unknown_series_names=[],
            no_recent_graph_data=True,
        )

    graph_json = graph.get("json")
    if not isinstance(graph_json, str) or not graph_json.strip():
        return GraphPayload(
            site_name=None,
            period_label=None,
            series=[],
            unknown_series_names=[],
            no_recent_graph_data=True,
        )

    try:
        chart = json.loads(graph_json)
    except json.JSONDecodeError as exc:
        raise GraphDataError(f"graphData chart JSON invalid: {exc}") from exc
    if not isinstance(chart, dict):
        raise GraphDataError("graphData chart value is not an object")

    title = chart.get("title")
    subtitle = chart.get("subtitle")
    site_name = title.get("text") if isinstance(title, dict) else None
    period_label = subtitle.get("text") if isinstance(subtitle, dict) else None
    if not isinstance(site_name, str):
        site_name = None
    if not isinstance(period_label, str):
        period_label = None

    parsed_series: List[GraphSeries] = []
    unknown_series_names: List[str] = []
    raw_series = chart.get("series")
    if not isinstance(raw_series, list):
        raise GraphDataError("graphData chart has no series array")

    for raw in raw_series:
        if not isinstance(raw, dict):
            continue
        source_name = str(raw.get("name") or "").strip()
        if not source_name:
            continue
        pollutant_code = pollutant_code_for_graph_series(source_name)
        if pollutant_code is None:
            unknown_series_names.append(source_name[:100])
            continue

        data = raw.get("data")
        if not isinstance(data, list):
            continue
        points: List[GraphPoint] = []
        for point in data:
            if not isinstance(point, list) or len(point) < 2:
                continue
            timestamp_ms, raw_value = point[0], point[1]
            if raw_value is None:
                continue
            if isinstance(timestamp_ms, bool) or not isinstance(timestamp_ms, (int, float)):
                continue
            if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
                continue
            if not math.isfinite(float(timestamp_ms)) or not math.isfinite(float(raw_value)):
                continue
            try:
                observed = datetime.fromtimestamp(
                    float(timestamp_ms) / 1000.0, tz=timezone.utc
                )
            except (OverflowError, OSError, ValueError):
                continue
            points.append(
                GraphPoint(
                    observed_at=observed.isoformat().replace("+00:00", "Z"),
                    value=float(raw_value),
                )
            )

        points.sort(key=lambda point: point.observed_at)
        parsed_series.append(
            GraphSeries(
                source_name=source_name,
                pollutant_code=pollutant_code,
                points=points,
            )
        )

    return GraphPayload(
        site_name=site_name,
        period_label=period_label,
        series=parsed_series,
        unknown_series_names=sorted(set(unknown_series_names)),
        no_recent_graph_data=False,
    )
