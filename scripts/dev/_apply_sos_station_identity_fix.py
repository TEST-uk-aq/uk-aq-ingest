from __future__ import annotations

import hashlib
from pathlib import Path

SOURCE = Path("scripts/sos/sos_ingest.py")
TEST = Path("tests/test_sos_station_identity.py")
EXPECTED_BLOB_SHA = "14eb91ebf2e5e4e110e4bbbd9fe1b3b38025ee3a"


def git_blob_sha(text: str) -> str:
    body = text.encode("utf-8")
    return hashlib.sha1(f"blob {len(body)}\0".encode("utf-8") + body).hexdigest()


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"Patch anchor {label!r} matched {count} times")
    return text.replace(old, new, 1)


source = SOURCE.read_text(encoding="utf-8")
actual_sha = git_blob_sha(source)
if actual_sha != EXPECTED_BLOB_SHA:
    raise RuntimeError(
        f"Refusing to patch changed SOS ingest: expected={EXPECTED_BLOB_SHA} actual={actual_sha}"
    )

source = replace_once(
    source,
    '''    def get_timeseries_id_map(
        self, connector_id: int, service_ref: str, timeseries_refs: Sequence[str]
    ) -> Dict[str, int]:
        return self.get_ref_id_map("timeseries", "timeseries_ref", timeseries_refs, connector_id, service_ref)

    def get_station_label_map''',
    '''    def get_timeseries_id_map(
        self, connector_id: int, service_ref: str, timeseries_refs: Sequence[str]
    ) -> Dict[str, int]:
        return self.get_ref_id_map("timeseries", "timeseries_ref", timeseries_refs, connector_id, service_ref)

    def get_timeseries_station_id_map(
        self, connector_id: int, service_ref: str, timeseries_refs: Sequence[str]
    ) -> Dict[str, Optional[int]]:
        mapping: Dict[str, Optional[int]] = {}
        refs = [str(ref) for ref in timeseries_refs if ref is not None and str(ref).strip()]
        for chunk in _chunked(refs, 500):
            resp = (
                self.core.table("timeseries")
                .select("timeseries_ref,station_id")
                .eq("connector_id", connector_id)
                .eq("service_ref", str(service_ref))
                .in_("timeseries_ref", chunk)
                .execute()
            )
            rows = resp.data if hasattr(resp, "data") else resp.get("data")
            for row in rows or []:
                ref = row.get("timeseries_ref")
                if ref is None:
                    continue
                station_id = row.get("station_id")
                mapping[str(ref)] = int(station_id) if station_id is not None else None
        return mapping

    def get_station_label_map''',
    "existing timeseries ownership reader",
)

source = replace_once(
    source,
    '''    def get_station_label_geometry_map(
        self, connector_id: int, service_ref: str
    ) -> Tuple[Dict[str, List[int]], Dict[int, str]]:''',
    '''    def get_station_label_geometry_map(
        self,
        connector_id: int,
        service_ref: str,
        allowed_station_ids: Optional[Set[int]] = None,
    ) -> Tuple[Dict[str, List[int]], Dict[int, str]]:''',
    "catalogue station label map signature",
)

source = replace_once(
    source,
    '''            for row in rows:
                label = row.get("label")
                if not label:
                    continue
                label_text = str(label)
                key_full = _normalize_station_label(label_text)''',
    '''            for row in rows:
                row_id = int(row["id"])
                if allowed_station_ids is not None and row_id not in allowed_station_ids:
                    continue
                label = row.get("label")
                if not label:
                    continue
                label_text = str(label)
                key_full = _normalize_station_label(label_text)''',
    "catalogue station label map filter",
)

source = replace_once(
    source,
    '''        rows = []
        label_match_count = 0
        for ts in series:
            station_ref = _extract_station_ref(ts)
            if station_ref is None:
                station_ref = _extract_station_ref_from_label(ts.get("label"))
            feature_payload = _extract_feature_payload(ts)
            feature_ref = _extract_ref_id(feature_payload) if feature_payload else None
            station_db_id = station_id_map.get(str(station_ref)) if station_ref is not None else None
            if station_db_id is None and station_label_map:
                descriptor = _extract_station_descriptor_from_label(ts.get("label"))
                if descriptor:
                    descriptor_key = _normalize_station_label(descriptor)
                    matches = station_label_map.get(descriptor_key) or []
                    chosen = _choose_station_id_by_geometry(matches, station_geometry_by_id)
                    if chosen is not None:
                        station_db_id = chosen
                        label_match_count += 1
                if station_db_id is None:
                    station_name = _extract_station_name_from_label(ts.get("label"))
                    if station_name:
                        label_key = _normalize_station_label(station_name)
                        matches = station_label_map.get(label_key) or []
                        chosen = _choose_station_id_by_geometry(matches, station_geometry_by_id)
                        if chosen is not None:
                            station_db_id = chosen
                            label_match_count += 1
            category_ref''',
    '''        series_rows = list(series)
        timeseries_refs = [
            str(ts.get("id"))
            for ts in series_rows
            if ts.get("id") is not None and str(ts.get("id")).strip()
        ]
        existing_station_ids = self.get_timeseries_station_id_map(
            connector_id,
            service_ref,
            timeseries_refs,
        )
        rows = []
        label_match_count = 0
        for ts in series_rows:
            timeseries_ref = str(ts.get("id")) if ts.get("id") is not None else None
            station_db_id, resolution = _resolve_timeseries_station_id(
                ts,
                station_id_map,
                station_label_map or {},
            )
            existing_station_id = existing_station_ids.get(timeseries_ref) if timeseries_ref else None
            station_db_id = _preserve_or_validate_timeseries_station_id(
                timeseries_ref,
                existing_station_id,
                station_db_id,
            )
            if resolution in {"label_descriptor", "label_name"}:
                label_match_count += 1
            feature_payload = _extract_feature_payload(ts)
            feature_ref = _extract_ref_id(feature_payload) if feature_payload else None
            category_ref''',
    "safe timeseries station resolution",
)

source = replace_once(
    source,
    '''            row: Dict[str, Any] = {
                "timeseries_ref": str(ts.get("id")) if ts.get("id") is not None else None,
                "label": ts.get("label"),
                "uom": ts.get("uom"),
                "station_id": station_db_id,
                "connector_id": connector_id,''',
    '''            row: Dict[str, Any] = {
                "timeseries_ref": timeseries_ref,
                "label": ts.get("label"),
                "uom": ts.get("uom"),
                "connector_id": connector_id,''',
    "avoid station null clobber",
)

source = replace_once(
    source,
    '''                "status_intervals": ts.get("statusIntervals"),
            }
            # Avoid clobbering existing values when source metadata omits scalar fields.''',
    '''                "status_intervals": ts.get("statusIntervals"),
            }
            if station_db_id is not None:
                row["station_id"] = station_db_id
            # Avoid clobbering existing values when source metadata omits scalar fields.''',
    "conditionally write station ownership",
)

source = replace_once(
    source,
    '''    def discover_timeseries(
        self,
        connector: ConnectorContext,
        station_refs: Optional[Sequence[str]],
        pollutants: Optional[Sequence[str]],
        batch_size: Optional[int],
        sample_count: int,
    ) -> List[Dict[str, Any]]:''',
    '''    def discover_timeseries(
        self,
        connector: ConnectorContext,
        station_refs: Optional[Sequence[str]],
        pollutants: Optional[Sequence[str]],
        batch_size: Optional[int],
        sample_count: int,
        catalogue_station_refs: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:''',
    "catalogue station refs argument",
)

source = replace_once(
    source,
    '''        station_refs_to_map: List[str] = []
        if station_refs is not None:
            station_refs_to_map = [
                str(station_ref) for station_ref in station_refs if station_ref is not None
            ]
        else:
            for ts in filtered:
                station_value = _extract_station_ref(ts)
                if station_value is None:
                    station_value = _extract_station_ref_from_label(ts.get("label"))
                if station_value is not None:
                    station_refs_to_map.append(str(station_value))
        station_refs_to_map = list(dict.fromkeys(station_refs_to_map))''',
    '''        trusted_station_refs = (
            catalogue_station_refs
            if catalogue_station_refs is not None
            else station_refs
        )
        station_refs_to_map = list(dict.fromkeys(
            str(station_ref)
            for station_ref in (trusted_station_refs or [])
            if station_ref is not None and str(station_ref).strip()
        ))''',
    "trusted catalogue station refs",
)

start = source.index('''        station_index = self.writer.get_station_geometry_index(
            connector.id,
            connector.service_ref,
        )
        created_rows = []''')
end = source.index('''        station_label_map, station_geometry_by_id = self.writer.get_station_label_geometry_map(
            connector.id, connector.service_ref
        )''', start)
replacement = '''        catalogue_station_ids = set(station_id_map.values())
        station_label_map, station_geometry_by_id = self.writer.get_station_label_geometry_map(
            connector.id,
            connector.service_ref,
            allowed_station_ids=catalogue_station_ids,
        )'''
old_block = source[start:end + len('''        station_label_map, station_geometry_by_id = self.writer.get_station_label_geometry_map(
            connector.id, connector.service_ref
        )''')]
source = source.replace(old_block, replacement, 1)

source = replace_once(
    source,
    '''            pollutants,
            batch_size,
            args.sample_timeseries,
        )''',
    '''            pollutants,
            batch_size,
            args.sample_timeseries,
            catalogue_station_refs=station_refs,
        )''',
    "main catalogue station refs",
)

source = replace_once(
    source,
    '''def _extract_station_ref(ts: Dict[str, Any]) -> Optional[str]:
    for key in ("station", "station_id", "stationId", "station_ref", "stationRef"):
        ref = _extract_ref_id(ts.get(key))
        if ref:
            return ref
    feature_payload = _extract_feature_payload(ts)
    if feature_payload:
        ref = _extract_ref_id(feature_payload)
        if ref:
            return ref
    return None
''',
    '''def _extract_station_ref(ts: Dict[str, Any]) -> Optional[str]:
    # Feature-of-interest identifiers and numeric tokens in timeseries labels are
    # not station catalogue identifiers. Only explicit station fields are trusted.
    for key in ("station", "station_id", "stationId", "station_ref", "stationRef"):
        ref = _extract_ref_id(ts.get(key))
        if ref:
            return ref
    return None
''',
    "do not treat feature ids as stations",
)

helper_anchor = '''def _choose_station_id_by_geometry(
    matches: List[int],
    geometry_by_id: Optional[Dict[int, str]],
) -> Optional[int]:
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]
    if not geometry_by_id:
        return None
    keys = [geometry_by_id.get(match) for match in matches]
    if any(key is None for key in keys):
        return None
    first = keys[0]
    if all(first == key for key in keys[1:]):
        return min(matches)
    return None


'''
helper_replacement = helper_anchor + '''def _resolve_timeseries_station_id(
    ts: Dict[str, Any],
    station_id_map: Dict[str, int],
    station_label_map: Dict[str, List[int]],
) -> Tuple[Optional[int], Optional[str]]:
    explicit_ref = _extract_station_ref(ts)
    if explicit_ref is not None:
        station_id = station_id_map.get(str(explicit_ref))
        if station_id is not None:
            return station_id, "station_ref"

    descriptor = _extract_station_descriptor_from_label(ts.get("label"))
    if descriptor:
        matches = station_label_map.get(_normalize_station_label(descriptor)) or []
        if len(matches) == 1:
            return matches[0], "label_descriptor"

    station_name = _extract_station_name_from_label(ts.get("label"))
    if station_name:
        matches = station_label_map.get(_normalize_station_label(station_name)) or []
        if len(matches) == 1:
            return matches[0], "label_name"

    return None, None


def _preserve_or_validate_timeseries_station_id(
    timeseries_ref: Optional[str],
    existing_station_id: Optional[int],
    resolved_station_id: Optional[int],
) -> Optional[int]:
    if (
        existing_station_id is not None
        and resolved_station_id is not None
        and existing_station_id != resolved_station_id
    ):
        raise RuntimeError(
            "SOS timeseries station ownership change refused "
            f"timeseries_ref={timeseries_ref or '(missing)'} "
            f"existing_station_id={existing_station_id} "
            f"resolved_station_id={resolved_station_id}"
        )
    return resolved_station_id if resolved_station_id is not None else existing_station_id


'''
source = replace_once(source, helper_anchor, helper_replacement, "station identity helpers")

SOURCE.write_text(source, encoding="utf-8")
TEST.parent.mkdir(parents=True, exist_ok=True)
TEST.write_text(
    '''import unittest

from scripts.sos.sos_ingest import (
    _extract_station_ref,
    _normalize_station_label,
    _preserve_or_validate_timeseries_station_id,
    _resolve_timeseries_station_id,
)


class SosStationIdentityTests(unittest.TestCase):
    def test_feature_identifier_is_not_a_station_ref(self):
        self.assertIsNone(_extract_station_ref({"feature": {"id": "10533"}}))

    def test_explicit_catalogue_station_ref_is_used(self):
        station_id, source = _resolve_timeseries_station_id(
            {"station": {"id": "788042"}, "label": "Tallington-Ozone (air)"},
            {"788042": 2319},
            {},
        )
        self.assertEqual((station_id, source), (2319, "station_ref"))

    def test_exact_descriptor_resolves_tallington_ozone(self):
        station_id, source = _resolve_timeseries_station_id(
            {
                "feature": {"id": "10533"},
                "label": (
                    "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7 "
                    "10533 - Tallington-Ozone (air), Tallington-Ozone (air)"
                ),
            },
            {},
            {_normalize_station_label("Tallington-Ozone (air)"): [2319]},
        )
        self.assertEqual((station_id, source), (2319, "label_descriptor"))

    def test_ambiguous_station_name_is_not_guessed(self):
        station_id, source = _resolve_timeseries_station_id(
            {"label": "Tallington"},
            {},
            {_normalize_station_label("Tallington"): [469, 2319, 2323]},
        )
        self.assertEqual((station_id, source), (None, None))

    def test_existing_ownership_is_preserved_when_unresolved(self):
        self.assertEqual(
            _preserve_or_validate_timeseries_station_id("5158", 2319, None),
            2319,
        )

    def test_station_reparenting_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "ownership change refused"):
            _preserve_or_validate_timeseries_station_id("5158", 7618, 2319)


if __name__ == "__main__":
    unittest.main()
''',
    encoding="utf-8",
)

print("Applied guarded SOS station identity fix")
