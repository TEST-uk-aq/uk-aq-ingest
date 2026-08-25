#!/usr/bin/env python3
"""Refresh station PCON/LA codes using the R2 shard lookup."""
from __future__ import annotations

import argparse
import binascii
import json
import math
import os
import sys
import time
import struct
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv()

CLOUDFLARE_API_BASE = "https://api.cloudflare.com/client/v4"
DEFAULT_BUCKET = "uk-aq-pcon-la-lookup"
DEFAULT_PREFIX = "v1"
DEFAULT_GRID_SIZE_DEGREES = 0.05
DEFAULT_PAGE_SIZE = 500
DEFAULT_LIMIT = 0
DEFAULT_SLEEP_SECONDS = 0.0
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504, 522}
RETRYABLE_ERROR_SUBSTRINGS = (
    "timed out",
    "timeout",
    "temporarily unavailable",
    "connection reset",
    "connection refused",
    "broken pipe",
    "remote end closed",
    "network error",
    "econnreset",
    "econnrefused",
    "eof",
)
MAX_ATTEMPTS = 5
RETRY_BASE_MS = 750
RETRY_MAX_MS = 8000
TILE_EPSILON = 1e-12


class LookupErrorWithContext(RuntimeError):
    pass


@dataclass(frozen=True)
class RefreshConfig:
    supabase_url: str
    sb_secret_key: str
    account_id: str
    api_token: str
    bucket: str
    prefix: str
    page_size: int
    limit: int
    sleep_seconds: float
    dry_run: bool


class SupabaseClient:
    def __init__(self, supabase_url: str, secret_key: str, schema: str = "uk_aq_core"):
        self.supabase_url = supabase_url.rstrip("/")
        self.secret_key = secret_key.strip()
        self.schema = schema

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.secret_key,
            "authorization": f"Bearer {self.secret_key}",
            "accept-profile": self.schema,
            "content-profile": self.schema,
            "accept": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.supabase_url}/rest/v1{path}"
        headers = self._headers()
        request_body = None
        if params:
            # requests handles query params directly; keep the shape simple.
            pass
        if body is not None:
            request_body = json.dumps(body).encode("utf-8")
            headers["content-type"] = "application/json"
        response = requests.request(
            method,
            url,
            headers=headers,
            params=params,
            data=request_body,
            timeout=90,
        )
        if response.status_code >= 400:
            raise LookupErrorWithContext(
                f"Supabase request failed ({method} {path}): {response.status_code} {response.reason} - {response.text}"
            )
        if not response.text:
            return []
        return response.json()

    def get_rows(
        self,
        path: str,
        *,
        select: str,
        order: str = "id.asc",
        page_size: int = 1000,
        extra_params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            params: dict[str, Any] = {
                "select": select,
                "order": order,
                "limit": page_size,
                "offset": offset,
            }
            if extra_params:
                params.update(extra_params)
            page = self._request("GET", path, params=params)
            if not isinstance(page, list):
                raise LookupErrorWithContext(f"Unexpected Supabase payload for {path}: {type(page).__name__}")
            rows.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
        return rows

    def get_page(
        self,
        path: str,
        *,
        select: str,
        order: str = "id.asc",
        page_size: int = 1000,
        offset: int = 0,
        extra_params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "select": select,
            "order": order,
            "limit": page_size,
            "offset": offset,
        }
        if extra_params:
            params.update(extra_params)
        page = self._request("GET", path, params=params)
        if not isinstance(page, list):
            raise LookupErrorWithContext(f"Unexpected Supabase payload for {path}: {type(page).__name__}")
        return page

    def get_stations(self) -> list[dict[str, Any]]:
        return self.get_rows(
            "/stations",
            select=(
                "id,station_name,label,geometry,pcon_code,pcon_version,"
                "la_code,la_version,connector_id,removed_at"
            ),
            order="id.asc",
            page_size=1000,
            extra_params={
                "geometry": "not.is.null",
                "or": "(pcon_code.is.null,la_code.is.null)",
            },
        )

    def patch_station(self, station_id: int, payload: dict[str, Any]) -> None:
        response = requests.patch(
            f"{self.supabase_url}/rest/v1/stations",
            headers=self._headers(),
            params={"id": f"eq.{station_id}"},
            json=payload,
            timeout=30,
        )
        response.raise_for_status()


class R2ObjectClient:
    def __init__(self, account_id: str, api_token: str, bucket: str, prefix: str):
        self.account_id = account_id.strip()
        self.api_token = api_token.strip()
        self.bucket = bucket.strip()
        self.prefix = normalize_prefix(prefix)
        self._cache: dict[str, Any] = {}
        self._manifest: dict[str, Any] | None = None

    def object_url(self, object_key: str) -> str:
        encoded_key = "/".join(quote(part, safe="") for part in object_key.split("/") if part)
        return f"{CLOUDFLARE_API_BASE}/accounts/{self.account_id}/r2/buckets/{self.bucket}/objects/{encoded_key}"

    @staticmethod
    def _is_retryable_exception(error: Exception) -> bool:
        message = str(error).lower()
        return any(token in message for token in RETRYABLE_ERROR_SUBSTRINGS)

    @staticmethod
    def _retry_delay_ms(attempt: int) -> int:
        return min(RETRY_MAX_MS, RETRY_BASE_MS * (2 ** max(0, attempt - 1)))

    def get_bytes(self, object_key: str) -> bytes:
        object_key = normalize_object_key(object_key)
        if object_key in self._cache:
            cached = self._cache[object_key]
            if isinstance(cached, bytes):
                return cached
            raise LookupErrorWithContext(f"Cached object is not bytes for {object_key}")

        url = self.object_url(object_key)
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                response = requests.get(
                    url,
                    headers={
                        "authorization": f"Bearer {self.api_token}",
                        "accept": "application/octet-stream",
                    },
                    timeout=90,
                )
            except requests.RequestException as exc:
                if attempt < MAX_ATTEMPTS and self._is_retryable_exception(exc):
                    time.sleep(self._retry_delay_ms(attempt) / 1000.0)
                    continue
                raise LookupErrorWithContext(f"Cloudflare API GET failed for {object_key}: {exc}") from exc

            if response.status_code >= 400:
                raw = response.text
                if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_ATTEMPTS:
                    time.sleep(self._retry_delay_ms(attempt) / 1000.0)
                    continue
                raise LookupErrorWithContext(
                    f"Cloudflare API GET failed for {object_key}: {response.status_code} {response.reason} - {raw}"
                )

            body = response.content
            self._cache[object_key] = body
            return body

        raise LookupErrorWithContext(f"Cloudflare API GET retry loop exhausted for {object_key}")

    def get_json(self, object_key: str) -> Any:
        raw = self.get_bytes(object_key)
        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))

    def load_manifest(self) -> dict[str, Any]:
        if self._manifest is not None:
            return self._manifest
        payload = self.get_json(f"{self.prefix}/manifest.json")
        if not isinstance(payload, dict):
            raise LookupErrorWithContext("Manifest did not parse as a JSON object")
        self._manifest = payload
        return payload


class GeoLookup:
    def __init__(self, r2_client: R2ObjectClient):
        self.r2 = r2_client
        self.manifest = self.r2.load_manifest()
        self.grid_size_degrees = float(self.manifest.get("grid_size_degrees") or DEFAULT_GRID_SIZE_DEGREES)
        self.boundary_detail = normalize_text(self.manifest.get("boundary_detail") or "detailed") or "detailed"
        layers_obj = self.manifest.get("layers")
        layers = layers_obj if isinstance(layers_obj, dict) else {}
        self.pcon_version = normalize_text((layers.get("pcon") or {}).get("boundary_version"))
        self.la_version = normalize_text((layers.get("la") or {}).get("boundary_version"))
        if not self.pcon_version or not self.la_version:
            raise LookupErrorWithContext("Manifest missing PCON or LA boundary version metadata")
        self._shard_cache: dict[str, dict[str, Any]] = {}
        self._geometry_cache: dict[str, dict[str, Any]] = {}

    def shard_object_key(self, layer: str, tile_key: str) -> str:
        grid_token = format_grid_token(self.grid_size_degrees)
        return f"{self.r2.prefix}/{layer}/{self.boundary_detail}/grid_{grid_token}/{tile_key}.json"

    def geometry_object_key(self, geometry_ref: str) -> str:
        return f"{self.r2.prefix}/{normalize_object_key(geometry_ref)}"

    def load_shard(self, layer: str, tile_key: str) -> dict[str, Any]:
        object_key = self.shard_object_key(layer, tile_key)
        if object_key not in self._shard_cache:
            try:
                payload = self.r2.get_json(object_key)
            except LookupErrorWithContext as exc:
                message = str(exc).lower()
                if "404" in message or "does not exist" in message or "not found" in message:
                    payload = {"features": []}
                else:
                    raise
            if not isinstance(payload, dict):
                raise LookupErrorWithContext(f"Shard did not parse as JSON object: {object_key}")
            self._shard_cache[object_key] = payload
        return self._shard_cache[object_key]

    def load_geometry(self, geometry_ref: str) -> dict[str, Any]:
        object_key = self.geometry_object_key(geometry_ref)
        if object_key not in self._geometry_cache:
            payload = self.r2.get_json(object_key)
            if not isinstance(payload, dict):
                raise LookupErrorWithContext(f"Geometry did not parse as JSON object: {object_key}")
            self._geometry_cache[object_key] = payload
        return self._geometry_cache[object_key]

    def lookup_layer(self, layer: str, lat: float, lon: float) -> dict[str, Any]:
        grid_size = self.grid_size_degrees
        exact_tile = tile_for_point(lat, lon, grid_size)
        candidate_tiles = [exact_tile, *neighbour_tiles(exact_tile, grid_size)]
        neighbour_tiles_checked: list[str] = []
        shard_keys_fetched: list[str] = []

        for index, tile in enumerate(candidate_tiles):
            if index > 0:
                neighbour_tiles_checked.append(tile["key"])
            shard_key = self.shard_object_key(layer, tile["key"])
            shard_keys_fetched.append(shard_key)
            shard = self.load_shard(layer, tile["key"])
            features = shard.get("features")
            if not isinstance(features, list) or not features:
                continue

            for feature in features:
                bbox = feature.get("bbox")
                if not bbox_overlap_point(bbox, lon, lat):
                    continue
                geometry_ref = normalize_text(feature.get("geometry_ref"))
                if not geometry_ref:
                    geometry = feature.get("geometry")
                    if point_in_geometry(lon, lat, geometry):
                        return {
                            "code": normalize_text(feature.get("code")) or None,
                            "name": normalize_text(feature.get("name")) or None,
                            "match_strategy": "exact_tile" if index == 0 else "neighbour_tile",
                            "tile_key": tile["key"],
                            "neighbour_tiles_checked": neighbour_tiles_checked,
                            "shard_keys_fetched": shard_keys_fetched,
                        }
                    continue
                geometry_payload = self.load_geometry(geometry_ref)
                geometry = geometry_payload.get("geometry")
                if not geometry:
                    continue
                if point_in_geometry(lon, lat, geometry):
                    return {
                        "code": normalize_text(feature.get("code")) or None,
                        "name": normalize_text(geometry_payload.get("name") or feature.get("name")) or None,
                        "match_strategy": "exact_tile" if index == 0 else "neighbour_tile",
                        "tile_key": tile["key"],
                        "neighbour_tiles_checked": neighbour_tiles_checked,
                        "shard_keys_fetched": shard_keys_fetched,
                    }

        return {
            "code": None,
            "name": None,
            "match_strategy": "no_match",
            "tile_key": exact_tile["key"],
            "neighbour_tiles_checked": neighbour_tiles_checked,
            "shard_keys_fetched": shard_keys_fetched,
        }

    def lookup_station_codes(self, lat: float, lon: float) -> tuple[dict[str, Any], dict[str, Any]]:
        return self.lookup_layer("pcon", lat, lon), self.lookup_layer("la", lat, lon)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh station PCON/LA codes via R2 shard lookup.")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Supabase page size.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max stations to process (0 = no limit).")
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS, help="Sleep between updates.")
    parser.add_argument("--bucket", default=None, help="Override the R2 bucket name.")
    parser.add_argument("--prefix", default=None, help="Override the R2 prefix root.")
    parser.add_argument("--dry-run", action="store_true", help="Log updates without writing.")
    return parser.parse_args()


def normalize_prefix(raw_prefix: str | None) -> str:
    return str(raw_prefix or "").strip().strip("/")


def normalize_object_key(raw_key: str | None) -> str:
    return str(raw_key or "").strip().lstrip("/")


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def grid_precision(grid_size: float) -> int:
    value = Decimal(str(grid_size))
    precision = 0
    scaled = value
    while precision < 8 and scaled != scaled.to_integral_value(rounding=ROUND_HALF_UP):
        precision += 1
        scaled = value * (Decimal(10) ** precision)
    return precision


def round_coord(value: float, precision: int) -> float:
    quant = Decimal("1").scaleb(-precision)
    rounded = Decimal(str(value)).quantize(quant, rounding=ROUND_HALF_UP)
    result = float(rounded)
    return 0.0 if result == -0.0 else result


def format_grid_token(grid_size: float) -> str:
    precision = grid_precision(grid_size)
    return f"{grid_size:.{precision}f}"


def build_tile_key(ix: int, iy: int) -> str:
    return f"iy{iy}_ix{ix}"


def tile_for_point(lat: float, lon: float, grid_size: float) -> dict[str, Any]:
    precision = grid_precision(grid_size)
    iy = math.floor(lat / grid_size)
    ix = math.floor(lon / grid_size)
    lat_min = round_coord(iy * grid_size, precision)
    lon_min = round_coord(ix * grid_size, precision)
    lat_max = round_coord(lat_min + grid_size, precision)
    lon_max = round_coord(lon_min + grid_size, precision)
    return {
        "key": build_tile_key(ix, iy),
        "ix": ix,
        "iy": iy,
        "lat_min": lat_min,
        "lat_max": lat_max,
        "lon_min": lon_min,
        "lon_max": lon_max,
    }


def neighbour_tiles(tile: dict[str, Any], grid_size: float) -> list[dict[str, Any]]:
    precision = grid_precision(grid_size)
    out: list[dict[str, Any]] = []
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            if dx == 0 and dy == 0:
                continue
            ix = int(tile["ix"]) + dx
            iy = int(tile["iy"]) + dy
            lat_min = round_coord(iy * grid_size, precision)
            lon_min = round_coord(ix * grid_size, precision)
            out.append(
                {
                    "key": build_tile_key(ix, iy),
                    "ix": ix,
                    "iy": iy,
                    "lat_min": lat_min,
                    "lat_max": round_coord(lat_min + grid_size, precision),
                    "lon_min": lon_min,
                    "lon_max": round_coord(lon_min + grid_size, precision),
                }
            )
    out.sort(key=lambda item: (item["iy"], item["ix"]))
    return out


def bbox_overlap_point(bbox: Any, lon: float, lat: float) -> bool:
    if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
        return False
    try:
        min_lon, min_lat, max_lon, max_lat = [float(value) for value in bbox]
    except (TypeError, ValueError):
        return False
    return (
        min_lon - TILE_EPSILON <= lon <= max_lon + TILE_EPSILON
        and min_lat - TILE_EPSILON <= lat <= max_lat + TILE_EPSILON
    )


def point_on_segment(lon: float, lat: float, p1: list[Any], p2: list[Any]) -> bool:
    try:
        x1, y1 = float(p1[0]), float(p1[1])
        x2, y2 = float(p2[0]), float(p2[1])
    except (TypeError, ValueError, IndexError):
        return False
    cross = (lon - x1) * (y2 - y1) - (lat - y1) * (x2 - x1)
    if abs(cross) > TILE_EPSILON:
        return False
    dot = (lon - x1) * (lon - x2) + (lat - y1) * (lat - y2)
    return dot <= TILE_EPSILON


def point_in_ring(lon: float, lat: float, ring: Any) -> bool:
    if not isinstance(ring, (list, tuple)) or len(ring) < 3:
        return False
    inside = False
    for idx in range(len(ring) - 1):
        current = ring[idx]
        nxt = ring[idx + 1]
        if point_on_segment(lon, lat, current, nxt):
            return True
        try:
            x1, y1 = float(current[0]), float(current[1])
            x2, y2 = float(nxt[0]), float(nxt[1])
        except (TypeError, ValueError, IndexError):
            continue
        intersects = ((y1 > lat) != (y2 > lat)) and (
            lon < ((x2 - x1) * (lat - y1) / ((y2 - y1) if y2 != y1 else 1e-30)) + x1
        )
        if intersects:
            inside = not inside
    return inside


def point_in_polygon(lon: float, lat: float, polygon: Any) -> bool:
    if not isinstance(polygon, (list, tuple)) or not polygon:
        return False
    rings = polygon
    outer = rings[0]
    if not point_in_ring(lon, lat, outer):
        return False
    for hole in rings[1:]:
        if point_in_ring(lon, lat, hole):
            return False
    return True


def point_in_geometry(lon: float, lat: float, geometry: Any) -> bool:
    if not isinstance(geometry, dict):
        return False
    geometry_type = normalize_text(geometry.get("type"))
    coordinates = geometry.get("coordinates")
    if geometry_type == "Polygon":
        return point_in_polygon(lon, lat, coordinates)
    if geometry_type == "MultiPolygon" and isinstance(coordinates, (list, tuple)):
        return any(point_in_polygon(lon, lat, polygon) for polygon in coordinates)
    return False


def decode_geometry_point(raw_geometry: Any) -> tuple[float, float, int | None]:
    if raw_geometry is None:
        raise ValueError("missing geometry")
    if isinstance(raw_geometry, dict):
        if normalize_text(raw_geometry.get("type")) != "Point":
            raise ValueError(f"unsupported geometry type: {raw_geometry.get('type')}")
        coordinates = raw_geometry.get("coordinates") or []
        if len(coordinates) < 2:
            raise ValueError("invalid geojson point coordinates")
        lon = float(coordinates[0])
        lat = float(coordinates[1])
        srid = raw_geometry.get("srid")
        try:
            srid_value = int(srid) if srid is not None else None
        except (TypeError, ValueError):
            srid_value = None
        return lon, lat, srid_value
    if isinstance(raw_geometry, (bytes, bytearray)):
        raw_bytes = bytes(raw_geometry)
    else:
        raw_text = normalize_text(raw_geometry)
        if not raw_text:
            raise ValueError("missing geometry")
        if raw_text.startswith("{"):
            payload = json.loads(raw_text)
            return decode_geometry_point(payload)
        try:
            raw_bytes = binascii.unhexlify(raw_text)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(f"invalid geometry hex: {exc}") from exc

    if len(raw_bytes) < 1 + 4 + 16:
        raise ValueError("geometry payload too short")
    byte_order = raw_bytes[0]
    if byte_order == 1:
        endian = "<"
    elif byte_order == 0:
        endian = ">"
    else:
        raise ValueError(f"unsupported byte order flag: {byte_order}")
    geom_type_with_flags = struct.unpack_from(f"{endian}I", raw_bytes, 1)[0]
    srid = None
    has_srid = bool(geom_type_with_flags & 0x20000000)
    geom_type = geom_type_with_flags & 0x0FFFFFFF
    offset = 5
    if has_srid:
        if len(raw_bytes) < offset + 4:
            raise ValueError("geometry missing SRID")
        srid = struct.unpack_from(f"{endian}I", raw_bytes, offset)[0]
        offset += 4
    if geom_type != 1:
        raise ValueError(f"unsupported geometry type id: {geom_type}")
    if len(raw_bytes) < offset + 16:
        raise ValueError("geometry point payload too short")
    lon, lat = struct.unpack_from(f"{endian}dd", raw_bytes, offset)
    return lon, lat, srid


def fetch_station_batch(
    client: SupabaseClient,
    last_id: int | None,
    page_size: int,
) -> list[dict[str, Any]]:
    extra_params: dict[str, Any] = {
        "geometry": "not.is.null",
        "or": "(pcon_code.is.null,la_code.is.null)",
    }
    if last_id is not None:
        extra_params["id"] = f"gt.{last_id}"
    return client.get_page(
        "/stations",
        select=(
            "id,station_name,label,geometry,pcon_code,pcon_version,"
            "la_code,la_version,connector_id,removed_at"
        ),
        order="id.asc",
        page_size=page_size,
        extra_params=extra_params,
    )


def patch_station(client: SupabaseClient, station_id: int, payload: dict[str, Any]) -> None:
    client.patch_station(station_id, payload)


def resolve_base_url(url: str) -> str:
    return url[:-1] if url.endswith("/") else url


def required_env(name: str) -> str:
    value = normalize_text(os.getenv(name))
    if not value:
        raise LookupErrorWithContext(f"Missing required environment variable: {name}")
    return value


def resolve_config(args: argparse.Namespace) -> RefreshConfig:
    supabase_url = required_env("SUPABASE_URL")
    sb_secret_key = required_env("SB_SECRET_KEY")
    account_id = normalize_text(
        os.getenv("UK_AQ_DOMAIN_CLOUDFLARE_ACCOUNT_ID")
        or os.getenv("CLOUDFLARE_ACCOUNT_ID")
    )
    api_token = normalize_text(
        os.getenv("UK_AQ_DOMAIN_CLOUDFLARE_API_TOKEN")
        or os.getenv("CLOUDFLARE_API_TOKEN")
    )
    if not account_id:
        raise LookupErrorWithContext(
            "Missing Cloudflare account id. Set UK_AQ_DOMAIN_CLOUDFLARE_ACCOUNT_ID."
        )
    if not api_token:
        raise LookupErrorWithContext(
            "Missing Cloudflare API token. Set UK_AQ_DOMAIN_CLOUDFLARE_API_TOKEN."
        )
    bucket = normalize_text(args.bucket or os.getenv("UK_AQ_GEO_R2_BUCKET") or DEFAULT_BUCKET) or DEFAULT_BUCKET
    prefix = normalize_prefix(args.prefix or os.getenv("UK_AQ_GEO_R2_PREFIX") or DEFAULT_PREFIX) or DEFAULT_PREFIX
    return RefreshConfig(
        supabase_url=supabase_url,
        sb_secret_key=sb_secret_key,
        account_id=account_id,
        api_token=api_token,
        bucket=bucket,
        prefix=prefix,
        page_size=max(1, int(args.page_size)),
        limit=max(0, int(args.limit)),
        sleep_seconds=max(0.0, float(args.sleep_seconds)),
        dry_run=bool(args.dry_run),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh station PCON/LA codes via R2 shard lookup.")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Supabase page size.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max stations to process (0 = no limit).")
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS, help="Sleep between updates.")
    parser.add_argument("--bucket", default=None, help="Override the R2 bucket name.")
    parser.add_argument("--prefix", default=None, help="Override the R2 prefix root.")
    parser.add_argument("--dry-run", action="store_true", help="Log updates without writing.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        config = resolve_config(args)
        supabase = SupabaseClient(resolve_base_url(config.supabase_url), config.sb_secret_key)
        r2 = R2ObjectClient(config.account_id, config.api_token, config.bucket, config.prefix)
        lookup = GeoLookup(r2)
    except LookupErrorWithContext as exc:
        print(f"Refresh station geo R2 failed: {exc}", file=sys.stderr)
        return 1

    stations_url = f"{resolve_base_url(config.supabase_url)}/rest/v1/stations"
    print(
        "start",
        f"bucket={config.bucket}",
        f"prefix={config.prefix}",
        f"dry_run={str(config.dry_run).lower()}",
        f"page_size={config.page_size}",
        f"limit={config.limit}",
    )
    print(
        "manifest",
        f"pcon_version={lookup.pcon_version}",
        f"la_version={lookup.la_version}",
        f"grid_size_degrees={lookup.grid_size_degrees}",
        f"boundary_detail={lookup.boundary_detail}",
    )
    print(f"db={stations_url}")

    last_id: int | None = None
    processed = 0
    updated = 0
    missing_coords = 0
    pcon_found = 0
    la_found = 0
    pcon_missing = 0
    la_missing = 0
    errors = 0
    batch_no = 0

    try:
        while True:
            batch = fetch_station_batch(supabase, last_id, config.page_size)
            if not batch:
                break
            batch_no += 1
            print(
                f"batch={batch_no}",
                f"fetched={len(batch)}",
                f"processed={processed}",
                f"updated={updated}",
                f"missing_coords={missing_coords}",
            )
            for row in batch:
                station_id = row.get("id")
                if station_id is not None:
                    try:
                        last_id = int(station_id)
                    except (TypeError, ValueError):
                        pass

                lon = lat = None
                try:
                    lon, lat, _srid = decode_geometry_point(row.get("geometry"))
                except ValueError:
                    missing_coords += 1
                    processed += 1
                    if config.limit and processed >= config.limit:
                        break
                    if config.sleep_seconds:
                        time.sleep(config.sleep_seconds)
                    continue

                payload: dict[str, Any] = {}
                if normalize_text(row.get("pcon_code")) == "":
                    pcon_lookup = lookup.lookup_layer("pcon", lat, lon)
                    if pcon_lookup["code"]:
                        payload["pcon_code"] = pcon_lookup["code"]
                        payload["pcon_version"] = lookup.pcon_version
                        pcon_found += 1
                    else:
                        pcon_missing += 1
                if normalize_text(row.get("la_code")) == "":
                    la_lookup = lookup.lookup_layer("la", lat, lon)
                    if la_lookup["code"]:
                        payload["la_code"] = la_lookup["code"]
                        payload["la_version"] = lookup.la_version
                        la_found += 1
                    else:
                        la_missing += 1

                if payload and station_id is not None:
                    if config.dry_run:
                        print(f"Dry-run station {station_id}: {json.dumps(payload, sort_keys=True)}")
                        updated += 1
                    else:
                        try:
                            patch_station(supabase, int(station_id), payload)
                            updated += 1
                        except requests.RequestException as exc:
                            errors += 1
                            print(f"Failed to update station {station_id}: {exc}", file=sys.stderr)

                processed += 1
                if config.limit and processed >= config.limit:
                    break
                if config.sleep_seconds:
                    time.sleep(config.sleep_seconds)

            if config.limit and processed >= config.limit:
                break
            if len(batch) < config.page_size:
                break
    except LookupErrorWithContext as exc:
        print(f"Refresh station geo R2 failed: {exc}", file=sys.stderr)
        return 1

    print(
        "Stations processed:",
        processed,
        "updated:",
        updated,
        "missing_coords:",
        missing_coords,
        "pcon_found:",
        pcon_found,
        "la_found:",
        la_found,
        "pcon_missing:",
        pcon_missing,
        "la_missing:",
        la_missing,
        "errors:",
        errors,
    )
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
