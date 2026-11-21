"""Validate source identities, published affiliations and ERG series metadata."""

from __future__ import annotations

from datetime import datetime, timezone
from math import isfinite
from typing import Any
from urllib.parse import urlsplit

from scripts.erg_laqn.source import (
    SPECIES, SPECIES_URL, SourceError, gmt_time, object_rows, source_code, text,
)

LAQN_LABELS = {"LAQN", "LONDON AIR QUALITY NETWORK"}
AMBIGUOUS_LABELS = {"MIXED", "LAQNLOCALITY"}
# The two publishers identify this site independently. This establishes site
# membership only. The nearby MR8/MY7 codes carry separate instrument evidence.
OFFICIAL_AURN_LINKS = {
    "MY1": {
        "uk_air_ref": "UKA00315",
        "checked_on": "2026-11-21",
        "defra_url": "https://uk-air.defra.gov.uk/networks/site-info?site_id=MY1",
        "erg_url": "https://www.londonair.org.uk/london/asp/publicdetails.asp?site=MY1",
    },
}


def site_rows(payload: dict) -> dict[str, dict]:
    envelope = payload.get("Sites")
    if not isinstance(envelope, dict):
        raise SourceError("ERG catalogue has no Sites envelope")
    result = {}
    for row in object_rows(envelope.get("Site"), "Sites.Site"):
        ref = source_code(row.get("@SiteCode"))
        if ref in result:
            raise SourceError(f"Duplicate ERG catalogue site {ref}")
        result[ref] = row
    return result


def affiliations(evidence: dict | None) -> dict[str, list[str]]:
    if evidence is None:
        return {}
    if not isinstance(evidence, dict) or not isinstance(evidence.get("sites"), dict):
        raise SourceError("Membership evidence must include a site-code map")
    url = urlsplit(text(evidence.get("source_url")))
    if url.scheme != "https" or url.hostname not in {
        "www.londonair.org.uk", "londonair.org.uk", "api.erg.ic.ac.uk",
    } or not gmt_time(evidence.get("checked_on")):
        raise SourceError("Membership evidence requires a dated ERG/LondonAir source")
    result = {}
    for key, labels in evidence["sites"].items():
        if not isinstance(labels, list) or not labels or any(
            not isinstance(label, str) or not text(label) for label in labels
        ):
            raise SourceError(f"Missing publisher affiliation labels for {key}")
        result[source_code(key)] = sorted({label.strip() for label in labels})
    return result


def is_laqn(networks: list[str]) -> bool:
    return bool({label.upper() for label in networks} & LAQN_LABELS)


def source_networks(row: dict) -> list[str]:
    value = row.get("@Network")
    if not value:
        return []
    if not isinstance(value, str):
        raise SourceError("Unexpected ERG @Network field")
    return [
        label.strip() for label in value.split(",")
        if label.strip() and label.strip().upper() not in AMBIGUOUS_LABELS
    ]


def build_catalogue(sites_payload: dict, species_payload: dict, evidence: dict | None) -> dict:
    sites = site_rows(sites_payload)
    species_sites = site_rows(species_payload)
    membership = affiliations(evidence)
    result = {}
    for ref in sorted(sites.keys() | species_sites.keys()):
        site, species_site = sites.get(ref, {}), species_sites.get(ref, {})
        declared = [labels for labels in (
            source_networks(site), source_networks(species_site), membership.get(ref, []),
        ) if labels]
        if len({is_laqn(labels) for labels in declared}) > 1:
            raise SourceError(f"Conflicting publisher affiliation evidence for {ref}")
        networks = sorted({label for labels in declared for label in labels})
        for field in ("@DateOpened", "@DateClosed"):
            if field in site and field in species_site and gmt_time(site[field]) != gmt_time(species_site[field]):
                raise SourceError(f"Conflicting ERG site lifecycle for {ref} ({field})")
        detail = {**species_site, **site}
        series = {}
        for row in object_rows(species_site.get("Species"), f"{ref} Species", allow_empty=True):
            code = source_code(row.get("@SpeciesCode"))
            series.setdefault(code, []).append(row)
        result[ref] = {
            "ref": ref, "detail": detail, "networks": networks,
            "laqn": is_laqn(networks), "in_both_catalogues": bool(site and species_site),
            "opened": gmt_time(detail.get("@DateOpened")),
            "closed": gmt_time(detail.get("@DateClosed")),
            "closure_known": "@DateClosed" in detail,
            "series": series, "sources": {"site": site, "site_species": species_site},
            "membership_evidence": (
                {key: evidence[key] for key in ("source_url", "checked_on")}
                if ref in membership else None
            ),
        }
    return result


def coordinate(value: Any, limit: int) -> float | None:
    if not text(value):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise SourceError(f"Invalid ERG coordinate: {value!r}") from None
    if not isfinite(result) or abs(result) > limit:
        raise SourceError(f"Out-of-range ERG coordinate: {value!r}")
    return result


def iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def concentration_series(site: dict, now: datetime) -> dict[str, dict]:
    """Build references for channels the hourly API can address unambiguously."""
    result = {}
    for code, entries in site["series"].items():
        if code not in SPECIES:
            continue
        for entry in entries:
            units = text(entry.get("@Units")).lower().replace("µ", "u").replace("μ", "u").replace("³", "3")
            if units and units not in {"ug/m3", "ug m-3", "ugm-3"}:
                raise SourceError(f"Unverified ERG mass unit for {site['ref']}:{code}: {units}")
        starts = [gmt_time(entry.get("@DateMeasurementStarted")) for entry in entries]
        ends = [gmt_time(entry.get("@DateMeasurementFinished")) for entry in entries]
        if any(end and start and end < start for start, end in zip(starts, ends)):
            raise SourceError(f"Contradictory ERG series lifecycle for {site['ref']}:{code}")
        if len(entries) != 1:
            # The hourly endpoint has no instrument selector. Retain every source
            # record in the register and do not route the values to either instrument.
            result[code] = {"pollable": False, "ambiguous": True, "source": entries}
            continue
        start, end = starts[0], ends[0]
        end = min(bound for bound in (end, site["closed"]) if bound) if end or site["closed"] else None
        start = start or site["opened"]
        if start and end and end < start:
            raise SourceError(f"Series outside ERG site lifecycle for {site['ref']}:{code}")
        # Publisher dates can describe future starts/ends. Keep those dates in
        # metadata while only persisting ended_at for an elapsed closure.
        result[code] = {
            "pollable": bool(start and start <= now and (not end or end > now)),
            "ambiguous": False, "source": entries, "started": iso(start),
            "ended": iso(end), "ended_at": iso(end) if end and end <= now else None,
            "source_url": SPECIES_URL,
        }
    return result


def aurn_identity_evidence(
    site_ref: str, networks: list[str], site_start: datetime | None,
    site_end: datetime | None, aurn_sites: list[dict],
) -> dict | None:
    """Return a dated publisher cross-reference that agrees with the Defra register."""
    evidence = OFFICIAL_AURN_LINKS.get(site_ref)
    if not evidence or not is_laqn(networks) or not site_start:
        return None
    targets = [row for row in aurn_sites if row["site_ref"].upper() == site_ref]
    if len(targets) != 1 or targets[0]["uk_air_ref"] != evidence["uk_air_ref"]:
        return None
    target = targets[0]
    defra_start = gmt_time(target.get("start_date"))
    defra_end = gmt_time(target.get("end_date"))
    if (not defra_start or (site_end and site_end < defra_start)
            or (defra_end and defra_end < site_start)):
        return None
    return evidence
