from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class PollutantSpec:
    openair_variable: str
    source_series: str
    observed_property_code: str
    uom: str


POLLUTANT_SPECS: Dict[str, PollutantSpec] = {
    "PM1": PollutantSpec("PM1", "PM1", "pm1", "ug/m3"),
    "PM2.5": PollutantSpec("PM2.5", "PM2.5", "pm25", "ug/m3"),
    "PM10": PollutantSpec("PM10", "PM10", "pm10", "ug/m3"),
    "NO": PollutantSpec("NO", "NO", "no", "ug/m3"),
    "NO2": PollutantSpec("NO2", "NO2", "no2", "ug/m3"),
    "NOX": PollutantSpec("NOx", "NOXasNO2", "nox_as_no2", "ug/m3"),
    "O3": PollutantSpec("O3", "O3", "o3", "ug/m3"),
    "SO2": PollutantSpec("SO2", "SO2", "so2", "ug/m3"),
    "CO": PollutantSpec("CO", "CO", "co", "mg/m3"),
    "BC": PollutantSpec("BC", "BC", "bc", "ug/m3"),
}

GRAPH_SERIES_TO_PROPERTY: Dict[str, str] = {
    spec.source_series.upper(): spec.observed_property_code
    for spec in POLLUTANT_SPECS.values()
}
GRAPH_SERIES_TO_PROPERTY.update(
    {
        # Ricardo portal hourly channel names used by WAQN/SAQN site graphs.
        "GE10": "pm10",
        "PM25": "pm25",
        "NOXASNO2": "nox_as_no2",
    }
)

PROPERTY_TO_SPEC: Dict[str, PollutantSpec] = {
    spec.observed_property_code: spec for spec in POLLUTANT_SPECS.values()
}


@dataclass(frozen=True)
class OfficialNetworkProfile:
    connector_code: str
    connector_id: int
    network_code: str
    network_id: int
    display_name: str
    sos_base_url: str
    sos_probe_enabled: bool
    site_page_url_template: str
    site_graph_html_supported: bool
    openair_metadata_url: str
    site_graph_url_template: Optional[str] = None

    def site_url(self, site_code: str) -> str:
        return self.site_page_url_template.format(site_code=site_code.upper())

    def site_graph_url(self, site_code: str, days: int) -> str:
        if not self.site_graph_url_template:
            raise ValueError(
                f"{self.connector_code} has no separate site graph endpoint"
            )
        return self.site_graph_url_template.format(
            site_code=site_code.upper(), days=days
        )


PROFILES: Dict[str, OfficialNetworkProfile] = {
    "waqn": OfficialNetworkProfile(
        connector_code="waqn",
        connector_id=9,
        network_code="waqn",
        network_id=7,
        display_name="Welsh Air Quality Network",
        sos_base_url="https://www.airquality.gov.wales/sos-waq/api/v1",
        sos_probe_enabled=False,
        site_page_url_template=(
            "https://www.airquality.gov.wales/air-pollution/site/{site_code}"
        ),
        site_graph_html_supported=True,
        openair_metadata_url=(
            "https://airquality.gov.wales/sites/default/files/openair/R_data/"
            "WAQ_metadata.RData"
        ),
    ),
    "saqn": OfficialNetworkProfile(
        connector_code="saqn",
        connector_id=10,
        network_code="saqn",
        network_id=8,
        display_name="Scottish Air Quality Network",
        sos_base_url="https://www.scottishairquality.scot/sos-scotland/api/v1",
        sos_probe_enabled=False,
        site_page_url_template=(
            "https://www.scottishairquality.scot/latest/site-info/{site_code}"
        ),
        site_graph_html_supported=True,
        openair_metadata_url=(
            "https://www.scottishairquality.scot/openair/R_data/"
            "SCOT_metadata.RData"
        ),
    ),
    "ni": OfficialNetworkProfile(
        connector_code="ni",
        connector_id=11,
        network_code="ni",
        network_id=9,
        display_name="Northern Ireland Air",
        sos_base_url="https://www.airqualityni.co.uk/sos-ni/api/v1",
        sos_probe_enabled=False,
        site_page_url_template="https://www.airqualityni.co.uk/site/{site_code}",
        site_graph_html_supported=True,
        openair_metadata_url=(
            "https://www.airqualityni.co.uk/openair/R_data/NI_metadata.RData"
        ),
        site_graph_url_template=(
            "https://www.airqualityni.co.uk/api/site/graph/{site_code}/{days}"
        ),
    ),
}


def get_profile(connector_code: str) -> OfficialNetworkProfile:
    key = (connector_code or "").strip().lower()
    try:
        return PROFILES[key]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported official-network connector code: {connector_code!r}"
        ) from exc


def pollutant_spec_for_openair(value: object) -> Optional[PollutantSpec]:
    text = str(value or "").strip()
    if not text:
        return None
    return POLLUTANT_SPECS.get(text.upper())


def pollutant_code_for_graph_series(value: object) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    return GRAPH_SERIES_TO_PROPERTY.get(text.upper())
