# LAQN TEST operator runbook

The independent `erg_laqn` connector uses network `laqn`. Imperial College London's Environmental Research Group publishes the source through LondonAir. ERG's API documentation specifies one fixed client IP and Open Government Licence v2. Use the same approved egress for catalogue, observations and source verification. Check any additional data-owner terms before public display. [ERG API documentation](https://api.erg.ic.ac.uk/AirQuality/Information/Documentation/pdf) · [LondonAir data terms](https://www.londonair.org.uk/london/asp/copyright.asp)

## Source verification and coverage

Run the capture on the approved fixed egress. It fetches the Groups, MonitoringSites and MonitoringSiteSpecies JSON and, when requested, samples PM2.5, PM10 and NO₂ for up to three chosen sites across at most seven days. End dates are exclusive in this command. Use an open joint site, a LAQN-only site and a closed or distinct co-located instrument as appropriate, and capture a separate period spanning the October clock change. Keep the unedited JSON and note the source URLs and capture time.

```bash
python -m scripts.erg_laqn.uk_aq_laqn_coverage capture \
  --site MY1 --start 2026-10-24 --end 2026-10-27 --output erg-clock-change.json

python -m scripts.erg_laqn.uk_aq_laqn_coverage capture \
  --site MY1 --start 2026-11-17 --end 2026-11-21 --output erg-recent.json

PGSERVICE=ukaq-test-readonly python -m scripts.erg_laqn.uk_aq_laqn_coverage export-test \
  --output test-ingest-inventory.json

python -m scripts.erg_laqn.uk_aq_laqn_coverage compare \
  --erg erg-recent.json --test test-ingest-inventory.json \
  --membership membership.json --hour-label beginning \
  --history-start 2026-10-24 --history-end 2026-11-21 --output laqn-coverage.json
```

The comparison accepts dated JSON exports from outside Codex Cloud. The `export-test` subcommand uses a single read-only, repeatable-read PostgreSQL transaction with the configured `psql` service or standard libpq settings. Use the same date for the TEST and ERG snapshots where practical. Supply `--hour-label` only after checking the API. Without it the report can use TEST latest-value times and leaves the captured sample out of the recent reporting totals. For a complete reporting comparison, supply externally acquired source samples in the capture format for every eligible site and species, or compare a coverage export after full TEST refresh. The bounded capture alone provides reporting evidence for individual sample sites.

Where the ERG JSON lacks network affiliation, prepare `membership.json` from the dated, publisher-provided LondonAir inventory or site affiliations. Use `source_url`, `checked_on` (ISO date), and a `sites` object keyed by ERG site code whose values are arrays of verbatim published network names. Record joint affiliations explicitly. Review labels such as `mixed` and `LAQNlocality` against the publisher's legend before translating them to a specific network. The report puts unresolved group sites in `unclassified_site_codes`. Review that list and the retained-reference report before accepting coverage. Supplying a newer capture after the catalogue changes can expose additional unresolved sites.

```json
{
  "source_url": "https://www.londonair.org.uk/london/asp/publicdetails.asp?site=MY1",
  "checked_on": "2026-11-21",
  "sites": {"MY1": ["LAQN", "AURN"]}
}
```

The optional `--identity-review` evidence is a JSON object keyed by ERG site code. Each entry needs `checked_on`, `source_url` and `reason`. Use `same_station_ids` or `independent_station_ids` for reviewed TEST station IDs, `independent_laqn_site_codes` for other ERG site codes backed by instrument evidence, or `additional_station: true` for an evidenced new physical station. This input affects the offline report. The runtime resolver accepts the dated Marylebone Road `MY1` / `UKA00315` publisher cross-reference when the latest Defra AURN register and the existing SOS bridge agree and the operating periods overlap. It keeps conflicting assignments and leaves other candidates for review. A candidate within 50 m in the report is a lead for review.

Compare recent and older JSON outputs against the corresponding publisher download or CSV. Check the default API concentration units and gravimetric method, hour label and UTC midnight across BST, exact site/species codes, blank values, genuine zero, unexpectedly repeated instrument rows, invalid sentinels and whether the API exposes the documented download flags `P` and `R`. The downloadable means are documented as period-beginning. The `Data/SiteSpecies` JSON mapping requires the companion capture before setting `hour_label`. If a tested JSON quality field exists, record it as `quality_field` and document its values. Record verified numeric missing sentinels in `invalid_values`. Other pollutants and any separately selectable instrument series require ERG evidence for their units, interval and identifier. [ERG hourly JSON route](https://api.erg.ic.ac.uk/AirQuality/help/operations/GetRawDataSitesSpeciesJSON) · [LondonAir download help](https://www.londonair.org.uk/london/asp/datasite.asp?site=MY1&info=help) · [Defra Marylebone Road](https://uk-air.defra.gov.uk/networks/site-info?site_id=MY1)

The independent evidence available on 21 November 2026 was Defra's Automatic London Network subset, with 14 current sites and 17 all-time sites. The project did not obtain a current full ERG inventory or TEST station/timeseries export for this implementation. Complete LAQN/AURN overlap, additional coverage and recent reporting totals require the inputs above. [Defra Automatic London Network](https://uk-air.defra.gov.uk/networks/network-info?view=aln)

## Apply and deploy after approval

1. Merge the TEST schema PR first and apply its migration to TEST IngestDB, with the existing canonical compact-writer and network-mirror prerequisites in place. Run the IngestDB verifier. Check that `erg_laqn` uses the `laqn` network and `google_cloud_run` backend, that both acquire flags are off, and the public/live-map flags are off.

   ```bash
   psql "$TEST_INGEST_DB_DSN" -X -v ON_ERROR_STOP=1 \
     -f schemas/migrations/20261121_001_ingest_erg_laqn_connector.sql
   psql "$TEST_INGEST_DB_DSN" -X -v ON_ERROR_STOP=1 \
     -f schemas/ingest_db/verify_canonical_ingest_db.sql
   ```

2. Merge the TEST ingest PR and confirm the TEST scheduler config sync registered `uk_aq_erg_laqn` with `enabled=false`. Confirm the deployer and Cloud Run service agent have the permissions to use the selected Direct VPC subnet and that outbound traffic from that subnet uses the ERG-approved fixed Cloud NAT address. The TEST repository's existing `GCP_PROJECT_NUMBER` and `GCP_REGION` must identify the deployed service. Dispatch only the LAQN deployment and check that it reconciles the URL on the disabled D1 row.

   ```bash
   gh workflow run uk_aq_official_networks_cloud_run_deploy.yml \
     --repo TEST-uk-aq/uk-aq-ingest --ref main \
     -f deploy_laqn=true -f laqn_vpc_network="$TEST_LAQN_VPC_NETWORK" \
     -f laqn_vpc_subnet="$TEST_LAQN_VPC_SUBNET"
   ```

3. After reviewing the capture, update the TEST connector's `config` with the verified `membership_evidence` object, when needed, and set `reference_enabled` to true. Keep `poll_enabled` and `networks.ingest_enabled` false. Dispatch `uk_aq_stations_daily.yml` for TEST. Require an eligible catalogue, no unresolved gaps for the accepted scope, valid stations, series and phenomenon mappings, initial and current source metadata, one dated reference snapshot and a successful final ObsAQIDB mirror. Confirm geography enrichment, network/match IDs and repeated discovery. When a refresh omits references, check the retained diagnostics and explicitly resolve partial input before scope acceptance. A failed source refresh fails this Daily Stations stage.

   ```bash
   gh workflow run uk_aq_stations_daily.yml --repo TEST-uk-aq/uk-aq-ingest --ref main
   ```

4. For the approved TEST observation run, keep the D1 scheduler job disabled. Add the verified `hour_label` (`beginning` or `ending`) and any verified `quality_field` or `invalid_values` to `connectors.config`. Enable `networks.ingest_enabled` and `connectors.poll_enabled` for LAQN. Invoke the authenticated service manually in TEST with the existing upstream secret. Use the service URL from the TEST deployment. Run enough batches to cover the eligible catalogue and verify checkpoints and delivery.

   ```bash
   curl --fail-with-body --silent --show-error --max-time 900 \
     -H "x-uk-aq-upstream-auth: $UK_AQ_EDGE_UPSTREAM_SECRET" \
     -H 'Content-Type: application/json' \
     --data '{"trigger_mode":"manual"}' "$TEST_LAQN_SERVICE_URL/run"
   ```

5. The service returns HTTP 207 and a persisted `partial` status when a run completes with some work unfulfilled. Review stations deferred by the time budget, failed or blank channels, publisher rate limits and checkpoint age. Repeated polls prioritise stations with the oldest attempt. A delivered watermark tracks the newest acknowledged value and the station completion time advances after a usable sweep of all eligible channels. Polling re-reads its configured recent overlap, 48 hours by default, and retains the earliest unfinished station window for the next sweep. Retry windows are capped at 72 hours and by the shared writable retention boundary. The run reports windows clipped by those bounds. For an approved recent replay supply `trigger_mode: manual` with explicit UTC `start_time` and `end_time` inside that boundary. Compare changes by connector/series/hour in IngestDB and the matching IDs in ObsAQIDB, and verify retries after an upstream or downstream failure.
6. Once those checks pass and scheduled TEST polling is approved, change only the TEST-owned `jobs.toml` job to `enabled=true` in its own change. Confirm the next automatic run belongs to LAQN, the AURN and Breathe London jobs continue independently, and there is no LAQN Edge scheduler target. Wait for a full catalogue rotation. Inspect both compact writes and the configured ObsAQIDB delivery path. Check consecutive responses for finite numeric revisions, genuine zero, no blank-derived zeros and absent quality staying unknown. Check representative cases for each of PM2.5, PM10 and NO₂. Confirm the real first-publication path in normal TEST history operations.
7. After separate public TEST approval, set the LAQN `public_display_enabled` and `live_map_enabled` network flags and run Daily Stations to mirror them. On the actual map, select AURN only, LAQN only, and both. Verify that AURN wins equal-time ties and the newer eligible value wins otherwise; check that `MY1` is one canonical station when both are selected, its two publisher histories remain addressable, and independent nearby Breathe London devices remain distinct. Historical charts still follow their selected single source. WHO source policy is separate.

## History and rollback

The recent writer submits finite values with source status when supplied and uses the existing IngestDB/ObsAQIDB delivery and canonical Prune path. Blank or invalid values have no reliable retraction meaning in the archived API sample. Preserve the accepted observation and send verified publisher withdrawals and revisions outside the writable window through the existing History Integrity pathway. Long-range ERG acquisition for late October and November 2026 needs a source-specific Ops adapter with dated site/species and method binding, immutable originals, explicit absence/withdrawal evidence and a fixed-v3 applicability contract. Its approved TEST backfill should verify paired AURN/LAQN histories, the complete connector-day scopes, independent instrument identities and selected R2 readback before supplying Health analysis.

For rollback, disable the TEST D1 job in TEST-owned config and verify it in D1. Set `erg_laqn.poll_enabled` and `reference_enabled` to false and turn off the LAQN network's ingest/public/live-map flags as appropriate. Run Daily Stations to mirror visibility. Retain observations, checkpoints, reference snapshots, matches and their IDs for recovery. If necessary, roll the TEST LAQN Cloud Run service back to the last accepted revision or remove its scheduler URL while disabled. The additive schema can stay in place. Keep the regular official-network services on their established rollout and keep this TEST scheduler file environment-owned.

Chat-mode documentation handover should record the two implementation PRs, the fixed-egress discovery and polling split, disabled defaults, affirmative MY1 evidence, shared compact writer with `erg_api` provenance, accepted coverage and quality findings when available, and outstanding ERG History Integrity and public acceptance.
