# Air Quality England cadence probe

Status: TEST-only exploratory probe.

This directory contains a long-term probe for the public Air Quality England (AQE) latest-data pages. It is intended to measure how quickly and how consistently the AQE stations update before a production-style UK AQ connector is designed.

The probe does **not** ingest observations into UK AQ and does **not** merge AQE station identities with AURN or any other network. AQE remains its own source/network. Any relationship between colocated or equivalent stations belongs in the existing UK AQ station-relationship layer.

## Storage decision

The probe uses a local SQLite database rather than the MacBook Pro MySQL database.

Reasons:

- this is exploratory probe state, not an active UK AQ schema contract;
- SQLite needs no database credentials or MySQL DDL/migration changes;
- there is a single lightweight writer, so SQLite is more than sufficient;
- the database can be queried/exported later if the results need to move into MySQL.

Keep the database and Python virtual environment **outside Dropbox**. The default state path is:

`~/Library/Application Support/UK AQ/aqe-probe/`

The repository contains only the code and requirements.

## Probe design

`aqe_probe.py` downloads `AQE_metadata.RData` and discovers the active AQE site IDs. The metadata is refreshed automatically when the local copy is at least 24 hours old.

Each active site ID is assigned deterministically to one of **60 one-minute shards** using a SHA-256 hash of the site ID. When the scheduled probe runs once per minute, the current shard is selected from epoch time. This means every active AQE station is checked once per hour without sending all requests at once.

With 256 active stations this is an average of about **4.3 station GETs per minute**. Requests within a shard are sequential and have a default 2-second pause between them. A single-instance file lock prevents overlapping probe processes.

The public URL queried is:

`https://www.airqualityengland.co.uk/site/latest?site_id=<SITE_ID>`

For each check the SQLite database records:

- probe time in UTC;
- AQE site ID and deterministic shard;
- HTTP status, elapsed time and response bytes;
- the AQE `last updated` timestamp;
- whether AQE reports current data or `no current data`;
- source age in minutes at the time of the probe;
- number of rows in the Latest Data table;
- SHA-256 of the normalised Latest Data table, so content changes can be detected without storing the full page;
- parse/HTTP errors.

AQE says its displayed monitoring timestamps are **GMT hour ending**, so the displayed clock timestamp is stored as UTC rather than being interpreted as British local time/BST.

Raw HTML is not retained during normal successful probing. A response is saved under the local `failures/` directory only when an HTTP 200 page cannot be parsed, so parser changes can be diagnosed.

## Expected load and storage

At 256 active sites the schedule makes 6,144 station requests per day. The exact transfer volume depends on the size of the AQE latest pages. The probe records `response_bytes` for every request, so the real daily/monthly network cost can be measured after the first sweep. For illustration, 50 KB per page would be about 307 MB/day; 100 KB per page would be about 614 MB/day.

The SQLite database stores compact probe metadata and hashes, not successful HTML pages. At 6,144 probe rows/day, SQLite should remain comfortably manageable for a long-running local experiment. Actual database growth should be measured after the first few days rather than assumed.

## Set up on the MacBook Pro

From the root of the synced `TEST-uk-aq-ingest` repository:

```bash
PROBE_HOME="$HOME/Library/Application Support/UK AQ/aqe-probe"
python3 -m venv "$PROBE_HOME/venv"
"$PROBE_HOME/venv/bin/python" -m pip install --upgrade pip
"$PROBE_HOME/venv/bin/pip" install -r scripts/aqe/requirements.txt
```

The virtual environment is deliberately outside the Dropbox repository.

## Manual validation

First initialise/refresh the AQE metadata and SQLite station inventory:

```bash
"$PROBE_HOME/venv/bin/python" scripts/aqe/aqe_probe.py --metadata-only
```

Show the distribution across the 60 shards:

```bash
"$PROBE_HOME/venv/bin/python" scripts/aqe/aqe_probe.py --inventory
```

Probe a couple of known stations:

```bash
"$PROBE_HOME/venv/bin/python" scripts/aqe/aqe_probe.py --site LHR2 --site T54
```

Then, if those parse correctly, perform one complete baseline sweep:

```bash
"$PROBE_HOME/venv/bin/python" scripts/aqe/aqe_probe.py --all
```

A full manual sweep deliberately uses the 2-second inter-request delay, so it will take several minutes. It is a one-off validation rather than the normal long-term request pattern.

Show a compact database summary afterwards:

```bash
"$PROBE_HOME/venv/bin/python" scripts/aqe/aqe_probe.py --summary
```

The database will be at:

`~/Library/Application Support/UK AQ/aqe-probe/aqe_probe.sqlite3`

## Scheduled mode

When the script is run with no `--all`, `--shard` or `--site` option, it automatically selects the current one-minute shard. The intended long-term scheduler therefore calls:

```bash
"$PROBE_HOME/venv/bin/python" scripts/aqe/aqe_probe.py --quiet
```

once every 60 seconds.

Set up the MacBook Pro launchd job only after the manual two-site and full-sweep checks have been reviewed. The final LaunchAgent should use the Pro's actual synced repository path rather than hard-coding a path from another machine.

## Useful options

- `--site SITE_ID` can be repeated for targeted checks.
- `--all` checks every active station once.
- `--shard 0-59` checks a specific shard.
- `--refresh-metadata` forces a fresh AQE metadata download.
- `--metadata-only` refreshes/synchronises inventory without station GETs.
- `--inventory` shows active station counts by shard.
- `--summary` shows probe count, date range, failures, response bytes and source-age statistics.
- `--delay` changes the pause between requests in a shard.
- `--timeout` changes the HTTP timeout.

## Dependencies

The probe has its own small requirements file:

- `requests`
- `rdata`
- `pandas`

`rdata`/`pandas` are used to read the public AQE OpenAir metadata file. SQLite itself is part of Python's standard library.
