# OpenAQ null observation history failure

## Incident

OpenAQ is connector `6`. On LIVE, the connector wrote observation rows with a
`NULL` value into both IngestDB and ObsAQIDB for timeseries `3201` and `3203`:

| Date | NULL rows in each database |
| --- | ---: |
| 09/09/2026 | 8 |
| 10/09/2026 | 16 |
| 11/09/2026 | 4 |
| 12/09/2026 | 12 |
| **Total** | **40** |

The 40 affected rows were manually removed from both LIVE databases. No cleanup
SQL or migration is required.

Before cleanup, Prune Daily Phase B failed connector 6 with:

> value must be a finite IEEE-754 binary64 number

After cleanup, the LIVE recovery run successfully completed connector 6 for
09–11 September with:

```text
completed_candidates=3
failed_candidates=0
blocked_days=0
total_written_rows=56186
```

The same run successfully deleted the retained 56,186 connector 6 rows from
IngestDB. The 12 September partition was not yet Phase-B eligible during that
run, but its NULL rows were also removed.

## Observation-value contract

Canonical R2 observation history requires every observation value to be a
finite IEEE-754 binary64 number. `NULL` is not a valid observation value, and
history publication must continue to reject it.

For OpenAQ, a missing or non-finite measurement represents the absence of an
observation. Such a record must not create an observation row or advance
valid-observation checkpoint/cadence state. A genuine numeric value of `0` is a
valid observation and must continue to be stored.

The OpenAQ hourly value precedence remains `summary.avg`, `summary.median`,
`summary.q50`, then `record.value`. Each candidate is eligible only when it is
already a finite number; values are not coerced, so `Number(null)` cannot turn a
missing measurement into zero. A timestamp is considered returned for hourly
gap detection only when its record also contains a valid observation value.
