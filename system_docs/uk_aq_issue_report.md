# UK Air Quality Networks — Issue Review & Fix Options

This report consolidates the two previously delivered outputs:
1) The in‑depth issue review with severity and suggested fixes.
2) The priority‑ordered list with filenames and citations.

Each issue below includes **fix options with pros/cons**. If only one viable fix exists, it is called out explicitly.

---

## 1) Public edge endpoint uses service‑role key with open CORS (High)
**File:** `supabase/functions/uk_aq_latest/index.ts`【F:supabase/functions/uk_aq_latest/index.ts†L8-L19】【F:supabase/functions/uk_aq_latest/index.ts†L25-L31】【F:supabase/functions/uk_aq_latest/index.ts†L60-L68】
**Related:** `supabase/functions/uk_aq_bristol_latest/index.ts`

**Issue:** The `uk_aq_latest` edge function uses the Supabase **service‑role** key (bypassing RLS) and allows `Access-Control-Allow-Origin: *` for a public GET endpoint. This allows any origin to read privileged data.

**Fix Options:**
1) **Use anon key + enforce RLS on the tables queried**
   - **Pros:** Preserves public access while limiting data to RLS‑approved rows; aligns with Supabase best practices for public APIs.
   - **Cons:** Requires RLS policies and potentially updated client expectations.
2) **Require authentication (JWT or Supabase auth) and restrict CORS**
   - **Pros:** Stronger security; limits access to authenticated callers and trusted origins.
   - **Cons:** Requires client auth integration and managing token lifecycle.
3) **Keep service‑role but restrict network access (CORS + edge protection + internal use only)**
   - **Pros:** Minimal code changes; preserves privileged access for internal services.
   - **Cons:** Easy to misconfigure; still risky if endpoint is exposed or CORS mis‑set.

---

## 2) Hard‑coded PurpleAir API key file path (High)
**File:** `scripts/purpleair/purpleair_get_uk_sensors.py`【F:scripts/purpleair/purpleair_get_uk_sensors.py†L32-L78】

**Issue:** The script loads the API key from a developer‑specific absolute path, and exits if it is missing. This breaks on any other machine or CI environment.

**Fix Options:**
1) **Read API key from env var (e.g., `PURPLEAIR_API_KEY`) and/or CLI flag**
   - **Pros:** Portable, secure, and CI‑friendly; aligns with 12‑factor app practices.
   - **Cons:** Requires setting an env var or adding CLI arguments in run scripts.
2) **Fallback to a configurable file path (e.g., `--api-key-file`)**
   - **Pros:** Supports local developer convenience without hardcoding.
   - **Cons:** Still requires file presence; not as clean for automation.

---

## 3) Supabase client created without validating required env vars (Medium)
**File:** `scripts/purpleair/purpleair_get_uk_sensors.py`【F:scripts/purpleair/purpleair_get_uk_sensors.py†L44-L49】

**Issue:** `SUPABASE_URL` and `SB_SECRET_KEY` are read, but the script calls `create_client()` without checking for missing values.

**Fix Options:**
1) **Validate env vars and exit with a clear error before creating the client**
   - **Pros:** Immediate, actionable error messages; prevents ambiguous runtime failures.
   - **Cons:** Minimal; adds small amount of validation code.

---

## 4) No timeouts for Supabase/PostgREST or Dropbox fetches in edge ingestion (Medium)
**File:** `supabase/functions/ingest_sos/index.ts`【F:supabase/functions/ingest_sos/index.ts†L97-L117】【F:supabase/functions/ingest_sos/index.ts†L808-L839】

**Issue:** `fetch()` calls to Supabase and Dropbox are made without timeouts. If either stalls, the edge function can hang until platform timeout.

**Fix Options:**
1) **Add `AbortController` timeouts to PostgREST and Dropbox fetches**
   - **Pros:** Predictable failure behavior; avoids long hangs.
   - **Cons:** Requires small refactor to pass `signal` and handle abort errors.
2) **Wrap fetch calls in a retry wrapper with per‑attempt timeouts**
   - **Pros:** More resilient to transient outages.
   - **Cons:** More complexity; careful tuning required to avoid extra latency.

---

## 5) PurpleAir API calls have no timeout (Medium)
**File:** `scripts/purpleair/purpleair_get_uk_sensors.py`【F:scripts/purpleair/purpleair_get_uk_sensors.py†L80-L87】

**Issue:** HTTP calls to PurpleAir are made without a timeout, which can cause the script to hang indefinitely.

**Fix Options:**
1) **Add a timeout to `requests.get()` (e.g., `timeout=30`)**
   - **Pros:** Simple, avoids indefinite hangs.
   - **Cons:** Needs a sensible default that won’t fail slow responses.
2) **Implement retry/backoff for 429/5xx with bounded timeouts**
   - **Pros:** Better resilience during throttling or outages.
   - **Cons:** Slightly more code and runtime complexity.

---

## 6) Observation inserts are append‑only, leading to duplicates (Medium)
**File:** `scripts/purpleair/purpleair_get_uk_sensors.py`【F:scripts/purpleair/purpleair_get_uk_sensors.py†L222-L260】

**Issue:** Observations are inserted without deduplication or conflict handling, so repeated runs create duplicates.

**Fix Options:**
1) **Use `upsert` with a unique constraint on (`sensor_index`, `observed_at`)**
   - **Pros:** Prevents duplicates while keeping the data model consistent.
   - **Cons:** Requires a DB schema change (unique index).
2) **Add a pre‑insert query to skip existing records**
   - **Pros:** No schema changes required.
   - **Cons:** Extra read queries; still a race condition without a unique constraint.

---

## Priority Order (Most Severe ➜ Least Severe)
1) Public edge endpoint uses service‑role key with open CORS — `supabase/functions/uk_aq_latest/index.ts` and `supabase/functions/uk_aq_bristol_latest/index.ts`【F:supabase/functions/uk_aq_latest/index.ts†L8-L19】【F:supabase/functions/uk_aq_latest/index.ts†L25-L31】【F:supabase/functions/uk_aq_latest/index.ts†L60-L68】
2) Hard‑coded PurpleAir API key file path — `scripts/purpleair/purpleair_get_uk_sensors.py`【F:scripts/purpleair/purpleair_get_uk_sensors.py†L32-L78】
3) Supabase client created without validating required env vars — `scripts/purpleair/purpleair_get_uk_sensors.py`【F:scripts/purpleair/purpleair_get_uk_sensors.py†L44-L49】
4) No timeouts for Supabase/PostgREST or Dropbox fetches in edge ingestion — `supabase/functions/ingest_sos/index.ts`【F:supabase/functions/ingest_sos/index.ts†L97-L117】【F:supabase/functions/ingest_sos/index.ts†L808-L839】
5) PurpleAir API calls have no timeout — `scripts/purpleair/purpleair_get_uk_sensors.py`【F:scripts/purpleair/purpleair_get_uk_sensors.py†L80-L87】
6) Observation inserts are append‑only, leading to duplicates — `scripts/purpleair/purpleair_get_uk_sensors.py`【F:scripts/purpleair/purpleair_get_uk_sensors.py†L222-L260】
