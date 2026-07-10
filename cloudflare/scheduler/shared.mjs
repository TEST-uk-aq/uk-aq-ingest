export async function readSecret(value) {
  if (typeof value === "string") {
    return value;
  }
  if (value && typeof value === "object") {
    if (typeof value.get === "function") {
      const resolved = await value.get();
      return typeof resolved === "string" ? resolved : String(resolved ?? "");
    }
    if (typeof value.then === "function") {
      const resolved = await value;
      return typeof resolved === "string" ? resolved : String(resolved ?? "");
    }
  }
  return value ? String(value) : "";
}

export function nowIso(nowMs = Date.now()) {
  return new Date(nowMs).toISOString();
}

export function logJson(workerName, event, payload = {}) {
  console.log(JSON.stringify({
    worker: workerName,
    event,
    timestamp: nowIso(),
    ...payload,
  }));
}

export function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}
