import { createServer } from "node:http";
import { spawn } from "node:child_process";

const PORT = Number(process.env.PORT || "8080");
const RUN_SCRIPT = "/app/index.mjs";
const ALLOWED_TRIGGER_MODES = new Set(["safety", "task", "manual"]);

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk.toString("utf8");
    });
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}

function resolveTriggerMode(req, bodyText) {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
    const queryMode = (url.searchParams.get("trigger_mode") || "").trim().toLowerCase();
    if (ALLOWED_TRIGGER_MODES.has(queryMode)) {
      return queryMode;
    }
  } catch {
    // fall through
  }

  const headerMode = (req.headers["x-scomm-trigger-mode"] || "")
    .toString()
    .trim()
    .toLowerCase();
  if (ALLOWED_TRIGGER_MODES.has(headerMode)) {
    return headerMode;
  }

  if (bodyText && bodyText.trim()) {
    try {
      const payload = JSON.parse(bodyText);
      const bodyMode = (payload?.trigger_mode || "").toString().trim().toLowerCase();
      if (ALLOWED_TRIGGER_MODES.has(bodyMode)) {
        return bodyMode;
      }
    } catch {
      // ignore invalid JSON body
    }
  }

  return "manual";
}

function runWorker(triggerMode) {
  return new Promise((resolve) => {
    const child = spawn("node", [RUN_SCRIPT], {
      stdio: "inherit",
      env: {
        ...process.env,
        SCOMM_TRIGGER_MODE: triggerMode,
      },
    });
    child.on("close", (code) => {
      resolve(Number(code || 0));
    });
  });
}

const server = createServer(async (req, res) => {
  if (req.method === "GET") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, service: "uk_aq_sensorcommunity_cloud_run" }));
    return;
  }

  if (req.method !== "POST") {
    res.writeHead(405, { "content-type": "text/plain" });
    res.end("Method not allowed");
    return;
  }

  const bodyText = await readBody(req).catch(() => "");
  const triggerMode = resolveTriggerMode(req, bodyText);
  const code = await runWorker(triggerMode);
  const ok = code === 0;

  res.writeHead(ok ? 200 : 500, { "content-type": "application/json" });
  res.end(JSON.stringify({ ok, code, trigger_mode: triggerMode }));
});

server.listen(PORT, () => {
  console.log(`Listening on http://localhost:${PORT}/`);
});
