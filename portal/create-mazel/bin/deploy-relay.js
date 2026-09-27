#!/usr/bin/env node
// deploy-relay: put the Mazel relay on a Cloudflare account in one command. Same token rules as
// create-mazel (typed hidden, stays on this machine, goes only to api.cloudflare.com).
// Creates the Worker "relay" and its RELAY KV namespace, deploys, enables workers.dev, verifies.
// Run again to update: the KV is found by title and kept. It is a cache: deleting it loses nothing.
import { stdin, stdout, argv, env, exit } from "node:process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const HERE = dirname(fileURLToPath(import.meta.url));
const CODE = join(HERE, "..", "worker", "relay.js");
const API = env.MAZEL_API_BASE || "https://api.cloudflare.com/client/v4";
const NAME = (() => { const i = argv.indexOf("--name"); return i > -1 && argv[i + 1] ? argv[i + 1] : "relay"; })();
const say = (s = "") => stdout.write(s + "\n");
const die = (s) => { say("\n✗ " + s); exit(1); };

const askHidden = (q) => new Promise((resolve) => {
  stdout.write(q);
  const wasRaw = stdin.isRaw;
  if (stdin.isTTY) stdin.setRawMode(true);
  stdin.resume();
  let buf = "";
  const onData = (ch) => {
    for (const k of ch.toString("utf8")) {
      if (k === "\r" || k === "\n") { stdin.removeListener("data", onData); if (stdin.isTTY) stdin.setRawMode(!!wasRaw); stdin.pause(); stdout.write("\n"); return resolve(buf.trim()); }
      if (k === "") { stdout.write("\n"); exit(1); }
      if (k === "" || k === "\b") buf = buf.slice(0, -1); else buf += k;
    }
  };
  stdin.on("data", onData);
});

async function cf(token, path, init = {}) {
  const res = await fetch(API + path, { ...init, headers: { authorization: `Bearer ${token}`, ...(init.headers || {}) } });
  const body = await res.json().catch(() => ({ success: false, errors: [{ message: `HTTP ${res.status}` }] }));
  if (!body.success) throw new Error((body.errors || []).map((e) => e.message).join("; ") || `HTTP ${res.status}`);
  return body.result;
}

say("\n  Mazel relay. It is a cache: deleting it loses nothing any portal doesn't hold.\n");
const token = env.CLOUDFLARE_API_TOKEN || (await askHidden("  Cloudflare API token for the hosting account (hidden): "));
const accounts = await cf(token, "/accounts").catch((e) => die(`token rejected: ${e.message}`));
const acc = accounts[0].id;
say(`  Account: ${accounts[0].name}`);

let sub;
try { sub = (await cf(token, `/accounts/${acc}/workers/subdomain`)).subdomain; } catch { sub = null; }
if (!sub) die("This account has no workers.dev name yet; run create-mazel once on it first.");

const title = `${NAME}-RELAY`;
const spaces = await cf(token, `/accounts/${acc}/storage/kv/namespaces?per_page=100`);
let ns = spaces.find((n) => n.title === title);
if (!ns) {
  ns = await cf(token, `/accounts/${acc}/storage/kv/namespaces`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ title }) });
  say("  Cache store created.");
} else say("  Cache store found.");

const code = readFileSync(CODE, "utf8");
const metadata = { main_module: "index.js", compatibility_date: "2026-01-01", bindings: [{ type: "kv_namespace", name: "RELAY", namespace_id: ns.id }] };
const form = new FormData();
form.set("metadata", new Blob([JSON.stringify(metadata)], { type: "application/json" }));
form.set("index.js", new Blob([code], { type: "application/javascript+module" }), "index.js");
await cf(token, `/accounts/${acc}/workers/scripts/${NAME}`, { method: "PUT", body: form }).catch((e) => die(`deploy failed: ${e.message}`));
await cf(token, `/accounts/${acc}/workers/scripts/${NAME}/subdomain`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ enabled: true }) }).catch(() => {});

const url = `https://${NAME}.${sub}.workers.dev`;
say(`  Deployed: ${url}`);
let info = null;
for (let i = 0; i < 60 && !info; i++) {
  try { const r = await fetch(`${url}/.well-known/relay.json`); if (r.ok) info = await r.json(); } catch {}
  if (!info) await new Promise((r) => setTimeout(r, 1500));
}
say(info
  ? `  Verified: relay ${info.version}, key ${info.keyId}.\n  Portals point at it with RELAY_URL=${url} (create-mazel sets this by default).\n`
  : `  Not answering yet at ${url}; new names can take a minute.\n`);
