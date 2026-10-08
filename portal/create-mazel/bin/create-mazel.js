#!/usr/bin/env node
// create-mazel: open your Mazel portal, or update it, in one command. It asks nothing.
//
// Sign-in is Cloudflare's own OAuth flow, the one `wrangler login` uses: your browser opens, you
// sign in (or make a free account) and click Allow. Nothing is stored on this machine; the access
// token lives in memory for this run only. Everything personal — your handle, your card — happens
// later, in your first conversation with your own AI, not here.
//
// Run it again on an existing portal to update the code; card, mailbox, key and connector survive.

import { createInterface } from "node:readline/promises";
import { stdin, stdout, argv, env } from "node:process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { randomBytes, createHash } from "node:crypto";
import { createServer } from "node:http";
import { spawn } from "node:child_process";

const HERE = dirname(fileURLToPath(import.meta.url));
const WORKER = join(HERE, "..", "worker", "index.js");
// The endpoint overrides exist for the suite, which stands up a fake Cloudflare on 127.0.0.1.
// They are pinned to the loopback: every cf() call carries a live Cloudflare token, and without
// this pin anything that could set one environment variable could redirect that token to a host
// of its choosing.
const loopbackOnly = (v, fallback) => {
  if (!v) return fallback;
  try {
    const h = new URL(v).hostname;
    if (h === "127.0.0.1" || h === "localhost" || h === "::1") return v;
  } catch { /* fall through */ }
  stdout.write(`  Ignoring ${v}: test endpoints must be on this machine.\n`);
  return fallback;
};
const API = loopbackOnly(env.MAZEL_API_BASE, "https://api.cloudflare.com/client/v4");
const flag = (n) => argv.includes(`--${n}`);
const opt = (n, d) => { const i = argv.indexOf(`--${n}`); return i > -1 && argv[i + 1] ? argv[i + 1] : d; };
const YES = flag("yes"), DRY = flag("dry-run"), ROTATE = flag("rotate-key");
// --store do moves an existing portal onto the storage object; --store kv brings it back. It is
// the only thing that changes where a portal keeps its state, and it is deliberately separate from
// an ordinary update, because the copy and the verify come first (migrate_store).
const STORE = opt("store", "");
const NAME = opt("name", "mazel");
const DEFAULT_RELAY = "https://relay.mazel-peer.workers.dev";
const RELAY = opt("relay", env.MAZEL_RELAY_URL || DEFAULT_RELAY);
const PULSE_CRON = opt("pulse", "*/30 * * * *"); // the pulse: every 30 minutes by default
const say = (s = "") => stdout.write(s + "\n");

// One exit path. Nothing in here calls process.exit: an exit while stdout is a pipe (which is
// what `npx` gives us) drops whatever has not been flushed, which is how the last line of a
// successful install used to disappear. Failures throw, main() catches, the process ends by itself.
class Stop extends Error {}
const die = (s) => { throw new Stop(s); };

let rl;
const ask = async (q, fallback = "") => {
  if (YES || !stdin.isTTY) return fallback;
  rl = rl || createInterface({ input: stdin, output: stdout });
  const a = (await rl.question(q)).trim();
  return a || fallback;
};

async function cf(token, path, init = {}) {
  const res = await fetch(API + path, {
    ...init,
    headers: { authorization: `Bearer ${token}`, ...(init.headers || {}) },
  });
  const text = await res.text();
  let body;
  try { body = JSON.parse(text); } catch { body = { success: false, errors: [{ message: text.slice(0, 300) }] }; }
  if (!body.success) {
    const msg = (body.errors || []).map((e) => `${e.code || ""} ${e.message}`.trim()).join("; ") || `HTTP ${res.status}`;
    const err = new Error(msg); err.status = res.status; err.body = body; throw err;
  }
  return body.result;
}

// ---------------------------------------------------------------------------
// Sign-in: Cloudflare OAuth, PKCE, one browser round trip. This is the same public client
// `wrangler login` uses, so the consent screen says Wrangler; we say so out loud before opening it.
// The endpoints and port are overridable so the suite can stand up a fake Cloudflare and drive
// the whole sign-in for real; nothing but the tests ever sets them.
const OAUTH = {
  client: "54d11594-84e4-41aa-b438-e81b8fa78ee7",
  auth: loopbackOnly(env.MAZEL_OAUTH_AUTH, "https://dash.cloudflare.com/oauth2/auth"),
  token: loopbackOnly(env.MAZEL_OAUTH_TOKEN, "https://dash.cloudflare.com/oauth2/token"),
  port: Number(env.MAZEL_OAUTH_PORT || 8976),
  scopes: ["account:read", "user:read", "workers:write", "workers_kv:write", "workers_scripts:write", "workers_routes:write", "offline_access"],
};
const b64u = (b) => Buffer.from(b).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

function openBrowser(url) {
  if (env.MAZEL_NO_BROWSER === "1") return;
  const cmd = process.platform === "darwin" ? ["open", [url]] : process.platform === "win32" ? ["cmd", ["/c", "start", "", url]] : ["xdg-open", [url]];
  try { spawn(cmd[0], cmd[1], { stdio: "ignore", detached: true }).unref(); } catch { /* the URL is printed too */ }
}

// Waits for Cloudflare to send the browser back here with a code. Resolves it, or explains why not.
// onReady fires once we are actually listening: nobody is sent to the sign-in page before then,
// or a quick browser would come back to a closed door.
function waitForCode(state, onReady) {
  return new Promise((resolve, reject) => {
    // Answer, then take the connection down with us: a browser holding the socket open would
    // otherwise keep this process alive long after the install had finished.
    const done = (res, body, out) => {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8", connection: "close" });
      res.end(`<!doctype html><meta charset=utf-8><title>Mazel</title><body style="font:16px system-ui;padding:3rem;max-width:32rem"><h1>🌀 ${body}</h1><p>You can close this tab and go back to your terminal.</p>`);
      server.close();
      server.closeAllConnections?.();
      out();
    };
    const server = createServer((req, res) => {
      const u = new URL(req.url, `http://127.0.0.1:${OAUTH.port}`);
      if (u.pathname !== "/oauth/callback") { res.writeHead(404); return res.end(); }
      const err = u.searchParams.get("error");
      if (err) return done(res, "Sign-in cancelled.", () => reject(new Stop(`Cloudflare said: ${u.searchParams.get("error_description") || err}`)));
      if (u.searchParams.get("state") !== state) return done(res, "Sign-in could not be verified.", () => reject(new Stop("the sign-in came back with the wrong state; nothing was done")));
      const code = u.searchParams.get("code");
      if (!code) return done(res, "Sign-in came back empty.", () => reject(new Stop("no code came back from Cloudflare")));
      done(res, "Signed in.", () => resolve(code));
    });
    server.on("error", (e) => reject(new Stop(e.code === "EADDRINUSE" ? `port ${OAUTH.port} is busy, which is where Cloudflare sends you back. Close whatever is using it (another wrangler login?) and run this again.` : e.message)));
    server.listen(OAUTH.port, "127.0.0.1", onReady);
    setTimeout(() => { server.close(); reject(new Stop("no answer from the browser after five minutes")); }, 5 * 60_000).unref();
  });
}

async function signIn() {
  const verifier = b64u(randomBytes(48));
  const challenge = b64u(createHash("sha256").update(verifier).digest());
  const state = b64u(randomBytes(16));
  const redirect = `http://localhost:${OAUTH.port}/oauth/callback`;
  const url = `${OAUTH.auth}?${new URLSearchParams({ response_type: "code", client_id: OAUTH.client, redirect_uri: redirect, scope: OAUTH.scopes.join(" "), state, code_challenge: challenge, code_challenge_method: "S256" })}`;

  say("  Opening Cloudflare so you can sign in. A free account is enough.");
  say("  The page will say Wrangler: that is Cloudflare's own deploy tool, whose sign-in this uses.");
  say("  Mazel never sees your password, and nothing is kept on this machine.\n");
  const code = await waitForCode(state, () => {
    say(`  If the browser does not open, paste this:\n\n    ${url}\n`);
    openBrowser(url);
  });

  const res = await fetch(OAUTH.token, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "authorization_code", code, redirect_uri: redirect, client_id: OAUTH.client, code_verifier: verifier }),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok || !body.access_token) die(`Cloudflare would not finish the sign-in: ${body.error_description || body.error || `HTTP ${res.status}`}`);
  return body.access_token;
}

// ---------------------------------------------------------------------------
async function main() {
  say("\n  Mazel: your own portal, on your own account.\n");

  // 1. Who we are talking to Cloudflare as. --token and CLOUDFLARE_API_TOKEN stay for CI.
  const token = opt("token", env.CLOUDFLARE_API_TOKEN || "") || (YES ? die("No token. In CI pass --token or set CLOUDFLARE_API_TOKEN.") : await signIn());

  // 2. Account
  const accounts = await cf(token, "/accounts").catch((e) => die(`Cloudflare turned us away: ${e.message}`));
  if (!accounts.length) die("That sign-in sees no accounts.");
  const wanted = opt("account", "");
  const account = (wanted && accounts.find((a) => a.id === wanted || a.name === wanted)) || accounts[0];
  if (!wanted && accounts.length > 1) say(`  Account: ${account.name}  (you have ${accounts.length}; --account <name> picks another)`);
  else say(`  Account: ${account.name}`);
  const acc = account.id;

  // 3. Existing portal?
  // GET /workers/scripts/{name} returns the raw script, not JSON, so list instead.
  let existing = null;
  try {
    const scripts = await cf(token, `/accounts/${acc}/workers/scripts`);
    existing = (scripts || []).find((w) => w.id === NAME) || null;
  } catch { existing = null; }
  const updating = !!existing;
  say(updating ? `  Found your portal "${NAME}". This updates its code and keeps everything else.` : `  Opening a new portal called "${NAME}".`);

  // 4. workers.dev name. An account's first Worker needs one; we pick a neutral one rather than
  //    ask, since nothing here should need a decision. --subdomain overrides.
  let sub;
  try { sub = (await cf(token, `/accounts/${acc}/workers/subdomain`)).subdomain; } catch { sub = null; }
  if (!sub) {
    let want = opt("subdomain", env.MAZEL_SUBDOMAIN || `mazel-${randomBytes(2).toString("hex")}`);
    for (let tries = 0; tries < 4 && !sub; tries++) {
      try {
        sub = (await cf(token, `/accounts/${acc}/workers/subdomain`, { method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify({ subdomain: want }) })).subdomain || want;
        say(`  workers.dev name: ${sub}`);
      } catch (e) {
        if (tries === 3) say(`  "${want}" did not work (${e.message}).`);
        want = opt("subdomain", "") ? await ask("  That name is taken. Another: ", "") || `mazel-${randomBytes(2).toString("hex")}` : `mazel-${randomBytes(2).toString("hex")}`;
      }
    }
    if (!sub) die("Could not set a workers.dev name. Open https://dash.cloudflare.com once, Workers & Pages, pick one, and run this again.");
  }
  const url = env.MAZEL_PORTAL_BASE || `https://${NAME}.${sub}.workers.dev`;

  if (DRY) {
    say(`\n  Plan: ${updating ? "update" : "create"} ${NAME} on ${account.name}; KV MAILBOX; deploy; pulse ${PULSE_CRON}.\n  Portal would be ${url}\n`);
    return;
  }

  // 5. KV namespace (idempotent)
  const title = `${NAME}-MAILBOX`;
  const spaces = await cf(token, `/accounts/${acc}/storage/kv/namespaces?per_page=100`);
  let ns = spaces.find((n) => n.title === title || n.title === "MAILBOX");
  if (!ns) {
    ns = await cf(token, `/accounts/${acc}/storage/kv/namespaces`, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ title }),
    });
    say("  Mailbox created.");
  } else say("  Mailbox found.");

  // 6. Upload the Worker. A new portal carries no card at all: it opens unclaimed, and the first
  //    conversation with the person's own AI sets the handle and writes the card. An update keeps
  //    whatever the portal already has by inheriting every binding it already carries, so a portal
  //    set up the old way (HANDLE/PERSONA/NEED/HAVE vars) keeps them and nothing is re-typed.
  const code = readFileSync(WORKER, "utf8");
  const bindings = [{ type: "kv_namespace", name: "MAILBOX", namespace_id: ns.id }];
  // The storage object. Every portal gets the binding and the class, on the SQLite backend, which
  // runs on Workers Free: 100,000 rows written a day against KV's 1,000 writes, and 2 MB a value
  // against 128 KiB. The KV namespace stays bound either way, because the published card lives
  // there and because a portal that has not been migrated still reads and writes KV.
  bindings.push({ type: "durable_object_namespace", name: "PORTAL", class_name: "Portal" });
  // Set fresh every time: inherit cannot add a var a portal lacks, and PORTAL_ORIGIN is what lets
  // the scheduled pulse know its own address, since a cron run has no request to read it from.
  bindings.push({ type: "plain_text", name: "RELAY_URL", text: RELAY });
  bindings.push({ type: "plain_text", name: "PORTAL_ORIGIN", text: url });
  let inboxToken = "";
  if (updating) {
    const settings = await cf(token, `/accounts/${acc}/workers/scripts/${NAME}/settings`).catch(() => ({ bindings: [] }));
    const mine = new Set(["MAILBOX", "RELAY_URL", "PORTAL_ORIGIN"]);
    if (STORE) mine.add("STORE");          // --store is the one thing that overrides what it carries
    for (const b of settings.bindings || []) {
      if (mine.has(b.name)) continue;
      if (ROTATE && b.name === "INBOX_TOKEN") continue;
      bindings.push({ type: "inherit", name: b.name });
    }
    if (ROTATE) {
      // New key; the old one stops working on deploy, so the connector has to be added again.
      inboxToken = env.MAZEL_TOKEN || randomBytes(32).toString("hex");
      bindings.push({ type: "secret_text", name: "INBOX_TOKEN", text: inboxToken });
    }
    if (STORE === "do") {
      bindings.push({ type: "plain_text", name: "STORE", text: "do" });
      say("  Storage: this portal will read and write its storage object from this deploy on. Its KV copy is left exactly as it is.");
    } else if (STORE === "kv") {
      say("  Storage: this portal goes back to KV from this deploy on. Anything written to the object since the move is only in the object; copy it back first with migrate_store direction to-kv.");
    } else if (STORE) {
      die(`--store takes do or kv, not ${STORE}`);
    }
    // STORE is deliberately not in `mine` unless --store said so: a portal already migrated keeps
    // STORE=do by inheriting it, and one still on KV keeps nothing, so an ordinary update never
    // moves anybody's storage.
  } else {
    inboxToken = env.MAZEL_TOKEN || randomBytes(32).toString("hex");
    bindings.push({ type: "secret_text", name: "INBOX_TOKEN", text: inboxToken });
    // A new portal has nothing to migrate, so it starts in the object and never touches the KV
    // write ceiling. An existing portal is left where it is.
    bindings.push({ type: "plain_text", name: "STORE", text: "do" });
  }
  // The class has to be declared as a new SQLite class the first time this script carries it. A
  // script that already has the migration applied is handed the same tag and Cloudflare refuses it,
  // so the refusal is caught and the deploy goes again without it: the class is already there.
  const metadata = { main_module: "index.js", compatibility_date: "2026-01-01", bindings,
    migrations: { new_tag: "do-v1", new_sqlite_classes: ["Portal"] } };
  const upload = async (meta) => {
    const form = new FormData();
    form.set("metadata", new Blob([JSON.stringify(meta)], { type: "application/json" }));
    form.set("index.js", new Blob([code], { type: "application/javascript+module" }), "index.js");
    return cf(token, `/accounts/${acc}/workers/scripts/${NAME}`, { method: "PUT", body: form });
  };
  await upload(metadata).catch(async (e) => {
    if (!/migration/i.test(e.message || "")) die(`Deploy failed: ${e.message}`);
    const { migrations, ...rest } = metadata;
    await upload(rest).catch((e2) => die(`Deploy failed: ${e2.message}`));
  });
  say("  Code deployed.");

  // 7. The pulse: a cron trigger, every 30 minutes by default.
  await cf(token, `/accounts/${acc}/workers/scripts/${NAME}/schedules`, {
    method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify([{ cron: PULSE_CRON }]),
  }).then(() => say(`  Pulse set: ${PULSE_CRON}.`)).catch((e) => say(`  Pulse not set (${e.message}); add a Cron Trigger of ${PULSE_CRON} in the dashboard if you want it.`));

  // 8. Make sure it is reachable on workers.dev
  await cf(token, `/accounts/${acc}/workers/scripts/${NAME}/subdomain`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ enabled: true }),
  }).catch(() => {});

  say(`\n  🌀 Your portal is ${updating ? (ROTATE ? "updated with a new key" : "updated") : "open"}.\n`);
  if (inboxToken) {
    say("  Paste this into your AI:\n");
    say(`    ${url}/mcp?token=${inboxToken}\n`);
    say("    Claude:  Settings, Connectors, Add custom connector, paste the link.");
    say("    ChatGPT: Settings, Connectors, Developer mode, paste the link.\n");
    say('  Then say "mazel". It asks you one question and writes your card with you.');
    if (ROTATE) say("  The old key stopped working just now: remove the old Mazel connector and add this one.");
    else say(`  If you lose the link it is on ${url} for the first hour, then run this again with --rotate-key.`);
  } else {
    say("  Your card, mailbox, key and connector are unchanged: nothing to re-paste.");
  }

  // Verify last, and patiently: a brand-new workers.dev name can take a minute to resolve.
  let live = false;
  for (let i = 0; i < 60 && !live; i++) {
    try { live = (await fetch(`${url}/card`)).ok; } catch { /* not up yet */ }
    if (!live) await new Promise((r) => setTimeout(r, 1500));
  }
  say(live ? `\n  Verified: ${url} is answering.\n` : `\n  Not answering yet at ${url}; new names can take a minute or two. Everything above is still correct.\n`);
}

main()
  .catch((e) => { say("\n✗ " + (e instanceof Stop ? e.message : e.message || String(e))); process.exitCode = 1; })
  .finally(() => { if (rl) rl.close(); });
