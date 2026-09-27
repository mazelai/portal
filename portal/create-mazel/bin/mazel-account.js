#!/usr/bin/env node
// mazel-account: small account chores through the Cloudflare API. Same token rules as create-mazel:
// the token is typed hidden, stays on this machine, and goes only to api.cloudflare.com.
//   node mazel-account.js rename-subdomain <name>   rename the account's workers.dev subdomain
import { stdin, stdout, argv, env, exit } from "node:process";
const API = env.MAZEL_API_BASE || "https://api.cloudflare.com/client/v4";
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
const [cmd, arg] = argv.slice(2);
if (cmd !== "rename-subdomain" || !arg) die("usage: mazel-account.js rename-subdomain <name>");
const token = env.CLOUDFLARE_API_TOKEN || (await askHidden("  Cloudflare API token for the account to change (hidden): "));
const accounts = await cf(token, "/accounts").catch((e) => die(`token rejected: ${e.message}`));
const acc = accounts[0];
const current = await cf(token, `/accounts/${acc.id}/workers/subdomain`).catch(() => ({ subdomain: null }));
say(`  Account: ${acc.name}\n  Current workers.dev name: ${current.subdomain || "(none)"}\n  Requested: ${arg}`);
if (current.subdomain === arg) { say("  Already set. Nothing to do."); exit(0); }
try {
  const r = await cf(token, `/accounts/${acc.id}/workers/subdomain`, { method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify({ subdomain: arg }) });
  say(`  ✓ workers.dev name is now ${r.subdomain || arg}. Every Worker on this account moved to *.${r.subdomain || arg}.workers.dev.`);
} catch (e) { die(`Cloudflare refused "${arg}": ${e.message}`); }
