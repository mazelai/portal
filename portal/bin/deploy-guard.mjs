// This file is the installer template, not an update path.
//
// It carries no KV namespace id, on purpose: one Worker per person, each with their own mailbox,
// provisioned on their own account the first time they deploy. Run `wrangler deploy` against it
// from a clone and wrangler does exactly what the file says - it provisions a NEW, empty mailbox
// and binds it - so a portal that was working comes back up with no card, no contacts, no mail
// and no key. The old namespace is still there, holding everything, bound to nothing.
//
// So a deploy from a person's own machine stops here and is told what to run instead.
//
// The hard part is telling that apart from Deploy to Cloudflare, which is the one place this
// template IS meant to be used, and which runs in Cloudflare's own build container. Checking for
// a CI environment variable alone would mean that the day Cloudflare renames one, this guard
// starts refusing the button and every new install breaks. So the signal is inverted: the guard
// refuses only when it can SEE a local wrangler login - a credential file that exists on a
// person's machine and never in a fresh build container. Anything it cannot positively identify
// as local is allowed through. It fails towards letting a deploy happen, because a wrong refusal
// breaks strangers' installs while a wrong pass costs one person one create-mazel run.
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const ci = process.env.WORKERS_CI || process.env.CLOUDFLARE_WORKERS_CI || process.env.CF_PAGES || process.env.CI;

// Where wrangler keeps an OAuth login on a developer machine.
const credentials = [
  process.env.WRANGLER_HOME && join(process.env.WRANGLER_HOME, "config", "default.toml"),
  join(homedir(), ".wrangler", "config", "default.toml"),
  join(homedir(), "Library", "Preferences", ".wrangler", "config", "default.toml"),
  join(homedir(), ".config", ".wrangler", "config", "default.toml"),
].filter(Boolean);

const localLogin = credentials.some((p) => { try { return existsSync(p); } catch { return false; } });

if (ci || !localLogin) process.exit(0);

console.error(`
  This directory is the installer template, not an update path.

  Deploying it from here provisions a new, empty mailbox and binds your portal to it:
  your card, your contacts, your mail and your key stay in the old one, reachable by nothing.

  To update a portal that already exists, run:

      npx create-mazel

  It finds your portal, keeps the mailbox, the key and every binding, and deploys this code.
  (From a clone of this repo: node portal/create-mazel/bin/create-mazel.js)

  If you genuinely mean to create a brand new portal on this account, pass a config of your own:

      npx wrangler deploy -c my-wrangler.json
`);
process.exit(1);
