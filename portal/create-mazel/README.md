# create-mazel

One command opens your Mazel portal on your own Cloudflare account, or updates a portal you already
have. **It asks you nothing.**

```
npx create-mazel
```

Your browser opens once so you can sign in to Cloudflare — a free account is enough. The page will
say *Wrangler*: that is Cloudflare's own deploy tool, whose sign-in this uses. Nothing is stored on
your machine; the access token lives in memory for the length of the run.

Then it creates the Worker, creates and binds your mailbox, sets the 30-minute pulse, deploys, and
prints one link ending in `/mcp?token=…`.

Paste that link into Claude (Settings → Connectors → Add custom connector) or ChatGPT
(Settings → Connectors → Developer mode), and say **mazel**. Your AI asks you one question — what
handle you want, like `lea@mazel` — drafts the rest of your card from what it already knows about
you, shows it to you in plain words, and writes it when you say yes. That is the whole setup.

Run it again any time to update: new code, same portal. Card, mailbox, key and connector all
survive, and there is nothing to re-paste.

## What it is

Mazel is an agent that finds people for you. Your portal holds a small public card — who you are,
what you're looking for, what you can offer — talks to other people's agents, and proposes an
introduction when two sides fit. Nobody meets unless both say yes. The portal runs on **your**
account, not ours.

## Flags

| | |
|---|---|
| `--rotate-key` | new mailbox key; the old one stops working on deploy, so re-add the connector |
| `--name <name>` | Worker name (default `mazel`) |
| `--relay <url>` | the relay to cast to (default the public one; run your own, or pass `none`) |
| `--pulse <cron>` | how often the portal reaches out (default `*/30 * * * *`) |
| `--account <name>` | pick an account when your sign-in covers several |
| `--subdomain <name>` | your workers.dev name, if the account has none yet |
| `--dry-run` | say what would happen, change nothing |

For CI, `--token <api-token>` or `CLOUDFLARE_API_TOKEN` skips the sign-in entirely.

## Also in here

- `node node_modules/create-mazel/bin/deploy-relay.js` — put a Mazel relay on an account of your
  own. It is a cache: deleting it loses nothing any portal doesn't already hold.

Source: [github.com/mazelai/portal](https://github.com/mazelai/portal) · MIT
