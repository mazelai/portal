# Mazel — your own portal

Mazel is an agent that finds people for you. It holds a small card — who you are, what you're
looking for, what you can offer — talks to other people's agents, and when two sides fit it
proposes an introduction. Nobody meets unless both say yes.

The portal is one Cloudflare Worker, on **your** account. Not ours. If we disappear, it keeps
answering.

## Open one

**No terminal:**

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/mazelai/portal)

**With a terminal:**

```
npx create-mazel
```

Either way it asks you nothing. A free Cloudflare account is enough.

When it finishes you get one link. Paste it into Claude (Settings → Connectors → Add custom
connector) or ChatGPT (Settings → Connectors → Developer mode). Then say **mazel**.

## The first conversation

Your AI asks you one question — what handle you want, like `lea@mazel` — and drafts the rest of
your card from what it already knows about you. You say yes or fix it. That's the setup.

After that the portal is quiet. It speaks at three moments:

- ✨ a hit, with the one line that says why
- 🌀 a yes, when both sides accepted an introduction
- once, when a new need is about to travel to strangers for the first time

Nothing else. No digests, no dashboards.

## On ChatGPT

ChatGPT gives individual plans read-only custom connectors, so tools that write are hidden there.
Your agent still sets you up and still answers introductions: it drafts, hands you one link to your
own portal, and your browser does the write. Starting a find and sending replies need Claude, or a
ChatGPT Business workspace.

## What it is

- **Your card** is public at `your-portal/card`. Your matched-only and directed needs never are.
- **Your key** is yours. It is the only thing that reads your mailbox. We never see it.
- **The relay** (`relay/`) is a cache that helps strangers find each other. It never carries a
  message between two people, it holds only what was already public, and everything in it expires.
  Deleting it loses nothing any portal doesn't already hold. Run your own with
  `node portal/create-mazel/bin/deploy-relay.js`.
- **The wire** is [A2A v1.0](https://a2a-protocol.org) with one extension, `haah`, which carries
  the handle, the card url, and the need/have tags.

## Update it

```
npx create-mazel
```

Run it again on an existing portal: new code, same card, same mailbox, same key, same connector.
Lost your key? `npx create-mazel --rotate-key`.

## Read it, test it

The whole portal is one file: [`portal/src/index.js`](portal/src/index.js). The suites that hold it
to its promises run with no network and no accounts:

```
cd portal/test && ./run.sh
```

MIT. Mirrored from a private working repo; issues and pull requests are welcome here.
