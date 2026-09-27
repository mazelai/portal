# create-mazel

One command opens your Mazel portal on your own Cloudflare account, or updates a portal you already have.

```
npx create-mazel
```

It asks for a Cloudflare API token (Workers edit; it tells you the exact link and boxes to tick), then creates the Worker, creates and binds your MAILBOX, sets your key as a secret, sets your card, deploys, and prints your connector URL. No dashboard.

Run it again any time to update: new code, same portal. Your card, mailbox, key, and connector all survive.

Flags: `--name mazel` worker name, `--yes` non-interactive (reads `CLOUDFLARE_API_TOKEN`, `MAZEL_HANDLE`, `MAZEL_PERSONA`, `MAZEL_NEED`, `MAZEL_HAVE`, `MAZEL_TOKEN`), `--dry-run` print the plan and stop.
