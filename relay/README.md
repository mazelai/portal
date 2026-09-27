# Mazel relay

**It is a cache: deleting it loses nothing any portal doesn't hold.**

The relay is the Fly stage's public carrier. It runs on the Mazel company account and never carries a message between two people (spec §7.5, the cache principle). What it does:

- **`POST /cast`** — a portal casts a signed public need (or its public card). Verified against the key inside it; stored 7 days. Only public-tier material; matched-only and directed needs never leave a portal.
- **`GET /search?q=&tags=`** — the same paraphrase scorer the portal uses, run over cached casts. Results are strangers' cards, which land in the searching portal as world-tier (🌍) known cards. The intro then goes portal to portal on the A2A wire, as always.
- **`POST /subscribe`** — a portal registers an A2A push config (signed). When a new cast fits the subscriber's haves, the relay POSTs a signed `find.hit` to the subscriber's door. 30 days, re-registered by the pulse.
- **`POST /publish`** and **`GET /.well-known/mazel/<name>.json`** — the `name@mazel` directory. A portal publishes its signed handle record (public key, card URL, timestamp, rotation chain); the directory serves it. A name already on file only changes keys through a valid rotation chain signed by the old key and countersigned by the new.
- **`GET /.well-known/relay.json`** — the relay's own public key, so a portal can verify a `find.hit` came from the relay it subscribed to.

**Hosting rule:** a Worker cannot fetch another Worker on its own Cloudflare account over workers.dev (error 1042), so the relay must never share an account with any portal it serves. The company relay serves portals on other accounts; a test peer has to live on its own account.

No accounts. A signature is the only auth. TTL on every record. The scorer and the signing helpers are lifted verbatim from `portal/src/index.js` at build (`./build.sh`); a test asserts the copies are identical.

Deploy: `node portal/create-mazel/bin/deploy-relay.js` with a Workers-edit token for the account that should host it. Address: `https://relay.<subdomain>.workers.dev`; portals point at it with the `RELAY_URL` var.
