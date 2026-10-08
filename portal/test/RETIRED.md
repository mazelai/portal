# Retired review scripts (2026-09-30, the reduction)

Eight of the forty-seven reviewer scripts tested a path the reduction removed. Their subject no
longer exists, so they are gone rather than kept failing on a precondition. Each is in git history
at the commit before this note.

| Script | Subject | Why moot |
|---|---|---|
| `sec-review-2026-09-28c/02-gossip-impostor-takes-a-directed-need.mjs` | an impostor answering a need on the gossip hop | gossip forwarding is gone; a need travels only as the caster's own signed `thread.open` |
| `sec-review-2026-09-28c/02-stranger-under-a-real-handle-relay-and-gossip.mjs` | a stranger under a real handle, via relay and via gossip | the gossip half is gone; the relay half is `28c/01-relay-impostor` and `29e/02`, which stand |
| `sec-review-2026-09-28c/05-matched-only-fingerprint-goes-to-a-stranger.mjs` | a stranger's gossip hit pulling a held need's buckets into a branch | `find.hit` is now only the blind answer from a card this portal holds; strangers cannot hit |
| `sec-review-2026-09-29e/01-router-names-a-third-party-who-never-asked.mjs` | a router's proposal naming a third party | `router` is off the wire; a proposal carrying it no longer verifies |
| `sec-review-2026-09-29e/03-blind-hit-reveals-a-held-need-to-a-relay-chosen-door.mjs` | a relay-signed blind hit naming a door | the relay never sees buckets and never signs a hit |
| `sec-review-2026-09-29e/04-router-names-a-third-partys-door-on-thread-open.mjs` | a router-kind `thread.open` naming a door | `thread.open` has one kind, a need, and names no door |
| `sec-review-2026-09-29f/01-router-plants-a-message-under-any-handle.mjs` | a router-kind `thread.open` planting message one | as above |
| `sec-review-2026-09-29f/08-relay-blind-hit-for-a-need-never-blind-cast-to-the-relay.mjs` | a relay blind hit for a need never cast to it | the relay never sees buckets |

What each of these protected is still held by a check in `sec-thread-test.mjs`: a proposal carrying
a `router` object is refused at the door; a `thread.open` naming anyone but the sender and this
portal opens nothing; a hit signed by anything but a card this portal holds is refused; a held
need's buckets go only to the circle the owner drew by hand.

The suites that tested the removed paths were rewritten to the model that stands, in the same
commit: `fly-test` (gossip and relay push → the relay is a cache a portal asks), `relay-test`
(subscriptions → none), `ghosts-test` (a stranger's ask on a gossip hop → returns with §12.3
item 11), `thread-test` (push configs → none stored; `thread.lapsed` on the wire → each side's own
clock), `a2a-conformance` (push configs answer, nothing kept), `seed-test` (`BLIND_TO_RELAY` → the
relay never holds buckets), `moved-test` (a gossip hit → a held card refreshed), `install-test`
(`carriers` → gone; the hidden list is eight), `sec-test` (push configs, tool names).
