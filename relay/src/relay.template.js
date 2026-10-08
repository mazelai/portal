// Mazel relay: a public cache for the Fly stage. Runs on the company account.
//
// It is a cache: deleting it loses nothing any portal doesn't hold.
//
// It never carries a message between two people (spec §7.5, the cache principle). It caches signed public
// casts and cards, answers searches over them, and keeps the name@mazel directory from signed
// records portals publish. It never pushes anything to anyone: every intro, reply and connect goes
// portal to portal on the A2A wire, and a portal finds what is cached by asking.
//
// No accounts. A signature is the only auth. TTL on every record.
//
// POST /cast            signed public need-cast or card-cast          (7-day TTL)
// GET  /search?q=&tags= the portal's paraphrase scorer over cached casts
// POST /publish         signed handle record or rotation record -> the directory  (90-day TTL)
// GET  /.well-known/mazel/<name>.json   the directory record for name@mazel
// GET  /.well-known/relay.json          this relay's public key
// GET  /                the sentence above

const RELAY_VERSION = "@@VERSION@@";   // stamped by relay/build.mjs from the portal
const HAAH_URI = "https://mazel.ai/ext/haah/v1";
const CAST_TTL = 60 * 60 * 24 * 7;
const DIR_TTL = 60 * 60 * 24 * 90;
const MAX_RESULTS = 10;
const SENTENCE = "It is a cache: deleting it loses nothing any portal doesn't hold.";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const fwd = (request.headers.get("x-forwarded-proto") || "").split(",")[0].trim();
    const origin = fwd === "https" || fwd === "http" ? `${fwd}://${url.host}` : url.origin;
    const p = url.pathname;
    try {
      if (p === "/" && request.method === "GET") {
        return text(`Mazel relay ${RELAY_VERSION}. ${SENTENCE}\nCasts: POST /cast   Search: GET /search?q=   Directory: GET /.well-known/mazel/<name>.json\n`);
      }
      if (p === "/.well-known/relay.json" && request.method === "GET") {
        const s = await getSigning(env);
        return json({ relay: origin, version: RELAY_VERSION, publicKey: s.pub, keyId: s.kid, sentence: SENTENCE });
      }
      const rec = p.match(/^\/\.well-known\/mazel\/([a-z0-9][a-z0-9._-]*)\.json$/);
      if (rec && request.method === "GET") return await directoryGet(env, rec[1].toLowerCase());
      if (p === "/publish" && request.method === "POST") return await publish(env, await bounded(request));
      if (p === "/cast" && request.method === "POST") return await cast(env, origin, await bounded(request));
      if (p === "/search" && request.method === "GET") return await search(env, url);
      return json({ error: "not found" }, 404);
    } catch (e) {
      return json({ error: e.message || String(e) }, 400);
    }
  },
};

// ---------------------------------------------------------------------------
// Storage helpers
// KV list() returns at most 1000 keys and a cursor. Reading one page and stopping meant that once
// the cache held more than a thousand casts, search silently only ever saw the first thousand.
//
// Listing at all is the expensive part. A free plan allows 1,000 list operations a day, and a cache
// that scans its own namespace to answer a search spends that budget on being read: the company
// account hit the ceiling on 2026-10-05. So the keys are kept in one index, written when a cast is
// written and read when a search is answered. A list happens once in a namespace's life, to build
// the index for a cache that predates it, and never again.
const MAX_SCAN = 5000;
const IDX_CASTS = "idx:casts";
async function kvList(env, prefix, cap = MAX_SCAN) {
  const out = [];
  let cursor;
  do {
    const list = await env.RELAY.list(cursor ? { prefix, cursor } : { prefix });
    for (const k of list.keys) {
      const v = await env.RELAY.get(k.name);
      if (v) out.push({ key: k.name, ...JSON.parse(v) });
      if (out.length >= cap) return out;
    }
    cursor = list.list_complete === false ? list.cursor : null;
  } while (cursor);
  return out;
}

// The index: every cast key, with the time it was written, so a search reads one key instead of
// scanning. Entries past the cast TTL are dropped as it is rewritten, which is also what keeps it
// from growing without end. It carries no TTL of its own: it is the map, not the territory.
async function castIndex(env) {
  const raw = await env.RELAY.get(IDX_CASTS);
  if (raw) { try { const i = JSON.parse(raw); if (Array.isArray(i.keys)) return i; } catch { /* rebuilt below */ } }
  // Cold: a cache that was filled before the index existed. Built once, from the only list in here.
  const keys = [];
  let cursor;
  do {
    const page = await env.RELAY.list(cursor ? { prefix: "cast:", cursor } : { prefix: "cast:" });
    for (const k of page.keys) keys.push([k.name, Date.now()]);
    cursor = page.list_complete === false ? page.cursor : null;
  } while (cursor && keys.length < MAX_SCAN);
  const idx = { v: 1, keys, updated: new Date().toISOString() };
  await env.RELAY.put(IDX_CASTS, JSON.stringify(idx));
  return idx;
}
async function indexCast(env, key) {
  const idx = await castIndex(env);
  const cut = Date.now() - CAST_TTL * 1000;
  const keys = idx.keys.filter(([k, at]) => k !== key && at > cut).slice(-MAX_SCAN + 1);
  keys.push([key, Date.now()]);
  await env.RELAY.put(IDX_CASTS, JSON.stringify({ v: 1, keys, updated: new Date().toISOString() }));
}

// Ceilings for anything an anonymous caller can put in the cache. Without them one request stores
// as much as it likes, and every later search pays to read it.
const MAX_CAST_BYTES = 16 * 1024;
// Every body this cache accepts has a ceiling before it is even parsed.
async function bounded(request) {
  if (Number(request.headers.get("content-length") || 0) > MAX_CAST_BYTES) throw new Error(`body over ${MAX_CAST_BYTES} bytes`);
  const raw = await request.text();
  if (raw.length > MAX_CAST_BYTES) throw new Error(`body over ${MAX_CAST_BYTES} bytes`);
  return JSON.parse(raw);
}
const CAST_FRESH_MS = 1000 * 60 * 60 * 24 * 2;
const fresh = (at) => {
  const t = Date.parse(at || "");
  if (!Number.isFinite(t)) return false;
  const age = Date.now() - t;
  return age > -60000 && age < CAST_FRESH_MS;
};
const MAX_TAGS_IN = 12;
const MAX_GLOSSES = 12;
const MAX_GLOSS_LEN = 200;
// Per key, per day: nothing bounded how many casts, so one anonymous key could fill the cache all
// day at the operator's cost.
const MAX_CASTS_PER_KEY_PER_DAY = 100;
async function underCap(env, what, cap) {
  const key = `cap:${what}:${new Date().toISOString().slice(0, 10)}`;
  const n = Number((await env.RELAY.get(key)) || 0);
  if (n >= cap) return false;
  await env.RELAY.put(key, String(n + 1), { expirationTtl: 60 * 60 * 36 });
  return true;
}
const capTags = (v) => (Array.isArray(v) ? v : []).map(normalizeTag).filter(Boolean).slice(0, MAX_TAGS_IN);
const capGlosses = (g) => {
  const out = {};
  if (!g || typeof g !== "object") return out;
  for (const k of Object.keys(g).slice(0, MAX_GLOSSES)) {
    const tag = normalizeTag(k);
    if (tag) out[tag] = String(g[k] == null ? "" : g[k]).slice(0, MAX_GLOSS_LEN);
  }
  return out;
};

// A name in this directory is ASCII, and the separators people use to make a lookalike are folded
// away: lea, l.e.a and le-a are one name, and anything outside a-z0-9 is refused outright rather
// than left to render as a homoglyph in a line of chat.
function dirName(local) {
  const folded = String(local || "").toLowerCase().normalize("NFKC").replace(/[._-]/g, "");
  if (!/^[a-z0-9]{1,32}$/.test(folded)) return null;
  return folded;
}
const put = (env, key, obj, ttl) => env.RELAY.put(key, JSON.stringify(obj), { expirationTtl: ttl });

async function stableId(...parts) {
  const data = new TextEncoder().encode(parts.join("|"));
  const hash = await crypto.subtle.digest("SHA-256", data);
  return [...new Uint8Array(hash)].map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 32);
}

// ---------------------------------------------------------------------------
// The relay's own key, served at /.well-known/relay.json so a relay can be named by it.
async function getSigning(env) {
  const raw = await env.RELAY.get("config:signing");
  if (raw) return JSON.parse(raw);
  const pair = await crypto.subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"]);
  const priv = await crypto.subtle.exportKey("jwk", pair.privateKey);
  const pub = b64u(await crypto.subtle.exportKey("raw", pair.publicKey));
  const rec = { priv, pub, kid: await keyId(pub), createdAt: new Date().toISOString() };
  await env.RELAY.put("config:signing", JSON.stringify(rec));
  return rec;
}
async function directoryGet(env, name) {
  const folded = dirName(name);
  if (!folded) return json({ error: "no such handle in this directory", name }, 404);
  const raw = await env.RELAY.get(`dir:${folded}`);
  if (!raw) return json({ error: "no such handle in this directory", name }, 404);
  const { storedAt, ...record } = JSON.parse(raw);
  return json(record);
}

async function chainLinks(fromKey, toKey, rotations) {
  let cur = fromKey;
  if (cur === toKey) return true;
  for (const r of rotations || []) {
    if (r.oldKey !== cur) continue;
    const { sig, kid, newSig, ...base } = r;
    const okOld = await verifyPayload({ ...base, sig }, r.oldKey);
    const okNew = await verifyPayload({ ...base, sig: newSig }, r.newKey);
    if (!okOld || !okNew) return false;
    cur = r.newKey;
    if (cur === toKey) return true;
  }
  return false;
}

// Ask the portal the record points at whether it agrees. It must serve the same name under the
// same key: that is what makes squatting cost a portal rather than a single HTTP request.
async function servesItself(record, name) {
  const base = String(record.rpc || record.cardUrl || "").replace(/\/(a2a|card|\.well-known\/.*)$/, "");
  if (!/^https:\/\//.test(base)) return false;
  try {
    const res = await fetch(`${base}/.well-known/mazel/${name}.json`, { headers: { accept: "application/json" } });
    if (!res.ok) return false;
    const served = await res.json();
    return served && served.publicKey === record.publicKey && (await verifyPayload(served, served.publicKey));
  } catch {
    return false;
  }
}

async function publish(env, record) {
  if (!record || typeof record !== "object") throw new Error("body must be a signed handle record");
  const handle = String(record.handle || "").toLowerCase();
  const m = handle.match(/^([a-z0-9][a-z0-9._-]*)@(mazel|mazel\.ai)$/);
  if (!m) throw new Error("this directory holds name@mazel (name@mazel.ai) only");
  if (!record.publicKey || !record.cardUrl || !record.sig) throw new Error("record needs publicKey, cardUrl, sig");
  if (!(await verifyPayload(record, record.publicKey))) throw new Error("record is not signed by the key it names");
  const name = dirName(m[1]);
  if (!name) throw new Error("a name in this directory is 1-32 characters of a-z and 0-9; dots, dashes and underscores fold away and anything else is refused");
  // The name has to be backed by a portal that serves the same record under the same key. A
  // directory that takes anyone's word for it hands out names to whoever asks first, for free.
  if (!(await servesItself(record, name))) {
    throw new Error(`the portal at ${record.rpc || record.cardUrl} does not serve this same record at /.well-known/mazel/${name}.json, so this name is not yours to publish`);
  }
  const existingRaw = await env.RELAY.get(`dir:${name}`);
  if (existingRaw) {
    const existing = JSON.parse(existingRaw);
    if (existing.publicKey !== record.publicKey) {
      const ok = await chainLinks(existing.publicKey, record.publicKey, record.rotations);
      if (!ok) throw new Error("name is held by another key and no valid rotation chain leads to this one");
    }
    if (existing.timestamp && record.timestamp && record.timestamp < existing.timestamp) throw new Error("record is older than the one on file");
  }
  // Stored verbatim: handle is inside the signed bytes, so the record is never rewritten (only storedAt rides alongside and is stripped on read).
  await put(env, `dir:${name}`, { ...record, storedAt: new Date().toISOString() }, DIR_TTL);
  return json({ ok: true, handle: `${name}@mazel.ai`, keyId: await keyId(record.publicKey), rotations: (record.rotations || []).length });
}

// ---------------------------------------------------------------------------
// Casts: a signed public need or card. The signer's public key rides in the cast; a cast whose
// signature does not verify is dropped. Only public-tier material is ever accepted here.
async function cast(env, origin, body) {
  if (!body || typeof body !== "object") throw new Error("body must be a signed cast");
  if (!body.publicKey || !body.sig) throw new Error("cast needs publicKey and sig");
  if (!(await verifyPayload(body, body.publicKey))) throw new Error("cast is not signed by its publicKey");
  if (body.kind === "blind") throw new Error("this relay holds no buckets: a held-back need travels only to the cards its owner chose");
  if (body.visibility && body.visibility !== "public") throw new Error("only public casts belong here");
  const kind = body.kind === "card" ? "card" : "need";
  const handle = String(body.handle || "").toLowerCase();
  if (!handle.includes("@")) throw new Error("cast needs a handle");
  // If this directory holds the name, only the key that holds it may cast under it.
  const local = dirName((handle.match(/^([^@]+)@(mazel|mazel\.ai)$/) || [])[1] || "");
  if (local) {
    const heldRaw = await env.RELAY.get(`dir:${local}`);
    const held = heldRaw ? JSON.parse(heldRaw) : null;
    if (held && held.publicKey !== body.publicKey) {
      throw new Error(`${handle} is held in this directory by another key; casts under that name must be signed by it`);
    }
    // And a cast's card lives where the record says it does: a copy of a card at some other host,
    // however right its key, is not the card portals will read for this handle (29e H2).
    const host = (u) => { try { return new URL(u).host; } catch { return null; } };
    if (held && body.cardUrl && host(body.cardUrl) !== host(held.rpc || held.cardUrl)) {
      throw new Error(`${handle}'s record names a card at ${host(held.rpc || held.cardUrl)}; a cast under that name carries that card, not one at ${host(body.cardUrl) || "an unreadable address"}`);
    }
  }
  if (!fresh(body.castAt)) throw new Error("cast is missing a recent castAt; a cast with no time in its signature can be replayed forever");
  if (!(await underCap(env, `cast:${body.publicKey.slice(0, 16)}`, MAX_CASTS_PER_KEY_PER_DAY))) throw new Error("that key has cast enough for one day here");
  // Keyed by the signing key, not by the handle. Keying by handle let anyone with a fresh keypair
  // overwrite the cached card of a handle they do not own, and every portal that searched picked up
  // their rpc for that person.
  const id = kind === "card"
    ? await stableId("card", body.publicKey)
    : await stableId(kind, body.publicKey, body.needId || body.needText || "");
  const record = {
    id, kind, handle, publicKey: body.publicKey, cardUrl: body.cardUrl || null, rpc: body.rpc || null,
    needId: body.needId || null, needText: String(body.needText || "").slice(0, 300), needTags: (body.needTags || []).map(normalizeTag).filter(Boolean),
    have: capTags(body.have), glosses: capGlosses(body.glosses),
    description: String(body.description || "").slice(0, 600), tier: "world", castAt: new Date().toISOString(),
  };
  record.castAt = body.castAt;   // the signed time, not the time it happened to arrive
  await put(env, `cast:${kind}:${id}`, record, CAST_TTL);
  await indexCast(env, `cast:${kind}:${id}`);
  return json({ ok: true, id, kind, expiresInSeconds: CAST_TTL, sentence: SENTENCE });
}

// A cast looks like a known card to the scorer: needs match against haves, and a need-cast is also
// searchable by its own text so two people looking for each other can meet.
function castAsCard(c) {
  return { url: c.cardUrl, handle: c.handle, rpc: c.rpc, description: c.description || c.needText || "", have: c.kind === "card" ? c.have : [], glosses: c.glosses || {}, tier: "world", need: c.needTags || [] };
}

async function search(env, url) {
  const q = String(url.searchParams.get("q") || "").trim();
  const tags = [...new Set(String(url.searchParams.get("tags") || "").split(",").map(normalizeTag).filter(Boolean))];
  if (!q && !tags.length) throw new Error("q or tags required");
  const needWords = needWordsFor(q, tags);
  // One read of the index, then one read per cast it names. A key the index still carries but KV
  // has expired reads back null and is simply skipped; the next cast prunes it.
  const idx = await castIndex(env);
  const casts = [];
  for (const [key] of idx.keys) {
    const v = await env.RELAY.get(key);
    if (v) { try { casts.push({ key, ...JSON.parse(v) }); } catch { /* a value we cannot read is not a result */ } }
  }
  const scored = [];
  for (const c of casts) {
    const card = castAsCard(c);
    // card-casts match on their haves; need-casts match on the words of the need itself
    const m = c.kind === "card" ? scoreCard(card, tags, needWords) : scoreNeedCast(c, tags, needWords);
    if (m.score >= 2 && m.matched.length) scored.push({ c, m, card });
  }
  scored.sort((a, b) => b.m.score - a.m.score || a.c.handle.localeCompare(b.c.handle));
  const results = scored.slice(0, MAX_RESULTS).map(({ c, m, card }) => ({
    kind: c.kind, handle: c.handle, cardUrl: c.cardUrl, rpc: c.rpc, publicKey: c.publicKey, tier: "world",
    matched: m.matched, score: m.score, why: c.kind === "card" ? whyLine(card, m, q || tags.join(" ")) : `${c.handle} is looking for the same thing: "${c.needText}".`,
    needText: c.needText || null, have: card.have, glosses: card.glosses, castAt: c.castAt,
  }));
  return json({ query: q, tags, searched: casts.length, results, sentence: SENTENCE });
}

function scoreNeedCast(c, tags, needWords) {
  const theirs = new Set([...(c.needTags || []).flatMap(words), ...words(c.needText)]);
  const matched = [];
  let score = 0;
  for (const t of tags) if ((c.needTags || []).includes(t)) { score += 3; matched.push(t); }
  let overlap = 0;
  for (const w of needWords) if (theirs.has(w)) overlap++;
  if (overlap >= 2) { score += overlap; if (!matched.length) matched.push(c.needTags[0] || "need"); }
  return { score, matched, descHits: 0 };
}

// ---------------------------------------------------------------------------
function json(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), { status, headers: { "content-type": "application/json", "access-control-allow-origin": "*" } });
}
function text(s, status = 200) {
  return new Response(s, { status, headers: { "content-type": "text/plain; charset=utf-8" } });
}

// @@SHARED-MATCH@@

// @@SHARED-SIGNING@@
