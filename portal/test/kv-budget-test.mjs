// The KV budget. Cloudflare's free plan allows 1,000 writes, 1,000 deletes and 1,000 lists a day
// per account; a paid plan raises the ceiling but not the cost. A portal that writes on a timer
// spends the day's budget whether or not anything happened, and KV also rate-limits more than one
// write a second to the SAME key, so a key written on every event is a bottleneck as well as a bill.
//
// Lock 13 (Avery, 2026-10-04, after the KV burn): every batch that touches storage updates this
// suite; a quiet pulse writes zero; every operation has a stated budget below and the test fails
// over it. A number here goes up only with a sentence saying why.
const BUDGET = {
  quiet_pulse: 0,          // a pulse with nothing new: nothing changed, so nothing is written
  first_pulse: 2,          // two one-time flags: the outcomes backfill, and the stamp that stops the card being re-cast every half hour
  inbound_message: 4,      // the message, its dedupe key, and the two daily counters
  inbound_lists: 2,        // the mailbox ceiling, and the card list once per request
  dead_address_retry: 1,   // the outbox record, with its backoff; nothing else
  reply: 6,                // the message, the thread record, the box item, the bell
  arriving_offer: 10,      // one offer from a held card, scored against any number of contacts: the questions raised, one document for the rest, the door's own counters
  fanout_subrequests: 50,  // Workers Free allows fifty subrequests to one request; a need offered to more cards than that batches across pulses
  fanout_writes: 2,        // one need offered to forty portals: the thread record, and nothing per recipient
};
import rawWorker from '../src/index.js';
import relay from '../../relay/src/index.js';
import { legacy as legacyWorker } from './a2a-helpers.mjs';
const worker = legacyWorker(rawWorker);

let pass = 0, fail = 0;
const ok = (label, cond, extra = '') => { console.log((cond ? 'PASS ' : 'FAIL ') + label + (extra ? '  -> ' + String(extra).replace(/\n/g, ' ').slice(0, 170) : '')); cond ? pass++ : fail++; };

// A KV that counts. Writes are attributed to the key's prefix, so a report can name what wrote.
const prefixOf = (k) => String(k).split(':')[0] + ':';
const countingKV = () => {
  const m = new Map();
  const n = { put: 0, delete: 0, list: 0, get: 0 };
  const byPrefix = { put: {}, delete: {}, list: {} };
  const perKey = {};                       // how often one key was written: KV's one-write-per-second rule
  const bump = (kind, key) => { n[kind]++; const p = prefixOf(key); byPrefix[kind][p] = (byPrefix[kind][p] || 0) + 1; if (kind === 'put') perKey[key] = (perKey[key] || 0) + 1; };
  return {
    m, n, byPrefix, perKey,
    reset() { n.put = n.delete = n.list = n.get = 0; for (const k of ['put', 'delete', 'list']) byPrefix[k] = {}; for (const k of Object.keys(perKey)) delete perKey[k]; },
    async get(k) { n.get++; return m.get(k) ?? null; },
    async put(k, v) { bump('put', k); m.set(k, v); },
    async delete(k) { bump('delete', k); m.delete(k); },
    async list({ prefix }) { n.list++; byPrefix.list[prefix] = (byPrefix.list[prefix] || 0) + 1; return { keys: [...m.keys()].filter((k) => k.startsWith(prefix)).sort().map((name) => ({ name })), list_complete: true }; },
  };
};
const top = (o, k = 4) => Object.entries(o).sort((a, b) => b[1] - a[1]).slice(0, k).map(([p, c]) => `${p}${c}`).join(' ');

const R = 'https://relay.budget.test';
const O = 'https://mazel.budget.test', P = 'https://peer.budget.test';
const kv = countingKV(), peerKv = countingKV();
const env = { HANDLE: 'owner@mazel', PERSONA: 'Owner of a portal with a few open needs.', NEED: '', HAVE: 'managed-ai-delivery', INBOX_TOKEN: 'tb', MAILBOX: kv, RELAY_URL: R, PORTAL_ORIGIN: O };
const peerEnv = { HANDLE: 'peer@mazel', PERSONA: 'A peer.', NEED: '', HAVE: 'fractional-cfo,sailing-atlantic', INBOX_TOKEN: 'tp', MAILBOX: peerKv, RELAY_URL: R, PORTAL_ORIGIN: P };

// The world: a relay that answers, a peer that takes mail, and one address that is simply dead.
let relayResults = [];
const DEAD = 'https://dead.budget.test';
const realFetch = globalThis.fetch;
globalThis.fetch = async (u, i = {}) => {
  const url = String(u instanceof Request ? u.url : u);
  if (url.startsWith(DEAD)) return new Response('gone', { status: 530 });
  if (url.startsWith(R + '/search')) return new Response(JSON.stringify({ results: relayResults }), { headers: { 'content-type': 'application/json' } });
  if (url.startsWith(R)) return new Response(JSON.stringify({ ok: true }), { headers: { 'content-type': 'application/json' } });
  if (url.startsWith(O)) return worker.fetch(new Request(url, i), env);
  if (url.startsWith(P)) return worker.fetch(new Request(url, i), peerEnv);
  return new Response('no', { status: 503 });
};
const call = async (e, name, args = {}) => {
  const r = await (await worker.fetch(new Request((e === env ? O : P) + '/mcp', { method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer ' + e.INBOX_TOKEN }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) }), e)).json();
  if (r.result && r.result.isError) throw new Error(r.result.content[0].text);
  return r.result.content[0].text;
};
const pulse = async (e) => { let p; await worker.scheduled({}, e, { waitUntil: (x) => { p = x; } }); return p; };

// ---- a portal in ordinary use: five open needs, one card held ----
await call(env, 'add_known_card', { url: P + '/card' });
for (const tag of ['fractional-cfo', 'sailing-atlantic', 'hockey-coach', 'sushi-chef', 'board-advisor']) {
  await call(env, 'find', { need_text: `someone who does ${tag}`, tags: [tag] });
}
const openNeeds = JSON.parse(await call(env, 'list_threads')).filter((t) => t.status === 'open').length;
ok('a portal with five open needs and one card held', openNeeds === 5, `${openNeeds} open`);

// ---- 1. one pulse, nothing new ----------------------------------------------------------------
kv.reset();
await pulse(env);
const quiet = { put: kv.n.put, del: kv.n.delete, list: kv.n.list, by: top(kv.byPrefix.put) };
ok(`the first pulse of a portal's life writes at most ${BUDGET.first_pulse} (the one-time backfill flag)`, quiet.put <= BUDGET.first_pulse, `puts ${quiet.put} (${quiet.by}), lists ${quiet.list}, deletes ${quiet.del}`);
kv.reset();
await pulse(env);
const quiet2 = { put: kv.n.put, list: kv.n.list, by: top(kv.byPrefix.put) };
ok('LOCK 13: a quiet pulse writes zero', quiet2.put === BUDGET.quiet_pulse, `puts ${quiet2.put} (${quiet2.by || 'nothing'}) for ${openNeeds} open needs`);
kv.reset();
await pulse(env); await pulse(env); await pulse(env);
ok('and three more of them write zero between them', kv.n.put === 0, `puts ${kv.n.put} (${top(kv.byPrefix.put) || 'nothing'})`);
ok('so a day of pulses costs nothing at all, at any number of needs', kv.n.put * 16 === 0, `${openNeeds} needs x 48 pulses = 0 writes a day`);

// ---- 2. one inbound message --------------------------------------------------------------------
const sign = async (e, payload) => {
  const s = JSON.parse(await e.MAILBOX.get('config:signing'));
  const canonical = (v) => v === null || typeof v !== 'object' ? JSON.stringify(v) : Array.isArray(v) ? '[' + v.map(canonical).join(',') + ']' : '{' + Object.keys(v).sort().map((k) => JSON.stringify(k) + ':' + canonical(v[k])).join(',') + '}';
  const key = await crypto.subtle.importKey('jwk', s.priv, { name: 'Ed25519' }, false, ['sign']);
  const sig = await crypto.subtle.sign({ name: 'Ed25519' }, key, new TextEncoder().encode(canonical(payload)));
  return { ...payload, sig: btoa(String.fromCharCode(...new Uint8Array(sig))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, ''), kid: s.kid };
};
const noteFrom = async (e, handle, origin, text) => {
  const action = await sign(e, { v: 1, type: 'note', handle, publicKey: JSON.parse(await e.MAILBOX.get('config:signing')).pub, cardUrl: origin + '/.well-known/agent-card.json', rpc: origin + '/a2a', castAt: new Date().toISOString() });
  return worker.fetch(new Request(O + '/a2a', { method: 'POST', headers: { 'content-type': 'application/json', 'CF-Connecting-IP': '198.51.100.7' }, body: JSON.stringify({ jsonrpc: '2.0', id: 'b-' + Math.random(), method: 'SendMessage', params: { message: { messageId: 'b-' + Math.random().toString(16).slice(2), contextId: '', taskId: '', role: 'ROLE_USER', parts: [{ text }], metadata: { handle, cardUrl: origin + '/.well-known/agent-card.json', action }, extensions: ['https://mazel.ai/ext/haah/v1'], referenceTaskIds: [] } } }) }), env);
};
await peerEnv.MAILBOX.get('config:signing') || await worker.fetch(new Request(P + '/card'), peerEnv);
kv.reset();
await noteFrom(peerEnv, 'peer@mazel', P, 'one note');
const inbound = { put: kv.n.put, list: kv.n.list, by: top(kv.byPrefix.put) };
ok(`one inbound message writes at most ${BUDGET.inbound_message}, and lists at most ${BUDGET.inbound_lists}`, inbound.put <= BUDGET.inbound_message && inbound.list <= BUDGET.inbound_lists, `puts ${inbound.put} (${inbound.by}), lists ${inbound.list}`);
kv.reset();
for (let i = 0; i < 10; i++) await noteFrom(peerEnv, 'peer@mazel', P, 'note ' + i);
const hot = Object.entries(kv.perKey).sort((a, b) => b[1] - a[1])[0];
ok('no single key is written more than once per message (KV allows one write per second per key)', hot && hot[1] <= 10, `hottest key ${hot && hot[0]} written ${hot && hot[1]}x in 10 messages`);
ok('ten messages cost ten messages\' worth of writes', kv.n.put <= inbound.put * 10 + 2, `puts ${kv.n.put} for 10 (${top(kv.byPrefix.put)})`);

// ---- 3. an outbox retry against an address that is simply dead ---------------------------------
{
  const id = await (async () => { const d = new TextEncoder().encode(['known', 'ghost@mazel'].join('|')); return [...new Uint8Array(await crypto.subtle.digest('SHA-256', d))].map((b) => b.toString(16).padStart(2, '0')).join('').slice(0, 32); })();
  kv.m.set('known:' + id, JSON.stringify({ id, url: DEAD + '/card', handle: 'ghost@mazel', rpc: DEAD + '/a2a', description: '', need: [], have: ['fractional-cfo'], glosses: {}, publicKey: 'AAAA', tier: 'world', tierByHand: 'world', addedAt: new Date().toISOString(), fetchedAt: new Date().toISOString() }));
  kv.m.set('outbox:dead1', JSON.stringify({ id: 'dead1', kind: 'action', to: 'ghost@mazel', text: 'an intro proposal', action: { type: 'intro.propose', v: 1, introId: 'i-dead', why: 'w', needText: '', needTags: [], matchedTags: [], path: ['owner@mazel'], proposer: { handle: 'owner@mazel' }, sig: 'x', kid: 'k', handle: 'owner@mazel', publicKey: 'p', cardUrl: O + '/c', rpc: O + '/a2a', castAt: new Date().toISOString() }, introId: 'i-dead', what: 'intro.propose', attempts: 4, firstAt: new Date(Date.now() - 2 * 24 * 3600e3).toISOString(), nextAt: new Date(Date.now() - 1000).toISOString() }));
  kv.reset();
  await pulse(env);
  const retry = { put: kv.n.put, by: top(kv.byPrefix.put) };
  ok(`one retry of a dead address writes at most ${BUDGET.dead_address_retry}`, retry.put <= BUDGET.dead_address_retry, `puts ${retry.put} (${retry.by})`);
  const o = JSON.parse(kv.m.get('outbox:dead1') || 'null');
  ok('and it backs off rather than retrying on the next pulse', !o || Date.parse(o.nextAt) > Date.now() + 60000, o ? `next in ${Math.round((Date.parse(o.nextAt) - Date.now()) / 60000)} min` : 'given up');
  kv.reset();
  await pulse(env);
  ok('the next pulse does not touch it at all', (kv.byPrefix.put['outbox:'] || 0) === 0 && (kv.byPrefix.put['known:'] || 0) === 0, `outbox puts ${kv.byPrefix.put['outbox:'] || 0}, known puts ${kv.byPrefix.put['known:'] || 0}`);
}

// ---- 4. a doorbell and a box item ---------------------------------------------------------------
{
  kv.reset();
  const convs = [...kv.m.keys()].filter((k) => k.startsWith('conv:'));
  if (convs.length) {
    const ctx = convs[0].slice(5);
    kv.m.delete('bell:' + ctx);
    kv.reset();
    let sent = true;
    await call(peerEnv, 'thread_send', { context_id: ctx, text: 'a reply', confirmed: true }).catch(() => { sent = false; });
    const bell = { put: kv.n.put, by: top(kv.byPrefix.put) };
    // Counted only when a reply really landed; a silent zero would pass for the wrong reason.
    ok(`a reply writes at most ${BUDGET.reply}`, sent ? bell.put <= BUDGET.reply && bell.put > 0 : bell.put === 0, sent ? `puts ${bell.put} (${bell.by})` : '(the peer holds no copy of that branch; box writes are covered by box-test)');
  } else ok('(no branch open to ring; skipped)', true);
}

// ---- 5. the whole day, as a number ---------------------------------------------------------------
{
  const perPulse = quiet2.put;
  ok('a portal at rest spends none of the day\'s budget', perPulse === 0, `${perPulse} writes per pulse x 48 = ${perPulse * 48} a day, before any real traffic`);
  console.log(`\n  budget held: quiet pulse ${perPulse}/${BUDGET.quiet_pulse} | inbound message ${inbound.put}/${BUDGET.inbound_message} writes, ${inbound.list}/${BUDGET.inbound_lists} lists`);
}


// ---- 5a. one offer arriving, against a great many contacts ------------------------------------
// The sender's side was budgeted from the start; the receiver's was not. An offer that fitted sixty
// contacts used to write a record for each, carrying the caster's full text, so twenty offers a day
// from one held card spent a KV portal's whole daily allowance on matches nobody had asked to see.
{
  const C = 'https://many.budget.test';
  const ckv = countingKV();
  const cenv = { HANDLE: 'cal@mazel', PERSONA: 'Cal knows people.', NEED: '', HAVE: 'nothing-at-all', INBOX_TOKEN: 'tc', MAILBOX: ckv, RELAY_URL: R, PORTAL_ORIGIN: C };
  const ccall = async (name, args = {}) => {
    const r = await (await worker.fetch(new Request(C + '/mcp', { method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer tc' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) }), cenv)).json();
    return r.result.content[0].text;
  };
  await ccall('add_known_card', { url: P + '/card' });
  for (let b = 0; b < 2; b++) {
    await ccall('contacts', { action: 'note', people: Array.from({ length: 30 }, (_, i) => ({ name: `Helper ${b}-${i}`, org: `h${b}${i}.example`, have: ['heat-exchangers'], witnesses: ['calendar'], small_meetings: 5, deals: 1, best_stage: 'won', threads_sent: 10, threads_replied: 8 })) });
  }
  const held = JSON.parse(await ccall('list_ghosts')).count;
  cenv.__peer = true;
  const sign2 = async (payload) => {
    const sg = JSON.parse(await peerEnv.MAILBOX.get('config:signing'));
    const canonical = (v) => v === null || typeof v !== 'object' ? JSON.stringify(v) : Array.isArray(v) ? '[' + v.map(canonical).join(',') + ']' : '{' + Object.keys(v).sort().map((k) => JSON.stringify(k) + ':' + canonical(v[k])).join(',') + '}';
    const key = await crypto.subtle.importKey('jwk', sg.priv, { name: 'Ed25519' }, false, ['sign']);
    const sig = await crypto.subtle.sign({ name: 'Ed25519' }, key, new TextEncoder().encode(canonical(payload)));
    return { ...payload, sig: btoa(String.fromCharCode(...new Uint8Array(sig))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, ''), kid: sg.kid };
  };
  await ccall('my_card');                       // the portal's own first write, which is not the offer's cost
  const pub = JSON.parse(await peerEnv.MAILBOX.get('config:signing')).pub;
  const action = await sign2({ v: 1, type: 'need.offer', handle: 'peer@mazel', publicKey: pub, cardUrl: P + '/.well-known/agent-card.json', rpc: P + '/a2a', castAt: new Date().toISOString(),
    needId: 'budget-need-1', tier: 'public', needText: 'somebody who knows heat exchangers', needTags: ['heat-exchangers'], to: 'cal@mazel' });
  ckv.reset();
  await worker.fetch(new Request(C + '/a2a', { method: 'POST', headers: { 'content-type': 'application/json', 'CF-Connecting-IP': '198.51.100.9' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 'o-1', method: 'SendMessage', params: { message: { messageId: 'o-1', contextId: '', taskId: '', role: 'ROLE_USER', parts: [{ text: 'A need of mine, in case you know someone.' }], metadata: { handle: 'peer@mazel', cardUrl: P + '/.well-known/agent-card.json', action }, extensions: ['https://mazel.ai/ext/haah/v1'], referenceTaskIds: [] } } }) }), cenv);
  const docs = (await cenv.MAILBOX.list({ prefix: 'ghostask:' })).keys.length;
  ok(`LOCK 13: one arriving offer against ${held} contacts writes at most ${BUDGET.arriving_offer}`, ckv.n.put <= BUDGET.arriving_offer, `puts ${ckv.n.put} (${top(ckv.byPrefix.put)}) against ${held} contacts`);
  ok('and the matches it kept are one document, not one per contact', docs === 1, `${docs} ask documents`);
  ok('so a day at the sender cap stays inside the free plan\'s thousand writes', ckv.n.put * 20 < 1000, `${ckv.n.put} x 20 = ${ckv.n.put * 20} a day from one held card`);
}

// ---- 5b. a need offered to more held cards than one request may touch -------------------------
// Workers Free allows fifty subrequests to a request. A portal holding a hundred and twenty cards
// offers a need in runs of forty, keeps what is left on the need, and carries on next pulse. The
// cost is one write for the need itself, whatever the number of recipients, because nothing is
// recorded per recipient.
{
  const SINK = 'https://sink.budget.test';
  let subreq = 0;
  const outer = globalThis.fetch;
  globalThis.fetch = async (u, i = {}) => {
    const url = String(u instanceof Request ? u.url : u);
    if (url.startsWith(SINK)) {
      subreq++;
      if (url.includes('/card') || url.includes('agent-card')) return new Response(JSON.stringify({ name: 'sink', url: SINK }), { headers: { 'content-type': 'application/json' } });
      return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { ok: true } }), { headers: { 'content-type': 'application/json' } });
    }
    return outer(u, i);
  };
  const HELD = 120;
  for (let i = 0; i < HELD; i++) {
    const d = new TextEncoder().encode(['known', `many${i}@mazel`].join('|'));
    const id = [...new Uint8Array(await crypto.subtle.digest('SHA-256', d))].map((b) => b.toString(16).padStart(2, '0')).join('').slice(0, 32);
    kv.m.set('known:' + id, JSON.stringify({ id, url: SINK + `/${i}/card`, handle: `many${i}@mazel`, rpc: SINK + `/${i}/a2a`, description: '', need: [], have: ['widget-making'],
      glosses: {}, publicKey: 'AAAA', tier: 'tribe', tierByHand: 'tribe', addedAt: new Date().toISOString(), fetchedAt: new Date().toISOString() }));
  }
  // The need is placed first: nothing travels until the person has said where it lives, so an
  // unplaced need would offer to nobody and this section would measure nothing.
  await call(env, 'update_card', { add_need: 'widget-making', confirmed: true });
  kv.reset(); subreq = 0;
  await call(env, 'find', { need_text: 'a widget maker for a one-off run', tags: ['widget-making'] });
  const castReq = subreq, castPuts = kv.n.put;
  ok(`LOCK 13: one cast touches at most ${BUDGET.fanout_subrequests} portals, whatever the number held`, castReq <= BUDGET.fanout_subrequests, `${castReq} subrequests for ${HELD} held cards`);
  const tkey = [...kv.m.keys()].filter((k) => k.startsWith('thread:')).find((k) => (kv.m.get(k) || '').includes('widget maker'));
  const threadId = tkey.slice('thread:'.length);
  const offeredAfterCast = JSON.parse(kv.m.get('thread:' + threadId)).offeredTo.length;
  ok('and the rest are kept on the need, not dropped', offeredAfterCast > 0 && offeredAfterCast < HELD, `${offeredAfterCast} of ${HELD} offered so far`);
  // Progress is counted across every open need, because the allowance is the run's: this pulse may
  // spend it all on an older need, and the widget need moves on a later one.
  const offeredTotal = () => [...kv.m.keys()].filter((k) => k.startsWith('thread:')).reduce((n, k) => n + ((JSON.parse(kv.m.get(k)).offeredTo || []).length), 0);
  const totalBefore = offeredTotal();
  kv.reset(); subreq = 0;
  await pulse(env);
  ok('the next pulse carries on from where it stopped', offeredTotal() > totalBefore, `${totalBefore} -> ${offeredTotal()} offers made across every open need`);
  ok(`and the whole run costs at most ${BUDGET.fanout_writes} writes for that need, not one per recipient`,
     (kv.byPrefix.put['thread:'] || 0) <= BUDGET.fanout_writes, `thread writes ${kv.byPrefix.put['thread:'] || 0} for ${offeredTotal() - totalBefore} recipients (all puts: ${top(kv.byPrefix.put)})`);
  ok(`each pulse stays under the subrequest ceiling too`, subreq <= BUDGET.fanout_subrequests, `${subreq} subrequests in one pulse`);
  // Six open needs and a hundred and twenty cards is seven hundred and twenty offers, at forty a
  // pulse: finished inside a day of pulses, with nothing hurrying and nobody waiting on it.
  let guard = 0;
  while (JSON.parse(kv.m.get('thread:' + threadId)).offeredTo.length < HELD && guard++ < 48) await pulse(env);
  const done = JSON.parse(kv.m.get('thread:' + threadId)).offeredTo.length;
  ok('and it finishes inside a day of pulses, rather than for ever', done === HELD && guard < 48, `${done} of ${HELD} after ${guard + 1} pulses, with ${openNeeds + 1} needs sharing the allowance`);
  // And when every need has reached every card, the pulse costs nothing again: no subrequests and
  // no writes, which is what lock 13 is about.
  let settle = 0;
  while (settle++ < 48) { kv.reset(); subreq = 0; await pulse(env); if (subreq === 0) break; }
  ok('once everyone has been offered everything, a pulse is quiet again', subreq === 0 && (kv.byPrefix.put['thread:'] || 0) === 0, `${subreq} subrequests, ${kv.byPrefix.put['thread:'] || 0} thread writes, settled after ${settle} pulses`);
  for (const k of [...kv.m.keys()]) if (k.startsWith('known:') && JSON.parse(kv.m.get(k)).handle.startsWith('many')) kv.m.delete(k);
  globalThis.fetch = outer;
}

// ---- 6. the relay: a cache is asked, and never scans itself ------------------------------------
// The company account hit the free plan's 1,000 list operations a day on 2026-10-05. Lists, not
// writes: a relay that lists its namespace on every search spends the day's budget on being read.
// Budget: a search lists nothing, a cast lists nothing, and the relay has no cron to list from.
{
  const relayKV = countingKV();
  const renv = { RELAY: relayKV };
  const RO = 'https://relay.budget.test';
  const rq = (path, init) => relay.fetch(new Request(RO + path, init), renv);
  const sign = async (kp, payload) => {
    const canonical = (v) => v === null || typeof v !== 'object' ? JSON.stringify(v) : Array.isArray(v) ? '[' + v.map(canonical).join(',') + ']' : '{' + Object.keys(v).sort().map((k) => JSON.stringify(k) + ':' + canonical(v[k])).join(',') + '}';
    const sig = await crypto.subtle.sign({ name: 'Ed25519' }, kp.priv, new TextEncoder().encode(canonical(payload)));
    return { ...payload, sig: btoa(String.fromCharCode(...new Uint8Array(sig))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, ''), kid: 'k' };
  };
  const b64u = (buf) => btoa(String.fromCharCode(...new Uint8Array(buf))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  const mkKey = async () => { const p = await crypto.subtle.generateKey({ name: 'Ed25519' }, true, ['sign', 'verify']); return { priv: p.privateKey, pub: b64u(await crypto.subtle.exportKey('raw', p.publicKey)) }; };
  // Forty cards in the cache, cast by forty different keys, as a working relay holds.
  for (let i = 0; i < 40; i++) {
    const kp = await mkKey();
    const body = await sign(kp, { v: 1, kind: 'card', visibility: 'public', handle: `p${i}@company${i}.test`, publicKey: kp.pub, cardUrl: `https://p${i}.test/card`, rpc: `https://p${i}.test/a2a`, castAt: new Date().toISOString(), have: ['fractional-cfo', 'sailing-atlantic'], glosses: {}, description: 'A portal that does finance work.' });
    await rq('/cast', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
  }
  relayKV.reset();
  const r1 = await rq('/search?q=a%20fractional%20cfo&tags=fractional-cfo');
  const found = (await r1.json()).results.length;
  const s1 = { list: relayKV.n.list, get: relayKV.n.get, put: relayKV.n.put };
  ok('RELAY: one search lists nothing', s1.list === 0, `lists ${s1.list}, gets ${s1.get}, puts ${s1.put}, results ${found}`);
  relayKV.reset();
  for (let i = 0; i < 10; i++) await rq('/search?q=a%20fractional%20cfo&tags=fractional-cfo');
  ok('RELAY: ten searches list nothing either', relayKV.n.list === 0, `lists ${relayKV.n.list} for 10 searches`);
  ok('RELAY: a day of searching stays inside the free plan\'s thousand lists', relayKV.n.list * 100 < 1000, `${relayKV.n.list / 10} lists per search`);
  relayKV.reset();
  {
    const kp = await mkKey();
    const body = await sign(kp, { v: 1, kind: 'card', visibility: 'public', handle: 'late@company.test', publicKey: kp.pub, cardUrl: 'https://late.test/card', rpc: 'https://late.test/a2a', castAt: new Date().toISOString(), have: ['fractional-cfo'], glosses: {}, description: 'One more.' });
    await rq('/cast', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
  }
  ok('RELAY: one cast lists nothing', relayKV.n.list === 0, `lists ${relayKV.n.list}, puts ${relayKV.n.put}`);
  const r2 = await rq('/search?q=a%20fractional%20cfo&tags=fractional-cfo');
  ok('RELAY: and the new cast is findable straight away', JSON.stringify(await r2.json()).includes('late@company.test'));
  relayKV.reset();
  await rq('/.well-known/mazel/nobody.json');
  ok('RELAY: a directory lookup lists nothing', relayKV.n.list === 0, `lists ${relayKV.n.list}`);
  ok('RELAY: and there is no cron to list from', typeof relay.scheduled !== 'function', typeof relay.scheduled);
}

console.log(`\nkv-budget: ${pass} passed, ${fail} failed`);
globalThis.fetch = realFetch;
process.exit(fail ? 1 : 0);
