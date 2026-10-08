// v0.5.9-live, batch 1: the portal's state in a Durable Object with the SQLite storage backend.
//
// The object answers the same four verbs KV answers, so nothing above the storage line changes.
// This suite proves two things: the object's semantics match KV's where the portal depends on them
// (expiry, prefix listing, overwrite, delete, read-after-write), and a whole portal driven through
// the object behaves exactly as the same portal driven through KV - same card, same find, same
// intro, same thread, same box.
//
// The SQLite storage API is stubbed here the way Cloudflare implements it: state.storage.sql.exec
// returning an iterable of rows. The portal code under test is the real `Portal` class.
import rawWorker, { Portal } from '../src/index.js';
import { legacy as legacyWorker } from './a2a-helpers.mjs';
const worker = legacyWorker(rawWorker);

let pass = 0, fail = 0;
const ok = (label, cond, extra = '') => { console.log((cond ? 'PASS ' : 'FAIL ') + label + (extra ? '  -> ' + String(extra).replace(/\n/g, ' ').slice(0, 165) : '')); cond ? pass++ : fail++; };

// ---- a SQLite storage stub with the shape Cloudflare gives a SQLite-backed object --------------
// Only the handful of statements the Portal class issues, answered against a Map. Narrow on
// purpose: a stub that accepts any SQL would hide a typo in the real one.
function sqlStub() {
  const rows = new Map();                       // k -> { v, exp }
  return {
    rows,                                       // so a test can read what landed, and its expiry
    exec(q, ...a) {
      const sql = q.replace(/\s+/g, ' ').trim();
      if (sql.startsWith('CREATE TABLE')) return [];
      if (sql.startsWith('SELECT v, exp FROM kv WHERE k = ?')) {
        const r = rows.get(a[0]);
        return r ? [{ v: r.v, exp: r.exp }] : [];
      }
      if (sql.startsWith('INSERT INTO kv')) { rows.set(a[0], { v: a[1], exp: a[2] }); return []; }
      if (sql.startsWith('DELETE FROM kv WHERE k = ?')) { rows.delete(a[0]); return []; }
      if (sql.startsWith('SELECT k, exp FROM kv WHERE k >= ? AND k < ?')) {
        return [...rows.entries()].filter(([k]) => k >= a[0] && k < a[1]).sort((x, y) => x[0] < y[0] ? -1 : 1).map(([k, r]) => ({ k, exp: r.exp }));
      }
      throw new Error('the Portal object issued SQL this stub does not know: ' + sql);
    },
    _rows: rows,
  };
}
const makeObject = () => {
  const sql = sqlStub();
  const o = new Portal({ storage: { sql } });
  return { stub: { fetch: (url, init) => o.fetch(new Request(url, init)) }, sql, rows: sql.rows };
};

// ---- 1. the object's own semantics -------------------------------------------------------------
{
  const { stub, sql } = makeObject();
  const call = async (body) => (await stub.fetch('https://portal/store', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })).json();
  ok('a key that was never written reads back null', (await call({ op: 'get', key: 'msg:none' })).value === null);
  await call({ op: 'put', key: 'msg:1', value: 'one' });
  ok('what is written is readable in the same breath, with no lag', (await call({ op: 'get', key: 'msg:1' })).value === 'one');
  await call({ op: 'put', key: 'msg:1', value: 'two' });
  ok('a second write replaces the first rather than adding a row', (await call({ op: 'get', key: 'msg:1' })).value === 'two' && sql._rows.size === 1);
  await call({ op: 'put', key: 'msg:2', value: 'x' });
  await call({ op: 'put', key: 'conv:a', value: 'y' });
  const listed = (await call({ op: 'list', prefix: 'msg:' })).keys.map((k) => k.name);
  ok('a prefix list returns that prefix, sorted, and nothing else', JSON.stringify(listed) === '["msg:1","msg:2"]', JSON.stringify(listed));
  await call({ op: 'delete', key: 'msg:1' });
  ok('a delete removes it from reads and from lists', (await call({ op: 'get', key: 'msg:1' })).value === null && (await call({ op: 'list', prefix: 'msg:' })).keys.length === 1);
  await call({ op: 'put', key: 'seen:gone', value: 'z', ttl: -1 });
  ok('a key past its expiry reads back null', (await call({ op: 'get', key: 'seen:gone' })).value === null);
  ok('and is gone from a list too, without a sweep', (await call({ op: 'list', prefix: 'seen:' })).keys.length === 0);
  ok('an unknown operation is refused rather than guessed at', (await stub.fetch('https://portal/store', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ op: 'truncate' }) })).status === 400);
}

// ---- 2. a whole portal, driven through the object ----------------------------------------------
// The same script run twice: once on KV, once on the object. Every answer must match.
const R = 'https://relay.do.test';
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k, v) => m.set(k, v), delete: async k => m.delete(k), list: async ({ prefix }) => ({ keys: [...m.keys()].filter(k => k.startsWith(prefix)).sort().map(name => ({ name })), list_complete: true }) }; };

async function runPortalScript(label, envFor) {
  const A = `https://a.${label}.test`, B = `https://b.${label}.test`;
  const envA = envFor(A, 'avery@mazel', 'Avery runs Halcyon and sells managed AI delivery.', 'managed-ai-delivery');
  const envB = envFor(B, 'sam@mazel', 'Sam is a fractional CFO for healthcare companies.', 'fractional-cfo');
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (u, i = {}) => {
    const url = String(u instanceof Request ? u.url : u);
    if (url.startsWith(R)) return new Response(JSON.stringify({ ok: true, results: [] }), { headers: { 'content-type': 'application/json' } });
    if (url.startsWith(A)) return worker.fetch(new Request(url, i), envA);
    if (url.startsWith(B)) return worker.fetch(new Request(url, i), envB);
    return new Response('no', { status: 503 });
  };
  const call = async (e, origin, name, args = {}) => {
    const r = await (await worker.fetch(new Request(origin + '/mcp', { method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer ' + e.INBOX_TOKEN }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) }), e)).json();
    if (r.result && r.result.isError) throw new Error(r.result.content[0].text);
    return r.result.content[0].text;
  };
  const out = {};
  out.card = JSON.parse(await call(envA, A, 'my_card')).handle;
  await call(envA, A, 'add_known_card', { url: B + '/card' });
  await call(envB, B, 'add_known_card', { url: A + '/card' });
  out.cards = JSON.parse(await call(envA, A, 'list_known_cards')).map(c => c.handle).sort().join(',');
  await call(envA, A, 'update_card', { add_need: 'fractional-cfo', confirmed: true });   // a proposal carries the need's words
  const f = JSON.parse(await call(envA, A, 'find', { need_text: 'a fractional cfo for a hospital group', tags: ['fractional-cfo'] }));
  out.candidates = (f.candidates || []).map(c => c.handle).join(',');
  await call(envA, A, 'propose_intro', { thread_id: f.thread_id, card_url: f.candidates[0].card_url, ask: { kind: 'call', size: '20 minutes' }, confirmed: true });
  const introId = JSON.parse(await call(envB, B, 'list_intros')).find(i => i.direction === 'received').intro_id;
  out.introState = JSON.parse(await call(envB, B, 'list_intros')).find(i => i.direction === 'received').state;
  await call(envB, B, 'respond_intro', { intro_id: introId, decision: 'accepted', confirmed: true });
  const tl = JSON.parse(await call(envB, B, 'thread_list'));
  const t = tl.threads.find(x => x.humans);
  out.thread = t ? `${t.with.join(',')}:${t.state}` : 'none';
  await call(envB, B, 'thread_send', { context_id: t.context_id, text: 'Thursday at 3 works.', confirmed: true });
  const read = JSON.parse(await call(envA, A, 'thread_read', { context_id: t.context_id }));
  out.lastMessage = read.messages[read.messages.length - 1].parts[0].text;
  const box = await call(envA, A, 'check_mailbox');
  out.box = box.startsWith('{') ? JSON.parse(box).box : {};
  globalThis.fetch = realFetch;
  return out;
}

const onKV = await runPortalScript('kv', (origin, handle, persona, have) => ({ HANDLE: handle, PERSONA: persona, NEED: '', HAVE: have, INBOX_TOKEN: 't', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: origin }));
const onDO = await runPortalScript('do', (origin, handle, persona, have) => {
  const { stub } = makeObject();
  return { HANDLE: handle, PERSONA: persona, NEED: '', HAVE: have, INBOX_TOKEN: 't', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: origin, STORE: 'do', PORTAL: { idFromName: () => 'portal', get: () => stub } };
});

ok('DO: the owner\'s own card reads the same as on KV', onDO.card === onKV.card, `${onDO.card} vs ${onKV.card}`);
ok('DO: the cards it holds are the same', onDO.cards === onKV.cards, `${onDO.cards} vs ${onKV.cards}`);
ok('DO: find returns the same candidate', onDO.candidates === onKV.candidates, `${onDO.candidates} vs ${onKV.candidates}`);
ok('DO: the intro arrives in the same state', onDO.introState === onKV.introState, `${onDO.introState} vs ${onKV.introState}`);
ok('DO: the yes opens the same thread', onDO.thread === onKV.thread, `${onDO.thread} vs ${onKV.thread}`);
ok('DO: a person\'s words arrive verbatim, as on KV', onDO.lastMessage === onKV.lastMessage && /Thursday at 3 works\./.test(onDO.lastMessage), onDO.lastMessage);
ok('DO: the box carries the same kinds', JSON.stringify(onDO.box) === JSON.stringify(onKV.box), `${JSON.stringify(onDO.box)} vs ${JSON.stringify(onKV.box)}`);

// ---- 3. a portal without the binding is untouched ----------------------------------------------
{
  const kv = mkKV();
  const env = { HANDLE: 'solo@mazel', PERSONA: 'p', NEED: '', HAVE: 'x', INBOX_TOKEN: 't', MAILBOX: kv, RELAY_URL: 'none', PORTAL_ORIGIN: 'https://solo.do.test' };
  await worker.fetch(new Request('https://solo.do.test/card'), env);
  ok('a portal with no binding still writes to KV, exactly as before', [...kv.m.keys()].some(k => k.startsWith('config:')), `${kv.m.size} keys`);
  const withBindingButNoFlag = { ...env, MAILBOX: mkKV(), PORTAL: { idFromName: () => 'portal', get: () => makeObject().stub } };
  await worker.fetch(new Request('https://solo.do.test/card'), withBindingButNoFlag);
  ok('and a portal with the binding but without STORE=do stays on KV too', withBindingButNoFlag.MAILBOX.m.size > 0);
}

// ---- 4. the published record: a portal's public face stays in KV ------------------------------
// Everything the owner touches lives in the object. One thing does not: the public card and the
// signed handle record, which every stranger in the network reads and which must not wake it.
{
  const kv = mkKV();
  const { stub } = makeObject();
  const O = 'https://pub.do.test';
  const env = { HANDLE: 'lea@mazel', PERSONA: 'Lea runs lab ops.', NEED: '', HAVE: 'lab-ops', INBOX_TOKEN: 't', MAILBOX: kv, RELAY_URL: 'none', PORTAL_ORIGIN: O, STORE: 'do', PORTAL: { idFromName: () => 'portal', get: () => stub } };
  const get = (path) => worker.fetch(new Request(O + path), env);
  const card1 = await (await get('/card')).json();
  ok('PUBLISHED: the card answers before anything has been published', card1.name === 'lea@mazel' || !!card1.capabilities, JSON.stringify(card1).slice(0, 60));
  // The pulse publishes it, and only the published key lands in KV.
  let p; await worker.scheduled({}, env, { waitUntil: (x) => { p = x; } }); await p;
  const kvKeys = [...kv.m.keys()];
  ok('PUBLISHED: the pulse writes it, and KV holds nothing else', kvKeys.length === 1 && kvKeys[0] === 'card:public', JSON.stringify(kvKeys));
  const pub = JSON.parse(kv.m.get('card:public'));
  ok('PUBLISHED: it carries the public card and the signed handle record', !!pub.card && !!pub.record && pub.record.handle === 'lea@mazel' && !!pub.record.sig);
  ok('PUBLISHED: and the state itself is in the object, not in KV', [...kv.m.keys()].every((k) => k === 'card:public'));
  // A stranger's fetch is answered from the published copy.
  const served = await (await get('/card')).json();
  ok('PUBLISHED: /card serves the published copy', JSON.stringify(served) === JSON.stringify(pub.card));
  const recServed = await (await get('/.well-known/mazel/lea.json')).json();
  ok('PUBLISHED: and so does the handle record', JSON.stringify(recServed) === JSON.stringify(pub.record));
  ok('PUBLISHED: a handle this portal is not keeps answering 404', (await get('/.well-known/mazel/someoneelse.json')).status === 404);
  // A second pulse changes nothing, so it writes nothing: lock 13 holds through the published copy.
  const before = kv.m.get('card:public');
  let p2; await worker.scheduled({}, env, { waitUntil: (x) => { p2 = x; } }); await p2;
  ok('PUBLISHED: a pulse that changed nothing republishes nothing', kv.m.get('card:public') === before);
  // A real change to the card does republish it.
  const call = async (name, args) => (await (await worker.fetch(new Request(O + '/mcp', { method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer t' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) }), env)).json());
  await call('update_card', { add_have: 'culture-media', witnesses: ['hubspot'], confirmed: true });
  ok('PUBLISHED: a change to the card republishes it at once', kv.m.get('card:public') !== before && /culture-media/.test(kv.m.get('card:public')));
  // And the public face survives an object that will not answer.
  const broken = { ...env, PORTAL: { idFromName: () => 'portal', get: () => ({ fetch: async () => { throw new Error('object unreachable'); } }) } };
  const stillServed = await worker.fetch(new Request(O + '/card'), broken);
  ok('PUBLISHED: a stranger still gets the card when the object is unreachable', stillServed.status === 200 && JSON.stringify(await stillServed.json()).includes('lea@mazel'));
}

// ---- 4b. the stub is never kept between requests ---------------------------------------------
// Cloudflare refuses I/O made in one request from another: "Cannot perform I/O on behalf of a
// different request". A worker reuses the same env object across requests in an isolate, so a stub
// cached on env belongs to the request that made it and poisons the next one. This is what the test
// peer hit on its first real mailbox read, and it is asserted here so it cannot come back.
{
  const O = 'https://mazel.perreq.test';
  const { stub } = makeObject();
  let handedOut = 0;
  const env = { HANDLE: 'lea@mazel', PERSONA: 'Lea runs lab ops.', NEED: '', HAVE: 'lab-ops', INBOX_TOKEN: 't', RELAY_URL: 'none', PORTAL_ORIGIN: O, STORE: 'do',
    MAILBOX: mkKV(), PORTAL: { idFromName: () => 'portal', get: () => { handedOut++; return stub; } } };
  const call = async (name, args = {}) => (await (await worker.fetch(new Request(O + '/mcp', { method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer t' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) }), env)).json()).result.content[0].text;
  await call('my_card');
  const afterOne = handedOut;
  await call('my_card');
  ok('PER-REQUEST: a second request gets its own stub rather than one cached on env', handedOut > afterOne, `${afterOne} stub(s) after one request, ${handedOut} after two`);
  ok('PER-REQUEST: and nothing is stashed on env between requests', !('__store' in env) && !Object.getOwnPropertyNames(env).some(k => k.startsWith('__')), JSON.stringify(Object.getOwnPropertyNames(env).filter(k => k.startsWith('__'))));
  const box = await call('check_mailbox');
  ok('PER-REQUEST: a mailbox read on the object works on a later request', !/different request|Error/.test(box), box.slice(0, 60));
}

// ---- 5. the migration: a copy, repeatable, with KV untouched until both sides agree ----------
// A portal that predates the object still reads KV. The migration copies its state across, counts
// both sides, and compares every value. It never deletes, so running it twice is running it once,
// and nothing switches over until STORE changes and the code is deployed.
{
  const O = 'https://mazel.migrate.test';
  const kv = mkKV();
  const { stub } = makeObject();
  // STORE is deliberately absent: this is a portal still on KV, which is when a migration is run.
  const env = { HANDLE: 'lea@mazel', PERSONA: 'Lea runs lab ops.', NEED: '', HAVE: 'lab-ops', INBOX_TOKEN: 't', RELAY_URL: 'none', PORTAL_ORIGIN: O,
    MAILBOX: kv, PORTAL: { idFromName: () => 'portal', get: () => stub } };
  const call = async (name, args = {}) => {
    const r = await (await worker.fetch(new Request(O + '/mcp', { method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer t' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) }), env)).json();
    if (r.result && r.result.isError) throw new Error(r.result.content[0].text);
    return r.result.content[0].text;
  };
  // Give the portal a life worth carrying: a card, contacts, a need, a message.
  await call('update_card', { add_have: 'lab-ops', witnesses: ['calendar'], confirmed: true });
  await call('contacts', { action: 'note', name: 'Wren Castellano', org: 'wren.example', have: ['assay-design'], witnesses: ['calendar'], small_meetings: 4 });
  await call('find', { need_text: 'an assay designer for a small panel', tags: ['assay-design'] });
  const kvKeysBefore = [...kv.m.keys()].length;
  ok('MIGRATE: the portal has state in KV to move', kvKeysBefore > 4, `${kvKeysBefore} keys in KV`);

  const first = JSON.parse(await call('migrate_store'));
  ok('MIGRATE: it reports the count on both sides', first.kv_keys === first.object_keys && first.object_keys > 0, `kv ${first.kv_keys}, object ${first.object_keys}`);
  ok('MIGRATE: and says every value matches', first.equal === true && first.mismatched_values === 0 && first.missing_on_target.length === 0, JSON.stringify({ equal: first.equal, mismatched: first.mismatched_values }));
  ok('MIGRATE: KV still holds everything it held: the copy took nothing away', [...kv.m.keys()].length === kvKeysBefore, `${kvKeysBefore} -> ${[...kv.m.keys()].length}`);
  ok('MIGRATE: and nothing has switched over yet', first.store_in_effect === 'kv', first.store_in_effect);

  const second = JSON.parse(await call('migrate_store'));
  ok('MIGRATE: running it again writes nothing, because it is a copy', second.written === 0 && second.already_identical === second.source_keys, `wrote ${second.written}, identical ${second.already_identical} of ${second.source_keys}`);
  ok('MIGRATE: and still says equal', second.equal === true);

  // The switch: the same portal, now reading the object, finds everything it had.
  const after = { ...env, STORE: 'do' };
  const callAfter = async (name, args = {}) => (await (await worker.fetch(new Request(O + '/mcp', { method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer t' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) }), after)).json()).result.content[0].text;
  const card = JSON.parse(await callAfter('my_card'));
  ok('MIGRATE: after the switch the card is the same card', (card.have || []).includes('lab-ops') && card.handle === 'lea@mazel', JSON.stringify(card.have));
  const people = JSON.parse(await callAfter('contacts', { action: 'list' }));
  ok('MIGRATE: and the contacts came across', people.count === 1 && people.people[0].name === 'Wren Castellano', `${people.count} contacts`);

  // Something arrives while it is on the object, then the rollback carries it back to KV.
  await callAfter('contacts', { action: 'note', name: 'Yusuf Mbeki', org: 'yusuf.example', have: ['panel-ops'], witnesses: ['gmail'] });
  const back = JSON.parse(await callAfter('migrate_store', { direction: 'to-kv' }));
  ok('MIGRATE: the copy back reports both sides too', back.equal === true && back.kv_keys === back.object_keys, `kv ${back.kv_keys}, object ${back.object_keys}`);
  const rolledBack = JSON.parse(await call('contacts', { action: 'list' }));
  ok('MIGRATE: and a portal rolled back to KV has what arrived while it was on the object',
     rolledBack.count === 2 && rolledBack.people.some(p => p.name === 'Yusuf Mbeki'), `${rolledBack.count} contacts back on KV`);
  ok('MIGRATE: the rollback is stated in the answer, not left to be worked out', /deploy with STORE unset/.test(first.rollback), first.rollback.slice(0, 70));

  // verify_only touches nothing.
  const kvNow = [...kv.m.keys()].length;
  await callAfter('contacts', { action: 'note', name: 'Zola Prinsloo', org: 'zola.example', have: ['bench-work'], witnesses: ['gmail'] });
  const check = JSON.parse(await callAfter('migrate_store', { direction: 'to-kv', verify_only: true }));
  ok('MIGRATE: a verify-only run writes nothing and says what differs', check.written === 0 && check.equal === false && [...kv.m.keys()].length === kvNow, `wrote ${check.written}, equal ${check.equal}, kv ${kvNow} -> ${[...kv.m.keys()].length}`);
  ok('MIGRATE: the published card is never moved into the object', !JSON.stringify(first).includes('"card:public"') && /left in KV/.test(first.published_card));
}

// ---- 6. a store that answers a list one page at a time --------------------------------------
// Workers KV returns at most 1,000 keys to a list and a cursor for the rest. A migration that reads
// one page copies one page, then counts both sides from that same page and calls it equal: the
// owner is told to read the counts back, switches, and loses everything past the first thousand.
{
  const O = 'https://mazel.paged.test';
  // A KV that pages exactly as Cloudflare's does.
  const pagedKV = (n) => {
    const m = new Map();
    for (let i = 0; i < n; i++) m.set(`msg:${String(i).padStart(5, '0')}`, JSON.stringify({ i }));
    m.set('config:card', JSON.stringify({ handle: 'lea@mazel', have: [], need: [], glosses: {} }));
    return { m,
      get: async (k) => m.get(k) ?? null,
      put: async (k, v) => m.set(k, v),
      delete: async (k) => m.delete(k),
      list: async ({ prefix = '', cursor } = {}) => {
        const all = [...m.keys()].filter((k) => k.startsWith(prefix)).sort();
        const from = cursor ? all.indexOf(cursor) + 1 : 0;
        const page = all.slice(from, from + 1000);
        const last = page[page.length - 1];
        const done = from + page.length >= all.length;
        return { keys: page.map((name) => ({ name })), list_complete: done, ...(done ? {} : { cursor: last }) };
      } };
  };
  const kv = pagedKV(1204);
  const { stub, rows: objRows } = makeObject();
  const obj = { rows: objRows };
  const env = { HANDLE: 'lea@mazel', PERSONA: 'Lea runs lab ops.', NEED: '', HAVE: 'lab-ops', INBOX_TOKEN: 't', RELAY_URL: 'none', PORTAL_ORIGIN: O,
    MAILBOX: kv, PORTAL: { idFromName: () => 'portal', get: () => stub } };
  const call = async (name, args = {}) => (await (await worker.fetch(new Request(O + '/mcp', { method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer t' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) }), env)).json()).result.content[0].text;
  const before = [...kv.m.keys()].length;
  ok('PAGED: a portal with more keys than one page holds', before > 1000, `${before} keys in KV`);
  const r = JSON.parse(await call('migrate_store'));
  ok('PAGED: the copy follows the cursor and takes every key', r.source_keys === before - 0 && r.object_keys === r.kv_keys, `source ${r.source_keys}, kv ${r.kv_keys}, object ${r.object_keys}`);
  ok('PAGED: and says equal only when it really is', r.equal === true && r.mismatched_values === 0 && r.missing_on_target.length === 0, JSON.stringify({ equal: r.equal, mismatched: r.mismatched_values }));
  const missed = [...kv.m.keys()].filter((k) => !obj.rows.has(k) && k !== 'card:public');
  ok('PAGED: nothing past the first thousand was left behind', missed.length === 0, `${missed.length} missing, e.g. ${missed.slice(0, 2).join(', ')}`);
  const again = JSON.parse(await call('migrate_store'));
  ok('PAGED: a second run writes nothing, because the first one finished', again.written === 0 && again.equal === true, `wrote ${again.written}`);
  // L4: a copied key keeps a life, so a mailbox that cleared itself still does.
  const row = obj.rows.get([...obj.rows.keys()].find((k) => k.startsWith('msg:')));
  ok('PAGED: a copied mailbox row carries an expiry rather than arriving immortal', !!row && !!row.exp, JSON.stringify(row && { exp: row.exp }));
  const cfg = obj.rows.get('config:card');
  ok('PAGED: and a key that never expired still does not', !!cfg && !cfg.exp, JSON.stringify(cfg && { exp: cfg.exp }));
}

console.log(`\ndo-store: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
