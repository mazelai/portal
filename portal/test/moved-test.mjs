// v0.5.7.1: what Avery's first real use found. A relay hit that only matched a word reached him;
// his answer to an intro sat five days because the peer had moved; his test peer showed up as
// relationship history; and the copy talked in ids.
import rawWorker from '../src/index.js';
import { legacy as legacyWorker } from './a2a-helpers.mjs';
const worker = legacyWorker(rawWorker);
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).sort().map(name=>({name})), list_complete: true }) }; };
const R = 'https://relay.moved', A = 'https://mazel.a.moved', B = 'https://mazel.b.moved', B2 = 'https://mazel.b-new.moved', S = 'https://mazel.stranger.moved', T = 'https://mazel.testpeer.moved', GOV = 'https://mazel.gov.moved', CFO = 'https://mazel.cfo.moved';
const portals = {
  [A]: { HANDLE:'avery@mazel', PERSONA:'Avery runs Halcyon.', NEED:'', HAVE:'managed-ai-delivery', INBOX_TOKEN:'ta', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: A },
  [B]: { HANDLE:'sam@mazel', PERSONA:'Sam, fractional CFO.', NEED:'', HAVE:'fractional-cfo', INBOX_TOKEN:'tb', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: B },
  [S]: { HANDLE:'stranger@mazel', PERSONA:'Runs AI governance programs for banks.', NEED:'', HAVE:'ai-governance-programs', INBOX_TOKEN:'ts', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: S },
  [T]: { HANDLE:'testpeer@mazel', PERSONA:'The test peer.', NEED:'', HAVE:'sailing-atlantic', INBOX_TOKEN:'tt', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: T },
  // Two real portals behind the relay's answers: a relay result is only a candidate once the card
  // at that address is fetched and found to be for the handle claimed (2026-09-28b H3).
  [GOV]: { HANDLE:'gov@mazel', PERSONA:'AI governance programs for hospital boards.', NEED:'', HAVE:'ai-governance-programs', INBOX_TOKEN:'tg', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: GOV },
  [CFO]: { HANDLE:'cfo@mazel', PERSONA:'A fractional CFO.', NEED:'', HAVE:'fractional-cfo', INBOX_TOKEN:'tcf', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: CFO },
};
let bMoved = false;                 // when true, B answers at B2 and its old door is dead
const wire = [];
const realFetch = globalThis.fetch;
globalThis.fetch = async (u, i = {}) => {
  const url = String(u instanceof Request ? u.url : u);
  wire.push({ url, body: String(i.body || '') });
  // The directory: name@mazel resolves at the relay, which serves the record the portal publishes.
  const dir = url.match(new RegExp('^' + R + '/\\.well-known/mazel/([a-z0-9]+)\\.json$'));
  if (dir) {
    const who = { avery: A, sam: bMoved ? B2 : B, stranger: S, testpeer: T, gov: GOV, cfo: CFO }[dir[1]];
    if (!who) return new Response('{}', { status: 404 });
    return worker.fetch(new Request(who + '/.well-known/mazel/' + dir[1] + '.json'), portals[who] || { ...portals[B], PORTAL_ORIGIN: B2 });
  }
  if (url.startsWith(R)) return new Response(JSON.stringify({ ok: true, hits: [], results: [] }), { headers: { 'content-type': 'application/json' } });
  const o = new URL(url).origin;
  if (o === B && bMoved) return new Response('gone', { status: 502 });
  if (o === B2) return bMoved ? worker.fetch(new Request(url, i), { ...portals[B], PORTAL_ORIGIN: B2 }) : new Response('not yet', { status: 502 });
  if (portals[o]) return worker.fetch(new Request(url, i), portals[o]);
  return new Response('no', { status: 503 });
};
const call = async (o, name, args, env) => {
  const e = env || portals[o];
  const j = JSON.parse(await (await worker.fetch(new Request(o + '/mcp', { method:'POST', headers:{ 'content-type':'application/json', authorization:'Bearer ' + e.INBOX_TOKEN }, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name, arguments: args || {} } }) }), e)).text());
  if (j.error) throw new Error(j.error.message);
  return j.result.content[0].text;
};
const J = async (...a) => JSON.parse(await call(...a));
const pulse = async (o, env) => { let p; await worker.scheduled({}, env || portals[o], { waitUntil: (x) => { p = x; } }); await p; };
let pass = 0, fail = 0; const ok = (l, c, x = '') => { console.log((c ? 'PASS ' : 'FAIL ') + l + (x ? '  -> ' + String(x).replace(/\n/g, ' ').slice(0, 140) : '')); c ? pass++ : fail++; };
const mail = async (o) => { const t = await call(o, 'check_mailbox'); return t.startsWith('📭') ? [] : JSON.parse(t).messages; };

// ---- 1. the fit bar holds for hits from the world ----
// Avery's live case: the relay answered a fractional-CFO, hospital-audit-prep need with a card whose
// only have was ai-governance-programs, on the strength of a gloss that shared two words with the
// need. It reached him, and his agent called it weak.
{
  let relayResults = [];
  const prev = globalThis.fetch;
  globalThis.fetch = async (u, i) => { const url = String(u instanceof Request ? u.url : u); if (url.startsWith(R + '/search')) return new Response(JSON.stringify({ results: relayResults }), { headers: { 'content-type': 'application/json' } }); return prev(u, i); };
  const f = await J(A, 'find', { need_text: 'a fractional cfo who has done audit prep for a hospital group', tags: ['fractional-cfo', 'audit-prep'] });
  const needId = f.thread_id;
  await call(GOV, 'update_card', { gloss_tag: 'ai-governance-programs', gloss_text: 'AI audit programs for hospital boards', confirmed: true });
  const weak = { handle: 'gov@mazel', cardUrl: GOV + '/card', rpc: GOV + '/a2a', publicKey: 'k1', have: ['ai-governance-programs'], glosses: { 'ai-governance-programs': 'AI audit programs for hospital boards' }, needText: '' };
  const strong = { handle: 'cfo@mazel', cardUrl: CFO + '/card', rpc: CFO + '/a2a', publicKey: 'k2', have: ['fractional-cfo'], glosses: {}, needText: '' };
  relayResults = [weak];
  await pulse(A);
  let cands = (await J(A, 'list_threads')).find(t => t.thread_id === needId).candidates;
  ok('a relay hit that only a gloss made stays silent', !cands.some(c => c.handle === 'gov@mazel'), JSON.stringify(cands.map(c => [c.handle, c.matched])));
  ok('and nothing about it reached the owner', !(await mail(A)).some(m => /gov@mazel/.test(m.text || '')));
  relayResults = [weak, strong];
  await pulse(A);
  cands = (await J(A, 'list_threads')).find(t => t.thread_id === needId).candidates;
  ok('a relay hit whose have answers the need comes through', cands.some(c => c.handle === 'cfo@mazel') && !cands.some(c => c.handle === 'gov@mazel'), JSON.stringify(cands.map(c => [c.handle, c.matched])));
  ok('and the owner hears about that one, in words', (await mail(A)).some(m => /new from the world/.test(m.text || '') && /cfo@mazel/.test(m.text)));
  // The same bar for a gossip hit: a stranger whose real card carries the need's tag is a candidate,
  // one that merely claims it is not, because the card is fetched and judged as it is.
  await call(A, 'add_known_card', { url: S + '/card', tier: 'tribe' });
  await call(S, 'update_card', { add_have: 'audit-prep', confirmed: true });
  await call(A, 'add_known_card', { url: S + '/card', tier: 'tribe' });   // refreshed by hand: what a held card carries is what its card says now
  const f2 = await J(A, 'find', { need_text: 'audit prep for a hospital group', tags: ['audit-prep'] });
  relayResults = [];
  await pulse(A);
  const c2 = (await J(A, 'list_threads')).find(t => t.thread_id === f2.thread_id).candidates;
  ok('a held card, refreshed, is a candidate on what it now carries', c2.some(c => c.handle === 'stranger@mazel' && c.matched.includes('audit-prep')), JSON.stringify(c2.map(c => [c.handle, c.matched])));
  globalThis.fetch = prev;
}

// ---- 2. a portal that moved ----
{
  await call(B, 'add_known_card', { url: A + '/card' });
  await call(A, 'add_known_card', { url: B + '/card' });
  // Sam proposes to Avery; Avery answers after Sam's portal has moved.
  await call(B, 'update_card', { add_need: 'managed-ai-delivery', confirmed: true });   // a proposal carries the need's words
  const f = await J(B, 'find', { need_text: 'someone who builds and runs AI in my own cloud', tags: ['managed-ai-delivery'] });
  const pr = await call(B, 'propose_intro', { thread_id: f.thread_id, card_url: f.candidates[0].card_url, confirmed: true });
  ok('a proposal reads as a name and a why, never an id', /^Proposed to avery@mazel: /.test(pr) && !/[a-f0-9]{32}/.test(pr), pr.slice(0, 100));
  const introId = ((await J(B, 'list_intros'))[0] || {}).intro_id;
  bMoved = true;                                        // Sam's portal moves: old door dead, directory updated
  const wireAt = wire.length;
  const ans = await call(A, 'respond_intro', { intro_id: introId, decision: 'accepted', confirmed: true });
  ok('a failed send looks the handle up again and gets through at the new door', /both said yes/.test(ans) && /portal had moved/.test(ans), ans.slice(0, 120));
  ok('the directory was asked, then the new door', wire.slice(wireAt).some(w => /\.well-known\/mazel\/sam\.json/.test(w.url)) && wire.slice(wireAt).some(w => w.url.startsWith(B2 + '/a2a') && /intro\.respond/.test(w.body)));
  ok('and the card now carries the new address', (await J(A, 'list_known_cards')).find(c => c.handle === 'sam@mazel').rpc === B2 + '/a2a');
  ok('the thread opened with them', [...portals[A].MAILBOX.m.entries()].filter(([k]) => k.startsWith('conv:')).map(([, v]) => JSON.parse(v)).some(c => c.humans && c.participants.some(p => p.handle === 'sam@mazel')));
  ok('no user-facing line in that carried an id', !/[a-f0-9]{32}/.test(ans));
  // A move the directory does not know about yet: queued, retried with re-resolution, and only then the owner.
  await call(A, 'update_card', { add_need: 'fractional-cfo', confirmed: true });   // a proposal carries the need's words
  const f2 = await J(A, 'find', { need_text: 'a fractional cfo', tags: ['fractional-cfo'] });
  const gone = new Set();
  const realB2 = bMoved; bMoved = false; // Sam is unreachable everywhere for a while
  const oldFetch = globalThis.fetch;
  globalThis.fetch = async (u, i) => { const url = String(u instanceof Request ? u.url : u); if (url.startsWith(B) || url.startsWith(B2) || /mazel\/sam\.json/.test(url)) { wire.push({ url, body: String((i || {}).body || '') }); return new Response('down', { status: 502 }); } return oldFetch(u, i); };
  const pr2 = await call(A, 'propose_intro', { thread_id: f2.thread_id, card_url: f2.candidates.find(c => c.handle === 'sam@mazel').card_url, confirmed: true });
  ok('when the lookup fails too, the proposal queues and says nothing to do', /queued and will be retried/.test(pr2) && /you will hear only if that fails/.test(pr2), pr2.slice(0, 120));
  const ob = () => [...portals[A].MAILBOX.m.entries()].filter(([k]) => k.startsWith('outbox:')).map(([, v]) => JSON.parse(v));
  ok('it sits in the outbox as an action', ob().length === 1 && ob()[0].kind === 'action' && ob()[0].what === 'intro.propose');
  ok('and no owner-facing failure was written', !(await mail(A)).some(m => /could not be delivered/.test(m.text || '')));
  // Sam comes back at the new address, and the directory knows.
  globalThis.fetch = oldFetch; bMoved = true;
  for (const [k, v] of portals[A].MAILBOX.m.entries()) if (k.startsWith('outbox:')) { const o = JSON.parse(v); o.nextAt = new Date(Date.now() - 1000).toISOString(); portals[A].MAILBOX.m.set(k, JSON.stringify(o)); }
  await pulse(A);
  ok('the pulse retries through the directory and the proposal lands at the new door', ob().length === 0, String(ob().length));
  const introB = (await J(B, 'list_intros', {}, { ...portals[B], PORTAL_ORIGIN: B2 })).find(i => i.direction === 'received' && /fractional cfo/.test(i.why || ''));
  ok('Sam\'s portal has it', !!introB, JSON.stringify(introB || {}).slice(0, 80));
  ok('the owner is told it got through, in words', (await mail(A)).some(m => /finally got through/.test(m.text || '')));
  // Seven days of nothing: then, and only then, the owner hears.
  globalThis.fetch = async (u, i) => { const url = String(u instanceof Request ? u.url : u); if (url.startsWith(B) || url.startsWith(B2) || /mazel\/sam\.json/.test(url)) return new Response('down', { status: 502 }); return oldFetch(u, i); };
  await call(A, 'update_card', { add_need: 'fractional-cfo', confirmed: true });   // a proposal carries the need's words
  const f3 = await J(A, 'find', { need_text: 'a cfo who knows hospitals', tags: ['fractional-cfo', 'hospitals'] });   // a new need, so a new proposal
  const pr3 = await call(A, 'propose_intro', { thread_id: f3.thread_id, card_url: f3.candidates.find(c => c.handle === 'sam@mazel').card_url, confirmed: true });
  ok('a second proposal to an unreachable portal queues too', /queued and will be retried/.test(pr3), pr3.slice(0, 100));
  for (const [k, v] of portals[A].MAILBOX.m.entries()) if (k.startsWith('outbox:')) { const o = JSON.parse(v); o.nextAt = new Date(Date.now() - 1000).toISOString(); o.firstAt = new Date(Date.now() - 8 * 24 * 3600 * 1000).toISOString(); portals[A].MAILBOX.m.set(k, JSON.stringify(o)); }
  await pulse(A);
  ok('after seven days the owner is told once, and the queue is clear', ob().length === 0 && (await mail(A)).some(m => /could not be delivered in seven days/.test(m.text || '') && /looking them up again found no new address/.test(m.text)), 'outbox ' + JSON.stringify(ob().map(o => [o.kind, o.what, o.attempts, o.lastReason])) + ' | mail ' + JSON.stringify((await mail(A)).filter(m => m.mine).map(m => (m.text || '').slice(0, 70))));
  globalThis.fetch = oldFetch;
}

// ---- 3. test traffic is marked, kept out of summaries, and clearable ----
{
  await call(A, 'add_known_card', { url: T + '/card' });
  await call(T, 'add_known_card', { url: A + '/card' });
  await call(A, 'update_card', { add_need: 'sailing-atlantic', confirmed: true });   // a proposal carries the need's words
  const f = await J(A, 'find', { need_text: 'someone who has sailed the atlantic', tags: ['sailing-atlantic'] });
  const pr = await call(A, 'propose_intro', { thread_id: f.thread_id, card_url: f.candidates[0].card_url, confirmed: true });
  const introId = (await J(A, 'list_intros')).find(i => i.with === 'testpeer@mazel').intro_id;
  await call(T, 'respond_intro', { intro_id: introId, decision: 'accepted', confirmed: true });
  const ctx = (await J(A, 'thread_list')).threads.find(t => t.with.includes('testpeer@mazel')).context_id;
  await call(T, 'thread_send', { context_id: ctx, text: 'test hello', confirmed: true });
  ok('intros with the test peer are marked test', (await J(A, 'list_intros')).find(i => i.with === 'testpeer@mazel').test === true);
  ok('and real ones are not', (await J(A, 'list_intros')).filter(i => i.with === 'sam@mazel').every(i => i.test === undefined));
  ok('threads with the test peer are marked test', (await J(A, 'thread_list')).threads.find(t => t.with.includes('testpeer@mazel')).test === true);
  ok('and so are the cards and the mailbox items', (await J(A, 'list_known_cards')).find(c => c.handle === 'testpeer@mazel').test === true && (await mail(A)).filter(m => m.doorbell && /testpeer/.test(m.text)).every(m => m.test === true));
  const tools = (await (await worker.fetch(new Request(A + '/mcp?token=ta', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list' }) }), portals[A])).json()).result.tools;
  for (const h of JSON.parse(await call(A, 'more_tools')).tools) tools.push(h);
  ok('the listings tell the agent to leave test traffic out of summaries, listed, merged and hidden', ['list_intros', 'thread_list', 'check_mailbox', 'cards'].every(n => /leave them out of any summary/.test(tools.find(t => t.name === n).description)), ['list_intros', 'thread_list', 'check_mailbox', 'list_known_cards', 'cards'].filter(n => !/leave them out of any summary/.test((tools.find(t => t.name === n) || {}).description || '')).join(','));
  const notYet = await call(A, 'clear_test_history');
  ok('clearing asks first and says what would go', /^Not cleared/.test(notYet) && /1 intros, 1 threads/.test(notYet), notYet.slice(0, 120));
  const wireAt = wire.length;
  const cleared = await call(A, 'clear_test_history', { confirmed: true });
  ok('and then clears it', /^Cleared/.test(cleared) && !(await J(A, 'list_intros')).some(i => i.with === 'testpeer@mazel') && (await call(A, 'thread_list')).startsWith('No conversations') === false && !(await J(A, 'thread_list')).threads.some(t => t.with.includes('testpeer@mazel')) && !(await J(A, 'list_known_cards')).some(c => c.handle === 'testpeer@mazel'));
  ok('the outcome went with it, and the real ones stayed', !portals[A].MAILBOX.m.has('outcome:' + introId) && [...portals[A].MAILBOX.m.keys()].some(k => k.startsWith('outcome:')));
  ok('nothing was sent and nobody told', wire.length === wireAt);
  ok('the test portal itself is untouched', (await J(T, 'thread_list')).count === 1);
  const custom = { ...portals[A], TEST_HANDLES: 'sam@mazel' };
  ok('which handles count as test is config', (await J(A, 'list_intros', {}, custom)).some(i => i.with === 'sam@mazel' && i.test === true));
}

// ---- 4. the copy: names and the why ----
{
  const tools = (await (await worker.fetch(new Request(A + '/mcp?token=ta', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list' }) }), portals[A])).json()).result.tools;
  const f = await J(A, 'find', { need_text: 'a cfo who knows hospitals', tags: ['fractional-cfo'] });
  ok('a find that stays open says so in words, not with a thread id', !/Thread [a-f0-9]{8}/.test(JSON.stringify(f)) && !/thread [a-f0-9]{8}/.test(f.note || ''), f.note || f.headline);
  const inv = (await mail(B)).find(m => (m.action || {}).type === 'intro.propose');
  ok('what the other agent reads names the sender and the why, not an intro id', inv && /Intro proposal from avery@mazel/.test(inv.text) && !/\(intro [a-f0-9]+\)/.test(inv.text), inv && inv.text.slice(0, 100));
}

globalThis.fetch = realFetch;
console.log(`\nmoved: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
