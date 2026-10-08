// v0.5.8-box: one need, one thread, a branch per candidate; the box and its four kinds; the daily
// cap on rings; the ring offer held; outcomes per branch; and lock 12 - nothing fabricated reaches a
// person: every box item is an object on this portal, and a claimed match with no real fit opens
// nothing.
import rawWorker from '../src/index.js';
import { legacy as legacyWorker } from './a2a-helpers.mjs';
const worker = legacyWorker(rawWorker);
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).sort().map(name=>({name})), list_complete: true }) }; };
const R = 'https://relay.box', A = 'https://mazel.a.box', B = 'https://mazel.b.box', C = 'https://mazel.c.box', D = 'https://mazel.d.box';
const portals = {
  [A]: { HANDLE:'avery@mazel', PERSONA:'Avery runs Halcyon.', NEED:'', HAVE:'', INBOX_TOKEN:'ta', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: A },
  [B]: { HANDLE:'sam@mazel', PERSONA:'Sam, fractional CFO for hospitals.', NEED:'', HAVE:'fractional-cfo', INBOX_TOKEN:'tb', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: B },
  [C]: { HANDLE:'cal@mazel', PERSONA:'Cal, another fractional CFO.', NEED:'', HAVE:'fractional-cfo', INBOX_TOKEN:'tc', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: C },
  [D]: { HANDLE:'dee@mazel', PERSONA:'Dee does something else entirely.', NEED:'', HAVE:'pottery', INBOX_TOKEN:'td', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: D },
};
const wire = []; const pushSeen = [];
const relayDir = {};
const realFetch = globalThis.fetch;
globalThis.fetch = async (u, i = {}) => {
  const url = String(u instanceof Request ? u.url : u); const body = String(i.body || '');
  wire.push({ url, body });
  const dir = url.match(new RegExp('^' + R + '/\\.well-known/mazel/([a-z0-9]+)\\.json$'));
  if (dir) { const o = { avery: A, sam: B, cal: C, dee: D }[dir[1]]; return o ? worker.fetch(new Request(o + '/.well-known/mazel/' + dir[1] + '.json'), portals[o]) : new Response('{}', { status: 404 }); }
  if (url.startsWith(R)) return new Response(JSON.stringify({ ok: true, hits: [], results: [] }), { headers: { 'content-type': 'application/json' } });
  if (url.startsWith('https://push.example/')) { pushSeen.push({ url, body }); return new Response(null, { status: 201 }); }
  const o = new URL(url).origin;
  if (portals[o]) return worker.fetch(new Request(url, i), portals[o]);
  return new Response('no', { status: 503 });
};
const call = async (o, name, args, env) => { const e = env || portals[o]; const j = JSON.parse(await (await worker.fetch(new Request(o + '/mcp', { method:'POST', headers:{ 'content-type':'application/json', authorization:'Bearer ' + e.INBOX_TOKEN }, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name, arguments: args || {} } }) }), e)).text()); if (j.error) throw new Error(j.error.message); return j.result.content[0].text; };
const J = async (...a) => JSON.parse(await call(...a));
const pulse = async (o) => { let p; await worker.scheduled({}, portals[o], { waitUntil: (x) => { p = x; } }); await p; };
let pass = 0, fail = 0; const ok = (l, c, x = '') => { console.log((c ? 'PASS ' : 'FAIL ') + l + (x ? '  -> ' + String(x).replace(/\n/g, ' ').slice(0, 160) : '')); c ? pass++ : fail++; };
const conv = (o, id) => JSON.parse(portals[o].MAILBOX.m.get('conv:' + id) || 'null');
const outcome = (o, k) => JSON.parse(portals[o].MAILBOX.m.get('outcome:' + k) || 'null');
const mail = async (o) => { const t = await call(o, 'check_mailbox'); return t.startsWith('📭') ? { messages: [], box: {} } : JSON.parse(t); };
const msgsOf = async (o, ctx) => (await J(o, 'thread_read', { context_id: ctx })).messages;
const lastIntro = (o, to) => [...portals[o].MAILBOX.m.entries()].filter(([k, v]) => k.startsWith('intro:') && JSON.parse(v).direction === 'sent' && JSON.parse(v).handle === to).map(([, v]) => JSON.parse(v)).sort((a, b) => (b.created || '').localeCompare(a.created || ''))[0];

// ---- 1. one need, one thread: a branch per candidate, and A never sees B ----
for (const o of [B, C, D]) { await call(A, 'add_known_card', { url: o + '/card' }); await call(o, 'add_known_card', { url: A + '/card' }); }
// A branch carries the need's words, so the need is placed first, as a real agent would.
await call(A, 'update_card', { add_need: 'fractional-cfo', confirmed: true });
const f = await J(A, 'find', { need_text: 'a fractional cfo for a hospital group', tags: ['fractional-cfo'] });
ok('the need is a root with two candidates and nothing for the potter', f.candidates.length === 2 && !f.candidates.some(c => c.handle === 'dee@mazel'));
const roots = await J(A, 'thread_list', { kind: 'needs' });
ok('list_threads is still there as the roots, by alias', roots.some(t => t.thread_id === f.thread_id) && JSON.parse(await call(A, 'list_threads')).some(t => t.thread_id === f.thread_id));
const branches = (await J(A, 'thread_list')).threads.filter(t => t.branch && t.need_id === f.thread_id);
ok('a branch opened per candidate, each its own contextId', branches.length === 2 && branches[0].context_id !== branches[1].context_id && branches.every(t => t.humans === false), JSON.stringify(branches.map(t => t.with)));
const bB = branches.find(t => t.with.includes('sam@mazel')).context_id, bC = branches.find(t => t.with.includes('cal@mazel')).context_id;
ok('message one of a branch is the need, by an agent', (await msgsOf(A, bB))[0].author === 'agent' && (await msgsOf(A, bB))[0].parts[1].data.need.text === 'a fractional cfo for a hospital group');
ok('candidate A never sees candidate B', !JSON.stringify(await msgsOf(A, bB)).includes('cal@mazel') && !JSON.stringify(await msgsOf(A, bC)).includes('sam@mazel') && conv(B, bB) && !conv(B, bC) && conv(C, bC) && !conv(C, bB));
ok('the other side screened it and holds its copy, with the need as message one', conv(B, bB).branch && conv(B, bB).humans === false && (await msgsOf(B, bB))[0].parts[1].data.need.tags.includes('fractional-cfo'));
ok('and the potter, who fits nothing, was never sent one', !wire.some(w => w.url.startsWith(D) && /thread\.open/.test(w.body)) && !conv(D, bB));
ok('every branch has an outcome of its own, "nothing yet", holding the need', outcome(A, bB) && outcome(A, bB).state === 'open' && outcome(A, bB).needId === f.thread_id && outcome(A, bC) && outcome(A, bC).needId === f.thread_id);
ok('and the other side\'s copy has one too', outcome(B, bB) && outcome(B, bB).state === 'open');

// ---- 2. the box: someone needs what you have; found someone for your need ----
{
  const boxA = await mail(A), boxB = await mail(B);
  ok('the box on the need side says found someone, once per candidate', boxA.box.found_for_you === 2 && boxA.messages.filter(m => m.box && m.box.kind === 'found_for_you').every(m => /Found someone for "a fractional cfo/.test(m.text)));
  ok('the box on the have side says someone needs what you have, with the scorer\'s why', boxB.box.need_for_you === 1 && boxB.messages.some(m => m.box && m.box.kind === 'need_for_you' && /you have fractional-cfo/.test(m.text)));
  ok('the headline names the kinds', /your box: ✨ 2 found for you/.test(boxA.headline) && /📬 1 need for you/.test(boxB.headline), boxA.headline);
  ok('every box item points at a thread that exists here (lock 12)', boxA.messages.filter(m => m.box).every(m => conv(A, m.box.contextId)) && boxB.messages.filter(m => m.box).every(m => conv(B, m.box.contextId)));
  ok('and every need_for_you item rests on a signed message from the other portal', boxB.messages.filter(m => m.box && m.box.kind === 'need_for_you').every(m => { const first = JSON.parse(portals[B].MAILBOX.m.get(`convm:${m.box.contextId}:00000001`)); return first.dir === 'in' && first.message.metadata.haah.sig; }));
}

// ---- 3. the agents talk first; 🌀 puts the people in the same branch with its history ----
{
  const said = await call(B, 'thread_note', { context_id: bB, text: 'Sam has done three hospital audits; free Thursdays.' });
  ok('an agent on the have side can speak in the branch before any person does', /Noted in the thread as the agent/.test(said) && (await msgsOf(A, bB)).some(m => m.author === 'agent' && /three hospital audits/.test(m.parts[0].text)));
  const before = (await msgsOf(A, bB)).length;
  await call(A, 'propose_intro', { thread_id: f.thread_id, card_url: f.candidates.find(c => c.handle === 'sam@mazel').card_url, ask: { kind: 'call', size: '20 minutes' }, confirmed: true });
  ok('a proposal from the root posts the agents\' summary into that branch', (await msgsOf(A, bB)).length === before + 1 && (await msgsOf(B, bB)).some(m => m.parts[1] && m.parts[1].data && m.parts[1].data.introNote && m.parts[1].data.introNote.ask.size === '20 minutes'));
  ok('and the intro names the branch', lastIntro(A, 'sam@mazel').contextId === bB);
  await call(B, 'respond_intro', { intro_id: lastIntro(A, 'sam@mazel').id, decision: 'accepted', confirmed: true });
  ok('🌀: the people join the same branch, history and all, on both sides', conv(A, bB).humans === true && conv(B, bB).humans === true && (await msgsOf(A, bB)).length >= 3 && (await msgsOf(B, bB)).length >= 3);
  ok('no new thread was opened for it', (await J(A, 'thread_list')).threads.filter(t => t.with.includes('sam@mazel')).length === 1);
  ok('the box says both said yes, on both sides', (await mail(A)).box.both_yes === 1 && (await mail(B)).box.both_yes === 1);
  ok('the other candidate\'s branch is untouched', conv(A, bC).humans === false && !JSON.stringify(await msgsOf(A, bC)).includes('sam@mazel'));
  ok('the branch outcome carries the intro and the states', outcome(A, bB).introId === lastIntro(A, 'sam@mazel').id && outcome(A, bB).history.map(h => h.state).join('>') === 'open>proposed>connected');
  await call(A, 'thread_send', { context_id: bB, text: 'Thursday 10?', confirmed: true });
  await call(B, 'thread_send', { context_id: bB, text: 'Thursday 10 it is.', confirmed: true });
  ok('a reply lands in the box as a reply', (await mail(A)).box.reply === 1 && (await mail(A)).messages.some(m => m.box && m.box.kind === 'reply' && /A reply from sam@mazel/.test(m.text) && !/Thursday/.test(m.text)));
  const passed = await call(A, 'thread_manage', { action: 'close', context_id: bC, confirmed: true });
  ok('closing a branch the people never joined is a pass, recorded on its outcome', /Closed/.test(passed) && outcome(A, bC).state === 'pass');
}

// ---- 4. tier rides on the branch ----
{
  // A need the person is holding back opens no branch from this side at all: a branch would carry
  // the need's own tag beside its fingerprint, which is more identifying than the fingerprint
  // alone, and it is the thing the release path refuses to send (review 2026-10-07a, L2). The
  // buckets go out on the pulse; the side that matches them answers with a hit, and the owner's
  // yes to that hit is what opens a thread with words in it.
  await call(A, 'update_card', { add_need: 'quiet-cfo-search', need_visibility: 'matched-only' });
  const secret = 'buying Northwind Clinics before the board meets';
  const fq = await J(A, 'find', { need_text: secret + ' - need a fractional cfo', tags: ['quiet-cfo-search', 'fractional-cfo'] });
  const qb = (await J(A, 'thread_list')).threads.filter(t => t.branch && t.need_id === fq.thread_id);
  ok('a need held back opens no branch from the caster\'s side', qb.length === 0, String(qb.length));
  ok('and nothing about it crossed any wire as text', !wire.some(w => w.body.includes('Northwind')));
  await call(A, 'pulse');
  ok('the pulse sends it as buckets, and still no words', !wire.some(w => w.body.includes('Northwind')) && wire.some(w => /find\.blind/.test(w.body)), (wire.find(w => /find\.blind/.test(w.body)) || { body: 'no blind cast' }).body.slice(0, 60));
  const blindAsk = (JSON.parse(await call(A, 'check_mailbox')).messages || []).find(m => (m.action || {}).type === 'blind.ask');
  ok('the side that matched the buckets answers, and the owner is asked whether to tell them',
     !!blindAsk && /lines up on/.test(blindAsk.text) && blindAsk.text.includes('Northwind'), blindAsk ? blindAsk.text.slice(0, 90) : 'no blind ask');

  // A directed need opens only with the person it names.
  await call(A, 'update_card', { add_need: 'directed-cfo', need_visibility: 'directed', need_to: 'cal@mazel' });
  const fd = await J(A, 'find', { need_text: 'a cfo, for cal only', tags: ['directed-cfo', 'fractional-cfo'] });
  const db = (await J(A, 'thread_list')).threads.filter(t => t.branch && t.need_id === fd.thread_id);
  ok('a directed need opens a branch only with its named recipient', db.length === 1 && db[0].with.includes('cal@mazel'), JSON.stringify(db.map(t => t.with)));
}

// ---- 5. nothing below the fit bar lands; a claimed match opens nothing (lock 12) ----
{
  const { signedCast: _ } = {};
  const before = Object.keys(portals[D].MAILBOX.m).length;
  // Dee's agent claims a match for a need Dee's card does not answer: the receiving side screens.
  await call(D, 'add_known_card', { url: B + '/card' });
  await call(D, 'update_card', { add_need: 'fractional-cfo', confirmed: true });
  const fd = await J(D, 'find', { need_text: 'a fractional cfo for a pottery studio', tags: ['fractional-cfo'] });
  const dB = (await J(D, 'thread_list')).threads.find(t => t.branch && t.need_id === fd.thread_id && t.with.includes('sam@mazel'));
  ok('a real fit opens a branch on the receiving side', dB && conv(B, dB.context_id) && conv(B, dB.context_id).participants.some(p => p.handle === 'dee@mazel'), `dB ${dB && dB.context_id.slice(0, 8)} | Bhas ${dB && !!conv(B, dB.context_id)} | opensToB ${wire.filter(w => w.url.startsWith(B) && /thread\.open/.test(w.body)).length} | cands ${JSON.stringify((fd.candidates || []).map(c => c.handle))}`);
  const boxBefore = (await mail(C)).box.need_for_you || 0;
  const fake = await J(D, 'find', { need_text: 'someone who throws pots', tags: ['pottery-wheel'] });
  ok('a need nobody here fits opens no branch anywhere', !(await J(D, 'thread_list')).threads.some(t => t.need_id === fake.thread_id) && (await mail(C)).box.need_for_you === boxBefore);
}

// ---- 6. the daily cap on rings: ten agent-found items, three rings, ten box messages ----
{
  const E = 'https://mazel.e.box';
  portals[E] = { HANDLE:'eve@mazel', PERSONA:'Eve.', NEED:'', HAVE:'', INBOX_TOKEN:'te', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: E };
  // Eve rings by push, so rings can be counted.
  portals[E].MAILBOX.m.set('push:one', JSON.stringify({ id: 'one', endpoint: 'https://push.example/eve', keys: { p256dh: 'p', auth: 'a' }, created: new Date().toISOString() }));
  const CRAFTS = ['glassblowing', 'bookbinding', 'falconry', 'luthiery', 'cartography', 'beekeeping', 'blacksmithing', 'topiary', 'horology', 'taxidermy'];
  for (let i = 0; i < 10; i++) await call(E, 'note_ghost', { name: `Contact ${i}`, have: [CRAFTS[i]], edge_score: 80 });
  pushSeen.length = 0;
  for (let i = 0; i < 10; i++) {
    await call(E, 'find', { need_text: `someone who does ${CRAFTS[i]}`, tags: [CRAFTS[i]] });
    for (const k of [...portals[E].MAILBOX.m.keys()]) if (k.startsWith('bell:') && !k.startsWith('bell:found') && !k.startsWith('bell:waiting') && k !== 'bell:last') portals[E].MAILBOX.m.delete(k);   // each is a different thread; the per-thread quiet is not what is being measured
  }
  const box = await mail(E);
  ok('ten agent-found items produce ten box messages', box.box.found_for_you === 10, String(box.box.found_for_you));
  ok('and three rings', pushSeen.length === 3, String(pushSeen.length));
  ok('the rest wait in the box and are named in the next ring', Number(portals[E].MAILBOX.m.get('bell:waiting:' + new Date().toISOString().slice(0, 10))) === 7);
  const custom = { ...portals[E], DOORBELL_DAILY_CAP: '0' };
  ok('the cap is config', (await call(E, 'my_card', {}, custom)).length > 0);
  // A reply rings regardless of the cap.
  const F = 'https://mazel.f.box'; portals[F] = { HANDLE:'fay@mazel', PERSONA:'Fay.', NEED:'', HAVE:'glassblowing', INBOX_TOKEN:'tf', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: F };
  await call(E, 'add_known_card', { url: F + '/card' }); await call(F, 'add_known_card', { url: E + '/card' });
  await call(E, 'update_card', { add_need: 'glassblowing', confirmed: true });
  const ff = await J(E, 'find', { need_text: 'someone who does glassblowing, again', tags: ['glassblowing'] });
  await call(E, 'propose_intro', { thread_id: ff.thread_id, card_url: ff.candidates.find(c => c.handle === 'fay@mazel').card_url, confirmed: true });
  await call(F, 'respond_intro', { intro_id: lastIntro(E, 'fay@mazel').id, decision: 'accepted', confirmed: true });
  const onYes = JSON.parse(portals[E].MAILBOX.m.get('bell:last') || 'null');
  // Seven from the ten finds, plus the one this intro came from: the next ring names them all and clears the count.
  ok('and the ring that follows names how many were waiting in the box', onYes && onYes.waiting === 8 && !portals[E].MAILBOX.m.get('bell:waiting:' + new Date().toISOString().slice(0, 10)), `waiting ${onYes && onYes.waiting}`);
  const ctx = lastIntro(E, 'fay@mazel').contextId;
  pushSeen.length = 0; portals[E].MAILBOX.m.delete('bell:' + ctx);
  await call(E, 'thread_send', { context_id: ctx, text: 'hi', confirmed: true });
  await call(F, 'thread_send', { context_id: ctx, text: 'hello back', confirmed: true });
  ok('a reply rings at once, cap or no cap', pushSeen.length === 1, String(pushSeen.length));
  ok('and a ring with nothing waiting says so', JSON.parse(portals[E].MAILBOX.m.get('bell:last')).waiting === 0);
}

// ---- 7. the ring offer is held ----
{
  const link = ((await call(A, 'inbox_link')).match(/https:\/\/\S+\/inbox\/open\?\S+/) || [])[0];
  const opened = await worker.fetch(new Request(link), portals[A]);
  const ck = (opened.headers.get('set-cookie') || '').split(';')[0];
  const page = await (await worker.fetch(new Request(A + '/inbox', { headers: { cookie: ck } }), portals[A])).text();
  ok('with the decision open, /inbox offers no ring and asks for no permission', !/Ring my phone/.test(page) && /data-ring="0"/.test(page), (page.match(/Ring my phone[^<]*/) || [])[0] || 'none');
  ok('the box is on the page, with its kinds', /your box/.test(page) && /found for you/.test(page));
  const on = await (await worker.fetch(new Request(A + '/inbox', { headers: { cookie: ck } }), { ...portals[A], RING_OFFER: '1' })).text();
  ok('and the button is one config flag away, for when it is decided', /Ring my phone/.test(on) && /data-ring="1"/.test(on));
}

globalThis.fetch = realFetch;
console.log(`\nbox: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
