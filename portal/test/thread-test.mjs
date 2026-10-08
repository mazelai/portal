// Threads between people (§4.3b): after 🌀 a conversation opens on both portals, built from A2A's
// own parts, and lives nowhere else. This suite is one long argument that a person's words go
// exactly where they meant them to, verbatim, and to nowhere else.
import rawWorker from '../src/index.js';
import { legacy as legacyWorker } from './a2a-helpers.mjs';
const worker = legacyWorker(rawWorker);
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).sort().map(name=>({name})), list_complete: true }) }; };
const R = 'https://relay.thread', A = 'https://mazel.a.thread', B = 'https://mazel.b.thread', C = 'https://mazel.c.thread', D = 'https://mazel.d.thread';
const portals = {
  [A]: { HANDLE:'avery@mazel', PERSONA:'Avery runs Halcyon.', NEED:'', HAVE:'', INBOX_TOKEN:'ta', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: A },
  [B]: { HANDLE:'sam@mazel', PERSONA:'Sam is a fractional CFO for healthcare companies.', NEED:'', HAVE:'fractional-cfo', INBOX_TOKEN:'tb', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: B },
  [C]: { HANDLE:'rae@mazel', PERSONA:'Rae routes things.', NEED:'', HAVE:'', INBOX_TOKEN:'tc', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: C },
  [D]: { HANDLE:'dee@mazel', PERSONA:'Dee, to be blocked.', NEED:'', HAVE:'', INBOX_TOKEN:'td', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: D },
};
const wire = [];          // every outbound body, by destination
const relaySeen = [];     // everything that reached the relay
const pushSeen = [];      // web push deliveries
let down = new Set();     // portals that are unreachable right now
const realFetch = globalThis.fetch;
globalThis.fetch = async (u, i = {}) => {
  const url = String(u instanceof Request ? u.url : u);
  const body = String(i.body || '');
  wire.push({ url, body });
  if (url.startsWith(R)) { relaySeen.push({ url, body }); return new Response(JSON.stringify({ ok: true, hits: [], results: [] }), { headers: { 'content-type': 'application/json' } }); }
  if (url.startsWith('https://push.example/')) { pushSeen.push({ url, headers: i.headers || {}, body }); return new Response(null, { status: 201 }); }
  const o = new URL(url).origin;
  if (down.has(o)) return new Response('unreachable', { status: 503 });
  if (portals[o]) return worker.fetch(new Request(url, i), portals[o]);
  return new Response('no', { status: 503 });
};
const call = async (o, name, args) => {
  const j = JSON.parse(await (await worker.fetch(new Request(o + '/mcp', { method:'POST', headers:{ 'content-type':'application/json', authorization:'Bearer ' + portals[o].INBOX_TOKEN }, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name, arguments: args || {} } }) }), portals[o])).text());
  if (j.error) throw new Error(j.error.message);
  return j.result.content[0].text;
};
const J = async (...a) => JSON.parse(await call(...a));
const pulse = async (o) => { let p; await worker.scheduled({}, portals[o], { waitUntil: (x) => { p = x; } }); await p; };
let pass = 0, fail = 0; const ok = (l, c, x = '') => { console.log((c ? 'PASS ' : 'FAIL ') + l + (x ? '  -> ' + String(x).replace(/\n/g, ' ').slice(0, 140) : '')); c ? pass++ : fail++; };
const conv = (o, id) => JSON.parse(portals[o].MAILBOX.m.get('conv:' + id) || 'null');
const lastIntro = (o, to) => [...portals[o].MAILBOX.m.entries()].filter(([k, v]) => k.startsWith('intro:') && JSON.parse(v).direction === 'sent' && JSON.parse(v).handle === to).map(([, v]) => JSON.parse(v)).sort((a, b) => (b.created || '').localeCompare(a.created || ''))[0].id;
const outcome = (o, k) => JSON.parse(portals[o].MAILBOX.m.get('outcome:' + k) || 'null');

// ---- 1. an intro that connects opens a thread on both sides ----
await call(A, 'add_known_card', { url: B + '/card' });
await call(B, 'add_known_card', { url: A + '/card' });
// A need travels only once its owner has placed it (the core-loop rule), so every cast here does
// what an agent does: ask, write the answer, then search.
const placed = {};
const cast = async (o, args) => {
  const tag = args.tags[0];
  if (!placed[o + tag]) { try { await call(o, 'update_card', { add_need: tag, confirmed: true }); } catch { /* already there, or capped */ } placed[o + tag] = true; }
  return J(o, 'find', args);
};
const found = await cast(A, { need_text: 'a fractional cfo who knows hospital systems', tags: ['fractional-cfo'] });
const cand = found.candidates.find(c => c.handle === 'sam@mazel');
const proposed = await call(A, 'propose_intro', { thread_id: found.thread_id, card_url: cand.card_url, ask: { kind: 'call', size: '20 minutes' }, confirmed: true });
const introId = lastIntro(A, 'sam@mazel');
ok('an intro is proposed with an ask and its size', /^Proposed to sam@mazel: /.test(proposed) && !/[a-f0-9]{32}/.test(proposed), proposed.slice(0, 60));
ok('and an outcome object exists for it from day one, on both portals', outcome(A, introId) && outcome(A, introId).state === 'proposed' && outcome(B, introId) && outcome(B, introId).state === 'proposed', JSON.stringify(outcome(A, introId) || {}).slice(0, 100));
ok('with the fields the ledger cannot reconstruct later', (() => { const o = outcome(A, introId); return o.origin === 'cast' && o.needTags.includes('fractional-cfo') && o.haveTags.includes('fractional-cfo') && o.history[0].state === 'proposed' && o.history[0].at; })());
const inbox = await J(B, 'check_mailbox');
const inv = inbox.messages.find(m => (m.action || {}).type === 'intro.propose');
ok('the ask travels in the proposal', inv.action.ask && inv.action.ask.kind === 'call' && /20 minutes/.test(inv.action.ask.size), JSON.stringify(inv.action.ask));
{ const pre = JSON.parse(await call(A, 'thread_list')); ok('before 🌀 the branch exists and the agents are in it, not the people', pre.count === 1 && pre.threads[0].branch === true && pre.threads[0].humans === false, JSON.stringify(pre.threads[0])); }
await call(B, 'respond_intro', { intro_id: introId, decision: 'accepted', confirmed: true });
const la = await J(A, 'thread_list'), lb = await J(B, 'thread_list');
ok('🌀 opens one thread on each side', la.count === 1 && lb.count === 1);
const ctx = la.threads[0].context_id;
ok('with the same contextId', lb.threads[0].context_id === ctx, ctx);
ok('state open, and it is the branch the agents opened, with the people now in it', la.threads[0].state === 'open' && lb.threads[0].state === 'open' && la.threads[0].origin === 'need' && la.threads[0].humans === true && lb.threads[0].humans === true, JSON.stringify([la.threads[0].origin, la.threads[0].humans, lb.threads[0].humans]));
const ra = await J(A, 'thread_read', { context_id: ctx }), rb = await J(B, 'thread_read', { context_id: ctx });
ok('message one is the need, by an agent, the same on both copies', ra.messages[0].author === 'agent' && rb.messages[0].author === 'agent' && ra.messages[0].parts[1].data.need.text === rb.messages[0].parts[1].data.need.text && /fractional cfo/.test(ra.messages[0].parts[0].text), ra.messages[0].parts[0].text);
const noteA = ra.messages.find(m => m.parts[1] && m.parts[1].data && m.parts[1].data.introNote), noteB = rb.messages.find(m => m.parts[1] && m.parts[1].data && m.parts[1].data.introNote);
ok('the intro note is the agents\' summary posted into the branch, on both copies, with the history before it', noteA && noteB && noteA.author === 'agent' && noteA.parts[1].data.introNote.why === noteB.parts[1].data.introNote.why && noteA.seq > 1, noteA && noteA.parts[0].text);
ok('and it carries the ask and its size', noteA.parts[1].data.introNote.ask.kind === 'call' && /20 minutes/.test(noteA.parts[0].text));
ok('direct match: the side with the need writes first', ra.first_writer === 'avery@mazel' && ra.pending.kind === 'first_message' && ra.pending.who === 'me' && rb.pending.who === 'them');
ok('the first writer is told to draft with calendar slots, for the person to send with one tap', /calendar/.test(ra.pending.hint) && /one tap/.test(ra.pending.hint));
ok('the intro\'s outcome moved proposed → connected on both', outcome(A, introId).history.map(h => h.state).join('>') === 'proposed>connected' && outcome(B, introId).history.map(h => h.state).join('>') === 'proposed>connected', outcome(A, introId).history.map(h => h.state).join('>'));
ok('and the branch has an outcome of its own: open, proposed, connected, holding the need and the intro', (() => { const o = outcome(A, ctx); return o && o.needId === found.thread_id && o.introId === introId && o.history.map(h => h.state).join('>') === 'open>proposed>connected'; })(), JSON.stringify(outcome(A, ctx) || {}).slice(0, 160));

// ---- 2. subscribe: A2A push notification config, no bearer anywhere ----
ok('no push-notification config is registered or stored on either side: delivery is the signed message at the door', !wire.some(w => /CreateTaskPushNotificationConfig/.test(w.body)) && ![...portals[B].MAILBOX.m.keys()].some(k => k.startsWith('pulse:')));

// ---- 3. a person's words, verbatim, straight to the other portal ----
const odd = 'Hi Sam — "twenty minutes"?  Tue 10:00,  Wed 14:30\n\nor Thu 9.\t— A';
const notYet = await call(A, 'thread_send', { context_id: ctx, text: odd });
ok('sending asks for the person\'s yes and shows exactly what will go', /^Not sent/.test(notYet) && notYet.includes(JSON.stringify([odd])));
const sent = await call(A, 'thread_send', { context_id: ctx, text: odd, confirmed: true });
ok('then sends it', /^Sent, as written/.test(sent) && /sam@mazel: delivered/.test(sent), sent.slice(0, 80));
const rb2 = await J(B, 'thread_read', { context_id: ctx });
const got = rb2.messages.find(m => m.author === 'human');
ok('it lands on the other portal exactly as written, whitespace and all', got && got.parts[0].text === odd && got.from === 'avery@mazel' && got.direction === 'in');
ok('state is sent on both sides', conv(A, ctx).state === 'sent' && conv(B, ctx).state === 'sent');
ok('the message is signed with the sender\'s key and the receiver checked it', (() => { const raw = [...portals[B].MAILBOX.m.entries()].find(([k]) => k.startsWith('convm:' + ctx + ':00000002')); return raw && JSON.parse(raw[1]).message.metadata.haah.sig && JSON.parse(raw[1]).message.metadata.haah.envelope === null; })());
ok('the box on B has a reply, with a name and no words', (await J(B, 'check_mailbox')).messages.some(m => m.box && m.box.kind === 'reply' && /A reply from avery@mazel/.test(m.text) && !m.text.includes('twenty') && !m.text.includes('Tue')));
const reply = 'Thu 9 works. Calendar link inside: https://cal.example/sam';
await call(B, 'thread_send', { context_id: ctx, text: reply, confirmed: true });
ok('a reply moves both sides to replied', conv(A, ctx).state === 'replied' && conv(B, ctx).state === 'replied');
ok('and the outcome records sent then replied, with times', outcome(A, introId).history.map(h => h.state).slice(-2).join('>') === 'sent>replied' && outcome(A, introId).history.every(h => h.at));
ok('contact details travel inside the thread when a person chooses, never required', (await J(A, 'thread_read', { context_id: ctx })).messages.some(m => m.parts[0].text === reply));

// ---- 4. the agent's own note is labelled as such ----
await call(A, 'thread_note', { context_id: ctx, text: 'Booked: Thu 9, 20 minutes. Invite sent.' });
const rb3 = await J(B, 'thread_read', { context_id: ctx });
ok('an agent note arrives marked agent, never mistaken for the person', rb3.messages.some(m => m.author === 'agent' && /Booked/.test(m.parts[0].text)));
ok('and it did not change the thread\'s state', conv(B, ctx).state === 'replied');

// ---- 5. parts: file and data, and the limits, each saying what happens ----
const small = btoa('hello file');
await call(A, 'thread_send', { context_id: ctx, text: 'deck attached', file: { name: 'deck.txt', mimeType: 'text/plain', bytes: small }, data: { slots: ['Thu 9'] }, confirmed: true });
const rb4 = await J(B, 'thread_read', { context_id: ctx });
const withFile = rb4.messages.find(m => m.parts.some(p => p.file));
ok('a message can carry text, a file and data parts together, and all arrive', withFile && withFile.parts.length === 3 && withFile.parts[1].file.bytes === small && withFile.parts[2].data.slots[0] === 'Thu 9');
const before = wire.length;
const big = 'A'.repeat(400 * 1024);
const tooBig = await call(A, 'thread_send', { context_id: ctx, file: { name: 'big.bin', mimeType: 'application/octet-stream', bytes: big }, confirmed: true }).catch(e => String(e.message));
ok('a file over 256 KB fails before anything leaves, and says so', /256 KB at most/.test(tooBig) && /Nothing was sent/.test(tooBig) && wire.length === before, String(tooBig).slice(0, 80));
const tooMuch = await call(A, 'thread_send', { context_id: ctx, text: 'x', data: { blob: 'B'.repeat(600 * 1024) }, confirmed: true }).catch(e => String(e.message));
ok('a message over 512 KB in all fails before anything leaves, and says so', /512 KB is the most/.test(tooMuch) && wire.length === before, String(tooMuch).slice(0, 80));

// ---- 6. paging: a long relationship is never a wall ----
for (let i = 0; i < 60; i++) await call(B, 'thread_send', { context_id: ctx, text: `note ${i}`, confirmed: true });
const p1 = await J(A, 'thread_read', { context_id: ctx });
ok('a page is fifty messages, newest', p1.messages.length === 50 && p1.messages[49].parts[0].text === 'note 59' && p1.older_before != null, `${p1.messages.length}, total ${p1.total}`);
const p2 = await J(A, 'thread_read', { context_id: ctx, before: p1.older_before });
ok('and the cursor pages back to the beginning', p2.messages.length > 0 && p2.messages[0].seq === 1 && p2.messages[p2.messages.length - 1].seq === p1.messages[0].seq - 1 && p2.older_before == null, `${p2.messages[0].seq}..${p2.messages[p2.messages.length - 1].seq}`);
ok('every message is its own key, not a wall inside one record', [...portals[A].MAILBOX.m.keys()].filter(k => k.startsWith('convm:' + ctx + ':')).length === p1.total && !JSON.stringify(conv(A, ctx)).includes('note 3'));

// Sixty messages in a minute is a day's worth of a real relationship: the suite spent the sender's
// daily allowance at A's door, which a person never would. Reset the counters it filled.
for (const k of [...portals[A].MAILBOX.m.keys()]) if (k.startsWith('cap:a2a:')) portals[A].MAILBOX.m.delete(k);

// ---- 7. the relay never carries a word of it; no thread text reaches a card or a cast ----
const secret = 'Sam-only-word-9f3';
await call(A, 'thread_send', { context_id: ctx, text: `Between us: ${secret}`, confirmed: true });
await call(A, 'update_card', { add_need: 'board-advisor', confirmed: true });
await pulse(A); await pulse(B);
const castAll = relaySeen.map(w => w.body).join('\n');
ok('the relay saw casts and cards in this run', relaySeen.length > 0);
ok('and never a word from a thread', !castAll.includes(secret) && !castAll.includes('twenty minutes') && !castAll.includes('note 3') && !castAll.includes('cal.example'), '');
const cardA = await (await worker.fetch(new Request(A + '/card'), portals[A])).text();
const memA = await call(A, 'my_memory');
ok('thread text never reaches the card or the memory file', !cardA.includes(secret) && !memA.includes(secret) && !cardA.includes('cal.example'));
const f2 = await cast(B, { need_text: 'someone who knows Sam-only-word-9f3', tags: ['sam-only-word-9f3'] });
ok('nor matching', !(f2.candidates || []).length && !JSON.stringify(f2).includes('Between us'));
ok('and nothing about a thread went to the relay at all', !relaySeen.some(w => /contextId":"[a-f0-9]{32}"|thread\.message|convm/.test(w.body)));

// ---- 8. unreachable portal: queue, retry with backoff, tell the owner after seven days ----
down.add(B);
const queued = await call(A, 'thread_send', { context_id: ctx, text: 'are you there?', confirmed: true });
ok('an unreachable portal means queued, not failed', /queued/.test(queued) && /seven days/.test(queued), queued.slice(0, 100));
const ob = () => [...portals[A].MAILBOX.m.entries()].filter(([k]) => k.startsWith('outbox:')).map(([, v]) => JSON.parse(v));
ok('the message waits in the outbox with a next try', ob().length === 1 && ob()[0].attempts === 1 && ob()[0].nextAt);
await pulse(A);
ok('a pulse before its time leaves it alone', ob().length === 1 && ob()[0].attempts === 1);
const age = (ms) => { for (const [k, v] of portals[A].MAILBOX.m.entries()) if (k.startsWith('outbox:')) { const o = JSON.parse(v); o.nextAt = new Date(Date.now() - 1000).toISOString(); o.firstAt = new Date(Date.now() - ms).toISOString(); portals[A].MAILBOX.m.set(k, JSON.stringify(o)); } };
age(60000); await pulse(A);
ok('when its time comes it is tried again, and backs off', ob().length === 1 && ob()[0].attempts === 2 && Date.parse(ob()[0].nextAt) > Date.now() + 4 * 60000, ob()[0] && ob()[0].nextAt);
down.delete(B);
age(60000); await pulse(A);
ok('and gets through when the portal is back', ob().length === 0 && (await J(B, 'thread_read', { context_id: ctx })).messages.some(m => m.parts[0].text === 'are you there?'));
down.add(B);
await call(A, 'thread_send', { context_id: ctx, text: 'still there?', confirmed: true });
age(8 * 24 * 3600 * 1000); await pulse(A);
ok('after seven days it gives up and the owner is told once', ob().length === 0 && (await J(A, 'check_mailbox')).messages.some(m => /could not be delivered in seven days/.test(m.text)));
down.delete(B);

// ---- 9. edge behaviour on the pulse ----
{
  // A second thread where A must write first and never does.
  const f = await cast(A, { need_text: 'a board advisor', tags: ['board-advisor'] });
  await call(B, 'update_card', { add_have: 'board-advisor', confirmed: true });
  await call(A, 'add_known_card', { url: B + '/card' });
  const f3 = await cast(A, { need_text: 'a board advisor', tags: ['board-advisor'] });
  const c3 = f3.candidates.find(c => c.handle === 'sam@mazel');
  const pr = await call(A, 'propose_intro', { thread_id: f3.thread_id, card_url: c3.card_url, ask: { kind: 'question', size: 'one question' }, confirmed: true });
  const intro2 = lastIntro(A, 'sam@mazel');
  await call(B, 'respond_intro', { intro_id: intro2, decision: 'accepted', confirmed: true });
  const ctx2 = (await J(A, 'thread_list')).threads.find(t => t.context_id !== ctx).context_id;
  const setCreated = (o, id, daysAgo) => { const c = conv(o, id); c.created = new Date(Date.now() - daysAgo * 24 * 3600 * 1000).toISOString(); portals[o].MAILBOX.m.set('conv:' + id, JSON.stringify(c)); };
  setCreated(A, ctx2, 3.1);
  let lines = await pulse(A) || '';
  const mail = async (o) => (await J(o, 'check_mailbox')).messages.map(m => m.text || '').join('\n');
  ok('day 3, first message unsent: one reminder to the owner', /the one to write first to sam@mazel/.test(await mail(A)), '');
  await pulse(A);
  ok('and only one', (await mail(A)).split('the one to write first').length - 1 === 1);
  setCreated(A, ctx2, 14.1); setCreated(B, ctx2, 14.1);   // the receiver honours a lapse only when its own clock agrees
  const wireBefore = wire.length;
  await pulse(A);
  ok('day 14: the thread lapses', conv(A, ctx2).state === 'lapsed' && outcome(A, intro2).state === 'lapsed');
  const lapsedWire = wire.slice(wireBefore).filter(w => /thread\.lapsed/.test(w.body));
  await pulse(B);
  ok('the other side lapses on its own clock, and nothing crossed the wire for it', lapsedWire.length === 0 && conv(B, ctx2).state === 'lapsed' && outcome(B, intro2).state === 'lapsed');
  ok('and no human-facing notice lands on the other side', !(await mail(B)).includes('lapsed') && !(await J(B, 'check_mailbox')).messages.some(m => (m.action || {}).type === 'thread.lapsed'));
  ok('a lapsed thread takes nothing more', /lapsed/.test(await call(A, 'thread_send', { context_id: ctx2, text: 'late', confirmed: true }).catch(e => e.message)));

  // No reply: one follow-up offered at day 5, then stop.
  const f4 = await cast(A, { need_text: 'a board advisor', tags: ['board-advisor'] });
  const cc = conv(A, ctx); cc.state = 'sent'; cc.lastHumanIn = undefined; cc.lastHumanOut = new Date(Date.now() - 5.1 * 24 * 3600 * 1000).toISOString(); cc.timers = {}; cc.pending = null; portals[A].MAILBOX.m.set('conv:' + ctx, JSON.stringify(cc));
  await pulse(A);
  ok('day 5 without a reply: one drafted follow-up is offered', conv(A, ctx).pending && conv(A, ctx).pending.kind === 'followup' && /No reply yet from sam@mazel/.test(await mail(A)));
  await pulse(A);
  ok('and then it stops', (await mail(A)).split('No reply yet from').length - 1 === 1);

  // No-show: one reschedule offer, then didn't meet.
  const ns1 = await call(A, 'thread_mark', { context_id: ctx, what: 'no_show' });
  ok('a no-show gets one reschedule offer', /One reschedule offer/.test(ns1) && conv(A, ctx).pending.kind === 'reschedule');
  const ns2 = await call(A, 'thread_mark', { context_id: ctx, what: 'no_show' });
  ok('a second is recorded as didn\'t meet, and nothing more is offered', /didn't meet/.test(ns2) && !conv(A, ctx).pending && outcome(A, introId).state === 'didnt_meet');
  ok('met is an outcome too', /they met/.test(await call(A, 'thread_mark', { context_id: ctx, what: 'met' })) && outcome(A, introId).state === 'met' && conv(A, ctx).state === 'met');
}

// ---- 10. close, block, delete, export ----
{
  const cl = await call(B, 'thread_close', { context_id: ctx });
  ok('closing asks first', /^Not done/.test(cl));
  await call(B, 'thread_close', { context_id: ctx, confirmed: true });
  ok('close ends it for both; each keeps its history', conv(A, ctx).state === 'closed' && conv(B, ctx).state === 'closed' && (await J(A, 'thread_read', { context_id: ctx })).total > 60 && (await J(B, 'thread_read', { context_id: ctx })).total > 60);
  ok('and the outcome says closed on both', outcome(A, introId).state === 'closed' && outcome(B, introId).state === 'closed');
  const exp = JSON.parse(await call(A, 'thread_export', { context_id: ctx }));
  ok('export is the whole thread, every message, as JSON', exp.messages.length === exp.thread.seq && exp.messages.some(m => m.message.parts[0].text === odd));
  // A fresh thread to block on, with a portal of its own: a block is portal-wide from here on.
  await call(D, 'add_known_card', { url: A + '/card' });
  await call(A, 'add_known_card', { url: D + '/card' });
  await call(A, 'update_card', { add_have: 'halcyon', confirmed: true });
  await call(D, 'add_known_card', { url: A + '/card' });
  await call(D, 'update_card', { add_need: 'halcyon', confirmed: true });   // a proposal carries the need's words
  const f6 = await J(D, 'find', { need_text: 'someone at halcyon', tags: ['halcyon'] });
  const pr = await call(D, 'propose_intro', { thread_id: f6.thread_id, card_url: f6.candidates[0].card_url, confirmed: true });
  const intro3 = lastIntro(D, 'avery@mazel');
  await call(A, 'respond_intro', { intro_id: intro3, decision: 'accepted', confirmed: true });
  const ctx3 = (await J(D, 'thread_list')).threads.find(t => t.state === 'open').context_id;
  await call(D, 'thread_send', { context_id: ctx3, text: 'hello', confirmed: true });
  await call(A, 'thread_block', { context_id: ctx3, report: true, confirmed: true });
  const wb = wire.length;
  const r = await call(D, 'thread_send', { context_id: ctx3, text: 'hello again', confirmed: true });
  ok('a blocked portal\'s message is refused at the door and not retried', /refused|queued/.test(r) && !(await J(A, 'thread_read', { context_id: ctx3 })).messages.some(m => m.parts[0].text === 'hello again'));
  ok('the block is silent: nothing about it went to the other portal', !wire.slice(wb).some(w => /block/i.test(w.body)));
  const trust = [...portals[A].MAILBOX.m.entries()].find(([k]) => k.startsWith('trust:'));
  ok('a report lowers that portal\'s trust input here', trust && JSON.parse(trust[1]).reports === 1);
  await call(A, 'thread_delete', { context_id: ctx3, confirmed: true });
  ok('delete removes only this copy', !conv(A, ctx3) && ![...portals[A].MAILBOX.m.keys()].some(k => k.startsWith('convm:' + ctx3)) && conv(D, ctx3) && (await J(D, 'thread_read', { context_id: ctx3 })).total >= 2);
}

for (const o of [A, B, C]) for (const k of [...portals[o].MAILBOX.m.keys()]) if (k.startsWith('cap:a2a:')) portals[o].MAILBOX.m.delete(k);

// ---- 11. routing for a third person is off the wire (29e H1) ----
{
  await call(C, 'add_known_card', { url: A + '/card' });
  await call(C, 'add_known_card', { url: B + '/card' });
  await call(B, 'add_known_card', { url: C + '/card' });
  await call(B, 'update_card', { add_have: 'ehr-migrations', confirmed: true });
  await call(C, 'add_known_card', { url: B + '/card' });
  await call(C, 'update_card', { add_need: 'ehr-migrations', confirmed: true });   // a proposal carries the need's words
  const f = await J(C, 'find', { need_text: 'someone who does EHR migrations, for Avery', tags: ['ehr-migrations'] });
  const introsBefore = [...portals[C].MAILBOX.m.keys()].filter(k => k.startsWith('intro:')).length;
  wire.length = 0;
  let said = '';
  try { said = await call(C, 'propose_intro', { thread_id: f.thread_id, card_url: f.candidates[0].card_url, router_for: 'avery@mazel', ask: { kind: 'call', size: '30 minutes' }, confirmed: true }); } catch (e) { said = String(e.message || e); }
  ok('a proposal for a third person is refused in words that say what to do instead', /not on the wire yet/.test(said) && /two ordinary intros/.test(said), said.slice(0, 120));
  ok('nothing left the portal and no intro was recorded', !wire.some(w => /intro\.propose/.test(w.body)) && [...portals[C].MAILBOX.m.keys()].filter(k => k.startsWith('intro:')).length === introsBefore);
  // The ordinary intro still works from the same thread: two people, both of whose yes each portal holds.
  await call(C, 'propose_intro', { thread_id: f.thread_id, card_url: f.candidates[0].card_url, ask: { kind: 'call', size: '30 minutes' }, confirmed: true });
  const intro4 = lastIntro(C, 'sam@mazel');
  await call(B, 'respond_intro', { intro_id: intro4, decision: 'accepted', confirmed: true });
  const ctx4 = JSON.parse(portals[C].MAILBOX.m.get('intro:' + intro4)).contextId;
  ok('the thread it opens has exactly the two of them', conv(C, ctx4) && conv(C, ctx4).participants.length === 2 && conv(B, ctx4) && conv(B, ctx4).participants.length === 2, JSON.stringify(conv(C, ctx4) && conv(C, ctx4).participants.map(p => p.handle)));
  ok('and the 🌀 line names only the person whose yes this portal holds', [...portals[B].MAILBOX.m.entries()].filter(([k]) => k.startsWith('msg:')).map(([, v]) => JSON.parse(v)).some(m => m.box && m.box.kind === 'both_yes' && m.box.contextId === ctx4 && /you and rae@mazel are in the thread/.test(m.text)), JSON.stringify([...portals[B].MAILBOX.m.entries()].filter(([k]) => k.startsWith('msg:')).map(([, v]) => JSON.parse(v)).filter(m => m.box && m.box.kind === 'both_yes').map(m => m.text.slice(0, 80))));
}

// ---- 12. a contact with no portal replies by link ----
{
  const gid = (await call(A, 'note_ghost', { name: 'Dana Ruiz', org: 'northline.health', have: ['revenue-cycle'], edge_score: 70, witnesses: ['hubspot'] })).match(/Their id is (\w+)/)[1];
  // A thread A opened with Sam; Dana is brought in by link.
  const f = await cast(A, { need_text: 'revenue cycle help', tags: ['revenue-cycle'] });
  await call(B, 'update_card', { add_have: 'revenue-cycle', confirmed: true });
  await call(A, 'add_known_card', { url: B + '/card' });
  const f2 = await cast(A, { need_text: 'revenue cycle help', tags: ['revenue-cycle'] });
  const pr = await call(A, 'propose_intro', { thread_id: f2.thread_id, card_url: f2.candidates[0].card_url, confirmed: true });
  const intro5 = lastIntro(A, 'sam@mazel');
  await call(B, 'respond_intro', { intro_id: intro5, decision: 'accepted', confirmed: true });
  const ctx5 = JSON.parse(portals[A].MAILBOX.m.get('intro:' + intro5)).contextId;   // the branch the intro named, which the people just joined
  await call(A, 'thread_send', { context_id: ctx5, text: 'Dana, Sam - you two should talk about Northline.', confirmed: true });
  const wireAt = wire.length;
  const share = await call(A, 'thread_share', { context_id: ctx5, contact_id: gid });
  const link = (share.match(/https:\/\/\S+\/t\/[a-f0-9]{32}\?s=\S+/) || [])[0];
  ok('thread_share makes a link scoped to one thread and one contact', !!link && /fourteen days/.test(share), share.slice(0, 90));
  ok('nothing was sent to anyone to make it', wire.length === wireAt);
  const page = await (await worker.fetch(new Request(link), portals[A])).text();
  ok('the page shows the thread and a reply box, and offers an agent of their own', /Northline/.test(page) && /<textarea/.test(page) && /mazel\.ai\/install/.test(page) && /Get your own agent/.test(page));
  ok('with no script on it at all', !/<script/.test(page));
  const u = new URL(link);
  const post = (text, l = link) => worker.fetch(new Request(new URL(l).origin + new URL(l).pathname, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' }, body: `s=${encodeURIComponent(new URL(l).searchParams.get('s'))}&text=${encodeURIComponent(text)}` }), portals[A]);
  const r1 = await post('Happy to. Tuesday works for me — Dana');
  ok('a reply lands on the sender\'s portal as the named contact\'s own words', r1.status === 302 && (await J(A, 'thread_read', { context_id: ctx5 })).messages.some(m => m.author === 'human' && m.from === 'Dana Ruiz' && m.parts[0].text === 'Happy to. Tuesday works for me — Dana'));
  ok('and lands in the box with her name only', (await J(A, 'check_mailbox')).messages.some(m => m.box && m.box.kind === 'reply' && /A reply from Dana Ruiz/.test(m.text) && !/Tuesday/.test(m.text)));
  ok('a forged signature gets a plain page and no thread, the same page an expired link gets', (await worker.fetch(new Request(link.replace(/s=\w+/, 's=nope')), portals[A])).status === 410 && !/Northline/.test(await (await worker.fetch(new Request(link.replace(/s=\w+/, 's=nope')), portals[A])).text()) && /not usable/.test(await (await worker.fetch(new Request(link.replace(/s=\w+/, 's=nope')), portals[A])).text()));
  const renew = await call(A, 'thread_share', { context_id: ctx5, contact_id: gid });
  ok('renewal keeps the same address', /^Renewed/.test(renew) && renew.includes(link.split('?')[0]));
  const tk = [...portals[A].MAILBOX.m.keys()].find(k => k.startsWith('tlink:'));
  const rec = JSON.parse(portals[A].MAILBOX.m.get(tk)); rec.replies = { day: new Date().toISOString().slice(0, 10), count: 30 }; portals[A].MAILBOX.m.set(tk, JSON.stringify(rec));
  ok('replies are rate-limited per link', (await post('one more')).status === 429);
  rec.exp = Date.now() - 1000; portals[A].MAILBOX.m.set(tk, JSON.stringify(rec));
  const expired = await worker.fetch(new Request(link), portals[A]);
  ok('an expired link shows a plain explanation, indistinguishable from a forged one', expired.status === 410 && /not usable/.test(await expired.text()) && /renew/.test(await (await worker.fetch(new Request(link), portals[A])).text()));
  await call(A, 'thread_share', { context_id: ctx5, contact_id: gid });
  await call(A, 'thread_close', { context_id: ctx5, confirmed: true });
  ok('and a closed thread\'s link says the conversation has ended', (await worker.fetch(new Request(link), portals[A])).status === 410 && /ended/.test(await (await worker.fetch(new Request(link), portals[A])).text()));
}

// ---- 13. /inbox: the phone page, and how it signs in ----
{
  const get = (path, headers = {}) => worker.fetch(new Request(A + path, { headers }), portals[A]);
  const r401 = await get('/inbox');
  ok('with no session, /inbox is a 401 page that says how to get in, with nothing to type', r401.status === 401 && /inbox_link/.test(await r401.text()) && !/<input/.test(await (await get('/inbox')).text()));
  ok('the owner\'s tools still read the mailbox over the header', (await get('/inbox', { authorization: 'Bearer ta' })).headers.get('content-type').includes('json'));
  const linkText = await call(A, 'inbox_link');
  const il = (linkText.match(/https:\/\/\S+\/inbox\/open\?\S+/) || [])[0];
  ok('inbox_link mints a ten-minute link', !!il && /ten minutes/.test(linkText));
  const opened = await get(il.slice(A.length));
  const cookie = opened.headers.get('set-cookie') || '';
  ok('opening it sets an HttpOnly, Secure, SameSite cookie scoped to /inbox and redirects', opened.status === 302 && opened.headers.get('location') === '/inbox' && /HttpOnly/.test(cookie) && /Secure/.test(cookie) && /SameSite=Lax/.test(cookie) && /Path=\/inbox/.test(cookie), cookie.slice(0, 80));
  ok('so the address bar never holds a credential', !/token/.test(opened.headers.get('location')));
  ok('the link works once', (await get(il.slice(A.length))).status === 400);
  const ck = cookie.split(';')[0];
  const list = await get('/inbox', { cookie: ck });
  const listHtml = await list.text();
  ok('the list page shows the box and the conversations', list.status === 200 && /sam@mazel/.test(listHtml) && /your box/.test(listHtml) && /Conversations/.test(listHtml));
  ok('it is an installable app with the iPhone guide', /manifest\.json/.test(listHtml) && /apple-mobile-web-app-capable/.test(listHtml) && /Add to Home Screen/.test(listHtml) && /Home Screen/.test(listHtml));
  ok('and every script and style is inline', !/<script src|<link rel="stylesheet"|@import|fonts\./.test(listHtml));
  const man = await get('/inbox/manifest.json');
  ok('the manifest and service worker are served', man.status === 200 && (await man.json()).display === 'standalone' && (await get('/inbox/sw.js')).status === 200 && (await get('/inbox/icon.svg')).status === 200);
  // A live thread with Sam for the pages below; the earlier ones are closed or lapsed by now.
  await call(B, 'update_card', { add_have: 'inbox-pages', confirmed: true });
  await call(A, 'add_known_card', { url: B + '/card' });
  { const fI = await cast(A, { need_text: 'inbox pages', tags: ['inbox-pages'] }); await call(A, 'propose_intro', { thread_id: fI.thread_id, card_url: fI.candidates.find(c => c.handle === 'sam@mazel').card_url, confirmed: true }); await call(B, 'respond_intro', { intro_id: lastIntro(A, 'sam@mazel'), decision: 'accepted', confirmed: true }); }
  const open = (await J(A, 'thread_list')).threads.find(t => (t.state === 'replied' || t.state === 'open' || t.state === 'sent') && t.humans && t.with.includes('sam@mazel') && !t.blocked);
  const tp = await get('/inbox/t/' + open.context_id, { cookie: ck });
  const tpHtml = await tp.text();
  ok('a thread page shows the messages and a reply box', tp.status === 200 && /<textarea/.test(tpHtml) && /csrf/.test(tpHtml));
  const csrf = (tpHtml.match(/name="csrf" value="([^"]+)"/) || [])[1];
  const bad = await worker.fetch(new Request(A + '/inbox/reply', { method: 'POST', headers: { cookie: ck, 'content-type': 'application/x-www-form-urlencoded' }, body: `context_id=${open.context_id}&csrf=wrong&text=hi` }), portals[A]);
  ok('a reply without the right CSRF field is refused', bad.status === 403);
  const good = await worker.fetch(new Request(A + '/inbox/reply', { method: 'POST', headers: { cookie: ck, 'content-type': 'application/x-www-form-urlencoded' }, body: `context_id=${open.context_id}&csrf=${encodeURIComponent(csrf)}&text=${encodeURIComponent('From my phone: Thursday, then.')}` }), portals[A]);
  ok('a reply from the phone goes out as the person\'s words', good.status === 302 && (await J(B, 'thread_read', { context_id: open.context_id })).messages.some(m => m.author === 'human' && m.parts[0].text === 'From my phone: Thursday, then.'));
  const ding = await get('/inbox/ding', { cookie: ck });
  const dj = await ding.json();
  ok('/inbox/ding says only who and where, never what', ding.status === 200 && (dj.from === undefined || typeof dj.from === 'string') && !JSON.stringify(dj).includes('Thursday'));
  const sess = [...portals[A].MAILBOX.m.keys()].find(k => k.startsWith('session:'));
  const s = JSON.parse(portals[A].MAILBOX.m.get(sess)); s.rotated = Date.now() - 8 * 24 * 3600 * 1000; portals[A].MAILBOX.m.set(sess, JSON.stringify(s));
  const rot = await get('/inbox', { cookie: ck });
  ok('a week-old session is rotated: a new cookie, the old id dead', rot.status === 200 && /mz_inbox=[a-f0-9]{64}/.test(rot.headers.get('set-cookie') || '') && !portals[A].MAILBOX.m.has(sess));
  const ck2 = (rot.headers.get('set-cookie') || '').split(';')[0];
  const envRotated = { ...portals[A], INBOX_TOKEN: 'ta-rotated' };
  ok('rotating the inbox token ends every session', (await worker.fetch(new Request(A + '/inbox', { headers: { cookie: ck2 } }), envRotated)).status === 401);
  ok('signing out ends the session', (await get('/inbox/out', { cookie: ck2 })).status === 302 && (await get('/inbox', { cookie: ck2 })).status === 401);
}

// ---- 14. the doorbell: web push with nothing in it ----
{
  const get = (path, headers = {}) => worker.fetch(new Request(A + path, { headers }), portals[A]);
  const il = ((await call(A, 'inbox_link')).match(/https:\/\/\S+\/inbox\/open\?\S+/) || [])[0];
  const ck = ((await get(il.slice(A.length))).headers.get('set-cookie') || '').split(';')[0];
  const page = await (await get('/inbox', { cookie: ck })).text();
  const csrf = (page.match(/data-csrf="([^"]+)"/) || [])[1];
  const vapid = await (await get('/inbox/vapid', { cookie: ck })).json();
  ok('the portal has a VAPID key of its own', /^[A-Za-z0-9_-]{80,}$/.test(vapid.key));
  const sub = await worker.fetch(new Request(A + '/inbox/push', { method: 'POST', headers: { cookie: ck, 'x-csrf': csrf, 'content-type': 'application/json' }, body: JSON.stringify({ endpoint: 'https://push.example/sub/abc', keys: { p256dh: 'p', auth: 'a' } }) }), portals[A]);
  ok('a phone can subscribe from the page', sub.status === 200);
  const open = (await J(A, 'thread_list')).threads.find(t => (t.state === 'replied' || t.state === 'open' || t.state === 'sent') && t.humans && t.with.includes('sam@mazel') && !t.blocked);
  pushSeen.length = 0;
  portals[A].MAILBOX.m.delete('bell:' + open.context_id);          // the ten-minute quiet from earlier rings in this run
  await call(B, 'thread_send', { context_id: open.context_id, text: 'ring ring, secret-bell-word', confirmed: true });
  ok('a new message rings the phone', pushSeen.length === 1, String(pushSeen.length));
  await call(B, 'thread_send', { context_id: open.context_id, text: 'and again', confirmed: true });
  ok('a second message in the same thread within ten minutes does not ring again', pushSeen.length === 1, String(pushSeen.length));
  ok('with an empty body and a VAPID signature: nothing to encrypt, nothing carried', pushSeen[0].body === '' && /^vapid t=.+, k=/.test(pushSeen[0].headers.authorization) && pushSeen[0].headers['content-length'] === '0');
  ok('the service worker fetches the name over the session, never the words', /fetch\('\/inbox\/ding'/.test(await (await get('/inbox/sw.js')).text()) && !JSON.stringify(pushSeen).includes('secret-bell-word'));
}

globalThis.fetch = realFetch;
console.log(`\nthread: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
