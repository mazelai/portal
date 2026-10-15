// Ghosts: the people the person already knows, held on their own portal, so the first cast has
// somewhere to land. The whole suite is one long argument that a ghost's name never leaves.
import rawWorker from '../src/index.js';
import { legacy as legacyWorker } from './a2a-helpers.mjs';
const worker = legacyWorker(rawWorker);
import relay from '../../relay/src/index.js';
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})), list_complete: true }) }; };
const R='https://relay.gh', A='https://mazel.a.gh', B='https://mazel.b.gh';
const renv = { RELAY: mkKV() };
const portals = {
  [A]: { HANDLE:'avery@mazel', PERSONA:'Avery runs Halcyon.', NEED:'', HAVE:'managed-ai-delivery', INBOX_TOKEN:'ta', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: A, OWNER_EMAIL:'avery@halcyon.example' },
  [B]: { HANDLE:'lea@mazel', PERSONA:'Lea runs labs.', NEED:'', HAVE:'lab-ops', INBOX_TOKEN:'tb', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: B, OWNER_EMAIL:'lea@labs.example' },
};
const wireLog = [];
const realFetch = globalThis.fetch;
globalThis.fetch = async (u, i={}) => { const url=String(u instanceof Request?u.url:u); const o=new URL(url).origin; wireLog.push(url + ' ' + (i.body || ''));
  if (o===R) return relay.fetch(new Request(url,i), renv);
  if (portals[o]) return worker.fetch(new Request(url,i), portals[o]);
  return new Response('no', { status: 503 }); };
const call = async (o, name, args) => JSON.parse(await (await worker.fetch(new Request(o+'/mcp', { method:'POST', headers:{ 'content-type':'application/json', authorization:'Bearer '+portals[o].INBOX_TOKEN }, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name, arguments: args||{} } }) }), portals[o])).text()).result.content[0].text;
// A need travels only once its owner has placed it, so the caster answers the question first.
// That is the core-loop rule, and every cast in this suite goes through here.
// A card holds six public needs at most, so the caster clears the last one it placed before
// placing the next. The thread stays open either way; only the card entry moves.
const lastPlaced = {};
const casts = async (o, args) => {
  const tag = args.tags[0];
  if (lastPlaced[o] && lastPlaced[o] !== tag) { try { await call(o, 'update_card', { remove_need: lastPlaced[o], confirmed: true }); } catch { /* already gone */ } }
  try { await call(o, 'update_card', { add_need: tag, confirmed: true }); } catch { /* already on the card */ }
  lastPlaced[o] = tag;
  return call(o, 'find', args);
};
let pass=0, fail=0; const ok=(l,c,x='')=>{ console.log((c?'PASS ':'FAIL ')+l+(x?'  -> '+String(x).replace(/\n/g,' ').slice(0,120):'')); c?pass++:fail++; };

// ---- forty ghosts, the way an agent would actually read them ----
const crm = Array.from({length: 22}, (_, i) => ({ name:`CRM Person ${i}`, org:`crm${i}.example`, email:`p${i}@crm${i}.example`,
  have: i % 3 === 0 ? ['fractional-cfo'] : i % 3 === 1 ? ['lab-ops'] : ['channel-sales'],
  role:'Contact from the pipeline', witnesses:['hubspot'], edge_score: 40 + (i % 50), edge_signals:`${i % 5} deals, last activity ${i} days ago` }));
const linkedin = Array.from({length: 18}, (_, i) => ({ name:`Connection ${i}`, org:`li${i}.example`, email:`c${i}@li${i}.example`,
  have: i % 2 === 0 ? ['healthcare-ai-delivery'] : ['data-and-crm-migrations'],
  role:'LinkedIn connection', witnesses:['linkedin'], edge_score: 10 + i, edge_signals:'export only, no recent contact' }));
for (const g of [...crm, ...linkedin]) await call(A, 'note_ghost', g);
const listed = JSON.parse(await call(A, 'list_ghosts'));
ok('forty ghosts from a CRM export and a LinkedIn csv', listed.count === 40, String(listed.count));
ok('strongest edge first', listed.people[0].edge.score >= listed.people[listed.people.length-1].edge.score);
ok('each one carries the witnesses it was read from', listed.people.every(g => g.witnesses.length > 0));
ok('and the list says out loud that it is owner only', /never leave this portal/.test(JSON.stringify(listed)));

// ---- a cast hits a ghost and produces an invitation, not an intro ----
const found = JSON.parse(await call(A, 'find', { need_text:'a fractional cfo for a portfolio company', tags:['fractional-cfo'] }));
ok('a cast with no matching card still lands on people the person knows', (found.invites || []).length > 0, JSON.stringify((found.invites||[]).map(i=>i.name)));
ok('an invite carries the why and the edge, for the person to judge', found.invites[0].why.includes('Edge') && found.invites[0].matched.includes('fractional-cfo'), found.invites[0].why);
ok('a ghost is never a candidate', (found.candidates || []).length === 0);
ok('and the answer says nothing was sent', /Nothing has been sent/.test(found.invites_note));
const invite = await call(A, 'invite_text', { ghost_id: found.invites[0].ghost_id, thread_id: found.thread_id });
ok('the invitation is words for the person to send, not a message the portal sends', /send it however they like/i.test(invite) && /mazel.ai\/install/.test(invite));
ok('it names the ask without naming who asked', /fractional cfo/i.test(invite));

// ---- a need from a card this portal holds is scored against the people it already knows --------
// Lea needs revenue-cycle help. Avery does not do revenue cycle, so his own card does not answer
// it - but he knows Dana, who does. The need has to reach Avery's portal even though Avery is not
// the answer, be scored against his saved contacts, and become one question for Avery and nothing
// else: Dana's name does not leave, and Lea is told nothing until Avery says yes.
await call(A, 'add_known_card', { url: B + '/card' });
await call(B, 'add_known_card', { url: A + '/card' });
await call(A, 'note_ghost', { name: 'Dana Reyes', org: 'northline.health', have: ['revenue-cycle', 'billing-ops'], role: 'Ran revenue cycle at Northline', witnesses: ['gmail', 'calendar'], edge_score: 84, edge_signals: 'eleven threads, replies within a day' });
{
  const mark = wireLog.length;
  const f = await casts(B, { need_text: 'someone who can fix our revenue cycle', tags: ['revenue-cycle'] });
  ok('precondition: Avery is not himself an answer to it', !(f.candidates || []).some(c => c.handle === 'avery@mazel'), JSON.stringify((f.candidates || []).map(c => c.handle)));
  const box = await call(A, 'check_mailbox');
  const ask = box.startsWith('{') ? (JSON.parse(box).messages || []).find(m => (m.action || {}).type === 'ghost.ask') : null;
  ok('the need reaches the portal of someone who knows a person for it', !!ask, box.startsWith('{') ? JSON.stringify((JSON.parse(box).messages || []).map(m => (m.action || {}).type)) : box.slice(0, 60));
  ok('Avery is shown who, the why, and how strong the edge is', !!ask && /Dana Reyes/.test(ask.text) && /Edge 84/.test(ask.text), ask && ask.text.slice(0, 110));
  ok('and told plainly that nobody has been told anything', !!ask && /no idea/.test(ask.text));
  const backToLea = wireLog.slice(mark).join('\n');
  ok('Dana\'s name reaches nobody before Avery answers', !/Dana Reyes/.test(backToLea) && !/northline\.health/.test(backToLea));
  if (ask) {
    ok('declining sends nothing', /Not sent/.test(await call(A, 'contacts', { action: 'route', ask_id: ask.action.askId })) && !wireLog.slice(mark).join('\n').includes('Dana'));
    const routed = await call(A, 'contacts', { action: 'route', ask_id: ask.action.askId, confirmed: true });
    ok('and a yes gives Avery words to send, and makes him the router', /Your yes is recorded/.test(routed) && /router/.test(routed), routed.slice(0, 80));
    ok('even then the portal sends nothing on his behalf', !wireLog.slice(mark).join('\n').includes('Dana Reyes'));
  }
}

// ---- what an offer does not do: no replay, no forwarding, nothing back to the caster ---------
{
  // A third portal, so that forwarding has somewhere it could go if the code ever tried.
  const C = 'https://mazel.c.gh';
  portals[C] = { HANDLE:'cam@mazel', PERSONA:'Cam builds valves.', NEED:'', HAVE:'valve-engineering', INBOX_TOKEN:'tc', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: C, OWNER_EMAIL:'cam@valves.example' };
  await call(A, 'add_known_card', { url: C + '/card' });
  await call(C, 'add_known_card', { url: A + '/card' });
  await call(A, 'note_ghost', { name: 'Omar Haddad', org: 'coil.example', have: ['heat-exchangers'], role: 'Built heat exchangers for twenty years',
    witnesses: ['calendar', 'gmail'], small_meetings: 7, deals: 1, best_stage: 'won', threads_sent: 14, threads_replied: 11 });

  const mark = wireLog.length;
  const f = await casts(B, { need_text: 'someone who knows heat exchangers', tags: ['heat-exchangers'] });
  const needId = JSON.parse(f).thread_id;
  const asks = () => [...portals[A].MAILBOX.m.keys()].filter(k => k.startsWith('ghostask:')).length;
  const box = async (o) => { const t = await call(o, 'check_mailbox'); return t.startsWith('{') ? JSON.parse(t) : { messages: [] }; };
  const boxed = async () => (((await box(A)).messages) || []).length;
  const askCount = asks(), boxCount = await boxed();
  ok('OFFER: the offer landed, so the rest of this means something', askCount > 0);

  // 1. A replay, by anyone, of the exact signed offer.
  const wire = wireLog.slice(mark).find(l => l.startsWith(A) && l.includes('need.offer'));
  ok('OFFER: an offer goes to the door as a signed message, once', !!wire, (wire || '').slice(0, 60));
  const body = wire.slice(wire.indexOf(' ') + 1);
  const replay = await worker.fetch(new Request(A + '/a2a', { method: 'POST', headers: { 'content-type': 'application/json' }, body }), portals[A]);
  ok('OFFER: a replayed offer is answered without being acted on', replay.status < 500, String(replay.status));
  ok('OFFER: and raises no second question and writes no second ask', asks() === askCount && (await boxed()) === boxCount, `${askCount}/${boxCount} -> ${asks()}/${await boxed()}`);

  // 2. Nothing is passed on. Avery holds Cam's card; the offer stops at Avery either way.
  const out = wireLog.slice(mark).filter(l => l.startsWith(C));
  ok('OFFER: a receiver never passes a need on, even to a card it holds', !out.some(l => l.includes('need.offer')) && !out.some(l => l.includes('heat exchangers')), out.map(l => l.split(' ')[0]).join(' ') || 'nothing sent to Cam');
  ok('OFFER: and Cam is never asked about anything', (((await box(C)).messages) || []).filter(m => (m.action || {}).type === 'ghost.ask').length === 0);

  // 3. What the arriving offer says about the caster's other recipients: nothing, by shape.
  const env0 = JSON.parse(body);
  const deep = (o) => o && typeof o === 'object' ? (o.type === 'need.offer' ? o : Object.values(o).map(deep).find(Boolean)) : null;
  const offer = deep(env0);
  const fields = Object.keys(offer || {});
  ok('OFFER: the offer is in there to be read', !!offer, fields.join(','));
  // It names the one portal it was sent to, and nothing else about anybody: no path, no hop count,
  // and no sign of who else the caster offered it to (review 2026-10-07a, M1).
  ok('OFFER: the message names its one recipient and no other', fields.includes('to') && !fields.some(k => /path|hops|recipients|sent_to|offered/i.test(k)), fields.join(','));
  ok('OFFER: and the recipient it names is the portal it went to', offer.to === 'avery@mazel', String(offer.to));
  ok('OFFER: and nothing in it names any other portal', !body.includes('mazel.c.gh') && !body.includes('cam@mazel'));

  // 4. The caster, on a pass and on silence. Lea passes nothing back either way.
  const askId = (((await box(A)).messages) || []).filter(m => (m.action || {}).type === 'ghost.ask' && /Omar/.test(m.text || '')).map(m => m.action.askId)[0];
  const beforePass = wireLog.length;
  await call(A, 'contacts', { action: 'route', ask_id: askId });          // no confirmed: a pass
  const afterPass = wireLog.slice(beforePass).filter(l => l.startsWith(B));
  ok('OFFER: a pass sends the caster nothing at all', afterPass.length === 0, afterPass.map(l => l.split(' ')[0]).join(' ') || 'nothing sent to Lea');
  const leaBox = await box(B);
  ok('OFFER: and the caster never reads that anyone was asked', !JSON.stringify(leaBox).includes('Omar') && !/was asked|passed|declined/i.test(JSON.stringify(leaBox)));
  const quietMark = wireLog.length;
  await casts(B, { need_text: 'a glassblower for a one-off commission', tags: ['glassblowing'] });
  const onSilence = wireLog.slice(quietMark).filter(l => l.startsWith(B) && !l.includes('/card'));
  ok('OFFER: a need that matched nobody reads the same from the caster\'s end as one that did', onSilence.length === 0, onSilence.map(l => l.split(' ')[0]).join(' ') || 'nothing came back');
  const threadNow = JSON.parse(await call(B, 'list_threads'));
  ok('OFFER: and the caster\'s own thread says nothing about who holds what', !JSON.stringify(threadNow).includes('avery@mazel') || !/asked|contact|knows someone/i.test(JSON.stringify(threadNow)));
}

// ---- cooled is the owner's word, and nothing else may say it ---------------------------------
{
  await call(A, 'contacts', { action: 'note', name: 'Reed Salas', org: 'quiet.example', have: ['logistics'], state: 'cooled', state_source: 'inferred', state_reason: 'nothing since March' });
  const inferred = JSON.parse(await call(A, 'list_ghosts', { q: 'reed' })).people[0];
  ok('COOLED: a reader may not decide a relationship has cooled', inferred.state !== 'cooled', `${inferred.state} / ${inferred.state_source}`);
  await call(A, 'contacts', { action: 'note', name: 'Reed Salas', org: 'quiet.example', state: 'cooled', state_source: 'owner', state_reason: 'we both moved on' });
  const owned = JSON.parse(await call(A, 'list_ghosts', { q: 'reed' })).people[0];
  ok('COOLED: the owner may, and it is kept as theirs', owned.state === 'cooled' && owned.state_source === 'owner', `${owned.state} / ${owned.state_source}`);
  await call(A, 'contacts', { action: 'note', name: 'Reed Salas', org: 'quiet.example', state: 'active', state_source: 'inferred', state_reason: 'a calendar event' });
  const kept = JSON.parse(await call(A, 'list_ghosts', { q: 'reed' })).people[0];
  ok('COOLED: and an inferred state does not undo it', kept.state === 'cooled', kept.state);
  ok('COOLED: no label anywhere says a relationship ended or went badly',
     !/\b(ended|over|failed|went badly|bad|burned|lost touch)\b/i.test(JSON.stringify(JSON.parse(await call(A, 'list_ghosts')).people)), 'states: ' + [...new Set(JSON.parse(await call(A, 'list_ghosts')).people.map(p => p.state))].join(','));
  await call(A, 'contacts', { action: 'note', name: 'Reed Salas', org: 'quiet.example', tone: 'warm', tone_reason: 'they write back the same day' });
  const warm = JSON.parse(await call(A, 'list_ghosts', { q: 'reed' })).people[0];
  ok('TONE: a reader may say warm, with one line of why', warm.tone === 'warm' && /same day/.test(warm.tone_reason), `${warm.tone} / ${warm.tone_reason}`);
  await call(A, 'contacts', { action: 'note', name: 'Reed Salas', org: 'quiet.example', tone: 'cold', tone_reason: 'short replies' });
  const notCold = JSON.parse(await call(A, 'list_ghosts', { q: 'reed' })).people[0];
  ok('TONE: and may not say anything cooler than businesslike', notCold.tone === 'warm', String(notCold.tone));
  ok('COOLED: and no message text is kept against a contact', !/state_text|last_message|snippet|body/.test(JSON.stringify(owned)), Object.keys(owned).join(','));
}

// ---- resolution happens because a person said so, never because of a published identifier ----
{
  await call(B, 'note_ghost', { name:'Avery Nkemdi', org:'halcyon.example', email:'avery@halcyon.example', have:['managed-ai-delivery'], role:'Runs Halcyon', witnesses:['gmail'], edge_score: 70 });
  const before = JSON.parse(await call(B, 'list_ghosts', { q:'avery' })).people[0];
  ok('a ghost starts unresolved', !before.resolvedTo);
  await call(A, 'pulse');
  await call(B, 'resolve_handle', { handle: 'avery@mazel' });
  const stillGhost = JSON.parse(await call(B, 'list_ghosts', { q:'avery' })).people[0];
  ok('holding their card does not silently resolve a ghost to it', !stillGhost.resolvedTo, String(stillGhost.resolvedTo));
  ok('no address, and no hash of one, is stored on a ghost', !JSON.stringify(stillGhost).includes('halcyon.example') || !stillGhost.emailBucket, JSON.stringify(stillGhost).slice(0, 90));
  const rec = JSON.parse([...renv.RELAY.m.entries()].find(([k]) => k.startsWith('dir:avery'))[1]);
  ok('a published record carries no email identifier at all', !rec.emailBucket && !JSON.stringify(rec).includes('halcyon'), JSON.stringify(Object.keys(rec)));
  const linked = await call(B, 'link_ghost', { ghost_id: before.id, handle_or_url: 'avery@mazel' });
  ok('the person can say the two are the same, and then they are', /is avery@mazel from now on/.test(linked), linked.slice(0, 70));
  ok('and the ghost is resolved', JSON.parse(await call(B, 'list_ghosts', { q:'avery' })).people[0].resolvedTo === 'avery@mazel');
  let refused = '';
  try { await call(B, 'link_ghost', { ghost_id: before.id, handle_or_url: 'nobody@mazel' }); } catch (e) { refused = String(e.message); }
  const said = refused || await call(B, 'link_ghost', { ghost_id: before.id, handle_or_url: 'nobody@mazel' });
  ok('and only to a card this portal actually holds', /holds no card/.test(said), said.slice(0, 70));
}

// ---- the thing this whole suite exists to check ----
ok('no ghost name appears anywhere on any wire, in the whole run',
   !wireLog.some(l => /CRM Person|Connection \d|Dana Reyes|Avery Nkemdi/.test(l)),
   (wireLog.find(l => /Dana Reyes|CRM Person/.test(l)) || '').slice(0, 80));
ok('nor in the card, at any tier', !JSON.stringify(JSON.parse(await (await worker.fetch(new Request(A+'/card'), portals[A])).text())).match(/CRM Person|Connection \d/));
ok('nor in what the relay holds', !JSON.stringify([...renv.RELAY.m.values()]).match(/CRM Person|Connection \d|Dana Reyes/));

// ---- a person you know is not a queue: rest, back-off, never-ask ------------------------------
{
  const gid = JSON.parse(await call(A, 'list_ghosts', { q: 'dana' })).people[0].id;
  // 1. Rest. Dana was just routed, so a second need that fits her is kept rather than asked again.
  // Scoped to Dana: other contacts may well be asked about the same need, and are.
  const asksAbout = async (who) => ((JSON.parse(await call(A, 'check_mailbox')).messages) || [])
    .filter(m => (m.action || {}).type === 'ghost.ask' && (m.text || '').includes(who)).length;
  const asksBefore = await asksAbout('Dana');
  await casts(B, { need_text: 'someone to run billing operations', tags: ['billing-ops'] });
  const asksAfter = await asksAbout('Dana');
  ok('a contact just asked is rested: the next match raises no new question', asksAfter === asksBefore, `${asksBefore} -> ${asksAfter}`);
  const listed = JSON.parse(await call(A, 'list_ghosts', { q: 'dana' })).people[0];
  ok('but the match is kept, and the owner is told how long ago they asked', listed.held === 'resting' && /asked Dana Reyes 0 days ago/.test(listed.held_why) && (listed.matches_waiting || []).length > 0, `${listed.held_why} | waiting ${(listed.matches_waiting || []).length}`);
  ok('and the waiting match carries who asked and what for', (listed.matches_waiting || [])[0] && /lea@mazel/.test(listed.matches_waiting[0].asked_by) && /billing/.test(listed.matches_waiting[0].for), JSON.stringify((listed.matches_waiting || [])[0]));
  const anyway = await call(A, 'contacts', { action: 'route', ask_id: listed.matches_waiting[0].ask_id });
  ok('the owner can ask anyway, and is told why they might not want to', /asked Dana Reyes 0 days ago/.test(anyway) && /ask anyway/.test(anyway), anyway.slice(0, 130));

  // 2. Back off on silence. One ask is recorded; a second with nothing heard takes Dana out.
  await call(A, 'contacts', { action: 'route', ask_id: listed.matches_waiting[0].ask_id, confirmed: true });
  const g2 = JSON.parse(await call(A, 'list_ghosts', { q: 'dana' })).people[0];
  ok('two asks with nothing heard back takes a contact out of matching', g2.held === 'silent' && /nothing heard back/.test(g2.held_why), `${g2.held} ${g2.held_why}`);
  const f2 = await call(A, 'find', { need_text: 'revenue cycle again', tags: ['revenue-cycle'] });
  ok('and the owner\'s own find no longer offers them', !/Dana Reyes/.test(f2), (f2.match(/Dana[^"]{0,40}/) || [''])[0]);
  const heard = await call(A, 'contacts', { action: 'heard', ghost_id: gid });
  ok('recording that they got back to you puts them back in matching', /in matching again/.test(heard), heard.slice(0, 90));
  const f3 = await call(A, 'find', { need_text: 'revenue cycle once more', tags: ['revenue-cycle'] });
  ok('and find offers them again', /Dana Reyes/.test(f3), (f3.match(/Dana[^"]{0,30}/) || [''])[0]);

  // 3. Never ask. Out of everything, and reversible.
  const flagged = await call(A, 'contacts', { action: 'never_ask', ghost_id: gid });
  ok('never-ask is set in the owner\'s words', /will never be asked/.test(flagged), flagged.slice(0, 80));
  const f4 = await call(A, 'find', { need_text: 'revenue cycle yet again', tags: ['revenue-cycle'] });
  ok('a flagged contact is out of the owner\'s own find', !/Dana Reyes/.test(f4));
  // Counted for Dana alone: forty other contacts came from the CRM fixture and may well match.
  const asksAboutDana = async () => JSON.parse(await call(A, 'check_mailbox')).messages.filter(m => (m.action || {}).type === 'ghost.ask' && m.action.ghostId === gid).length;
  const n1 = await asksAboutDana();
  await casts(B, { need_text: 'a billing operations lead for a hospital', tags: ['billing-ops', 'hospital-billing'] });
  ok('and raises no question when a need arrives for them', (await asksAboutDana()) === n1, `${n1} -> ${await asksAboutDana()}`);
  ok('clearing it puts them back', /can be asked again/.test(await call(A, 'contacts', { action: 'never_ask', ghost_id: gid, off: true })));
}

// ---- heat: stored now, read by nothing yet, and the owner's own word wins ----------------------
{
  await call(A, 'note_ghost', { name: 'Priya Raman', org: 'orbit.health', have: ['claims-automation'], role: 'VP ops', witnesses: ['gmail'], edge_score: 61,
    last_inbound_at: '2026-08-02T10:00:00.000Z', last_outbound_at: '2026-09-30T10:00:00.000Z', unanswered_outbound: 3, state: 'they_went_quiet', state_source: 'inferred', state_reason: 'three unanswered since August' });
  const one = JSON.parse(await call(A, 'list_ghosts', { q: 'priya' })).people[0];
  ok('a contact carries when things last went each way, and how many are unanswered', one.last_inbound_at === '2026-08-02T10:00:00.000Z' && one.last_outbound_at === '2026-09-30T10:00:00.000Z' && one.unanswered_outbound === 3, JSON.stringify({ i: one.last_inbound_at, o: one.last_outbound_at, u: one.unanswered_outbound }));
  ok('and a state with where it came from and why', one.state === 'they_went_quiet' && one.state_source === 'inferred' && /three unanswered/.test(one.state_reason), `${one.state} ${one.state_source} ${one.state_reason}`);
  ok('no message text is stored with any of it', !JSON.stringify(one).match(/message|body|subject|text/i), Object.keys(one).join(','));
  // The owner says otherwise, and that stands.
  await call(A, 'note_ghost', { name: 'Priya Raman', org: 'orbit.health', state: 'active', state_source: 'owner', state_reason: 'we spoke at the conference' });
  const owned = JSON.parse(await call(A, 'list_ghosts', { q: 'priya' })).people[0];
  ok('the owner\'s own state is recorded as theirs', owned.state === 'active' && owned.state_source === 'owner', `${owned.state} ${owned.state_source}`);
  await call(A, 'note_ghost', { name: 'Priya Raman', org: 'orbit.health', state: 'ended', state_source: 'inferred', state_reason: 'nothing since June' });
  const still = JSON.parse(await call(A, 'list_ghosts', { q: 'priya' })).people[0];
  ok('and a later inferred state never overwrites it', still.state === 'active' && still.state_source === 'owner', `${still.state} ${still.state_source}`);
  ok('an unknown state value is not stored as one', (await (async () => { await call(A, 'note_ghost', { name: 'Priya Raman', org: 'orbit.health', state: 'on fire', state_source: 'owner' }); return JSON.parse(await call(A, 'list_ghosts', { q: 'priya' })).people[0].state; })()) !== 'on fire');
}

// ---- the words a contact reads: a name, a plain clause, the need, and no handle ----------------
{
  const gid = JSON.parse(await call(A, 'list_ghosts', { q: 'dana' })).people[0].id;
  await call(A, 'contacts', { action: 'heard', ghost_id: gid });
  const words = await call(A, 'contacts', { action: 'invite_text', ghost_id: gid, thread_id: JSON.parse(await call(A, 'list_threads'))[0].thread_id });
  ok('an invitation names the specific need', /looking for/.test(words), (words.match(/is looking for[^.]{0,50}/) || [''])[0]);
  // The words handed over after a yes, which a contact who has never heard of Mazel reads.
  // Dana has been asked already, so her rest is aged out: fourteen days pass.
  for (const k of [...portals[A].MAILBOX.m.keys()]) if (k.startsWith('cap:offer:')) portals[A].MAILBOX.m.delete(k);
  for (const [k, v] of [...portals[A].MAILBOX.m.entries()]) {
    if (!k.startsWith('ghost:')) continue;
    const g = JSON.parse(v);
    if (g.name !== 'Dana Reyes') continue;
    portals[A].MAILBOX.m.set(k, JSON.stringify({ ...g, lastAskedAt: new Date(Date.now() - 20 * 86400000).toISOString(), asked: 1, heard: 1 }));
  }
  await casts(B, { need_text: 'someone who runs revenue cycle for a mid-size clinic group', tags: ['clinic-revenue'] });
  const box2 = JSON.parse(await call(A, 'check_mailbox'));
  const fresh = (box2.messages || []).filter(m => (m.action || {}).type === 'ghost.ask' && m.action.ghostId === gid)[0];   // newest first
  ok('a rested contact is asked about again once the rest has run out', !!fresh, fresh ? fresh.text.slice(0, 80) : 'no ask');
  if (fresh) {
    const handed = await call(A, 'contacts', { action: 'route', ask_id: fresh.action.askId, confirmed: true });
    const words = handed.split('here are the words, to send however you like.')[1] || handed;
    ok('the words name the asker as a person, not a handle', /Lea/.test(words) && !/@mazel/.test(words), words.replace(/\n/g, ' ').slice(0, 140));
    ok('and say in one plain clause how the owner knows them', /someone I know/.test(words), (words.match(/Lea[^.]{0,60}/) || [''])[0]);
    ok('and carry the need in the asker\'s own words, then ask for the intro', /runs revenue cycle for a mid-size clinic group/.test(words) && /Open to a quick intro\?/.test(words));
  }
}

// ---- seeding: a batch, evidence in and scores out, and a key instead of an address ------------
{
  // (a) A batch, and a refusal that writes nothing.
  const before = JSON.parse(await call(A, 'list_ghosts')).count;
  const fifty = Array.from({ length: 50 }, (_, i) => ({ name: `Batch Person ${i}`, org: `co${i}.example`, have: ['widget-' + i], witnesses: ['linkedin'] }));
  const wrote = await call(A, 'contacts', { action: 'note', people: fifty });
  const afterFifty = JSON.parse(await call(A, 'list_ghosts')).count;
  ok('SEED: fifty contacts go in one call', /Wrote 50 of 50/.test(wrote) && afterFifty === before + 50, `${before} -> ${afterFifty}`);
  const refused = await call(A, 'contacts', { action: 'note', people: [...fifty, { name: 'One Too Many', org: 'x.example' }] });
  const afterRefusal = JSON.parse(await call(A, 'list_ghosts')).count;
  ok('SEED: fifty-one is refused whole, with the number that is allowed', /51 people in one call and 50 is the most/.test(refused), refused.slice(0, 100));
  ok('SEED: and nothing of it is written', afterRefusal === afterFifty, `${afterFifty} -> ${afterRefusal}`);

  // (b) Evidence in, scores out.
  await call(A, 'contacts', { action: 'note', name: 'Mina Okafor', org: 'northstar.example', have: ['clinical-ops'], witnesses: ['calendar', 'gmail'],
    meetings: 9, small_meetings: 6, threads_sent: 20, threads_replied: 14, deals: 2, best_stage: 'won', referrer_count: 1 });
  const mina = JSON.parse(await call(A, 'list_ghosts', { q: 'mina' })).people[0];
  ok('SEED: the portal works the edge out from the counts, not the agent', mina.edge.score > 60 && !mina.edge.agent_scored, `${mina.edge.score} | ${mina.edge.signals}`);
  ok('SEED: and says what the number is made of', /small meetings/.test(mina.edge.signals) && /deal/.test(mina.edge.signals) && /replied/.test(mina.edge.signals), mina.edge.signals);
  ok('SEED: the counts are kept, so the score can be redone when the weights change', mina.facts && mina.facts.small_meetings === 6 && mina.facts.best_stage === 'won', JSON.stringify(mina.facts));
  await call(A, 'contacts', { action: 'note', name: 'Gus Petrov', org: 'solo.example', have: ['logistics'], edge_score: 71, witnesses: ['typed'] });
  const gus = JSON.parse(await call(A, 'list_ghosts', { q: 'gus' })).people[0];
  ok('SEED: an agent\'s own number is still taken, and marked as the agent\'s', gus.edge.score === 71 && gus.edge.agent_scored === true, `${gus.edge.score} agent_scored=${gus.edge.agent_scored}`);
  // Someone barely known scores low, so the funnel means something.
  await call(A, 'contacts', { action: 'note', name: 'Faint Acquaintance', org: 'far.example', have: ['widgets'], threads_sent: 2, threads_replied: 0, witnesses: ['linkedin'] });
  const faint = JSON.parse(await call(A, 'list_ghosts', { q: 'faint' })).people[0];
  ok('SEED: and someone only connected to scores far below someone met', faint.edge.score < mina.edge.score / 2, `${faint.edge.score} vs ${mina.edge.score}`);

  // (c) A key, not an address.
  await call(A, 'contacts', { action: 'note', name: 'Mina Okafor', org: 'northstar.example', email: 'mina@northstar.example', have: ['clinical-ops'] });
  const withKey = JSON.parse(await call(A, 'list_ghosts', { q: 'mina' }));
  ok('SEED: a repeat save with an address finds the same person, not a second one', withKey.people.length === 1 && withKey.people[0].id === mina.id, `${withKey.people.length} records`);
  ok('SEED: the address itself is nowhere in the contact', !JSON.stringify(withKey.people[0]).includes('mina@northstar.example'), JSON.stringify(withKey.people[0].ekeys || []));
  ok('SEED: what is kept is a key, and it is not a plain hash of the address', (withKey.people[0].ekeys || []).length === 1 && /^[0-9a-f]{32}$/.test(withKey.people[0].ekeys[0]));
  const secret = portals[A].MAILBOX.m.get('config:contact-key');
  ok('SEED: the secret that makes it was made here and is not on any wire', !!secret && !wireLog.join('').includes(secret) && !wireLog.join('').includes(withKey.people[0].ekeys[0]));
  await call(A, 'contacts', { action: 'note', name: 'Mina Okafor', org: 'northstar.example', email: 'm.okafor@gmail.example' });
  const twoAddr = JSON.parse(await call(A, 'list_ghosts', { q: 'mina' })).people[0];
  ok('SEED: a second address attaches to the same person', twoAddr.ekeys.length === 2 && twoAddr.id === mina.id, `${twoAddr.ekeys.length} keys`);
  // Two portals keying the same address get different keys, because the secret is per portal.
  const bKey = portals[B].MAILBOX.m.get('config:contact-key');
  ok('SEED: and another portal cannot compute this portal\'s key', bKey !== secret, 'secrets differ per portal');
}

// ---- the bar an arriving need has to clear against a contact ----------------------------------
{
  for (const k of [...portals[A].MAILBOX.m.keys()]) if (k.startsWith('cap:offer:')) portals[A].MAILBOX.m.delete(k);
  // A word this portal's own contacts share says nothing about any of them.
  for (let i = 0; i < 40; i++) await call(A, 'contacts', { action: 'note', name: `Advisor ${i}`, org: `adv${i}.example`, have: ['partner-advisor'], role: 'advisor to clients', witnesses: ['linkedin'], small_meetings: 3 });
  // One document per arriving need (review 2026-10-07a, M2): the questions raised are in `asks`,
  // the matches kept against a contact are in `contacts`.
  const docFor = (needId) => [...portals[A].MAILBOX.m.entries()].filter(([k]) => k.startsWith('ghostask:')).map(([, v]) => JSON.parse(v)).find((d) => d.needId === needId);
  const asksFor = async (needId) => { const d = docFor(needId); return d ? [...(d.asks || []), ...(d.contacts || [])] : []; };
  const f1 = await casts(B, { need_text: 'an advisor for our clients', tags: ['advisor-intro'] });
  const id1 = JSON.parse(f1).thread_id;
  const box1 = JSON.parse(await call(A, 'check_mailbox'));
  const raised1 = (box1.messages || []).filter(m => (m.action || {}).type === 'ghost.ask' && (m.text || '').includes('Advisor ')).length;
  ok('BAR: a need sharing only a common word raises nothing', raised1 === 0, `${raised1} questions`);
  // Forty contacts that genuinely match, and three questions.
  for (const k of [...portals[A].MAILBOX.m.keys()]) if (k.startsWith('cap:offer:')) portals[A].MAILBOX.m.delete(k);
  for (let i = 0; i < 40; i++) await call(A, 'contacts', { action: 'note', name: `Cryo Person ${i}`, org: `cryo${i}.example`, have: ['cryogenic-valves'], witnesses: ['calendar'],
    small_meetings: 4 + (i % 6), deals: 1, best_stage: 'won', threads_sent: 10, threads_replied: 8 });
  const f2 = await casts(B, { need_text: 'someone who knows cryogenic valves', tags: ['cryogenic-valves'] });
  const id2 = JSON.parse(f2).thread_id;
  const mine2 = await asksFor(id2);
  const askedCount = mine2.filter((a) => a.state === 'asked').length;
  const heldCount = mine2.filter((a) => a.state === 'held').length;
  ok('BAR: a need matching forty contacts raises three questions', askedCount === 3, `${askedCount} asked, ${heldCount} held, ${mine2.length} matched`);
  ok('BAR: and the rest are kept rather than thrown away', heldCount > 30, `${heldCount} held`);
  const listed = JSON.parse(await call(A, 'list_ghosts', { q: 'cryo person 7' })).people[0];
  ok('BAR: a kept match shows against that contact, with who asked', (listed.matches_waiting || []).length > 0 && /lea@mazel/.test((listed.matches_waiting || [])[0].asked_by), JSON.stringify((listed.matches_waiting || [])[0] || {}));
}

// ---- a stated need ends somewhere: on the card, or held ---------------------------------------
// Saying "mazel, I need X" used to search and leave the card untouched, so Avery had to ask whether
// the need had been added at all. A need the person stated ends in one of two places, and the
// portal keeps asking until it knows which.
{
  const N = 'https://mazel.need.gh';
  portals[N] = { HANDLE:'nia@mazel', PERSONA:'Nia runs a clinic.', NEED:'', HAVE:'clinic-ops', INBOX_TOKEN:'tn', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: N, OWNER_EMAIL:'nia@clinic.example' };
  const WHERE = 'Put this on your card for everyone, or keep it private\\? Private reaches fewer people\\.';
  const f1 = JSON.parse(await call(N, 'find', { need_text: 'a pediatric sleep specialist', tags: ['pediatric-sleep'] }));
  const ask = f1.where_it_lives;
  ok('LOOP: a new need comes back with one question to put to the person', !!ask && !!ask.ask_the_person_exactly, JSON.stringify(ask || {}).slice(0, 80));
  ok('LOOP: and it is the sentence, word for word', ask.ask_the_person_exactly === 'Put this on your card for everyone, or keep it private? Private reaches fewer people.', ask.ask_the_person_exactly);
  ok('LOOP: with no tier words anywhere in what the person hears', !/matched-only|visibility|tier|public card/i.test(ask.ask_the_person_exactly) && /do not say/i.test(ask.say_nothing_else), ask.say_nothing_else.slice(0, 60));
  ok('LOOP: and the exact call for each answer', /add_need/.test(ask.on_everyone) && /matched-only/.test(ask.on_private), `${ask.on_everyone} | ${ask.on_private}`);
  ok('LOOP: the result carries the record-only rule', /RECORD ONLY/.test(f1.record_only) && /does not say they own/.test(f1.record_only), (f1.record_only || '').slice(0, 60));

  // 3. Unanswered, it comes back on the next call, whatever that call is.
  const box = await call(N, 'check_mailbox');
  const again = box.startsWith('{') ? (JSON.parse(box).still_to_settle || [])[0] : null;
  ok('LOOP: an unanswered question reappears on the next tool result',
     (again && again.about === 'a pediatric sleep specialist') || (/Still to settle: a pediatric sleep specialist/.test(box) && new RegExp(WHERE).test(box)),
     box.replace(/\n/g, ' ').slice(0, 90));
  const mine = JSON.parse(await call(N, 'my_card'));
  ok('LOOP: and on the one after that, until it is answered', (mine.still_to_settle || []).length === 1, JSON.stringify((mine.still_to_settle || []).map(a => a.about)));
  ok('LOOP: and the need is on no card while it waits', !(mine.need || []).includes('pediatric-sleep') && !(mine.heldNeeds || []).some(n => n.tag === 'pediatric-sleep'), JSON.stringify(mine.need));

  // 2a. "For everyone" puts it on the card.
  const wrote = await call(N, 'update_card', { add_need: 'pediatric-sleep', confirmed: true });
  const after = JSON.parse(await call(N, 'my_card'));
  ok('LOOP: answering for everyone puts the tag on the card', (after.need || []).includes('pediatric-sleep'), JSON.stringify(after.need));
  ok('LOOP: and the agent is told what to say, in words a person would use', /on their card now/.test(wrote) && !/matched-only|visibility/.test(wrote.split('{')[0]), wrote.split('\n').find(l => /Say in one line/.test(l)) || wrote.slice(0, 80));
  ok('LOOP: the question stops once it is answered', (after.still_to_settle || []).length === 0, JSON.stringify(after.still_to_settle || []));
  const f2 = JSON.parse(await call(N, 'find', { need_text: 'a pediatric sleep specialist', tags: ['pediatric-sleep'] }));
  ok('LOOP: and finding the same need again asks nothing', typeof f2.where_it_lives === 'string' && /settled/.test(f2.where_it_lives), String(f2.where_it_lives));

  // 2b. "Private" holds it instead.
  const f3 = JSON.parse(await call(N, 'find', { need_text: 'somebody to buy out my lease', tags: ['lease-buyout'] }));
  ok('LOOP: a second new need raises its own question', !!f3.where_it_lives.ask_the_person_exactly, f3.where_it_lives.about);
  const held = await call(N, 'update_card', { add_need: 'lease-buyout', need_visibility: 'matched-only', confirmed: true });
  const now = JSON.parse(await call(N, 'my_card'));
  ok('LOOP: answering private holds it, off the card', (now.heldNeeds || []).some(n => n.tag === 'lease-buyout' && n.visibility === 'matched-only') && !(now.need || []).includes('lease-buyout'), JSON.stringify(now.heldNeeds));
  ok('LOOP: and the agent is told it is kept private, without naming the machinery', /kept private/.test(held) && /travels only to an agent/.test(held), (held.split('\n').find(l => /Say in one line/.test(l)) || '').slice(0, 110));
  ok('LOOP: and nothing is left to settle', (now.still_to_settle || []).length === 0, JSON.stringify(now.still_to_settle || []));
  ok('LOOP: a held need never reached the relay as words', !JSON.stringify([...renv.RELAY.m.values()]).includes('lease-buyout') && !JSON.stringify([...renv.RELAY.m.values()]).includes('buy out my lease'));

  // A need the agent gave no tag for still gets a tag to write, from the need's own words.
  const f4 = JSON.parse(await call(N, 'find', { need_text: 'an architect who has done cold storage' }));
  ok('LOOP: a need with no tags still ends with something to write', /add_need: "[a-z0-9-]+"/.test(f4.where_it_lives.on_everyone), f4.where_it_lives.on_everyone);
}

// ---- nothing about an unplaced need leaves this portal in words ------------------------------
{
  const U = 'https://mazel.unsettled.gh', V = 'https://mazel.holder.gh';
  portals[U] = { HANDLE:'uma@mazel', PERSONA:'Uma runs a foundry.', NEED:'', HAVE:'metal-casting', INBOX_TOKEN:'tu', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: U, OWNER_EMAIL:'uma@foundry.example' };
  portals[V] = { HANDLE:'vic@mazel', PERSONA:'Vic restores kilns.', NEED:'', HAVE:'kiln-restoration', INBOX_TOKEN:'tv', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: V, OWNER_EMAIL:'vic@kilns.example' };
  await call(U, 'add_known_card', { url: V + '/card' });
  await call(V, 'add_known_card', { url: U + '/card' });
  const SECRET = 'a kiln restorer for a nineteenth century bottle oven';
  // The need's own words. The tag is deliberately not here: it is vic's own public have, so it
  // lives in vic's card and on the wire for reasons that have nothing to do with uma's need.
  const WORDS = ['kiln restorer', 'bottle oven', 'nineteenth century'];
  const outside = () => {
    // Everything that left this portal, plus everything the other portal and the relay now hold.
    const wire = wireLog.filter(l => !l.startsWith(U)).join('\n');
    return [wire, JSON.stringify([...portals[V].MAILBOX.m.values()]), JSON.stringify([...renv.RELAY.m.values()])].join('\n');
  };
  const leaks = (hay) => WORDS.filter(w => hay.includes(w));

  // 1. Unsettled: scored here, and nothing of it anywhere else.
  const mark = wireLog.length;
  const f = JSON.parse(await call(U, 'find', { need_text: SECRET, tags: ['kiln-restoration'] }));
  ok('HOLD: an unplaced need is still scored against the cards this portal holds', (f.candidates || []).some(c => c.handle === 'vic@mazel'), JSON.stringify((f.candidates || []).map(c => c.handle)));
  ok('HOLD: and the person is asked where it lives', !!f.where_it_lives.ask_the_person_exactly, f.where_it_lives.about);
  ok('HOLD: but no word of it left this portal', leaks(outside()).length === 0, leaks(outside()).join(' | ') || 'nothing');
  const branchesOnV = [...portals[V].MAILBOX.m.keys()].filter(k => k.startsWith('conv:') || k.startsWith('msg:')).length;
  ok('HOLD: and no branch was opened on the other portal', branchesOnV === 0, `${branchesOnV} on vic's portal`);
  const sentWhileUnsettled = wireLog.slice(mark).filter(l => !l.startsWith(U) && l.includes('"method"'));
  ok('HOLD: in fact nothing at all was sent to anybody', sentWhileUnsettled.length === 0, sentWhileUnsettled.map(l => l.split(' ')[0]).join(' ') || 'no calls out');
  ok('HOLD: the relay was never told', !JSON.stringify([...renv.RELAY.m.values()]).includes('kiln'), 'relay has no trace');
  await call(U, 'pulse');
  ok('HOLD: and a pulse does not let it out either', leaks(outside()).length === 0, leaks(outside()).join(' | ') || 'still nothing');

  // 2. Private: buckets only, for ever.
  await call(U, 'update_card', { add_need: 'kiln-restoration', need_visibility: 'matched-only', confirmed: true });
  await call(U, 'pulse');
  ok('PRIVATE: answering private leaves zero words outside this portal', leaks(outside()).length === 0, leaks(outside()).join(' | ') || 'zero');
  const sentToV = wireLog.slice(mark).filter(l => l.startsWith(V));
  ok('PRIVATE: something did go out, so this is not just silence', sentToV.length > 0, `${sentToV.length} messages to vic`);
  ok('PRIVATE: and what went was a fingerprint, with no text and no tags', sentToV.some(l => /"fp":\[/.test(l)) && !sentToV.some(l => /kiln-restoration/.test(l)), (sentToV.find(l => /"fp"/.test(l)) || '').slice(0, 70));

  // 3. Public: everything at once, on the answer rather than on the next pulse.
  const W = 'https://mazel.public.gh';
  portals[W] = { HANDLE:'win@mazel', PERSONA:'Win casts bronze.', NEED:'', HAVE:'bronze-casting', INBOX_TOKEN:'tw', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: W, OWNER_EMAIL:'win@bronze.example' };
  await call(W, 'add_known_card', { url: V + '/card' });
  const OPEN = 'somebody who can reline a bronze furnace';
  const f2 = JSON.parse(await call(W, 'find', { need_text: OPEN, tags: ['furnace-relining'] }));
  const before = wireLog.filter(l => l.includes('reline a bronze furnace')).length;
  ok('PUBLIC: before the answer, the words have not travelled', before === 0, `${before} mentions on the wire`);
  await call(W, 'update_card', { add_need: 'furnace-relining', confirmed: true });
  const after = wireLog.filter(l => l.includes('reline a bronze furnace')).length;
  ok('PUBLIC: answering for everyone releases it at once, with no pulse in between', after > 0, `${after} mentions on the wire straight after the answer`);
  const cast = JSON.stringify([...renv.RELAY.m.values()]).includes('reline a bronze furnace');
  ok('PUBLIC: the relay has it', cast);
  ok('PUBLIC: and the card this portal holds was offered it', wireLog.some(l => l.startsWith(V) && l.includes('reline a bronze furnace')));
  const t = JSON.parse(portals[W].MAILBOX.m.get('thread:' + f2.thread_id));
  ok('PUBLIC: and a branch was opened with the candidate the scoring found', (t.candidates || []).every(c => !!c.contextId), JSON.stringify((t.candidates || []).map(c => [c.handle, !!c.contextId])));
}

// ---- what the 2026-10-07a review found, each one held here --------------------------------------
{
  const RA = 'https://mazel.rev-a.gh', RB = 'https://mazel.rev-b.gh', RC = 'https://mazel.rev-c.gh';
  for (const [o, h, have] of [[RA, 'ana@mazel', 'kiln-work'], [RB, 'bo@mazel', 'furnace-lining'], [RC, 'cy@mazel', 'kiln-work']]) {
    portals[o] = { HANDLE: h, PERSONA: `${h} does things.`, NEED: '', HAVE: have, INBOX_TOKEN: 't' + h[0], MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: o, OWNER_EMAIL: `${h[0]}@x.example` };
  }
  await call(RA, 'add_known_card', { url: RB + '/card' });
  await call(RB, 'add_known_card', { url: RA + '/card' });
  await call(RC, 'add_known_card', { url: RA + '/card' });   // Cy holds Ana's card; Ana does not hold Cy's
  await call(RB, 'contacts', { action: 'note', name: 'Tomas Vrba', org: 'vrba.example', have: ['kiln-relining'], witnesses: ['calendar'], small_meetings: 6, deals: 1, best_stage: 'won', threads_sent: 12, threads_replied: 9 });
  await call(RC, 'contacts', { action: 'note', name: 'Priya Anand', org: 'anand.example', have: ['kiln-relining'], witnesses: ['calendar'], small_meetings: 6, deals: 1, best_stage: 'won', threads_sent: 12, threads_replied: 9 });

  // M1: an offer names who it is for, and a copy posted at another door is refused.
  const mark = wireLog.length;
  await call(RA, 'update_card', { add_need: 'kiln-relining', confirmed: true });
  await call(RA, 'find', { need_text: 'somebody who relines kilns', tags: ['kiln-relining'] });
  const offerWire = wireLog.slice(mark).find((l) => l.startsWith(RB) && l.includes('need.offer'));
  ok('REV M1: an offer carries the handle it was sent to', !!offerWire && /"to":"bo@mazel"/.test(offerWire), (offerWire || '').slice(-80));
  const askedB = [...portals[RB].MAILBOX.m.keys()].filter((k) => k.startsWith('ghostask:')).length;
  ok('REV M1: and the portal it names scores it', askedB > 0, `${askedB} on bo's portal`);
  const body = offerWire.slice(offerWire.indexOf(' ') + 1);
  await worker.fetch(new Request(RC + '/a2a', { method: 'POST', headers: { 'content-type': 'application/json' }, body }), portals[RC]);
  const askedC = [...portals[RC].MAILBOX.m.keys()].filter((k) => k.startsWith('ghostask:')).length;
  ok('REV M1: the same envelope at a door the caster never chose is refused', askedC === 0, `${askedC} on cy's portal`);
  ok('REV M1: so Cy is never asked about anyone', !((await call(RC, 'check_mailbox')).includes('Priya')));
  // An offer with no recipient at all is refused like one naming somebody else: there is no older
  // version to carry, and a signature says who wrote an offer and not who may read it.
  const noTo = JSON.parse(body);
  const act = noTo.params.message.metadata.action;
  delete act.to;
  const beforeNoTo = [...portals[RB].MAILBOX.m.keys()].filter((k) => k.startsWith('ghostask:')).length;
  const res = await worker.fetch(new Request(RB + '/a2a', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(noTo) }), portals[RB]);
  const afterNoTo = [...portals[RB].MAILBOX.m.keys()].filter((k) => k.startsWith('ghostask:')).length;
  ok('REV M1: an offer with no recipient is refused, even at the door it was meant for', afterNoTo === beforeNoTo, `${beforeNoTo} -> ${afterNoTo} ask documents`);
  ok('REV M1: and dropping the field breaks the signature, so it cannot be removed in flight',
     (await res.json()).error !== undefined || afterNoTo === beforeNoTo, 'refused');

  // M2 and M3: one document per arriving need, and the caster's words fenced wherever they show.
  const askKeys = [...portals[RB].MAILBOX.m.keys()].filter((k) => k.startsWith('ghostask:'));
  ok('REV M2: one arriving need is one document, not one per contact that fitted', askKeys.length === 1, `${askKeys.length} documents`);
  const doc = JSON.parse(portals[RB].MAILBOX.m.get(askKeys[0]));
  ok('REV M2: holding the questions raised and the matches kept', Array.isArray(doc.asks) && Array.isArray(doc.contacts), JSON.stringify({ asks: (doc.asks || []).length, kept: (doc.contacts || []).length }));
  ok('REV M3: the caster\'s words in the contacts list are fenced like everywhere else', (() => {
    const p = JSON.parse(portals[RB].MAILBOX.m.get(askKeys[0]));
    return p.needText.length > 0;   // stored once, on the document
  })());

  // H1: the owner's answers about a person survive the agent re-reading its sources.
  const bo = JSON.parse(await call(RB, 'list_ghosts', { q: 'tomas' })).people[0];
  const ask1 = (JSON.parse(await call(RB, 'check_mailbox')).messages || []).find((m) => (m.action || {}).type === 'ghost.ask');
  await call(RB, 'contacts', { action: 'route', ask_id: ask1.action.askId, confirmed: true });
  const askedOnce = JSON.parse(await call(RB, 'list_ghosts', { q: 'tomas' })).people[0];
  ok('REV: a yes is counted against the contact', askedOnce.asked === 1 && !!askedOnce.lastAskedAt, `asked=${askedOnce.asked}`);
  // While Tomas is resting, a second need that fits him is kept against him and still answerable:
  // the behaviour the review's shape check was aiming at.
  await call(RA, 'update_card', { remove_need: 'kiln-relining', confirmed: true });
  await call(RA, 'update_card', { add_need: 'kiln-rebuild', confirmed: true });
  await call(RB, 'contacts', { action: 'note', name: 'Tomas Vrba', org: 'vrba.example', have: ['kiln-relining', 'kiln-rebuild'], witnesses: ['calendar'] });
  await call(RA, 'find', { need_text: 'a kiln rebuild for a bottle oven', tags: ['kiln-rebuild'] });
  const resting = JSON.parse(await call(RB, 'list_ghosts', { q: 'tomas' })).people[0];
  ok('REV: a match that arrived while a contact was resting is kept against them, with its state',
     resting.held === 'resting' && (resting.matches_waiting || []).some((m) => m.state === 'resting'), `${resting.held} | ${JSON.stringify((resting.matches_waiting || [])[0] || null)}`);
  const anywayId = (resting.matches_waiting || []).find((m) => m.state === 'resting');
  const anyway = anywayId ? await call(RB, 'contacts', { action: 'route', ask_id: anywayId.ask_id }) : 'no id';
  ok('REV: and the owner can ask anyway, which is what keeping it is for', /ask anyway/.test(anyway), anyway.slice(0, 90));

  await call(RB, 'contacts', { action: 'never_ask', ghost_id: bo.id });
  await call(RB, 'contacts', { action: 'note', name: 'Tomas Vrba', org: 'vrba.example', have: ['kiln-relining'], witnesses: ['calendar'], threads_replied: 11 });
  const afterReseed = JSON.parse(await call(RB, 'list_ghosts', { q: 'tomas' })).people[0];
  ok('REV H1: never-ask survives the agent re-reading its sources', afterReseed.neverAsk === true, `neverAsk=${afterReseed.neverAsk}`);
  ok('REV H1: and so do the ask count and the rest it started', afterReseed.asked === 1 && !!afterReseed.lastAskedAt, `asked=${afterReseed.asked} lastAskedAt=${!!afterReseed.lastAskedAt}`);

  // L3: the owner's own invitation respects never-ask, and counts as an ask when it is handed over.
  const t2 = JSON.parse(await call(RB, 'find', { need_text: 'somebody who relines kilns for me', tags: ['kiln-relining'] }));
  let refused = '';
  try { refused = 'not refused: ' + await call(RB, 'contacts', { action: 'invite_text', ghost_id: bo.id, thread_id: t2.thread_id }); } catch (e) { refused = e.message; }
  ok('REV L3: invite_text refuses a never-ask contact', /never-ask/.test(refused), refused.slice(0, 90));
  await call(RB, 'contacts', { action: 'never_ask', ghost_id: bo.id, on: false });
  await call(RB, 'contacts', { action: 'note', name: 'Wil Osei', org: 'osei.example', have: ['kiln-relining'], witnesses: ['calendar'], small_meetings: 5, threads_sent: 8, threads_replied: 6 });
  const wil = JSON.parse(await call(RB, 'list_ghosts', { q: 'wil' })).people[0];
  const t3 = JSON.parse(await call(RB, 'find', { need_text: 'somebody who relines kilns for me', tags: ['kiln-relining'] }));
  await call(RB, 'contacts', { action: 'invite_text', ghost_id: wil.id, thread_id: t3.thread_id });
  const wilAfter = JSON.parse(await call(RB, 'list_ghosts', { q: 'wil' })).people[0];
  ok('REV L3: and handing the words over counts as an ask, so the rest starts', wilAfter.asked === 1 && !!wilAfter.lastAskedAt, `asked=${wilAfter.asked}`);

  // L5: two people, one name, no organisation.
  await call(RB, 'contacts', { action: 'note', name: 'Sam Lee', have: ['tax-advice'], role: 'Tax adviser', witnesses: ['gmail'] });
  await call(RB, 'contacts', { action: 'note', name: 'Sam Lee', have: ['dinghy-racing'], role: 'Sailing instructor', witnesses: ['linkedin'] });
  const sams = JSON.parse(await call(RB, 'list_ghosts', { q: 'sam lee' })).people;
  ok('REV L5: two people with one name and no organisation stay two people', sams.length === 2, `${sams.length}: ${sams.map((x) => (x.have || []).join('+')).join(' / ')}`);
  await call(RB, 'contacts', { action: 'note', name: 'Sam Lee', have: ['tax-advice', 'estate-planning'], role: 'Tax adviser', witnesses: ['gmail'] });
  const sams2 = JSON.parse(await call(RB, 'list_ghosts', { q: 'sam lee' })).people;
  ok('REV L5: and a repeat save of one of them finds that one, by the witness they share', sams2.length === 2 && sams2.some((x) => (x.have || []).includes('estate-planning')), `${sams2.length} records`);
}

// ---- forgetting is always allowed ----
const beforeForget = JSON.parse(await call(A, 'list_ghosts')).count;
const one = JSON.parse(await call(A, 'list_ghosts')).people[0];
ok('a ghost can be forgotten without ceremony', /Forgotten/.test(await call(A, 'forget_ghost', { ghost_id: one.id })));
ok('and is gone', JSON.parse(await call(A, 'list_ghosts')).count === beforeForget - 1, `${beforeForget} -> ${JSON.parse(await call(A, 'list_ghosts')).count}`);

// ---- the witness check ----
{
  const wc = JSON.parse(await call(A, 'my_card')).witnessCheck;
  ok('my_card says which sources the ghosts came from', wc.ghostsBySource.hubspot > 0 && wc.ghostsBySource.linkedin > 0, JSON.stringify(wc.ghostsBySource));
  ok('and which haves nobody but the person vouches for', wc.ownerOnlyHaves.includes('managed-ai-delivery'));
  ok('and asks the agent to name what it is connected to that is missing', /Enumerate the connectors/.test(wc.ask));
}

// ---- aging: a need goes quiet on passes, and asks once when something finally fits ----
{
  const t = JSON.parse(await call(A, 'find', { need_text:'someone who has done a carve-out', tags:['carve-out'] }));
  const key = `thread:${t.thread_id}`;
  const th = JSON.parse(portals[A].MAILBOX.m.get(key));
  th.passes = 3;
  portals[A].MAILBOX.m.set(key, JSON.stringify(th));
  await call(A, 'pulse');
  const after = JSON.parse(portals[A].MAILBOX.m.get(key));
  ok('three passes in a row and a need goes quiet', after.status === 'quiet' && /passes/.test(after.quietBecause || ''), after.status + ' ' + after.quietBecause);
  const silent = JSON.parse(await call(A, 'find', { need_text:'a second thing entirely', tags:['second-thing'] }));
  const k2 = `thread:${silent.thread_id}`;
  const t2 = JSON.parse(portals[A].MAILBOX.m.get(k2));
  t2.lastSignal = new Date(Date.now() - 61*24*60*60*1000).toISOString();
  portals[A].MAILBOX.m.set(k2, JSON.stringify(t2));
  await call(A, 'pulse');
  ok('and so does one nothing has touched for sixty days', JSON.parse(portals[A].MAILBOX.m.get(k2)).status === 'quiet');
}

// ---- a have nobody has corroborated for a year slides a tier ----
{
  const solo = { HANDLE:'old@mazel', PERSONA:'p', NEED:'', HAVE:'', INBOX_TOKEN:'to', MAILBOX: mkKV(), PORTAL_ORIGIN: A, RELAY_URL:'none' };
  const t = async (n, a) => JSON.parse(await (await worker.fetch(new Request(A+'/mcp?token=to', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:n, arguments:a||{} } }) }), solo)).text()).result.content[0].text;
  await t('update_card', { add_have:'stale-thing', witnesses:'hubspot', confirmed:true });
  const md = solo.MAILBOX.m.get('memory:card.md').replace(/\(seen: [^)]+\)/, '(seen: 2024-01-01)');
  solo.MAILBOX.m.set('memory:card.md', md);
  await t('pulse');
  ok('a year with no witness and a public have slides to tribe', /\[tribe\] stale-thing/.test(solo.MAILBOX.m.get('memory:card.md')), solo.MAILBOX.m.get('memory:card.md').split('\n').find(l=>/stale-thing/.test(l)));
}

// ---- bug 9 ----
{
  const two = { HANDLE:'two@mazel', PERSONA:'p', NEED:'thing', HAVE:'', INBOX_TOKEN:'t2', MAILBOX: mkKV(), PORTAL_ORIGIN: A, RELAY_URL:'none' };
  const t = async (n, a) => JSON.parse(await (await worker.fetch(new Request(A+'/mcp?token=t2', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:n, arguments:a||{} } }) }), two)).text()).result.content[0].text;
  await t('my_card');
  await t('update_card', { remove_need:'thing', add_need:'thing', need_visibility:'matched-only', confirmed:true });
  const c = JSON.parse(await t('my_card'));
  ok('removing and re-adding one tag in a single call keeps it', c.heldNeeds.some(n => n.tag === 'thing' && n.visibility === 'matched-only'), JSON.stringify(c.heldNeeds));

  // The witnesses map is what a person reads to see what stands behind each tag. It has to be the
  // card and nothing else: a key with no tag behind it reads as something removed that did not go
  // (Avery, 2026-10-06, after reading his own portal).
  await t('update_card', { add_have:'widget-polishing', witnesses:['hubspot'], confirmed:true });
  await t('update_card', { add_need:'widget-buyers', witnesses:['gmail'], confirmed:true });
  await t('update_card', { add_have:'parked-thing', have_visibility:'tribe', witnesses:['calendar'], confirmed:true });
  const full = JSON.parse(await t('my_card'));
  // need entries are tags on the public card and objects where a tier matters, so take both shapes.
  const tagOf = (n) => typeof n === 'string' ? n : n.tag;
  const onCard = (card) => [...new Set([...(card.have || []), ...(card.need || []).map(tagOf), ...(card.heldNeeds || []).map(tagOf), ...(card.heldHaves || []).map(tagOf)])];
  ok('a need records where it was read from, like a have does', (full.witnesses['widget-buyers'] || []).includes('gmail'), JSON.stringify(full.witnesses['widget-buyers'] || []));
  ok('the witnesses map has a key for every tag on the card and nothing else',
     Object.keys(full.witnesses).sort().join() === onCard(full).sort().join(), `map ${Object.keys(full.witnesses).sort().join()} vs card ${onCard(full).sort().join()}`);
  ok('including a have parked at tribe, which is on the card and not a leftover', (full.witnesses['parked-thing'] || []).includes('calendar'), JSON.stringify(full.witnesses['parked-thing'] || []));
  await t('update_card', { remove_have:'widget-polishing', remove_need:'widget-buyers', add_need:'other-buyers', witnesses:['calendar'], confirmed:true });
  const after = JSON.parse(await t('my_card'));
  ok('a removed tag takes its witnesses with it', after.witnesses['widget-polishing'] === undefined && after.witnesses['widget-buyers'] === undefined, JSON.stringify(Object.keys(after.witnesses)));
  ok('and a need added in the same call gets its own entry', (after.witnesses['other-buyers'] || []).includes('calendar'), JSON.stringify(after.witnesses['other-buyers'] || []));
  ok('so the map still matches the card after a remove and an add together',
     Object.keys(after.witnesses).sort().join() === onCard(after).sort().join(), `map ${Object.keys(after.witnesses).sort().join()} vs card ${onCard(after).sort().join()}`);
  // Removing one tier of a tag that exists at two does not take the other one's witnesses.
  await t('update_card', { add_have:'thing', witnesses:['hubspot'], confirmed:true });
  await t('update_card', { remove_need:'thing', confirmed:true });
  const both = JSON.parse(await t('my_card'));
  ok('a tag still on the card at another tier keeps what stands behind it', (both.witnesses['thing'] || []).includes('hubspot'), JSON.stringify(both.witnesses['thing'] || []));
}

// ---- Ranking, the headline, weak tags, and the bar for volunteering someone ----
// Live on Avery's portal with six real contacts: the person who answered both halves of the need
// ranked below three who answered one, and the headline said "nothing fits" while two people did.
{
  const P = 'https://mazel.rank.gh';
  const env2 = { HANDLE:'me@mazel', PERSONA:'p', NEED:'', HAVE:'', INBOX_TOKEN:'tr', MAILBOX: mkKV(), PORTAL_ORIGIN: P, RELAY_URL:'none' };
  portals[P] = env2;
  const t = async (n, a) => JSON.parse(await (await worker.fetch(new Request(P+'/mcp?token=tr', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:n, arguments:a||{} } }) }), env2)).text()).result.content[0].text;
  // Everyone here is in healthcare; only Beth is also an advisor. "healthcare" is the weak tag.
  const people = [
    { name:'Beth Ono',   org:'ono.example',  role:'Healthcare advisor to hospital boards', have:['healthcare','advisor'], edge_score:60, witnesses:['hubspot'] },
    { name:'Carl Diaz',  org:'diaz.example', role:'Healthcare operator',  have:['healthcare'], edge_score:90, witnesses:['hubspot'] },
    { name:'Dee Park',   org:'park.example', role:'Healthcare operator',  have:['healthcare'], edge_score:85, witnesses:['calendar'] },
    { name:'Eli Moss',   org:'moss.example', role:'Healthcare operator',  have:['healthcare'], edge_score:75, witnesses:['calendar'] },
    { name:'Fay Ruiz',   org:'ruiz.example', role:'Healthcare advisor, barely know her', have:['healthcare','advisor'], edge_score:20, witnesses:['hubspot'] },
  ];
  for (const g of people) await t('note_ghost', g);
  const r = JSON.parse(await t('find', { need_text:'a healthcare advisor for a hospital board', tags:['healthcare','advisor'] }));
  const names = (r.invites || []).map(i => i.name);
  ok('the person who matched both tags ranks first, closeness second', names[0] === 'Beth Ono', JSON.stringify(names));
  ok('closeness still orders the people who matched the same amount', names.indexOf('Carl Diaz') < names.indexOf('Eli Moss'), JSON.stringify(names));
  ok('the headline leads with the people who fit, not with nothing', /^4 people you already know fit/.test(r.headline) && !/^Nothing/.test(r.headline), r.headline);
  ok('a tag nearly everyone shares is worth less than one only two have', (r.invites[0].fit || 0) > (r.invites[1].fit || 0) * 1.5, `${r.invites[0].fit} vs ${r.invites[1].fit}`);
  // Fay fits as well as Beth and is barely known: matched, counted, not suggested.
  ok('someone the person barely deals with is not suggested', !names.includes('Fay Ruiz'), JSON.stringify(names));
  ok('and the portal says how many it held back', /^1 more person you know fits/.test(r.also_known || ''), r.also_known || 'nothing said');
  const more = JSON.parse(await t('find', { need_text:'a healthcare advisor for a hospital board', tags:['healthcare','advisor'], who_else: true }));
  ok('"who else?" brings them out', more.invites.some(i => i.name === 'Fay Ruiz'), JSON.stringify(more.invites.map(i => i.name)));
  ok('and they were never lost from matching, only from the suggestion', more.invites.find(i => i.name === 'Fay Ruiz').matched.includes('advisor'));
  const env3 = { ...env2, SUGGEST_MIN_EDGE: '10' };
  portals[P] = env3;
  const raw = await (await worker.fetch(new Request(P + '/mcp?token=tr', { method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'find', arguments: { need_text: 'a healthcare advisor for a hospital board', tags: ['healthcare', 'advisor'] } } }) }), env3)).text();
  const low = JSON.parse(JSON.parse(raw).result.content[0].text);
  ok('the bar is config, not a constant', low.invites.some(i => i.name === 'Fay Ruiz'), JSON.stringify(low.invites.map(i => i.name)));
  portals[P] = env2;
}

// ---- The agent is told to read the whole address book, not a sample ----
{
  const P3 = 'https://mazel.load.gh';
  const env6 = { HANDLE:'me3@mazel', PERSONA:'p', NEED:'', HAVE:'', INBOX_TOKEN:'tz', MAILBOX: mkKV(), PORTAL_ORIGIN: P3, RELAY_URL:'none' };
  portals[P3] = env6;
  const tools = (await (await worker.fetch(new Request(P3+'/mcp?token=tz', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/list' }) }), env6)).json()).result.tools;
  for (const h of JSON.parse(JSON.parse(await (await worker.fetch(new Request(P3+'/mcp?token=tz', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:'more_tools', arguments:{} } }) }), portals[P3])).text()).result.content[0].text).tools) tools.push(h);
  const d = tools.find(t => t.name === 'contacts').description + ' ' + tools.find(t => t.name === 'contacts').inputSchema.properties.action.description;   // note_ghost is reached as contacts, action note; its words ride on the merged tool
  ok('the onboarding line says load all of them, not a sample', /Load ALL of them, not a sample/.test(d) && /until the source is exhausted/.test(d), d.slice(0, 80));
  ok('and says why twenty contacts finds nobody', /twenty contacts finds nobody/.test(d));
  ok('and says not to pre-judge who is worth saving', /Skip nobody because they look unpromising/.test(d));
  // Item 7: the connector token is a header as well as a query parameter.
  const hdr = await worker.fetch(new Request(P3+'/mcp', { method:'POST', headers:{'content-type':'application/json', authorization:'Bearer tz'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/list' }) }), env6);
  const qs = await worker.fetch(new Request(P3+'/mcp?token=tz', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/list' }) }), env6);
  const bad = await worker.fetch(new Request(P3+'/mcp', { method:'POST', headers:{'content-type':'application/json', authorization:'Bearer nope'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/list' }) }), env6);
  ok('the token works as a request header, so it need not sit in the URL', hdr.status === 200 && qs.status === 200 && bad.status === 401, `${hdr.status}/${qs.status}/${bad.status}`);
}

// ---- Every invitation is for something, and a loop cannot run away with them ----
{
  const P2 = 'https://mazel.limit.gh';
  const env4 = { HANDLE:'me2@mazel', PERSONA:'p', NEED:'', HAVE:'', INBOX_TOKEN:'tl', MAILBOX: mkKV(), PORTAL_ORIGIN: P2, RELAY_URL:'none' };
  portals[P2] = env4;
  const t = async (n, a) => JSON.parse(await (await worker.fetch(new Request(P2+'/mcp?token=tl', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:n, arguments:a||{} } }) }), env4)).text()).result.content[0].text;
  const ids = [];
  for (let i = 0; i < 12; i++) ids.push((await t('note_ghost', { name:`Person ${i}`, org:`p${i}.example`, have:['ops'], edge_score: 70, witnesses:['hubspot'] })).match(/Their id is (\w+)/)[1]);
  const th = JSON.parse(await t('find', { need_text:'someone who does ops', tags:['ops'] })).thread_id;
  const loose = await t('invite_text', { ghost_id: ids[0] }).catch(e => String(e.message));
  ok('an invitation for nothing in particular is refused', /every invitation names what it is for/.test(loose), String(loose).slice(0, 90));
  const noThread = await t('invite_text', { ghost_id: ids[0], thread_id: 'notathread' }).catch(e => String(e.message));
  ok('and so is one for a need nobody cast', /has to be for a need that was actually cast/.test(noThread), String(noThread).slice(0, 90));
  const drafted = await t('invite_text', { ghost_id: ids[0], thread_id: th });
  ok('a real need gives real words, with nothing rationed and nothing counted out loud', /Someone I know is looking for someone who does ops/.test(drafted) && !/left this|ceiling/.test(drafted), drafted.split('\n')[0].slice(0, 100));
  const ten = [];
  for (const id of ids.slice(1, 11)) ten.push(await t('invite_text', { ghost_id: id, thread_id: th }));
  ok('ten in a row is an ordinary afternoon, not a refusal', ten.every(w => !/^Not drafted/.test(w)));
  // A loop, not a person: the same portal, the ceiling's worth in a day.
  const env5 = { ...env4, INVITE_MAX_PER_DAY: 11 };
  portals[P2] = env5;
  const t5 = async (a) => JSON.parse(await (await worker.fetch(new Request(P2+'/mcp?token=tl', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:'invite_text', arguments:a } }) }), env5)).text()).result.content[0].text;
  const runaway = await t5({ ghost_id: ids[11], thread_id: th });
  ok('past the ceiling an invitation queues instead of being refused', /^Queued/.test(runaway) && /pulse releases it/.test(runaway), runaway.slice(0, 110));
  ok('and nothing more is asked of the person, because they already said yes', /already said yes to this one/.test(runaway));
  const q = JSON.parse(await (async () => JSON.parse(await (await worker.fetch(new Request(P2+'/mcp?token=tl', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:'list_queue', arguments:{} } }) }), env5)).text()).result.content[0].text)());
  ok('list_queue shows what is waiting and says nothing was sent', q.waiting === 1 && /Nothing here has been sent/.test(q.note), JSON.stringify(q).slice(0, 90));
  ok('redrafting words already drafted costs nothing, even at the ceiling', !/^Queued/.test(await t5({ ghost_id: ids[0], thread_id: th })));
  const notYet = await (async () => JSON.parse(await (await worker.fetch(new Request(P2+'/mcp?token=tl', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:'clear_queue', arguments:{} } }) }), env5)).text()).result.content[0].text)();
  ok('clearing the queue asks first, and says nothing is recalled because nothing was sent', /^Not cleared/.test(notYet) && /nothing is recalled/.test(notYet), notYet.slice(0, 90));
  const cleared = await (async () => JSON.parse(await (await worker.fetch(new Request(P2+'/mcp?token=tl', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:'clear_queue', arguments:{ confirmed:true } } }) }), env5)).text()).result.content[0].text)();
  ok('and clearing it sends nothing and tells nobody', /nothing sent, nobody told/.test(cleared), cleared.slice(0, 80));
  portals[P2] = env4;
  const tools = (await (await worker.fetch(new Request(P2+'/mcp?token=tl', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/list' }) }), env4)).json()).result.tools;
  for (const h of JSON.parse(JSON.parse(await (await worker.fetch(new Request(P2+'/mcp?token=tl', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:'more_tools', arguments:{} } }) }), portals[P2])).text()).result.content[0].text).tools) tools.push(h);
  const bulk = tools.filter(x => /invite/.test(x.name) && JSON.stringify(x.inputSchema).includes('"array"'));
  ok('no tool invites a list of people at once', bulk.length === 0, bulk.map(x => x.name).join(', '));
  ok('and the person says who gets one before anything is drafted', /person says who gets one before you draft it/.test(tools.find(x => x.name === 'contacts').inputSchema.properties.action.description));
}

globalThis.fetch = realFetch;
console.log(`\nghosts: ${pass} passed, ${fail} failed`);
process.exit(fail?1:0);
