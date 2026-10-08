// Tribes: a group that works before anyone installs. Membership is an edge, joining is an intro,
// and the tier two people end up at is computed from what they share rather than chosen.
import rawWorker from '../src/index.js';
import { legacy as legacyWorker } from './a2a-helpers.mjs';
const worker = legacyWorker(rawWorker);
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})), list_complete: true }) }; };
const ORG='https://mazel.org.tribe', M1='https://mazel.m1.tribe', M2='https://mazel.m2.tribe', M3='https://mazel.m3.tribe';
const portals = {
  [ORG]: { HANDLE:'organizer@mazel', PERSONA:'Runs the network.', NEED:'', HAVE:'', INBOX_TOKEN:'to', MAILBOX: mkKV(), RELAY_URL:'none', PORTAL_ORIGIN: ORG },
  [M1]:  { HANDLE:'lea@mazel', PERSONA:'Lea runs lab operations for hospital systems.', NEED:'', HAVE:'lab-ops', INBOX_TOKEN:'t1', MAILBOX: mkKV(), RELAY_URL:'none', PORTAL_ORIGIN: M1 },
  [M2]:  { HANDLE:'sam@mazel', PERSONA:'Sam is a fractional CFO for healthcare companies.', NEED:'', HAVE:'fractional-cfo', INBOX_TOKEN:'t2', MAILBOX: mkKV(), RELAY_URL:'none', PORTAL_ORIGIN: M2 },
  [M3]:  { HANDLE:'kim@mazel', PERSONA:'Kim does data migrations.', NEED:'', HAVE:'data-migrations', INBOX_TOKEN:'t3', MAILBOX: mkKV(), RELAY_URL:'none', PORTAL_ORIGIN: M3 },
};
const wire = [];
const realFetch = globalThis.fetch;
// A directory: name@mazel resolves at the relay, which serves the record each portal publishes.
// Since the inbound gate (2026-09-28) a roster's cards, like everything else, bind a handle to a
// key only through it.
const R = 'https://relay.tribe';
globalThis.fetch = async (u, i={}) => { const url=String(u instanceof Request?u.url:u); wire.push(url + ' ' + (i.body || '')); const o=new URL(url).origin;
  if (o === R) {
    const m = new URL(url).pathname.match(/^\/\.well-known\/mazel\/([a-z0-9._-]+)\.json$/);
    if (m) { const who = Object.entries(portals).find(([, p]) => (p.HANDLE || '').toLowerCase() === m[1] + '@mazel'); return who ? worker.fetch(new Request(who[0] + new URL(url).pathname), who[1]) : new Response('{"error":"no such handle"}', { status: 404 }); }
    return new Response(JSON.stringify({ ok: true, hits: [], results: [] }), { headers: { 'content-type': 'application/json' } });
  }
  if (portals[o]) return worker.fetch(new Request(url,i), portals[o]);
  return new Response('no', { status: 503 }); };
for (const p of Object.values(portals)) p.RELAY_URL = R;
const call = async (o, name, args) => JSON.parse(await (await worker.fetch(new Request(o+'/mcp', { method:'POST', headers:{ 'content-type':'application/json', authorization:'Bearer '+portals[o].INBOX_TOKEN }, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name, arguments: args||{} } }) }), portals[o])).text()).result.content[0].text;
const cards = async o => JSON.parse(await call(o, 'list_known_cards'));
let pass=0, fail=0; const ok=(l,c,x='')=>{ console.log((c?'PASS ':'FAIL ')+l+(x?'  -> '+String(x).replace(/\n/g,' ').slice(0,130):'')); c?pass++:fail++; };

// ---- forty members, read from their documents, nobody installed ----
const made = await call(ORG, 'tribe_create', { name:'Health Systems Circle', purpose:'operators who buy and build in hospital systems' });
const tribeId = made.match(/id is (\S+)/)[1];
ok('a tribe exists as an entity with an organizer', /Health Systems Circle/.test(made) && tribeId.startsWith('tribe:'), tribeId);

const docs = Array.from({length: 40}, (_, i) => ({
  name: `Member ${i}`, org: `member${i}.example`,
  have: i === 7 ? ['fractional-cfo'] : i % 3 === 0 ? ['lab-ops'] : i % 3 === 1 ? ['revenue-cycle'] : ['data-migrations'],
  role: `Operator at member${i}`, witnesses: ['one-pager'], edge_score: i === 7 ? 80 : 30 + (i % 40),
}));
let seededFromNote = 0;
for (const d of docs) {
  // Seeding is note_ghost then tribe_invite, straight through: the id comes back with the person,
  // because a listing in between waits on KV, once per member document.
  const noted = await call(ORG, 'note_ghost', d);
  const id = (noted.match(/Their id is (\w+)/) || [])[1];
  if (id) seededFromNote++;
  await call(ORG, 'tribe_invite', { tribe_id: tribeId, contact_id: id, confirmed: true });
}
ok('a saved contact comes back with an id, so seeding never waits on a listing', seededFromNote === 40, String(seededFromNote));
const status = JSON.parse(await call(ORG, 'tribe_status', { tribe_id: tribeId }));
ok('forty people are members before anyone has installed', status.members === 41 && status.withPortals === 1, JSON.stringify(status).slice(0,90));
ok('the organizer sees counts and nothing else', !JSON.stringify(status).includes('Member 7') && /Counts only/.test(status.note));

// ---- the organizer's own need matches across the members, and produces words to send ----
const found = JSON.parse(await call(ORG, 'find', { need_text:'a fractional cfo for a hospital system', tags:['fractional-cfo'] }));
ok('a need from the organizer lands on a member who has not installed', (found.invites || []).some(i => i.name === 'Member 7'), JSON.stringify((found.invites||[]).map(i=>i.name)));
const gid7 = found.invites.find(i => i.name === 'Member 7').ghost_id;
const draft = await call(ORG, 'invite_text', { ghost_id: gid7, thread_id: found.thread_id });
ok('and the answer is draft words the organizer sends by hand', /fractional cfo/i.test(draft) && /send it however they like/i.test(draft));
ok('the words say which group it came out of, not "someone I know"', /Someone in Health Systems Circle is looking for/.test(draft) && !/Someone I know/.test(draft), draft.split('\n').find(l => /looking for/.test(l)) || '');
ok('nothing was transmitted to anyone', !wire.some(w => /Member 7/.test(w)));

// ---- three install, and convert with their memberships intact ----
for (const [origin, name] of [[M1,'Member 0'], [M2,'Member 7'], [M3,'Member 2']]) {
  const gid = JSON.parse(await call(ORG, 'list_ghosts', { q: name })).people[0].id;
  await call(ORG, 'add_known_card', { url: origin + '/card' });
  await call(ORG, 'link_ghost', { ghost_id: gid, handle_or_url: origin + '/card' });
  await call(origin, 'add_known_card', { url: ORG + '/card' });   // the organizer's card comes with the invitation
}
const afterInstall = JSON.parse(await call(ORG, 'tribe_status', { tribe_id: tribeId }));
ok('installing converts a saved contact to a real card', afterInstall.withPortals === 4, JSON.stringify(afterInstall).slice(0,80));
ok('and the membership comes with them', afterInstall.members === 41, String(afterInstall.members));

// ---- the invitation is an intro, the yes is the join, and the tribe wires itself together ----
for (const o of [M1, M2, M3]) {
  const h = portals[o].HANDLE;
  await call(ORG, 'tribe_invite', { tribe_id: tribeId, handle: h, confirmed: true });
  const inv = JSON.parse(await call(o, 'check_mailbox')).messages.filter(m => (m.action||{}).type === 'intro.propose' && /Health Systems Circle/.test(m.action.tribeName)).pop();
  const said = await call(o, 'respond_intro', { intro_id: inv.action.introId, decision: 'accepted', confirmed: true });
  if (o === M1) ok('the yes reads as a join, not a meeting', /In Health Systems Circle/.test(said) && !/take it from here/.test(said), said.slice(0, 110));
}
ok('a yes to an invitation is the join', JSON.parse(await call(M1, 'tribe_status')).some(t => t.name === 'Health Systems Circle'));
const sam = (await cards(M1)).find(c => c.handle === 'sam@mazel');
ok('and members hold each other without adding anyone by hand', !!sam, JSON.stringify((await cards(M1)).map(c=>c.handle)));
ok('a card that arrived that way sits at tribe tier and was never chosen by hand', sam && sam.tier === 'tribe' && !sam.tierByHand, JSON.stringify(sam||{}).slice(0,90));

// ---- a member's need reaches another member in one pulse ----
const hit = JSON.parse(await call(M1, 'find', { need_text:'a fractional cfo who knows healthcare', tags:['fractional-cfo'] }));
ok("one member's need finds another member", (hit.candidates || []).some(c => c.handle === 'sam@mazel'), JSON.stringify((hit.candidates||[]).map(c=>c.handle)));
ok('and the why says they are in the same group', /in Health Systems Circle/.test(hit.candidates[0].why), hit.candidates[0].why);

// ---- sharing two tribes beats sharing one ----
{
  const second = await call(M1, 'tribe_create', { name:'Boston Operators', purpose:'people who build here' });
  const secondId = second.match(/id is (\S+)/)[1];
  await call(M1, 'add_known_card', { url: M3 + '/card' });
  await call(M1, 'note_ghost', { name:'x', have:['fractional-cfo'] });   // noise
  const before = JSON.parse(await call(M1, 'find', { need_text:'someone who does operations work', tags:['lab-ops','fractional-cfo','data-migrations'] }));
  const rankOf = (r, h) => (r.candidates || []).findIndex(c => c.handle === h);
  await call(M1, 'tribe_invite', { tribe_id: secondId, handle:'kim@mazel', confirmed: true });
  const invite = JSON.parse(await call(M3, 'check_mailbox')).messages.filter(m => (m.action||{}).type === 'intro.propose' && /Boston Operators/.test(m.action.tribeName)).pop();
  ok('an invitation to a tribe arrives as an intro with origin tribe', !!invite && invite.action.origin === 'tribe', JSON.stringify((invite||{}).action || {}).slice(0,80));
  await call(M3, 'respond_intro', { intro_id: invite.action.introId, decision:'accepted', confirmed: true });
  const after = JSON.parse(await call(M1, 'find', { need_text:'someone who does operations work', tags:['lab-ops','fractional-cfo','data-migrations'] }));
  ok('sharing two tribes ranks above sharing one', rankOf(after, 'kim@mazel') <= rankOf(before, 'kim@mazel'), `${rankOf(before,'kim@mazel')} -> ${rankOf(after,'kim@mazel')}`);
  const st = await call(M3, 'tribe_status');
  const edges3 = [...portals[M3].MAILBOX.m.entries()].filter(([k]) => k.startsWith('edge:')).map(([,v]) => JSON.parse(v));
  const ents3 = [...portals[M3].MAILBOX.m.keys()].filter(k => k.startsWith('entity:'));
  if (!/Boston Operators/.test(st)) console.log('DEBUG-M3', JSON.stringify(edges3), ents3, 'secondId=', secondId, 'resp=', await call(M3, 'list_intros'));
  ok('and the yes to a tribe invitation is what made the membership', /Boston Operators/.test(st));
}

// ---- leaving is silent, and nothing that happened is undone ----
{
  const beforeTier = JSON.parse(await call(M1, 'list_known_cards')).find(c => c.handle === 'kim@mazel').tier;
  const mark = wire.length;
  const left = await call(M3, 'tribe_leave', { tribe_id: tribeId });
  ok("leaving needs nobody's permission", /Out of Health Systems Circle/.test(left), left.slice(0, 60));
  ok('and says nothing to anyone', !wire.slice(mark).some(w => /Health Systems/.test(w)));
  const edges = [...portals[M3].MAILBOX.m.entries()].filter(([k]) => k.startsWith('edge:')).map(([, v]) => JSON.parse(v));
  const ended = edges.find(e => e.to === tribeId && e.from === 'kim@mazel');
  ok('the membership keeps its row with an end date', !!ended && !!ended.until, JSON.stringify(ended || {}).slice(0, 80));
  ok('past tiers fall back to what the person chose', ['tribe','world','inner'].includes(beforeTier));
}

// ---- removal is the same, and the person is told once ----
{
  const mark = wire.length;
  const removed = await call(ORG, 'tribe_remove', { tribe_id: tribeId, handle:'lea@mazel', confirmed: true });
  ok('an organizer can remove someone', /out of Health Systems Circle/.test(removed), removed.slice(0, 70));
  ok('and their agent is told once', /told, once/.test(removed) && wire.slice(mark).some(w => /no longer a member/.test(w)));
  const notMine = await call(M1, 'tribe_remove', { tribe_id: tribeId, handle:'sam@mazel', confirmed: true }).catch(e => String(e.message));
  ok('but nobody else can', /only .* organizer/.test(notMine), String(notMine).slice(0, 70));
}

// ---- an unlisted tribe blocks a public membership ----
{
  const quiet = await call(ORG, 'tribe_create', { name:'Quiet Room', purpose:'no names', unlisted: true });
  const quietId = quiet.match(/id is (\S+)/)[1];
  const listing = JSON.parse(await call(ORG, 'tribe_status'));
  ok('an unlisted tribe is marked unlisted', listing.find(t => t.tribe_id === quietId).unlisted === true);
  const blocked = await call(ORG, 'tribe_join', { tribe_id: quietId, visibility: 'public', confirmed: true }).catch(e => String(e.message));
  ok('a membership in an unlisted tribe cannot be made public', /unlisted/.test(blocked) && /cannot be public/.test(blocked), String(blocked).slice(0, 90));
  const shown = await call(M2, 'tribe_join', { tribe_id: tribeId, visibility: 'public', confirmed: true });
  ok('but in a listed one the member decides who sees it', /visible to anyone/.test(shown) && /No card carries memberships yet/.test(shown), shown.slice(0, 120));
  const card = await (await worker.fetch(new Request(ORG + '/card'), portals[ORG])).json();
  ok('and no tribe of any kind appears on a card', !JSON.stringify(card).includes('Quiet Room') && !JSON.stringify(card).includes('Health Systems'));
  const f = JSON.parse(await call(ORG, 'find', { need_text:'a fractional cfo for a hospital system', tags:['fractional-cfo'] }));
  ok('an unlisted tribe still routes, without being named', !JSON.stringify(f).includes('Quiet Room'));
}

// ---- a 300-member tribe, one approval, finishing across two days ----
// The ceiling is a speed limit for a runaway loop, never a wall for a real seed: the organizer
// says yes once, works through every document in one sitting, and the rest arrives tomorrow.
{
  const BIG = 'https://mazel.big.tribe';
  const env7 = { HANDLE:'chair@mazel', PERSONA:'Runs a big room.', NEED:'', HAVE:'', INBOX_TOKEN:'tbig', MAILBOX: mkKV(), RELAY_URL:'none', PORTAL_ORIGIN: BIG, TRIBE_INVITE_MAX_PER_DAY: 200 };
  portals[BIG] = env7;
  const c = async (n, a) => JSON.parse(await (await worker.fetch(new Request(BIG+'/mcp', { method:'POST', headers:{'content-type':'application/json', authorization:'Bearer tbig'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:n, arguments:a||{} } }) }), env7)).text()).result.content[0].text;
  const big = (await c('tribe_create', { name:'The Big Room', purpose:'three hundred operators' })).match(/id is (\S+)/)[1];
  let recorded = 0, queued = 0;
  for (let i = 0; i < 300; i++) {
    const gid = (await c('note_ghost', { name:`Seat ${i}`, org:`seat${i}.example`, have:['ops'], edge_score: 55, witnesses:['one-pager'] })).match(/Their id is (\w+)/)[1];
    const r = await c('tribe_invite', { tribe_id: big, contact_id: gid, confirmed: true });
    if (/^Queued/.test(r)) queued++; else recorded++;
  }
  ok('the day takes its ceiling and the rest queues, on the one yes already given', recorded === 200 && queued === 100, `${recorded} recorded, ${queued} queued`);
  ok('the organizer was never asked again', queued > 0 && /on the yes you already gave/.test(await c('tribe_invite', { tribe_id: big, contact_id: (await c('note_ghost', { name:'Seat 300', have:['ops'], edge_score:55 })).match(/Their id is (\w+)/)[1], confirmed: true })));
  ok('day one has 201 members, the organizer included', JSON.parse(await c('tribe_status', { tribe_id: big })).members === 201, String(JSON.parse(await c('tribe_status', { tribe_id: big })).members));
  const q1 = JSON.parse(await c('list_queue'));
  ok('and the rest are visibly waiting, not lost', q1.waiting === 101 && q1.tribe_memberships === 101, JSON.stringify({ w: q1.waiting, t: q1.tribe_memberships }));
  // Tomorrow: the day's counter is what turns over, so age it by a day and pulse.
  const aged = JSON.parse(await env7.MAILBOX.get('config:tribeinvites')).map(u => ({ ...u, at: new Date(Date.parse(u.at) - 25 * 60 * 60 * 1000).toISOString() }));
  await env7.MAILBOX.put('config:tribeinvites', JSON.stringify(aged));
  let pulse = null;
  await worker.scheduled({}, env7, { waitUntil: (p) => { pulse = p; } });
  await pulse;
  const q2 = await c('list_queue');
  ok("the next day's pulse releases the rest, oldest first", /^Nothing waiting/.test(q2), q2.slice(0, 80));
  ok('and the tribe is whole: 301 members without a second approval', JSON.parse(await c('tribe_status', { tribe_id: big })).members === 302, String(JSON.parse(await c('tribe_status', { tribe_id: big })).members));
  const box = JSON.parse(await c('check_mailbox'));
  const drafts = box.messages.filter(m => /already said yes to/.test(m.text || ''));
  ok('each released invitation arrives as words the person sends, never as something sent', drafts.length > 0 && drafts.every(m => /Nothing has been sent/.test(m.text) && m.draft), String(drafts.length));
  ok('nothing about any of them crossed a wire', !wire.some(w => /Seat 2\d\d/.test(w)));
}

// ---- a runaway loop is still caught, and says so ----
{
  const low = { ...portals[ORG], TRIBE_INVITE_MAX_PER_DAY: 40 };   // 40 recorded above, by the seed
  const one = async (env, args) => JSON.parse(await (await worker.fetch(new Request(ORG+'/mcp', { method:'POST', headers:{'content-type':'application/json', authorization:'Bearer to'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:'tribe_invite', arguments: args } }) }), env)).text()).result.content[0].text;
  const gid = (await call(ORG, 'note_ghost', { name:'One More', have:['ops'], edge_score: 70 })).match(/Their id is (\w+)/)[1];
  const stopped = await one(low, { tribe_id: tribeId, contact_id: gid, confirmed: true });
  ok('past the ceiling it queues rather than refusing', /^Queued/.test(stopped) && /released oldest first/.test(stopped), stopped.slice(0, 90));
  const fine = await one(portals[ORG], { tribe_id: tribeId, contact_id: gid, confirmed: true });
  ok('and a real seed never comes near the ceiling', /is a member of Health Systems Circle/.test(fine), fine.slice(0, 70));
}

// ---- entities stay here ----
// The one tribe thing that crosses a wire is the roster the organizer sends its own members, and
// it carries handles and addresses only: what anyone needs or has is still theirs to say.
const rosters = wire.filter(w => /"tribe.roster"/.test(w));
ok('the only tribe traffic is the roster, from the organizer', rosters.length > 0 && !wire.some(w => /member_of|"entity"|edge:/.test(w)), (wire.find(w => /member_of|edge:/.test(w)) || '').slice(0, 80));
ok('and a roster carries handles and addresses, nothing about anyone', !rosters.some(w => /"have"|"need"|"purpose"|Member 7|one-pager/.test(w)));
ok('nor on a card', !JSON.stringify(await (await worker.fetch(new Request(ORG + '/card'), portals[ORG])).json()).match(/member_of|entity|tribe:/));

// ---- and the agent is told to write it down ----
{
  const tools = (await (await worker.fetch(new Request(M1 + '/mcp?token=t1', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/list' }) }), portals[M1])).json()).result.tools;
  const byName = Object.fromEntries(tools.map(t => [t.name, t.description]));
  ok('the tools that produce a yes carry the write-back instruction', ['respond_intro','propose_intro','check_mailbox'].every(n => /WRITE IT DOWN/.test(byName[n] || '')));
  ok('and every user-facing string says someone you already know, never ghost', !Object.values(byName).some(d => /\bghosts?\b/i.test(d.replace(/ghost\.ask|note_ghost|list_ghosts|forget_ghost|link_ghost|route_ghost/g, ''))), Object.entries(byName).find(([, d]) => /\bghosts?\b/i.test(d.replace(/ghost\.ask|note_ghost|list_ghosts|forget_ghost|link_ghost|route_ghost/g, '')))?.[0] || '');
}

globalThis.fetch = realFetch;
console.log(`\ntribe: ${pass} passed, ${fail} failed`);
process.exit(fail?1:0);
