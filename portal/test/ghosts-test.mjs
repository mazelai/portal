// Ghosts: the people the person already knows, held on their own portal, so the first cast has
// somewhere to land. The whole suite is one long argument that a ghost's name never leaves.
import worker from '../src/index.js';
import relay from '../../relay/src/index.js';
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})), list_complete: true }) }; };
const R='https://relay.gh', A='https://mazel.a.gh', B='https://mazel.b.gh';
const renv = { RELAY: mkKV() };
const portals = {
  [A]: { HANDLE:'ariel@mazel', PERSONA:'Ariel runs Paragon.', NEED:'', HAVE:'managed-ai-delivery', INBOX_TOKEN:'ta', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: A, OWNER_EMAIL:'ariel@paragoncto.com' },
  [B]: { HANDLE:'lea@mazel', PERSONA:'Lea runs labs.', NEED:'', HAVE:'lab-ops', INBOX_TOKEN:'tb', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: B, OWNER_EMAIL:'lea@labs.example' },
};
const wireLog = [];
const realFetch = globalThis.fetch;
globalThis.fetch = async (u, i={}) => { const url=String(u instanceof Request?u.url:u); const o=new URL(url).origin; wireLog.push(url + ' ' + (i.body || ''));
  if (o===R) return relay.fetch(new Request(url,i), renv);
  if (portals[o]) return worker.fetch(new Request(url,i), portals[o]);
  return new Response('no', { status: 503 }); };
const call = async (o, name, args) => JSON.parse(await (await worker.fetch(new Request(o+'/mcp', { method:'POST', headers:{ 'content-type':'application/json', authorization:'Bearer '+portals[o].INBOX_TOKEN }, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name, arguments: args||{} } }) }), portals[o])).text()).result.content[0].text;
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
ok('strongest edge first', listed.ghosts[0].edge.score >= listed.ghosts[listed.ghosts.length-1].edge.score);
ok('each one carries the witnesses it was read from', listed.ghosts.every(g => g.witnesses.length > 0));
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

// ---- a gossip hop touches a ghost on the peer, and the peer's owner is asked first ----
await call(A, 'add_known_card', { url: B + '/card' });
await call(B, 'add_known_card', { url: A + '/card' });
await call(B, 'note_ghost', { name:'Dana Reyes', org:'dana.example', email:'dana@dana.example', have:['fractional-cfo'], role:'CFO she worked with', witnesses:['gmail','calendar'], edge_score: 88, edge_signals:'42 threads, replies within a day, three meetings this quarter' });
const mark = wireLog.length;
await call(A, 'find', { need_text:'a fractional cfo who has done a carve-out', tags:['fractional-cfo'] });
await call(A, 'pulse');
const boxB = await call(B, 'check_mailbox');
const askB = boxB.startsWith('{') ? (JSON.parse(boxB).messages || []).find(m => (m.action||{}).type === 'ghost.ask') : null;
ok("a stranger's ask reaches the ghosts on the portal it lands at", !!askB, boxB.slice(0, 80));
ok('the owner is shown who, the why, and how strong the edge is', !!askB && /Dana Reyes/.test(askB.text) && /Edge 88/.test(askB.text), askB && askB.text.slice(0, 90));
ok('and told plainly that nobody has been told anything', !!askB && /has no idea/.test(askB.text));
const backToA = wireLog.slice(mark).join('\n');
ok('the asker is told nothing at all until the owner answers', !/Dana Reyes/.test(backToA) && !/dana.example/.test(backToA));
ok('no is silent: declining sends nothing', /Not sent/.test(await call(B, 'route_ghost', { ask_id: askB.action.askId })) && !wireLog.slice(mark).join('\n').includes('Dana'));
const routed = await call(B, 'route_ghost', { ask_id: askB.action.askId, confirmed: true });
ok('yes gives the owner words to send, and makes them the router', /Your yes is recorded/.test(routed) && /router/.test(routed), routed.slice(0, 70));
ok('even then the portal sends nothing on their behalf', !wireLog.slice(mark).join('\n').includes('Dana Reyes'));

// ---- resolution through a bucket, local and opt-in ----
{
  await call(B, 'note_ghost', { name:'Ariel Jalali', org:'paragoncto.com', email:'ariel@paragoncto.com', have:['managed-ai-delivery'], role:'Runs Paragon', witnesses:['gmail'], edge_score: 70 });
  const beforeLink = JSON.parse(await call(B, 'list_ghosts', { q:'ariel' })).ghosts[0];
  ok('a ghost starts unresolved', !beforeLink.resolvedTo);
  await call(A, 'pulse');                                  // A publishes a record carrying its bucket
  await call(B, 'resolve_handle', { handle: 'ariel@mazel' });
  const after = JSON.parse(await call(B, 'list_ghosts', { q:'ariel' })).ghosts[0];
  ok('a ghost in the same bucket as a published record becomes an edge to that card', after.resolvedTo === 'ariel@mazel', String(after.resolvedTo));
  ok('what is published is a bucket, not an address', !/paragoncto\.com/.test(JSON.stringify([...renv.RELAY.m.values()])) , 'address found in the cache');
  const rec = JSON.parse([...renv.RELAY.m.entries()].find(([k]) => k.startsWith('dir:ariel'))[1]);
  ok('and the bucket is short enough to be shared', typeof rec.emailBucket === 'string' && rec.emailBucket.length === 3, String(rec.emailBucket));
  const noOptIn = { ...portals[B], OWNER_EMAIL: undefined };
  const card = JSON.parse(await (await worker.fetch(new Request(B + '/.well-known/mazel/lea.json'), noOptIn)).text());
  ok('no opt-in, no bucket', !card.emailBucket);
}

// ---- the thing this whole suite exists to check ----
ok('no ghost name appears anywhere on any wire, in the whole run',
   !wireLog.some(l => /CRM Person|Connection \d|Dana Reyes|Ariel Jalali/.test(l)),
   (wireLog.find(l => /Dana Reyes|CRM Person/.test(l)) || '').slice(0, 80));
ok('nor in the card, at any tier', !JSON.stringify(JSON.parse(await (await worker.fetch(new Request(A+'/card'), portals[A])).text())).match(/CRM Person|Connection \d/));
ok('nor in what the relay holds', !JSON.stringify([...renv.RELAY.m.values()]).match(/CRM Person|Connection \d|Dana Reyes/));

// ---- forgetting is always allowed ----
const one = JSON.parse(await call(A, 'list_ghosts')).ghosts[0];
ok('a ghost can be forgotten without ceremony', /Forgotten/.test(await call(A, 'forget_ghost', { ghost_id: one.id })));
ok('and is gone', JSON.parse(await call(A, 'list_ghosts')).count === 39);

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
}

globalThis.fetch = realFetch;
console.log(`\nghosts: ${pass} passed, ${fail} failed`);
process.exit(fail?1:0);
