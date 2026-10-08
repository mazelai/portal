// Seeding, part one: the card writes itself from what the agent already knows.
// The memory file is the source and the card is a projection of it; a have carries its witnesses;
// a need the person holds back travels as buckets and never as words.
import rawWorker from '../src/index.js';
import { legacy as legacyWorker } from './a2a-helpers.mjs';
const worker = legacyWorker(rawWorker);
import relay from '../../relay/src/index.js';
import { memoryDoc } from './a2a-helpers.mjs';
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix, cursor}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})), list_complete: true }) }; };
const R='https://relay.seed', A='https://mazel.a.seed', B='https://mazel.b.seed';
const renv = { RELAY: mkKV() };
const portals = {
  [A]: { HANDLE:'avery@mazel', PERSONA:'Avery runs Halcyon.', NEED:'', HAVE:'managed-ai-delivery', INBOX_TOKEN:'ta', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: A },
  [B]: { HANDLE:'lea@mazel', PERSONA:'Lea runs labs.', NEED:'', HAVE:'lab-ops', INBOX_TOKEN:'tb', MAILBOX: mkKV(), RELAY_URL: R, PORTAL_ORIGIN: B },
};
const seen = [];
const realFetch = globalThis.fetch;
globalThis.fetch = async (u, i={}) => { const url=String(u instanceof Request?u.url:u); const o=new URL(url).origin; seen.push({ url, body: i.body || '' });
  if (o===R) return relay.fetch(new Request(url,i), renv);
  if (portals[o]) return worker.fetch(new Request(url,i), portals[o]);
  return new Response('no', { status: 503 }); };
const call = async (o, name, args) => JSON.parse(await (await worker.fetch(new Request(o+'/mcp', { method:'POST', headers:{ 'content-type':'application/json', authorization:'Bearer '+portals[o].INBOX_TOKEN }, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name, arguments: args||{} } }) }), portals[o])).text()).result.content[0].text;
let pass=0, fail=0; const ok=(l,c,x='')=>{ console.log((c?'PASS ':'FAIL ')+l+(x?'  -> '+String(x).replace(/\n/g,' ').slice(0,120):'')); c?pass++:fail++; };

// ---- the file is the source ----
await call(A, 'my_card');
let md = await call(A, 'my_memory');
ok('the portal keeps a memory file, in markdown, with tiers and witnesses', /## Persona/.test(md) && /## Have/.test(md) && /\[public\] managed-ai-delivery \(witnesses: owner\)/.test(md), md.split('\n').find(l=>/managed-ai/.test(l)));
ok('it says what it is for, in the file itself', /projection of it/.test(md) && /never as text/.test(md));

// A person edits the file by hand; the card follows.
portals[A].MAILBOX.m.set('memory:card.md', memoryDoc({ handle:'avery@mazel', persona:'Avery runs Halcyon, an AI delivery shop.',
  have: [{ tag:'managed-ai-delivery', gloss:'builds AI inside your own cloud', witnesses:['hubspot','gmail'] }, { tag:'board-prep', tier:'inner', witnesses:['drive'] }],
  need: [{ tag:'tech-advisor-partners' }, { tag:'quiet-cofounder-search', tier:'matched-only' }] }));
const card = JSON.parse(await (await worker.fetch(new Request(A+'/card'), portals[A])).text());
const haah = card.capabilities.extensions.find(e=>/haah/.test(e.uri)).params;
ok('the open card is the public projection of the file', haah.have.includes('managed-ai-delivery') && haah.need.includes('tech-advisor-partners'), JSON.stringify(haah.have));
ok('an inner have is not on the open card', !haah.have.includes('board-prep'));
ok('a matched-only need is not on the open card', !haah.need.includes('quiet-cofounder-search'));
ok('a hand edit survives the round trip', /builds AI inside your own cloud/.test(await call(A, 'my_memory')));
ok('the gloss the person wrote is what strangers read', haah.glosses['managed-ai-delivery'] === 'builds AI inside your own cloud');

// ---- a peer pulls the tier it was put at, by signing for it ----
await call(A, 'add_known_card', { url: B + '/card' });
await call(B, 'add_known_card', { url: A + '/card' });
const idB = JSON.parse(await call(B, 'my_identity'));
const b64u = (b) => btoa(String.fromCharCode(...new Uint8Array(b))).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
const canon = (o) => JSON.stringify(Object.keys(o).sort().reduce((a,k)=>(o[k]===undefined?a:(a[k]=o[k],a)),{}));
const signAs = async (env, payload) => {
  const jwk = JSON.parse(env.MAILBOX.m.get('config:signing')).priv;
  const key = await crypto.subtle.importKey('jwk', jwk, { name:'Ed25519' }, false, ['sign']);
  return b64u(await crypto.subtle.sign({ name:'Ed25519' }, key, new TextEncoder().encode(canon(payload))));
};
const pull = async (as, env, tamper) => {
  const e = Date.now() + 60000;
  const sig = await signAs(env, { as, e, at: A });
  const u = `${A}/card?as=${encodeURIComponent(as)}&e=${e}&sig=${encodeURIComponent(tamper ? (sig[0] === 'x' ? 'y' : 'x') + sig.slice(1) : sig)}`;
  return worker.fetch(new Request(u), portals[A]);
};
const asTribe = await pull('lea@mazel', portals[B]);
const tribeParams = (await asTribe.json()).capabilities.extensions.find(e=>/haah/.test(e.uri)).params;
ok('a peer that signs gets the projection for the tier it was put at', tribeParams.have.includes('managed-ai-delivery'), JSON.stringify(tribeParams.have));
ok('and still not what is held above that tier', !tribeParams.have.includes('board-prep'), JSON.stringify(tribeParams.have));
ok('a matched-only need is invisible at every tier', !JSON.stringify(tribeParams).includes('quiet-cofounder-search'));
ok('a bad signature gets nothing', (await pull('lea@mazel', portals[B], true)).status === 403);
ok('a stranger cannot ask for a tier at all', (await pull('nobody@mazel', portals[B])).status === 403);

// ---- a held-back need travels as buckets, never as words ----
// B is someone who would fit, without either side ever saying so.
portals[B].MAILBOX.m.set('memory:card.md', memoryDoc({ handle:'lea@mazel', persona:'Lea runs a quiet cofounder search practice.',
  have: [{ tag:'cofounder-search', gloss:'finds cofounders quietly', witnesses:['hubspot'] }] }));
const before = seen.length;
await call(B, 'pulse');                                   // B subscribes with its have fingerprint
await call(A, 'find', { need_text:'a quiet cofounder search', tags:['quiet-cofounder-search'] });
await call(A, 'pulse');                                   // A casts it blind
const wire = seen.slice(before).map(x => x.url + ' ' + x.body).join('\n');
const fromA = seen.slice(before).filter(x => /avery@mazel/.test(x.body || '')).map(x => x.url + ' ' + x.body).join('\n');
ok('the words of a held-back need appear on no wire this portal wrote', !/quiet cofounder search/i.test(fromA), (fromA.match(/.{0,50}cofounder.{0,30}/) || [''])[0]);
ok('nor does its tag', !/quiet-cofounder-search/.test(wire), (wire.match(/.{0,40}quiet.{0,40}/) || [''])[0]);
// By default a held-back need does not reach the public cache at all: an unkeyed fingerprint lets
// whoever holds it confirm a guess, so it goes only to cards the person already holds.
ok('a held-back need does not reach the public cache by default', [...renv.RELAY.m.keys()].filter(k => k.startsWith('cast:blind:')).length === 0);
{
  const loud = { ...portals[A], BLIND_TO_RELAY: '1' };
  await worker.fetch(new Request(A+'/mcp?token=ta', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:'pulse', arguments:{} } }) }), loud);
  const casts = [...renv.RELAY.m.entries()].filter(([k]) => k.startsWith('cast:blind:'));
  ok('the relay never holds buckets, flag or no flag', casts.length === 0, String(casts.length));
}

// The circle that already knows them can answer, without the cache ever being involved.
{
  const box = await call(A, 'check_mailbox');
  ok('a card the person holds can answer a held-back need directly', box.startsWith('{') && JSON.parse(box).messages.some(m => (m.action || {}).type === 'find.hit' && m.action.blind), box.slice(0, 70));
}

// ---- and the person decides whether any words are ever said ----
const boxRaw = await call(A, 'check_mailbox');
const box = boxRaw.startsWith('{') ? JSON.parse(boxRaw) : { messages: [] };
const ask = (box.messages || []).find(m => (m.action || {}).type === 'blind.ask');
ok('a portal that lines up becomes one private question, not a candidate', !!ask, JSON.stringify((box.messages||[]).map(m => (m.action||{}).type)));
if (ask) {
  const thread = JSON.parse(await call(A, 'list_threads'))[0];
  ok('and nothing was added to the thread', (thread.candidates || []).length === 0);
  ok('reveal refuses without an explicit yes', /Not sent/.test(await call(A, 'reveal_need', { blind_id: ask.action.blindId })));
  const said = await call(A, 'reveal_need', { blind_id: ask.action.blindId, confirmed: true });
  ok('on yes, the words travel to that portal and nowhere else', /^Sent to/.test(said), said.slice(0, 70));
  const boxB = JSON.parse(await call(B, 'check_mailbox'));
  ok('and that portal is the first to hear them', JSON.stringify(boxB).includes('quiet cofounder search'));
}

// ---- the cap is on what goes public, not on what the file knows ----
{
  const many = { HANDLE:'many@mazel', PERSONA:'p', NEED:'', HAVE:'h1,h2,h3,h4,h5,h6', INBOX_TOKEN:'tm', MAILBOX: mkKV(), PORTAL_ORIGIN: A, RELAY_URL:'none' };
  const t = async (n, a) => JSON.parse(await (await worker.fetch(new Request(A+'/mcp?token=tm', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:n, arguments:a||{} } }) }), many)).text()).result.content[0].text;
  await t('my_card');
  const refused = await t('update_card', { add_have:'h7', confirmed:true });
  ok('a seventh public have is refused, and the message says what to do instead', /already shows 6 public haves/.test(refused) && /have_visibility/.test(refused), refused.slice(0, 90));
  const parked = await t('update_card', { add_have:'data-and-crm-migrations', have_visibility:'tribe', witnesses:'hubspot', confirmed:true });
  ok('the same have goes in at tribe, past the public cap', /Written/.test(parked), parked.split('\n')[0].slice(0, 90));
  const c = JSON.parse(await t('my_card'));
  ok('and it is held, not published', c.heldHaves.some(h => h.tag === 'data-and-crm-migrations' && h.tier === 'tribe') && !c.have.includes('data-and-crm-migrations'), JSON.stringify(c.heldHaves));
  ok('an owner-only have says so, and names the parameter that fixes it', /witness: owner only/.test(await t('update_card', { add_have:'h8', have_visibility:'inner', confirmed:true })));
}

// ---- the flag that raises the bar in v0.6.0-trust ----
{
  const strict = { ...portals[A], RELAY_REQUIRES_WITNESS: '1' };
  portals[A].MAILBOX.m.set('memory:card.md', memoryDoc({ handle:'avery@mazel', persona:'p',
    have: [{ tag:'owner-only-thing', witnesses:['owner'] }, { tag:'seen-elsewhere', witnesses:['hubspot'] }] }));
  const at = seen.length;
  await worker.fetch(new Request(A+'/mcp?token=ta', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:'pulse', arguments:{} } }) }), strict);
  const cast = seen.slice(at).map(x => x.body).join('\n');
  ok('with RELAY_REQUIRES_WITNESS on, an owner-only have is kept back from the cache', !/owner-only-thing/.test(cast) && /seen-elsewhere/.test(cast));
  const openCard = JSON.parse(await (await worker.fetch(new Request(A+'/card'), strict)).text());
  ok('but it stays on the card for people who already hold it', JSON.stringify(openCard).includes('owner-only-thing'));
}

globalThis.fetch = realFetch;

// ---- A blind cast lives a day; a need that is still open gets it sent again ----
{
  const BP = 'https://mazel.blind.seed';
  const OTHER = 'https://mazel.blindpeer.seed';
  const kv = mkKV();
  const benv = { HANDLE:'quiet@mazel', PERSONA:'Holds things back.', NEED:'', HAVE:'', INBOX_TOKEN:'tq', MAILBOX: kv, RELAY_URL:'none', PORTAL_ORIGIN: BP };
  const penv = { HANDLE:'peer@mazel', PERSONA:'Does audits.', NEED:'', HAVE:'audit-prep', INBOX_TOKEN:'tp', MAILBOX: mkKV(), RELAY_URL:'none', PORTAL_ORIGIN: OTHER };
  const casts = [];
  const prev = globalThis.fetch;
  globalThis.fetch = async (u, i = {}) => {
    const url = String(u instanceof Request ? u.url : u);
    if (/"find.blind"/.test(String(i.body || ''))) casts.push(Date.now());
    if (url.startsWith(OTHER)) return worker.fetch(new Request(url, i), penv);
    if (url.startsWith(BP)) return worker.fetch(new Request(url, i), benv);
    return new Response('no', { status: 503 });
  };
  const c = async (n, a) => JSON.parse(await (await worker.fetch(new Request(BP+'/mcp?token=tq', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:n, arguments:a||{} } }) }), benv)).text()).result.content[0].text;
  await c('add_known_card', { url: OTHER + '/card', tier: 'tribe' });
  await worker.fetch(new Request(OTHER+'/mcp?token=tp', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:'add_known_card', arguments:{ url: BP + '/card' } } }) }), penv);   // the peer holds the caster: a blind cast is taken at its door, not acked and dropped (29f H2)
  await c('update_card', { add_need:'quiet-thing', need_visibility:'matched-only' });
  await c('find', { need_text:'a quiet thing nobody should read', tags:['quiet-thing'] });
  await c('pulse');                       // the first pulse is what puts it out
  const first = casts.length;
  ok('the pulse casts a held-back need as buckets', first > 0, String(first));
  await c('pulse');
  ok('and does not repeat it every half hour for nothing', casts.length === first, `${first} -> ${casts.length}`);
  // Age it past the day it lives anywhere, with the need still open.
  const tid = [...kv.m.keys()].find(k => k.startsWith('thread:'));
  const th = JSON.parse(kv.m.get(tid));
  th.blindCastAt = new Date(Date.now() - 25 * 60 * 60 * 1000).toISOString();
  kv.m.set(tid, JSON.stringify(th));
  const said = await c('pulse');
  ok('but one that has expired while its need is still open goes out again', casts.length > first, `${first} -> ${casts.length}`);
  ok('and the person is told, in the words of what it is', /only lives a day/.test(said), said.split('\n').find(l => /buckets/.test(l)) || said.slice(0, 90));
  ok('the fresh cast is stamped, so the next pulse leaves it alone', Date.parse(JSON.parse(kv.m.get(tid)).blindCastAt) > Date.now() - 60000);
  const after = casts.length;
  await c('pulse');
  ok('which it does', casts.length === after, `${after} -> ${casts.length}`);
  // A need that is closed stops travelling.
  const th2 = JSON.parse(kv.m.get(tid));
  th2.blindCastAt = new Date(Date.now() - 25 * 60 * 60 * 1000).toISOString();
  th2.status = 'closed';
  kv.m.set(tid, JSON.stringify(th2));
  const n2 = casts.length;
  await c('pulse');
  ok('a closed need is not re-cast at all', casts.length === n2, `${n2} -> ${casts.length}`);
  globalThis.fetch = prev;
}

console.log(`\nseed: ${pass} passed, ${fail} failed`);
process.exit(fail?1:0);
