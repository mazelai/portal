// Relay: directory (publish/resolve/rotation chain), casts, search, subscribe -> signed find.hit delivery,
// signature-only auth, public-only. Portals are real in-process Workers; the relay is the built Worker.
import worker from '../src/index.js';
import relay from '../../relay/src/index.js';
import { readFileSync } from 'node:fs';
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})) }) }; };
const R='https://relay.test', A='https://mazel.a.workers.dev', B='https://mazel.b.workers.dev';
const renv = { RELAY: mkKV() };
const portals = {
  [A]: { HANDLE:'ariel@mazel', PERSONA:'Ariel runs Paragon.', NEED:'', HAVE:'managed-ai-delivery', INBOX_TOKEN:'ta', MAILBOX: mkKV(), RELAY_URL: R },
  [B]: { HANDLE:'sailor@mazel', PERSONA:'Sails.', NEED:'', HAVE:'sailing-atlantic', INBOX_TOKEN:'tb', MAILBOX: mkKV(), RELAY_URL: R },
};
const seenAtB = [];
const realFetch = globalThis.fetch;
globalThis.fetch = async (u, i={}) => { const url=String(u instanceof Request?u.url:u); const o=new URL(url).origin; if (o===R) return relay.fetch(new Request(url,i), renv); if (portals[o]) { if (o===B && new URL(url).pathname==='/a2a') seenAtB.push(JSON.parse(i.body)); return worker.fetch(new Request(url,i), portals[o]); } return realFetch(u,i); };
const call = async (o, name, args) => JSON.parse(await (await worker.fetch(new Request(o+'/mcp', { method:'POST', headers:{ 'content-type':'application/json', authorization:'Bearer '+portals[o].INBOX_TOKEN }, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name, arguments: args||{} } }) }), portals[o])).text()).result.content[0].text;
const rpost = async (path, body) => { const r = await fetch(R+path, { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify(body) }); return { status: r.status, body: await r.json() }; };
let pass=0, fail=0; const ok=(l,c,x='')=>{ console.log((c?'PASS ':'FAIL ')+l+(x?'  -> '+String(x).replace(/\n/g,' ').slice(0,120):'')); c?pass++:fail++; };

// lifted blocks are identical to the portal's
const P = readFileSync(new URL('../src/index.js', import.meta.url), 'utf8'), RL = readFileSync(new URL('../../relay/src/index.js', import.meta.url), 'utf8');
const lift = (s,b,e) => s.slice(s.indexOf(b), s.indexOf('\n', s.indexOf(e, s.indexOf(b)))+1);
ok('relay carries the portal matcher verbatim', lift(P,'// ---- SHARED MATCH BEGIN','// ---- SHARED MATCH END')===lift(RL,'// ---- SHARED MATCH BEGIN','// ---- SHARED MATCH END'));
ok('relay carries the portal signing helpers verbatim', lift(P,'// ---- SHARED SIGNING BEGIN','// ---- SHARED SIGNING END')===lift(RL,'// ---- SHARED SIGNING BEGIN','// ---- SHARED SIGNING END'));

// directory
const recA = await (await fetch(A+'/.well-known/mazel/ariel.json')).json();
let r = await rpost('/publish', recA); ok('publish a signed handle record', r.status===200 && r.body.ok, JSON.stringify(r.body));
const got = await (await fetch(R+'/.well-known/mazel/ariel.json')).json(); ok('directory serves the record under name.json with the handle exactly as signed', got.handle==='ariel@mazel' && got.publicKey===recA.publicKey);
{ const back = await (await fetch(R+'/.well-known/mazel/ariel.json')).json();
  ok('served record is the signed one verbatim (handle untouched)', JSON.stringify(back)===JSON.stringify(recA), JSON.stringify(back).slice(0,100));
  const res = await call(A, 'resolve_handle', { handle: 'ariel@mazel' });
  ok('a portal resolves name@mazel through the relay and the signature verifies', !/^Error/.test(res) && /cardUrl|card/.test(res), res.slice(0,100)); }
r = await rpost('/publish', { ...recA, cardUrl: 'https://evil/card' }); ok('tampered record refused', r.status===400 && /not signed/.test(r.body.error), r.body.error);
const recB = await (await fetch(B+'/.well-known/mazel/sailor.json')).json();
r = await rpost('/publish', { ...recB, handle: 'ariel@mazel' }); ok('another key cannot take a held name (no chain)', r.status===400 && /another key/.test(r.body.error) || /not signed/.test(r.body.error), r.body.error);
await call(A,'rotate_key',{confirmed:true});
const got2 = await (await fetch(R+'/.well-known/mazel/ariel.json')).json(); ok('rotation accepted through the chain: directory now holds the new key', got2.publicKey!==recA.publicKey && got2.rotations.length===1);
r = await rpost('/publish', recA); ok('old record is refused after rotation (older + wrong key)', r.status===400, r.body.error);

// casts: signed, public only
const idB = JSON.parse(await call(B,'my_identity'));
r = await rpost('/cast', { kind:'card', handle:'sailor@mazel', have:['sailing-atlantic'], glosses:{'sailing-atlantic':'has sailed across the Atlantic twice'}, cardUrl:B+'/card', rpc:B+'/a2a', publicKey: idB.publicKey });
ok('unsigned cast refused', r.status===400 && /sig/.test(r.body.error), r.body.error);
// have the portal cast for real: find on a public need casts + card; use B's pulse to publish its card cast
const pulseB = await call(B,'pulse'); ok('pulse casts the card to the relay', /cast/i.test(pulseB), pulseB.slice(0,100));
const s = await (await fetch(R+'/search?q='+encodeURIComponent("someone who's crossed the Atlantic by sailboat")+'&tags=sailing,atlantic-crossing')).json();
ok('search over cached casts finds the sailor with a why', s.results.some(x=>x.handle==='sailor@mazel' && /sailing-atlantic/.test(x.why)), s.results[0] && s.results[0].why);
ok('search results are world tier', s.results.every(x=>x.tier==='world'));

// subscribe -> find.hit delivered to the subscriber's door, signed by the relay
const subB = await call(B,'pulse'); // pulse also (re)subscribes
const subs = [...renv.RELAY.m.keys()].filter(k=>k.startsWith('sub:'));
ok('pulse registered a subscription for the sailor', subs.length===1);
const before = seenAtB.length;
const f = JSON.parse(await call(A,'find',{need_text:"someone who's crossed the Atlantic by sailboat", tags:['sailing','atlantic-crossing']}));
ok('a public need cast from A reaches the relay and the relay delivers a find.hit to B', seenAtB.length>before && seenAtB.slice(before).some(m=>m.params.message.metadata.action.type==='find.hit'), seenAtB.slice(before).map(m=>m.params.message.metadata.action.type).join(','));
const hit = seenAtB.slice(before).find(m=>m.params.message.metadata.action.type==='find.hit').params.message.metadata.action;
const relayKey = (await (await fetch(R+'/.well-known/relay.json')).json()).publicKey;
ok('find.hit is signed by the relay key', hit.kid && hit.sig && hit.via==='relay');
ok("find from A also learned about B through search: candidate is world tier", f.candidates.some(c=>c.handle==='sailor@mazel' && c.tier==='world'), JSON.stringify(f.candidates.map(c=>[c.handle,c.tier])));
const mbB = JSON.parse(await call(B,'check_mailbox'));
ok("B's mailbox shows the hit as a readable message", mbB.messages.some(m=>m.action && m.action.type==='find.hit' && /looking for/.test(m.text)));
ok('matched-only needs never cast', (await (async()=>{ await call(A,'update_card',{add_need:'secret-hire', need_visibility:'matched-only'}); const n=[...renv.RELAY.m.keys()].filter(k=>k.startsWith('cast:need:')).length; await call(A,'pulse'); return [...renv.RELAY.m.keys()].filter(k=>k.startsWith('cast:need:')).length===n; })()));
console.log(`\nrelay: ${pass} passed, ${fail} failed`);
process.exit(fail?1:0);
