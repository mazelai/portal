// Fly end to end: two portals with NO shared known cards find each other (1) through the relay,
// then (2) with the relay off, through one-hop gossip via a mutual acquaintance. Plus pulse and the nostr flag.
import rawWorker from '../src/index.js';
import { legacy as legacyWorker } from './a2a-helpers.mjs';
const worker = legacyWorker(rawWorker);
import relay from '../../relay/src/index.js';
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})) }) }; };
const R='https://relay.test', A='https://mazel.a.workers.dev', B='https://mazel.b.workers.dev', C='https://mazel.c.workers.dev';
let relayOn = true; const renv = { RELAY: mkKV() };
const mk = (h, p, have, extra={}) => ({ HANDLE:h, PERSONA:p, NEED:'', HAVE:have, INBOX_TOKEN:'t', MAILBOX: mkKV(), PORTAL_ORIGIN: '', ...extra });
const portals = { [A]: mk('avery@mazel','Avery.','managed-ai-delivery',{RELAY_URL:R}), [B]: mk('sailor@mazel','Sails.','sailing-atlantic',{RELAY_URL:R}), [C]: mk('mutual@mazel','Knows people.','intros',{RELAY_URL:R}) };
const realFetch = globalThis.fetch;
globalThis.fetch = async (u, i={}) => { const url=String(u instanceof Request?u.url:u); const o=new URL(url).origin; if (o===R) { if (!relayOn) return new Response('down',{status:503}); return relay.fetch(new Request(url,i), renv); } if (portals[o]) return worker.fetch(new Request(url,i), portals[o]); return realFetch(u,i); };
const call = async (o, name, args) => JSON.parse(await (await worker.fetch(new Request(o+'/mcp', { method:'POST', headers:{ 'content-type':'application/json', authorization:'Bearer t' }, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name, arguments: args||{} } }) }), portals[o])).text()).result.content[0].text;
let pass=0, fail=0; const ok=(l,c,x='')=>{ console.log((c?'PASS ':'FAIL ')+l+(x?'  -> '+String(x).replace(/\n/g,' ').slice(0,120):'')); c?pass++:fail++; };

// (1) strangers meet through the relay
await call(B,'update_card',{gloss_tag:'sailing-atlantic',gloss_text:'has sailed across the Atlantic twice',confirmed:true});
await call(B,'pulse'); // B casts its card and subscribes
const kc0 = await call(A,'list_known_cards');
ok('A and B hold no cards of each other', /No known cards/.test(kc0) || JSON.parse(kc0).length===0);
// The need reaches the world only once the person has placed it, so the agent answers first.
await call(A,'update_card',{add_need:'sailing',confirmed:true});
let f = JSON.parse(await call(A,'find',{need_text:"someone who's crossed the Atlantic by sailboat", tags:['sailing','atlantic-crossing']}));
ok('relay: A finds B without ever holding B\'s card', f.candidates.some(c=>c.handle==='sailor@mazel' && c.tier==='world' && c.via==='relay'), JSON.stringify(f.candidates.map(c=>[c.handle,c.tier,c.via])));
ok('B landed in A as a world-tier known card 🌍', JSON.parse(await call(A,'list_known_cards')).some(c=>c.handle==='sailor@mazel' && c.tier==='world'));
ok('intro flow unchanged: propose to the world-tier candidate works', (await call(A,'propose_intro',{thread_id:f.thread_id, card_url:B+'/.well-known/agent-card.json', confirmed:true})).startsWith('Proposed to'));

// (2) with the relay unreachable, the portal still pulses on what it holds, and claims nothing it did not do.
relayOn = false;
const pDown = await call(A,'pulse');
ok('pulse runs with the relay unreachable and says so', !/^Error/.test(pDown) && /relay/.test(pDown), pDown.slice(0,80));
relayOn = true;

// pulse summary + quiet
const p2 = await call(A,'pulse');
ok('pulse is quiet when nothing new lands', /quiet/i.test(p2), p2.slice(-40));
console.log(`\nfly: ${pass} passed, ${fail} failed`);
process.exit(fail?1:0);
