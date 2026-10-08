// Writes fixtures from the live Worker code: the A2A card, a SendMessage envelope as deliver() emits it,
// the ack the door returns, and the typed actions. Rust round-trips these; drift on either side fails CI.
import worker from '../src/index.js';
import { writeFileSync } from 'node:fs';
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})) }) }; };
const O = 'https://mazel.fixture.workers.dev', PEER = 'https://mazel.peer-fixture.workers.dev';
const env = { HANDLE:'avery@mazel', PERSONA:'Avery runs Halcyon.', NEED:'tech-advisor-partners', HAVE:'managed-ai-delivery', INBOX_TOKEN:'tok', MAILBOX: mkKV() };
const peer = { HANDLE:'peer@mazel', PERSONA:'A peer.', NEED:'', HAVE:'sailing-atlantic', INBOX_TOKEN:'tok', MAILBOX: mkKV() };
let captured = null;
const realFetch = globalThis.fetch;
globalThis.fetch = async (u, i={}) => { const url=String(u instanceof Request?u.url:u); if (url.startsWith(PEER)) { const r=new Request(url,i); if (r.method==='POST') { const b = JSON.parse(await r.clone().text()); if (b.method === 'SendMessage' && (((b.params||{}).message||{}).metadata||{}).action && b.params.message.metadata.action.type === 'intro.propose') captured = b; } return worker.fetch(r, peer); } if (url.startsWith(O)) return worker.fetch(new Request(url,i), env); return realFetch(u,i); };
const tool = async (name,args) => JSON.parse(await (await worker.fetch(new Request(O+'/mcp',{method:'POST',headers:{'content-type':'application/json',authorization:'Bearer tok'},body:JSON.stringify({jsonrpc:'2.0',id:1,method:'tools/call',params:{name,arguments:args||{}}})}), env)).text()).result.content[0].text;
await tool('update_card',{gloss_tag:'managed-ai-delivery',gloss_text:'A team that builds and runs AI in your cloud',confirmed:true});
const card = await (await worker.fetch(new Request(O+'/.well-known/agent-card.json'), env)).json();
writeFileSync('fixtures/agent-card.json', JSON.stringify(card, null, 2)+'\n');
await tool('cards',{action:'add',url:PEER+'/card'});
await tool('update_card',{add_need:'sailing', confirmed:true});   // a proposal carries the need's words, so it is placed first
const f = JSON.parse(await tool('find',{need_text:'someone who has crossed the Atlantic by sailboat',tags:['sailing','atlantic-crossing']}));
await tool('propose_intro',{thread_id:f.thread_id, card_url:PEER+'/card', confirmed:true});
writeFileSync('fixtures/send-message.json', JSON.stringify(captured, null, 2)+'\n');
// The ack is the peer's door taking the proposal: the peer holds the proposer's card, so the gate lets it through (29f H2 refuses what it cannot verify).
await worker.fetch(new Request(PEER+'/mcp',{method:'POST',headers:{'content-type':'application/json',authorization:'Bearer tok'},body:JSON.stringify({jsonrpc:'2.0',id:1,method:'tools/call',params:{name:'cards',arguments:{action:'add',url:O+'/card'}}})}), peer);
const ack = await (await worker.fetch(new Request(PEER+'/a2a',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(captured)}), peer)).json();
writeFileSync('fixtures/ack.json', JSON.stringify(ack, null, 2)+'\n');
const actions = [ {type:'note',v:1}, captured.params.message.metadata.action, {type:'intro.respond',v:1,introId:captured.params.message.metadata.action.introId,decision:'accepted',note:'Yes.',path:['avery@mazel','peer@mazel']} ];
writeFileSync('fixtures/actions.json', JSON.stringify(actions, null, 2)+'\n');
const pulse = await (await worker.fetch(new Request(O+'/a2a',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({jsonrpc:'2.0',id:'p1',method:'CreateTaskPushNotificationConfig',params:{tenant:'',id:'pulse-1',taskId:'*',url:'https://example.invalid/pulse',token:'t'}})}), env)).json();
writeFileSync('fixtures/pulse-config.json', JSON.stringify(pulse.result, null, 2)+'\n');
// Fly: signed handle record, a rotation record, a signed cast, and a relay-style find.hit
const rec = await (await worker.fetch(new Request(O+'/.well-known/mazel/avery.json'), env)).json();
writeFileSync('fixtures/handle-record.json', JSON.stringify(rec, null, 2)+'\n');
await tool('rotate_key',{confirmed:true});
const rec2 = await (await worker.fetch(new Request(O+'/.well-known/mazel/avery.json'), env)).json();
writeFileSync('fixtures/rotation-record.json', JSON.stringify(rec2.rotations[0], null, 2)+'\n');
const castBody = { v:1, handle:'avery@mazel', publicKey: rec2.publicKey, cardUrl: O+'/.well-known/agent-card.json', rpc: O+'/a2a', kind:'need', visibility:'public', needId: f.thread_id, needText: f.need_text, needTags: f.tags, sig:'<sig>', kid: rec2.kid };
writeFileSync('fixtures/cast.json', JSON.stringify(castBody, null, 2)+'\n');
console.log('fixtures written:', ['agent-card','send-message','ack','actions','pulse-config','handle-record','rotation-record','cast'].join(', '));
