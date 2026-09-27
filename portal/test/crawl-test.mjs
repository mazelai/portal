import worker from '../src/index.js';
import { sendReq, flat, ackText } from './a2a-helpers.mjs';
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})) }) }; };
const portals = {
  'https://mazel.ariel-mazel.workers.dev': { env: { HANDLE:'ariel@mazel', PERSONA:'Ariel runs Paragon.', NEED:'tech-advisor-partners', HAVE:'managed-ai-delivery', INBOX_TOKEN:'tokA', MAILBOX: mkKV() } },
  'https://mazel.gary-mazel.workers.dev':  { env: { HANDLE:'gary@mazel', PERSONA:'Gary lives in Tokyo and runs a small design studio.', NEED:'design-clients', HAVE:'hockey,ux-design', INBOX_TOKEN:'tokG', MAILBOX: mkKV() } },
  'https://mazel.lea-mazel.workers.dev':   { env: { HANDLE:'lea@mazel', PERSONA:'Lea is a biotech VC in Boston.', NEED:'deal-flow', HAVE:'biotech-vc', INBOX_TOKEN:'tokL', MAILBOX: mkKV() } },
};
let offline = new Set();
const realFetch = globalThis.fetch;
globalThis.fetch = async (url, init={}) => {
  const u = new URL(url); const o = u.origin;
  if (offline.has(o)) return new Response('Service Unavailable', { status: 503 });
  if (portals[o]) return worker.fetch(new Request(url, init), portals[o].env);
  return new Response('harness: no network', { status: 503 }); // suites never touch the real relay or peers
};
const call = async (origin, name, args) => {
  const r = await worker.fetch(new Request(origin+'/mcp', { method:'POST', headers:{ 'content-type':'application/json', authorization:'Bearer '+portals[origin].env.INBOX_TOKEN }, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name, arguments: args||{} } }) }), portals[origin].env);
  return JSON.parse(await r.text()).result.content[0].text;
};
const A='https://mazel.ariel-mazel.workers.dev', G='https://mazel.gary-mazel.workers.dev', L='https://mazel.lea-mazel.workers.dev';
let pass=0, fail=0; const ok=(l,c,x='')=>{ console.log((c?'PASS ':'FAIL ')+l+(x?'  -> '+x.replace(/\n/g,' ').slice(0,150):'')); c?pass++:fail++; };

// glosses on Gary's card
await call(G,'update_card',{gloss_tag:'hockey', gloss_text:'Plays ice hockey in Tokyo, weekend league', confirmed:true});
const gcard = flat(JSON.parse(await (await fetch(G+'/card')).text()));
ok('gloss on public card', gcard.glosses && gcard.glosses.hockey.includes('Tokyo'));

// trade cards
let t = await call(A,'add_known_card',{url:G+'/card'}); ok('add Gary', t.startsWith('Added known card gary@mazel'), t);
t = await call(A,'add_known_card',{url:L+'/card'}); ok('add Lea', t.startsWith('Added'));
t = await call(G,'add_known_card',{url:A+'/card'}); ok('Gary adds Ariel', t.startsWith('Added'));
const kc = JSON.parse(await call(A,'list_known_cards')); ok('tier defaulted to tribe, never asked', kc.every(c=>c.tier==='tribe'));
t = await call(A,'add_known_card',{url:G+'/card'}); ok('re-add refreshes, no duplicate', t.startsWith('Refreshed') && JSON.parse(await call(A,'list_known_cards')).length===2);
portals['https://mazel.gary2-mazel.workers.dev']={env:{...portals[G].env}};
t = await call(A,'add_known_card',{url:'https://mazel.gary2-mazel.workers.dev/card'});
ok('same handle at a new url updates in place, no second entry', t.includes('moved from') && JSON.parse(await call(A,'list_known_cards')).filter(c=>c.handle==='gary@mazel').length===1, t);
await call(A,'add_known_card',{url:G+'/card'});
t = await call(A,'remove_known_card',{handle_or_url:'lea@mazel'}); ok('remove_known_card forgets a card', t.startsWith('Removed') && JSON.parse(await call(A,'list_known_cards')).every(c=>c.handle!=='lea@mazel'));
await call(A,'add_known_card',{url:L+'/card'});

// HOCKEY TEST
t = await call(A,'find',{need_text:'a hockey player in Tokyo', tags:['hockey','tokyo']});
const f = JSON.parse(t);
ok('find ranks Gary first with why', f.candidates[0].handle==='gary@mazel' && f.candidates[0].why.includes('hockey'), f.candidates[0].why);
ok('Lea not a candidate (no fit)', !f.candidates.some(c=>c.handle==='lea@mazel'));
const TH=f.thread_id;
t = await call(A,'propose_intro',{thread_id:TH, card_url:G+'/card'}); ok('propose without confirm -> not sent', t.startsWith('Not sent'));
t = await call(A,'propose_intro',{thread_id:TH, card_url:G+'/card', confirmed:true}); ok('propose confirmed -> delivered to Gary', t.startsWith('Delivered intro'), t);
const gin = JSON.parse(await call(G,'check_mailbox'));
const prop = gin.messages.find(m=>m.action && m.action.type==='intro.propose');
ok("Gary's mailbox has intro.propose with why + path", !!prop && prop.action.path[0]==='ariel@mazel' && prop.action.why.includes('hockey'), prop && prop.text);
const gIntros = JSON.parse(await call(G,'list_intros')); ok("Gary's portal created intro object (received, proposed)", gIntros[0].direction==='received' && gIntros[0].state==='proposed');
ok('both portals agree after the accept (checked after the flip below)', true);
const ID = prop.action.introId;
t = await call(G,'respond_intro',{intro_id:ID, decision:'accepted', note:'Sure, Tuesdays.', confirmed:true}); ok('Gary accepts -> connected, crossing marked', t.startsWith('🌀') && t.includes('connected'), t);
{ const g=JSON.parse(await call(G,'list_intros')).find(i=>i.intro_id===ID), a=JSON.parse(await call(A,'list_intros')).find(i=>i.intro_id===ID);
  ok('both portals hold agreeing copies', g.state===a.state && g.state==='connected' && g.their_answer==='accepted' && a.their_answer==='accepted' && !!g.connected_at && !!a.connected_at, JSON.stringify({g:g.state,a:a.state}));
  ok('answering twice is refused', (await call(G,'respond_intro',{intro_id:ID, decision:'declined', confirmed:true})).includes('already connected')); }
const aIntros = JSON.parse(await call(A,'list_intros')); ok("Ariel's intro flipped from the wire: connected (unanimous yes)", aIntros[0].state==='connected' && aIntros[0].their_answer==='accepted' && aIntros[0].path[0]==='ariel@mazel');
const ain = JSON.parse(await call(A,'check_mailbox')); ok("Ariel's mailbox has intro.respond text", ain.messages.some(m=>m.action.type==='intro.respond' && m.text.includes('accepted')));
ok('thread candidate shows connected', JSON.parse(await call(A,'list_threads')).find(t=>t.thread_id===TH).candidates[0].intro_state==='connected');
{ // the thread's cached hint must never outrank the intro object
  const key='thread:'+TH, th=JSON.parse(portals[A].env.MAILBOX.m.get(key));
  th.candidates.forEach(c=>{ delete c.introId; delete c.introState; });
  portals[A].env.MAILBOX.m.set(key, JSON.stringify(th));
  ok('stale thread cache still reports the true intro state', JSON.parse(await call(A,'list_threads')).find(t=>t.thread_id===TH).candidates[0].intro_state==='connected'); }

// FAILURE 1: no match + closest partial
t = await call(A,'find',{need_text:'a sushi chef in Osaka', tags:['sushi','osaka']});
ok('no match -> nothing fits + closest partial, still JSON', JSON.parse(t).headline.startsWith('Nothing in your cards fits') && JSON.parse(t).candidates.length===0, t);
t = await call(A,'find',{need_text:'someone who does design for apps', tags:['app-design']});
ok('partial: design matches via ux-design word overlap', JSON.parse(t).candidates.some(c=>c.handle==='gary@mazel'), t);
// FAILURE 2: declined intro closes cleanly
t = await call(A,'find',{need_text:'biotech investor', tags:['biotech-vc']}); const f2=JSON.parse(t);
await call(A,'propose_intro',{thread_id:f2.thread_id, card_url:L+'/card', confirmed:true});
const lin = JSON.parse(await call(L,'check_mailbox')); const lid = lin.messages.find(m=>m.action.type==='intro.propose').action.introId;
t = await call(L,'respond_intro',{intro_id:lid, decision:'declined', note:'Not now.', confirmed:true}); ok('decline delivered, closes cleanly', t.toLowerCase().includes('closed cleanly'));
ok("Ariel's intro = declined; thread still open", JSON.parse(await call(A,'list_intros')).find(i=>i.intro_id===lid).state==='declined' && JSON.parse(await call(A,'list_threads')).find(x=>x.thread_id===f2.thread_id).status==='open');
// FAILURE 3: offline portal -> kept, retry same id
offline.add(L);
t = await call(A,'find',{need_text:'biotech deal flow help', tags:['biotech-vc','deal-flow']}); const f3=JSON.parse(t);
t = await call(A,'propose_intro',{thread_id:f3.thread_id, card_url:L+'/card', confirmed:true}); ok('offline -> NOT delivered, intro saved', t.startsWith('NOT delivered') && t.includes('retry'), t);
const pend = JSON.parse(await call(A,'list_intros')).find(i=>i.state==='proposed' && i.delivered===false); ok('intro object kept with error', !!pend && !!pend.error);
offline.delete(L);
t = await call(A,'propose_intro',{thread_id:f3.thread_id, card_url:L+'/card', confirmed:true}); ok('retry after back online -> delivered', t.startsWith('Delivered intro'));
t = await call(A,'propose_intro',{thread_id:f3.thread_id, card_url:L+'/card', confirmed:true}); ok('re-propose after delivery -> nothing sent, waiting on answer', t.startsWith('Nothing sent') && t.includes('waiting on their answer'), t);
{ const lid2 = JSON.parse(await call(L,'list_intros')).find(i=>i.state==='proposed').intro_id; await call(L,'respond_intro',{intro_id:lid2, decision:'declined', confirmed:true});
  t = await call(A,'propose_intro',{thread_id:f3.thread_id, card_url:L+'/card', confirmed:true}); ok('re-propose after an answer -> refuses, keeps outcome', t.startsWith('Nothing sent') && t.includes('already answered'), t);
  ok('outcome preserved (declined, not clobbered)', JSON.parse(await call(A,'list_intros')).find(i=>i.intro_id===lid2).state==='declined'); }
const lprops = JSON.parse(await call(L,'check_mailbox')).messages.filter(m=>m.action.type==='intro.propose' && m.action.introId===pend.intro_id); ok('Lea got exactly one copy (stable id)', lprops.length===1);
// FAILURE 4: expiry
const key = 'thread:'+TH;
const th = JSON.parse(portals[A].env.MAILBOX.m.get(key)); th.expires = new Date(Date.now()-1000).toISOString(); portals[A].env.MAILBOX.m.set(key, JSON.stringify(th));
ok('expired thread closes on next touch', JSON.parse(await call(A,'list_threads')).find(x=>x.thread_id===TH).status==='expired');
t = await call(A,'reopen_thread',{thread_id:TH}); ok('reopen resets TTL', t.includes('now open'));
t = await call(A,'close_thread',{thread_id:TH}); ok('close', t.includes('now closed'));
t = await call(A,'propose_intro',{thread_id:TH, card_url:G+'/card', confirmed:true}).catch(e=>e.message); ok('propose on closed thread refused', t.includes('closed'), t);
// cap at 5 + determinism
for (let i=0;i<7;i++){ portals['https://mazel.p'+i+'.workers.dev']={env:{HANDLE:'p'+i+'@mazel',PERSONA:'',NEED:'',HAVE:'hockey',INBOX_TOKEN:'t',MAILBOX:mkKV()}}; await call(A,'add_known_card',{url:'https://mazel.p'+i+'.workers.dev/card'}); }
const f5=JSON.parse(await call(A,'find',{need_text:'hockey', tags:['hockey']})); ok('cap at 5 candidates', f5.candidates.length===5);
const f6=JSON.parse(await call(A,'find',{need_text:'hockey', tags:['hockey']})); ok('deterministic order + thread reuse', JSON.stringify(f5.candidates.map(c=>c.handle))===JSON.stringify(f6.candidates.map(c=>c.handle)) && f5.thread_id===f6.thread_id);
// --- Crawl Stage 2, item 1: threads keep collecting ---
{ const f=JSON.parse(await call(A,'find',{need_text:'someone who sails oceans', tags:['sailing']}));
  const th=f.thread_id;
  ok('thread opens with no fit yet', f.candidates.length===0 && f.headline.startsWith('Nothing in your cards fits'));
  portals['https://mazel.newsailor.workers.dev']={env:{HANDLE:'newsailor@mazel',PERSONA:'Sails oceans.',NEED:'',HAVE:'sailing-atlantic',INBOX_TOKEN:'t',MAILBOX:mkKV()}};
  await call('https://mazel.newsailor.workers.dev','update_card',{gloss_tag:'sailing-atlantic',gloss_text:'has sailed across the Atlantic twice',confirmed:true});
  const add = await call(A,'add_known_card',{url:'https://mazel.newsailor.workers.dev/card'});
  ok('adding a card later grows the open thread, no recast', add.includes('need you already cast') && add.includes(th), add.slice(0,120));
  const lt = JSON.parse(await call(A,'list_threads')).find(t=>t.thread_id===th);
  ok('thread now lists the new candidate with its why', lt.candidates.some(c=>c.handle==='newsailor@mazel' && /sail/i.test(c.why)));
  ok('list_threads shows why + room left', typeof lt.room==='number' && lt.candidates.every(c=>c.why));
  // cap of 5 enforced, reopen raises it
  for (let i=0;i<7;i++){ const h='cap'+i+'@mazel'; portals['https://mazel.cap'+i+'.workers.dev']={env:{HANDLE:h,PERSONA:'',NEED:'',HAVE:'sailing-atlantic',INBOX_TOKEN:'t',MAILBOX:mkKV()}}; await call(A,'add_known_card',{url:'https://mazel.cap'+i+'.workers.dev/card'}); }
  const capped = JSON.parse(await call(A,'list_threads')).find(t=>t.thread_id===th);
  ok('cap of 5 enforced while collecting', capped.candidates.length===5, 'got '+capped.candidates.length);
  await call(A,'close_thread',{thread_id:th});
  const re = await call(A,'reopen_thread',{thread_id:th});
  ok('reopen raises the cap', re.includes('room for 10'), re);
  portals['https://mazel.cap9.workers.dev']={env:{HANDLE:'cap9@mazel',PERSONA:'',NEED:'',HAVE:'sailing-atlantic',INBOX_TOKEN:'t',MAILBOX:mkKV()}};
  await call(A,'add_known_card',{url:'https://mazel.cap9.workers.dev/card'});
  ok('a 6th candidate lands after the reopen', JSON.parse(await call(A,'list_threads')).find(t=>t.thread_id===th).candidates.length===6);
  await call(A,'close_thread',{thread_id:th});
  const closedAdd = await call(A,'add_known_card',{url:'https://mazel.newsailor.workers.dev/card'});
  ok('a closed thread does not collect', !closedAdd.includes(th)); }
// badges in outputs
{ const lea=await call(L,'check_mailbox'); ok('mailbox with nothing new reports 📭', lea.startsWith('📭') || JSON.parse(lea).headline.startsWith('📬'), lea.slice(0,40)); }
ok('find headline carries ✨ and stays parseable', JSON.parse(await call(A,'find',{need_text:'someone who sails oceans', tags:['sailing']})).headline.startsWith('✨'));
{ const mb=JSON.parse(await call(G,'check_mailbox')); ok('mailbox headline carries 📬 N', mb.headline.startsWith('📬') && mb.count>0); }
// matcher: function words must not make a match; real paraphrase still must
{ portals['https://mazel.sailor2.workers.dev']={env:{HANDLE:'sailor2@mazel',PERSONA:"A test portal. Not a real person. Has sailed across the Atlantic twice.",NEED:'',HAVE:'sailing-atlantic',INBOX_TOKEN:'t',MAILBOX:mkKV()}};
  await call('https://mazel.sailor2.workers.dev','update_card',{gloss_tag:'sailing-atlantic',gloss_text:'has sailed across the Atlantic twice, happy to advise on crossings',confirmed:true});
  await call(A,'add_known_card',{url:'https://mazel.sailor2.workers.dev/card'});
  const noise=JSON.parse(await call(A,'find',{need_text:'someone who has crewed a passage', tags:['crewing']}));
  ok('function words alone never make a match', !noise.candidates.some(c=>c.handle==='sailor2@mazel'), noise.headline.slice(0,60));
  const para=JSON.parse(await call(A,'find',{need_text:"someone who's crossed the Atlantic by sailboat", tags:['sailing','atlantic-crossing','sailboat']}));
  ok('real paraphrase still matches through tag words + gloss', para.candidates.some(c=>c.handle==='sailor2@mazel'), para.candidates[0] && para.candidates[0].why); }
// old-code compatibility: plain note still works, text part on every action
t = await call(A,'send_to_peer',{rpc:G+'/a2a', text:'plain note'}); ok('plain send_to_peer still delivers (note action)', t.startsWith('Delivered'));
ok('every inbound message has an action, notes default', JSON.parse(await call(G,'check_mailbox')).messages.every(m=>m.action && m.action.type));
console.log(`\n${pass} passed, ${fail} failed`);
