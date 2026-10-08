// Identity: keys, canonical signing, handle records, resolution, rotation. Two in-process portals,
// one acting as a directory for its own name; fetch routed in-process.
import rawWorker from '../src/index.js';
import { legacy as legacyWorker } from './a2a-helpers.mjs';
const worker = legacyWorker(rawWorker);
import { flat, HAAH } from './a2a-helpers.mjs';
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})) }) }; };
const A='https://mazel.a.workers.dev', B='https://example.com';
const portals = {
  [A]: { HANDLE:'avery@mazel', PERSONA:'p', NEED:'', HAVE:'managed-ai-delivery', INBOX_TOKEN:'ta', MAILBOX: mkKV(), RELAY_URL: 'https://relay.test' },
  [B]: { HANDLE:'lea@example.com', PERSONA:'Lea', NEED:'', HAVE:'biotech-vc', INBOX_TOKEN:'tb', MAILBOX: mkKV(), RELAY_URL: 'https://relay.test' },
};
const relayStore = new Map(); // relay stub: /publish stores records; /.well-known/mazel/<name>.json serves them
const realFetch = globalThis.fetch;
globalThis.fetch = async (u, i={}) => {
  const url = String(u instanceof Request ? u.url : u); const o = new URL(url).origin;
  if (portals[o]) return worker.fetch(new Request(url, i), portals[o]);
  if (o === 'https://relay.test') {
    const p = new URL(url).pathname;
    if (p === '/publish') { const rec = JSON.parse(i.body); relayStore.set(rec.handle.split('@')[0], rec); return new Response('{"ok":true}', { status: 200 }); }
    const m = p.match(/^\/\.well-known\/mazel\/([^/]+)\.json$/);
    if (m) { const r = relayStore.get(m[1]); return new Response(r ? JSON.stringify(r) : 'no', { status: r ? 200 : 404, headers: { 'content-type': 'application/json' } }); }
  }
  return realFetch(u, i);
};
const call = async (o, name, args) => JSON.parse(await (await worker.fetch(new Request(o+'/mcp', { method:'POST', headers:{ 'content-type':'application/json', authorization:'Bearer '+portals[o].INBOX_TOKEN }, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name, arguments: args||{} } }) }), portals[o])).text()).result.content[0].text;
let pass=0, fail=0; const ok=(l,c,x='')=>{ console.log((c?'PASS ':'FAIL ')+l+(x?'  -> '+String(x).replace(/\n/g,' ').slice(0,120):'')); c?pass++:fail++; };

// keys exist and ride the card
const cardA = JSON.parse(await (await fetch(A+'/.well-known/agent-card.json')).text());
const pA = flat(cardA).ext.params;
ok('card carries a public key and key id in HAAH params', typeof pA.publicKey==='string' && pA.publicKey.length>30 && typeof pA.keyId==='string');
const idA = JSON.parse(await call(A,'my_identity'));
ok('my_identity matches the card', idA.publicKey===pA.publicKey && idA.keyId===pA.keyId && idA.rotations===0);
ok('private key never appears on the card or identity view', !JSON.stringify(cardA).includes('"d"') && !('priv' in idA));

// handle record served by the portal itself (any domain can be a directory)
const recRes = await fetch(B+'/.well-known/mazel/lea.json');
const rec = await recRes.json();
ok('portal serves its signed handle record at /.well-known/mazel/<name>.json', recRes.status===200 && rec.handle==='lea@example.com' && rec.publicKey && rec.sig, rec.cardUrl);
ok('record for another name is 404', (await fetch(B+'/.well-known/mazel/bob.json')).status===404);

// resolution: name@domain
let t = await call(A,'resolve_handle',{handle:'lea@example.com'});
ok('resolve_handle verifies the record and stores the card', t.startsWith('Resolved lea@example.com') && /Added known card lea@example.com/.test(t), t.slice(0,90));
ok('known card remembers the peer public key', JSON.parse(await call(A,'list_known_cards')).some(c=>c.handle==='lea@example.com'));

// tampered record is rejected
const tampered = { ...rec, cardUrl: 'https://evil.example/card' };
portals['https://evil.example'] = { ...portals[B] };
const origFetch = globalThis.fetch;
globalThis.fetch = async (u,i={}) => { const url=String(u instanceof Request?u.url:u); if (url==='https://example.com/.well-known/mazel/lea.json') return new Response(JSON.stringify(tampered),{status:200,headers:{'content-type':'application/json'}}); return origFetch(u,i); };
t = await call(A,'resolve_handle',{handle:'lea@example.com'});
ok('a record whose contents were changed fails signature verification', /not signed by the key it names/.test(t), t.slice(0,80));
globalThis.fetch = origFetch;

// name@mazel resolves through the relay directory after the portal publishes
await call(A,'rotate_key',{confirmed:true}); // also publishes; but first make sure a plain publish path exists via rotation
const dir = relayStore.get('avery');
ok('rotation published a handle record to the directory', !!dir && dir.handle==='avery@mazel' && dir.rotations.length===1);
const idA2 = JSON.parse(await call(A,'my_identity'));
ok('rotation changed the key and kept the handle', idA2.publicKey!==idA.publicKey && idA2.handle==='avery@mazel' && idA2.rotations===1);
t = await call(B,'resolve_handle',{handle:'avery@mazel'});
ok('name@mazel resolves via the directory with the NEW key', t.startsWith('Resolved avery@mazel') && /1 rotation/.test(t), t.slice(0,100));

// rotation chain verifies old -> new
const r = dir.rotations[0];
const { sig, kid, newSig, ...base } = r;
ok('rotation record is signed by the old key', await (async()=>{ const k=await crypto.subtle.importKey('raw', Uint8Array.from(atob(r.oldKey.replace(/-/g,'+').replace(/_/g,'/')+'='.repeat((4-r.oldKey.length%4)%4)),c=>c.charCodeAt(0)), {name:'Ed25519'}, false, ['verify']); const canon=(v)=>v===null||typeof v!=='object'?JSON.stringify(v):Array.isArray(v)?'['+v.map(canon).join(',')+']':'{'+Object.keys(v).sort().map(k=>JSON.stringify(k)+':'+canon(v[k])).join(',')+'}'; return crypto.subtle.verify({name:'Ed25519'}, k, Uint8Array.from(atob(sig.replace(/-/g,'+').replace(/_/g,'/')+'='.repeat((4-sig.length%4)%4)),c=>c.charCodeAt(0)), new TextEncoder().encode(canon(base))); })());
ok('rotation record is countersigned by the new key', r.newKey===idA2.publicKey && typeof newSig==='string' && newSig.length>40);

// threads survive a rotation
const f = JSON.parse(await call(A,'find',{need_text:'a biotech investor',tags:['biotech-vc']}));
ok('find works after rotation and still sees known cards', f.candidates.some(c=>c.handle==='lea@example.com'));
await call(A,'rotate_key',{confirmed:true});
const lt = JSON.parse(await call(A,'list_threads')).find(x=>x.thread_id===f.thread_id);
ok('thread survives a second rotation', !!lt && lt.status==='open' && lt.candidates.length===1);
ok('directory now holds a 2-link chain', relayStore.get('avery').rotations.length===2);
console.log(`\nidentity: ${pass} passed, ${fail} failed`);
process.exit(fail?1:0);
