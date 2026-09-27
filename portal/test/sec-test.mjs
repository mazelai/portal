import worker from '../src/index.js';
import { sendReq, flat, ackText } from './a2a-helpers.mjs';
const store = new Map();
const kv = { get: async k => store.get(k) ?? null, put: async (k,v) => store.set(k,v), delete: async k => store.delete(k), list: async ({prefix}) => ({ keys: [...store.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})) }) };
const env = { HANDLE:'ariel@mazel', PERSONA:'p', NEED:'a', HAVE:'b', INBOX_TOKEN:'tok', MAILBOX: kv };
const envNoTok = { ...env, INBOX_TOKEN: undefined };
const O='https://mazel.test.workers.dev';
const req=(path,method='GET',body,auth=true)=>new Request(O+path,{method,headers:{'content-type':'application/json',...(auth?{authorization:'Bearer tok'}:{})},body:body?JSON.stringify(body):undefined});
const mcp=(name,args)=>req('/mcp','POST',{jsonrpc:'2.0',id:1,method:'tools/call',params:{name,arguments:args}});
const tool=async(name,args)=>JSON.parse(await (await worker.fetch(mcp(name,args),env)).text()).result.content[0].text;
let pass=0, failN=0; const ok=(label,cond,extra='')=>{ console.log((cond?'PASS ':'FAIL ')+label+(extra?'  -> '+extra:'')); cond?pass++:failN++; };

// ---- BUG 1 ----
const home = await (await worker.fetch(req('/'),env)).text();
ok('homepage has no token / connector url', !home.includes('tok') && !home.includes('/mcp'), home.replace(/\n/g,' | '));
ok('homepage lists card + rpc', home.includes('/card') && home.includes('/a2a'));
ok('no config:token written', !store.has('config:token') && !store.has('config:token_shown'));
ok('no secret set -> /inbox 401', (await worker.fetch(req('/inbox'),envNoTok)).status===401);
ok('no secret set -> /mcp 401', (await worker.fetch(mcp('my_card',{}),envNoTok)).status===401);
ok('wrong token 401', (await worker.fetch(new Request(O+'/inbox',{headers:{authorization:'Bearer nope'}}),env)).status===401);

// ---- receive ----
const a2a = (text, messageId, cardUrl) => req('/a2a','POST', sendReq('x', text, { handle:'gary@mazel', cardUrl, messageId }), false);
const r1 = JSON.parse(await (await worker.fetch(a2a('hello ariel','gary-msg-1','https://peer.example/card'),env)).text());
const r1b = JSON.parse(await (await worker.fetch(a2a('hello ariel','gary-msg-1','https://peer.example/card'),env)).text());
const inbox = JSON.parse(await tool('check_mailbox',{}));
ok('receive stores exactly one message for a retried send (dedupe)', inbox.count===1, `count=${inbox.count}`);
ok('retried send gets the identical ack id', r1.result.message.messageId===r1b.result.message.messageId);
const key = inbox.messages[0].key;

// ---- BUG 2: mock peer door ----
const realFetch = globalThis.fetch; let mode='ok'; let received=[];
globalThis.fetch = async (url, init) => {
  received.push(JSON.parse(init.body));
  if (mode==='503') return new Response('Service Unavailable',{status:503});
  if (mode==='rpcerr') return new Response(JSON.stringify({jsonrpc:'2.0',id:1,error:{code:-32601,message:'Method not found'}}),{status:200,headers:{'content-type':'application/json'}});
  if (mode==='html') return new Response('<html>oops</html>',{status:200});
  if (mode==='neterr') throw new Error('ECONNRESET');
  return new Response(JSON.stringify({jsonrpc:'2.0',id:1,result:{message:{messageId:'ack',role:'ROLE_AGENT',parts:[{text:'Delivered to gary@mazel\'s mailbox.'}],extensions:[]}}}),{status:200});
};
const RPC='https://peer.example/a2a';
mode='503'; let t=await tool('send_to_peer',{rpc:RPC,text:'reply 1',in_reply_to:key});
ok('503 -> NOT delivered', t.startsWith('NOT delivered') && t.includes('HTTP 503'), t.slice(0,80));
let c=await tool('clear_messages',{keys:[key]});
ok('clear after failed reply -> kept', c.includes('Kept 1') && store.has(key), c.slice(0,90));
mode='rpcerr'; t=await tool('send_to_peer',{rpc:RPC,text:'reply 1',in_reply_to:key});
ok('JSON-RPC error -> NOT delivered', t.startsWith('NOT delivered') && t.includes('JSON-RPC error -32601'), t.slice(0,80));
mode='html'; t=await tool('send_to_peer',{rpc:RPC,text:'reply 1',in_reply_to:key});
ok('200 non-JSON -> NOT delivered', t.startsWith('NOT delivered') && t.includes('not JSON'));
mode='neterr'; t=await tool('send_to_peer',{rpc:RPC,text:'reply 1',in_reply_to:key});
ok('network error -> NOT delivered', t.startsWith('NOT delivered') && t.includes('network error'));
c=await tool('clear_messages',{keys:[key]});
ok('still kept after all failures', c.includes('Kept 1') && store.has(key));
const ids=new Set(received.map(b=>b.params.message.messageId));
ok('all retries carried the same message id', ids.size===1, [...ids][0]);
mode='ok'; t=await tool('send_to_peer',{rpc:RPC,text:'reply 1',in_reply_to:key});
ok('success -> Delivered + marked replied', t.startsWith('Delivered') && t.includes('Marked '+key), t.slice(0,90));
const rec=JSON.parse(store.get(key));
ok('record has repliedAt and no lastReplyError', !!rec.repliedAt && !rec.lastReplyError);
c=await tool('clear_messages',{keys:[key]});
ok('clear after confirmed reply -> cleared', c.startsWith('Cleared 1') && !store.has(key), c);
// force path
await worker.fetch(a2a('spam','spam-1',null),env);
const k2=JSON.parse(await tool('check_mailbox',{})).messages[0].key;
mode='503'; await tool('send_to_peer',{rpc:RPC,text:'r',in_reply_to:k2});
c=await tool('clear_messages',{keys:[k2],force:true});
ok('force clears a failed one', c.startsWith('Cleared 1') && !store.has(k2));
ok('different text -> different message id', (await tool('send_to_peer',{rpc:RPC,text:'other'})).match(/message id ([0-9a-f]+)/)[1] !== [...ids][0]);
globalThis.fetch = realFetch;
console.log(`\n${pass} passed, ${failN} failed`);
