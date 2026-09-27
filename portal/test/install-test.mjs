// Install: a portal nobody has claimed yet hands out its own setup link, the first conversation
// claims it with one call, and from then on the link is gone and the handle is fixed.
import worker from '../src/index.js';
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})) }) }; };
const O = 'https://mazel.fresh.workers.dev';
const realFetch = globalThis.fetch;
globalThis.fetch = async () => new Response('harness: no network', { status: 503 });
let pass=0, fail=0; const ok=(l,c,x='')=>{ console.log((c?'PASS ':'FAIL ')+l+(x?'  -> '+String(x).replace(/\n/g,' ').slice(0,120):'')); c?pass++:fail++; };

// A button deploy: wrangler.json vars only, no secret, no handle.
const fresh = { HANDLE: 'you@mazel', PERSONA: '', NEED: '', HAVE: '', MAILBOX: mkKV(), RELAY_URL: 'https://relay.test' };
const get = (env, path) => worker.fetch(new Request(O + path), env);
const mcp = async (env, name, args, token) => {
  const res = await worker.fetch(new Request(`${O}/mcp?token=${token}`, { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name, arguments: args||{} } }) }), env);
  const j = await res.json();
  return j.result ? j.result.content[0].text : JSON.stringify(j.error);
};

// 1. Unclaimed: the welcome page carries a working setup link.
let page = await (await get(fresh, '/')).text();
const link = (page.match(/https:\/\/\S+\/mcp\?token=[a-f0-9]+/) || [])[0];
ok('an unclaimed portal serves the setup link on /', !!link, page.split('\n')[0]);
ok('the page says what to do with it', /paste this into your AI/i.test(page));
ok('the page says the link expires and why', /stops the moment|60 minutes|more minute/i.test(page));
const token = link ? link.split('token=')[1] : '';
ok('the link it prints actually opens the portal', /"tools"|unclaimed/.test(await (await worker.fetch(new Request(`${O}/mcp?token=${token}`, { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/list' }) }), fresh)).text()));
ok('a wrong token still gets nothing', (await worker.fetch(new Request(`${O}/mcp?token=nope`, { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/list' }) }), fresh)).status === 401);

// 2. First contact: my_card is the script, not a card.
const script = await mcp(fresh, 'my_card', {}, token);
ok('my_card on an unclaimed portal returns the first conversation', /nobody has claimed it yet/.test(script) && /one question/.test(script), script.slice(0,80));
ok('it asks for a handle and nothing else', /handle/.test(script) && /only thing you ask/.test(script));
ok('it tells the agent to draft from what it already knows, not from their mail', /already know/.test(script) && /Do not read their email/.test(script));

// 3. The claim: one call writes the whole card.
const unconfirmed = await mcp(fresh, 'update_card', { handle:'lea@mazel', persona:'Lea builds biotech teams.', add_have:['biotech-recruiting','lab-ops'], add_need:'seed-investors' }, token);
ok('the draft is not written without a yes', /Not written/.test(unconfirmed), unconfirmed.slice(0,60));
const claimed = await mcp(fresh, 'update_card', { handle:'lea@mazel', persona:'Lea builds biotech teams.', add_have:['biotech-recruiting','lab-ops'], add_need:'seed-investors', confirmed:true }, token);
ok('one confirmed call claims the portal and writes the whole card', /This portal is lea@mazel from now on/.test(claimed), claimed.slice(0,90));
const card = JSON.parse(await (await get(fresh, '/card')).text());
const haah = card.capabilities.extensions.find(e => /haah/.test(e.uri)).params;
ok('the card is live with both haves and the need', haah.handle==='lea@mazel' && haah.have.includes('biotech-recruiting') && haah.have.includes('lab-ops') && haah.need.includes('seed-investors'), JSON.stringify(haah).slice(0,120));

// 4. Claimed: the link is gone for good, and so is changing the handle.
page = await (await get(fresh, '/')).text();
ok('/ is the public card page once claimed', /lea@mazel has a Mazel portal here/.test(page), page.split('\n')[0]);
ok('/ never shows the token again', !page.includes(token) && !/mcp\?token/.test(page));
ok('the handle cannot be changed after the claim', /fixed once it is claimed/.test(await mcp(fresh, 'update_card', { handle:'someone@mazel', confirmed:true }, token)));
ok('my_card is a card again', JSON.parse(await mcp(fresh, 'my_card', {}, token)).handle === 'lea@mazel');

// 5. A portal installed before the claim existed is already someone's: never a setup link.
const old = { HANDLE:'ariel@mazel', PERSONA:'p', NEED:'', HAVE:'managed-ai-delivery', INBOX_TOKEN:'ta', MAILBOX: mkKV(), RELAY_URL:'https://relay.test' };
page = await (await get(old, '/')).text();
ok('a portal deployed with a handle is claimed from the start', /ariel@mazel has a Mazel portal here/.test(page) && !/token/.test(page));

// 6. The window closes: the link stops, and the page says how to get a fresh one.
const late = { HANDLE:'you@mazel', PERSONA:'', NEED:'', HAVE:'', MAILBOX: mkKV(), RELAY_URL:'https://relay.test' };
await get(late, '/');
late.MAILBOX.m.set('config:opened', new Date(Date.now() - 61*60000).toISOString());
page = await (await get(late, '/')).text();
ok('after 60 minutes an unclaimed portal stops showing the link', !/mcp\?token=[a-f0-9]{16,}/.test(page), page.split('\n')[0]);
ok('and says how to get a fresh one with a terminal and without', /npx create-mazel --rotate-key/.test(page) && /INBOX_TOKEN/.test(page));

// 6b. A one-click deploy cannot be told its own address, so the portal learns it from the first
// visit; without that, the scheduled pulse would have nowhere to send from.
ok('a portal with no PORTAL_ORIGIN remembers its address from the first request', late.MAILBOX.m.get('config:origin') === O, late.MAILBOX.m.get('config:origin'));
const told = { HANDLE:'you@mazel', PERSONA:'', NEED:'', HAVE:'', MAILBOX: mkKV(), PORTAL_ORIGIN: O, RELAY_URL:'https://relay.test' };
await get(told, '/');
ok('a portal that was told its address does not write one', !told.MAILBOX.m.has('config:origin'));

// 7. The quiet rule rides on the tools that can produce something to say.
const tools = (await (await worker.fetch(new Request(`${O}/mcp?token=${token}`, { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/list' }) }), fresh)).json()).result.tools;
const byName = Object.fromEntries(tools.map(t => [t.name, t.description]));
ok('the quiet rule is on the tools that can speak', ['my_card','check_mailbox','pulse','find'].every(n => /three moments only/.test(byName[n])));
ok('and not on the ones that cannot', !/three moments only/.test(byName['close_thread']));
ok('the three moments are named', /✨ a hit/.test(byName['check_mailbox']) && /🌀 a yes/.test(byName['check_mailbox']) && /travel to strangers/.test(byName['check_mailbox']));
ok('first contact is pointed at from the first tools an agent calls', /FIRST CONTACT/.test(byName['my_card']) && /FIRST CONTACT/.test(byName['check_mailbox']));

globalThis.fetch = realFetch;
console.log(`\ninstall: ${pass} passed, ${fail} failed`);
process.exit(fail?1:0);
