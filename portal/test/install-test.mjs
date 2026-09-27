// Install: a portal nobody has claimed yet hands out its own setup link, the first conversation
// claims it with one call, and from then on the link is gone and the handle is fixed.
import worker from '../src/index.js';
const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})) }) }; };
const O = 'https://mazel.fresh.workers.dev';
const realFetch = globalThis.fetch;
globalThis.fetch = async () => new Response('harness: no network', { status: 503 });
const isClaimedNow = async (env) => !!env.MAILBOX.m.get('config:claimed');
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
ok('it tells the agent to draft from what it already knows, not from their mail', /already know about me from our conversations/.test(script) && /Don't read my email/.test(script));
ok('it carries the canonical card prompt, closer and all', /Two things: is anything/.test(script) && /what's burning right now/.test(script));
ok('it says where a sensitive need goes instead of the public card', /matched-only/.test(script) && /directed/.test(script));

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
// The setup key dies at the claim and the claim hands over the real one.
const freshReal = (claimed.match(/mcp\?token=([a-f0-9]+)/) || [])[1];
ok('the claim hands over the real connector link', !!freshReal && freshReal !== token);
ok('the setup key stops working at the claim', /unauthor/i.test(await mcp(fresh, 'my_card', {}, token)));
ok('the handle cannot be changed after the claim', /fixed once it is claimed/.test(await mcp(fresh, 'update_card', { handle:'someone@mazel', confirmed:true }, freshReal)));
ok('my_card is a card again', JSON.parse(await mcp(fresh, 'my_card', {}, freshReal)).handle === 'lea@mazel');

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

// 6c. A relay is optional: RELAY_URL=none turns the carrier off and nothing reaches out to a cache.
const solo = { HANDLE:'solo@mazel', PERSONA:'p', NEED:'', HAVE:'x', INBOX_TOKEN:'ts', MAILBOX: mkKV(), RELAY_URL:'none', PORTAL_ORIGIN: O };
const carriers = JSON.parse(await mcp(solo, 'carriers', {}, 'ts'));
ok('RELAY_URL=none turns the relay carrier off', carriers.relay === false && carriers.relayUrl === null && carriers.known === true && carriers.gossip === true, JSON.stringify(carriers));
const pulsed = await mcp(solo, 'pulse', {}, 'ts');
ok('a portal with no relay still pulses, on the carriers it has, and claims nothing it did not do', !/^Error/.test(pulsed) && /cast on known, gossip\./.test(pulsed) && !/subscription/.test(pulsed), pulsed.split('\n')[0]);
ok('and says plainly that name@mazel cannot be resolved without one', /has no relay/.test(await mcp(solo, 'resolve_handle', { handle:'lea@mazel' }, 'ts')));

// 6d. Signed links: what a read-only connector can still do. The agent drafts, the person's own
// browser writes. Nothing is stored to mint a link, so minting one is genuinely a read.
const gpt = { HANDLE:'you@mazel', PERSONA:'', NEED:'', HAVE:'', MAILBOX: mkKV(), RELAY_URL:'https://relay.test', PORTAL_ORIGIN: O };
await get(gpt, '/');
const gptToken = (await (await get(gpt, '/')).text()).match(/token=([a-f0-9]+)/)[1];
await get(gpt, '/card');   // the signing key is minted lazily; mint it before measuring writes
const kvBefore = gpt.MAILBOX.m.size;
const linkOut = await mcp(gpt, 'claim_link', { handle:'lea@mazel', persona:'Lea builds biotech teams.', have:['biotech-recruiting','lab-ops'], need:'seed-investors', held_need:'quiet-cofounder-search' }, gptToken);
const claimUrl = (linkOut.match(/https:\/\/\S+\/claim\?\S+/) || [])[0];
ok('claim_link hands back a link to the person own portal', !!claimUrl && claimUrl.startsWith(O + '/claim?'), linkOut.split('\n')[0]);
ok('minting a link writes nothing, so a read-only host can do it', gpt.MAILBOX.m.size === kvBefore, `${kvBefore} -> ${gpt.MAILBOX.m.size}`);
const toolList = (await (await worker.fetch(new Request(`${O}/mcp?token=${gptToken}`, { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/list' }) }), gpt)).json()).result.tools;
const linkTools = toolList.filter(t => t.name === 'claim_link' || t.name === 'respond_intro_link');
ok('both link tools declare themselves read-only, which is what the host gates on', linkTools.length === 2 && linkTools.every(t => t.annotations && t.annotations.readOnlyHint === true));

const showUrl = new URL(claimUrl);
page = await (await worker.fetch(new Request(claimUrl), gpt)).text();
ok('opening the link shows the draft, not a form to fill', /lea@mazel/.test(page) && /Lea builds biotech teams/.test(page) && /biotech-recruiting/.test(page) && !/<input type="text"|<textarea/.test(page));
ok('it shows a held need as held, and says nothing is public yet', /quiet-cofounder-search/.test(page) && /matched-only/.test(page) && /Nothing is public until you press/.test(page));
ok('it never puts the mailbox key on the page', !page.includes(gptToken));
ok('looking is not saving', !(await isClaimedNow(gpt)), 'claimed too early');

const post = (url, body) => worker.fetch(new Request(url, { method:'POST', headers:{'content-type':'application/x-www-form-urlencoded'}, body }), gpt);
const form = `d=${encodeURIComponent(showUrl.searchParams.get('d'))}&e=${encodeURIComponent(showUrl.searchParams.get('e'))}&s=${encodeURIComponent(showUrl.searchParams.get('s'))}`;
page = await (await post(O + '/claim', form)).text();
ok('pressing the button claims the portal', /portal is open/i.test(page) && /lea@mazel/.test(page), page.slice(0,80));
const gptCard = JSON.parse(await (await get(gpt, '/card')).text());
const gptHaah = gptCard.capabilities.extensions.find(e => /haah/.test(e.uri)).params;
ok('the card it wrote is the card the agent drafted', gptHaah.handle === 'lea@mazel' && gptHaah.have.includes('lab-ops') && gptHaah.need.includes('seed-investors'));
ok('a held need never reaches the public card', !JSON.stringify(gptHaah).includes('quiet-cofounder-search'), JSON.stringify(gptHaah.need));
ok('the setup link stops once the portal is claimed this way', !/mcp\?token=[a-f0-9]{16,}/.test(await (await get(gpt, '/')).text()));
// The key the welcome page showed is a setup key and dies at the claim; the page hands over the
// real one, which is a different string.
const realToken = (page.match(/mcp\?token=([a-f0-9]+)/) || [])[1];
ok('the claim hands over the real connector link, and it is not the setup key', !!realToken && realToken !== gptToken);
ok('the setup key stops working the moment the portal is claimed', /unauthor/i.test(await mcp(gpt, 'my_card', {}, gptToken)));
ok('the real key works', JSON.parse(await mcp(gpt, 'my_card', {}, realToken)).handle === 'lea@mazel');
ok('claim_link refuses once there is an owner', /already belongs to/.test(await mcp(gpt, 'claim_link', { handle:'someone@mazel' }, realToken)));

// A link nobody could have minted, and one that has aged out, are both refused.
const other = { HANDLE:'you@mazel', PERSONA:'', NEED:'', HAVE:'', MAILBOX: mkKV(), RELAY_URL:'https://relay.test', PORTAL_ORIGIN: O };
ok('a link from another portal is refused', /not made by this portal/.test(await (await worker.fetch(new Request(claimUrl), other)).text()));
ok('a tampered draft is refused', /not made by this portal/.test(await (await worker.fetch(new Request(claimUrl.replace(/d=[^&]+/, 'd=' + Buffer.from('{"handle":"attacker@mazel"}').toString('base64url'))), other)).text()));
const stale = claimUrl.replace(/e=\d+/, 'e=' + (Date.now() - 1000));
ok('an aged-out link is refused', /expired/.test(await (await worker.fetch(new Request(stale), gpt)).text()));

// The same rule for answering an intro.
const peerSeen = [];
const netlessFetch = globalThis.fetch;
globalThis.fetch = async (u, i = {}) => {
  if (String(u).startsWith('https://peer.test')) { peerSeen.push(JSON.parse(i.body)); return new Response(JSON.stringify({ jsonrpc:'2.0', id:'1', result:{ message:{ messageId:'ack', role:'ROLE_AGENT', parts:[{ text:'ack' }] } } }), { headers:{'content-type':'application/json'} }); }
  return netlessFetch(u, i);
};
gpt.MAILBOX.m.set('intro:i1', JSON.stringify({ intro_id:'i1', direction:'received', state:'proposed', why:'You have lab-ops; sam@mazel needs it.', path:['sam@mazel'], from:{ handle:'sam@mazel', rpc:'https://peer.test/a2a' }, created:new Date().toISOString() }));
const introOut = await mcp(gpt, 'respond_intro_link', { intro_id:'i1', decision:'accepted', note:'Tuesdays work.' }, realToken);
const introUrl = (introOut.match(/https:\/\/\S+\/intro\?\S+/) || [])[0];
ok('respond_intro_link hands back a link for the answer', !!introUrl, introOut.split('\n')[0]);
page = await (await worker.fetch(new Request(introUrl), gpt)).text();
ok('it shows who, the why and the path before anything is sent', /sam@mazel/.test(page) && /needs it/.test(page) && /Nothing is sent until you press/.test(page));
ok('looking does not answer', peerSeen.length === 0 && JSON.parse(gpt.MAILBOX.m.get('intro:i1')).state === 'proposed');
const iu = new URL(introUrl);
page = await (await post(O + '/intro', `d=${encodeURIComponent(iu.searchParams.get('d'))}&e=${encodeURIComponent(iu.searchParams.get('e'))}&s=${encodeURIComponent(iu.searchParams.get('s'))}`)).text();
ok('pressing the button sends the answer to the proposer', peerSeen.some(m => (m.params.message.metadata.action || {}).type === 'intro.respond' && m.params.message.metadata.action.decision === 'accepted'), JSON.stringify(peerSeen.map(m => (m.params.message.metadata.action||{}).type)));
ok('the note the agent drafted travels with it', peerSeen.some(m => (m.params.message.metadata.action || {}).note === 'Tuesdays work.'));
ok('answering twice is refused', /already/i.test(await (await worker.fetch(new Request(introUrl), gpt)).text()));
globalThis.fetch = netlessFetch;

// 7. The quiet rule rides on the tools that can produce something to say.
const tools = (await (await worker.fetch(new Request(`${O}/mcp?token=${freshReal}`, { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/list' }) }), fresh)).json()).result.tools;
const byName = Object.fromEntries(tools.map(t => [t.name, t.description]));
ok('the quiet rule is on the tools that can speak', ['my_card','check_mailbox','pulse','find'].every(n => /three moments only/.test(byName[n])));
ok('and not on the ones that cannot', !/three moments only/.test(byName['close_thread']));
ok('the three moments are named', /✨ a hit/.test(byName['check_mailbox']) && /🌀 a yes/.test(byName['check_mailbox']) && /travel to strangers/.test(byName['check_mailbox']));
ok('first contact is pointed at from the first tools an agent calls', /FIRST CONTACT/.test(byName['my_card']) && /FIRST CONTACT/.test(byName['check_mailbox']));

globalThis.fetch = realFetch;
console.log(`\ninstall: ${pass} passed, ${fail} failed`);
process.exit(fail?1:0);
