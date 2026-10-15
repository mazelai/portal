// Install: a portal nobody has claimed yet hands out its own setup link, the first conversation
// claims it with one call, and from then on the link is gone and the handle is fixed.
import rawWorker from '../src/index.js';
import { legacy as legacyWorker } from './a2a-helpers.mjs';
const worker = legacyWorker(rawWorker);
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
const unconfirmed = await mcp(fresh, 'update_card', { handle:'lea@mazel', persona:'Lea builds biotech teams.', add_have:['biotech-recruiting','lab-ops'], witnesses:['hubspot','gmail'], add_need:'seed-investors' }, token);
ok('the draft is not written without a yes', /Not written/.test(unconfirmed), unconfirmed.slice(0,60));
const claimed = await mcp(fresh, 'update_card', { handle:'lea@mazel', persona:'Lea builds biotech teams.', add_have:['biotech-recruiting','lab-ops'], witnesses:['hubspot','gmail'], add_need:'seed-investors', confirmed:true }, token);
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
const old = { HANDLE:'avery@mazel', PERSONA:'p', NEED:'', HAVE:'managed-ai-delivery', INBOX_TOKEN:'ta', MAILBOX: mkKV(), RELAY_URL:'https://relay.test' };
page = await (await get(old, '/')).text();
ok('a portal deployed with a handle is claimed from the start', /avery@mazel has a Mazel portal here/.test(page) && !/token/.test(page));

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
const pulsed = await mcp(solo, 'pulse', {}, 'ts');
ok('a portal with no relay still pulses, on the carriers it has, and claims nothing it did not do', !/^Error/.test(pulsed) && /, no relay/.test(pulsed) && !/subscription/.test(pulsed), pulsed.split('\n')[0]);
ok('and says plainly that name@mazel cannot be resolved without one', /has no relay/.test(await mcp(solo, 'resolve_handle', { handle:'lea@mazel' }, 'ts')));

// 6d. Signed links: what a read-only connector can still do. The agent drafts, the person's own
// browser writes. Nothing is stored to mint a link, so minting one is genuinely a read.
const gpt = { HANDLE:'you@mazel', PERSONA:'', NEED:'', HAVE:'', MAILBOX: mkKV(), RELAY_URL:'https://relay.test', PORTAL_ORIGIN: O };
await get(gpt, '/');
const gptToken = (await (await get(gpt, '/')).text()).match(/token=([a-f0-9]+)/)[1];
await get(gpt, '/card');   // the signing key is minted lazily; mint it before measuring writes
const kvBefore = gpt.MAILBOX.m.size;
const linkOut = await mcp(gpt, 'claim_link', { handle:'lea@mazel', persona:'Lea builds biotech teams.', have:['biotech-recruiting','lab-ops'], witnesses:['hubspot','gmail'], need:'seed-investors', held_need:'quiet-cofounder-search' }, gptToken);
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
{ const sid = [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(['known','sam@mazel'].join('|'))))].map(b => b.toString(16).padStart(2, '0')).join('').slice(0, 32); gpt.MAILBOX.m.set('known:' + sid, JSON.stringify({ id: sid, url:'https://peer.test/card', handle:'sam@mazel', rpc:'https://peer.test/a2a', description:'', need:[], have:[], glosses:{}, publicKey:null, tier:'tribe', tierByHand:'tribe', addedAt:new Date().toISOString(), fetchedAt:new Date().toISOString() })); }   // the door is read from the card this portal holds
gpt.MAILBOX.m.set('intro:i1', JSON.stringify({ intro_id:'i1', direction:'received', state:'proposed', why:'You have lab-ops; sam@mazel needs it.', path:['sam@mazel'], from:{ handle:'sam@mazel', rpc:'https://peer.test/a2a' }, created:new Date().toISOString() }));
const introOut = await mcp(gpt, 'respond_intro_link', { intro_id:'i1', decision:'accepted', note:'Tuesdays work.' }, realToken);
const introUrl = (introOut.match(/https:\/\/\S+\/intro\?\S+/) || [])[0];
ok('respond_intro_link hands back a link for the answer', !!introUrl, introOut.split('\n')[0]);
page = await (await worker.fetch(new Request(introUrl), gpt)).text();
ok('it shows who, the why and the path before anything is sent', /sam@mazel/.test(page) && /needs it/.test(page) && /Nothing is sent until you press/.test(page));
ok('looking does not answer', peerSeen.length === 0 && JSON.parse(gpt.MAILBOX.m.get('intro:i1')).state === 'proposed');
const iu = new URL(introUrl);
page = await (await post(O + '/intro', `d=${encodeURIComponent(iu.searchParams.get('d'))}&e=${encodeURIComponent(iu.searchParams.get('e'))}&s=${encodeURIComponent(iu.searchParams.get('s'))}`)).text();
ok('pressing the button sends the answer to the proposer', peerSeen.some(m => ((m.params.message || {metadata:{}}).metadata.action || {}).type === 'intro.respond' && (m.params.message || {metadata:{}}).metadata.action.decision === 'accepted'), JSON.stringify(peerSeen.map(m => ((m.params.message || {metadata:{}}).metadata.action||{}).type)));
ok('the note the agent drafted travels with it', peerSeen.some(m => ((m.params.message || {metadata:{}}).metadata.action || {}).note === 'Tuesdays work.'));
ok('answering twice is refused', /already/i.test(await (await worker.fetch(new Request(introUrl), gpt)).text()));
globalThis.fetch = netlessFetch;

// 7. The quiet rule rides on the tools that can produce something to say.
const tools = (await (await worker.fetch(new Request(`${O}/mcp?token=${freshReal}`, { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/list' }) }), fresh)).json()).result.tools;
const hiddenTools = JSON.parse(await mcp(fresh, 'more_tools', {}, freshReal)).tools;
const byName = Object.fromEntries([...tools, ...hiddenTools].map(t => [t.name, t.description]));
ok('the quiet rule is on the tools that can speak, listed or hidden', ['my_card','check_mailbox','pulse','find'].every(n => /three moments only/.test(byName[n])), ['my_card','check_mailbox','pulse','find'].filter(n => !/three moments only/.test(byName[n] || '')).join(','));
ok('and not on the ones that cannot', !/three moments only/.test(byName['close_thread']));
ok('the three moments are named', /✨ a hit/.test(byName['check_mailbox']) && /🌀 a yes/.test(byName['check_mailbox']) && /travel to strangers/.test(byName['check_mailbox']));
ok('first contact is pointed at from the first tools an agent calls', /FIRST CONTACT/.test(byName['my_card']) && /FIRST CONTACT/.test(byName['check_mailbox']));

globalThis.fetch = realFetch;

// ---- The install file describes the box, and offers no ring while the decision is open ----
// Rule from v0.5.8: any batch that changes what a new user sees updates the install file with it.
{
  const fs = await import('node:fs');
  const site = new URL('../../site/', import.meta.url).pathname;
  for (const f of ['install.html', 'install-squarespace.html']) {
    const page = fs.readFileSync(site + f, 'utf8');
    ok(`${f} describes the box and its four kinds`, /Mazel box/.test(page) && /needs what you have/.test(page) && /fits a need of yours/.test(page) && /has replied/.test(page) && /both said yes/.test(page));
    ok(`${f} says where conversations live`, /live on your portal and on the other person/.test(page));
    ok(`${f} offers no notifications while the ring is undecided`, !/notification|push|ring my phone/i.test(page));
  }
  // A portal nobody has claimed yet: this is what a new person's agent actually reads.
  const brandNew = { HANDLE:'', PERSONA:'', NEED:'', HAVE:'', MAILBOX: mkKV(), PORTAL_ORIGIN: O };
  const setup = JSON.parse(await (await worker.fetch(new Request(O + '/mcp', { method:'POST', headers:{'content-type':'application/json', authorization:'Bearer x'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/call', params:{ name:'my_card', arguments:{} } }) }), { ...brandNew, INBOX_TOKEN: 'x' })).text()).result.content[0].text;
  ok('the onboarding document tells the agent what the box is', /Four kinds of thing arrive in their box/.test(setup) && /check_mailbox is that box/.test(setup), setup.slice(0, 60));
  ok('and tells it never to describe a match a tool did not return (lock 12)', /never describe a match, a yes, or a message that a tool did not return/.test(setup));
  const welcome = await (await worker.fetch(new Request(O + '/'), brandNew)).text();
  ok('the welcome page says what will arrive, in the person\'s words', /arrive in your box/.test(welcome) && /both said yes/.test(welcome));
  ok('and asks for no notification permission', !/notification|ring my phone/i.test(welcome));
}

// ---- The listed surface: nineteen tools a new person's agent sees; the rest callable, off the list ----
// Fifty-two tools and the agent picked worse as the list grew (Avery, 2026-09-28). The loop - card,
// need, find, yes, thread, box, outcome - is listed; four merged tools carry an action; more_tools
// shows what is hidden; claim_link and respond_intro_link stay listed for ChatGPT Plus and Pro.
{
  const listed = (await (await worker.fetch(new Request(`${O}/mcp?token=${freshReal}`, { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ jsonrpc:'2.0', id:1, method:'tools/list' }) }), fresh)).json()).result.tools;
  const names = listed.map(t => t.name);
  ok('nineteen tools are listed', names.length === 19, names.join(' '));
  ok('the loop is on the list', ['my_card','update_card','find','propose_intro','respond_intro','check_mailbox','thread_list','thread_read','thread_send','thread_mark','inbox_link','list_intros'].every(n => names.includes(n)));
  ok('the four merged tools and more_tools are on it', ['contacts','tribe','cards','thread_manage','more_tools'].every(n => names.includes(n)));
  ok('the ChatGPT write path stays listed', names.includes('claim_link') && names.includes('respond_intro_link'));
  ok('plumbing and the old names are off it', !['pulse','carriers','rotate_key','send_to_peer','note_ghost','tribe_create','add_known_card','thread_close','thread_block','list_threads'].some(n => names.includes(n)));
  const hidden = JSON.parse(await mcp(fresh, 'more_tools', {}, freshReal));
  ok('more_tools lists what is hidden, all callable by name', hidden.count === 9 && hidden.tools.some(t => t.name === 'pulse') && hidden.tools.some(t => t.name === 'migrate_store') && !hidden.tools.some(t => t.name === 'add_known_card') && /Callable by name/.test(hidden.note), String(hidden.count));
  ok('a hidden tool still answers by its old name', /Pulse|cast|quiet/i.test(await mcp(fresh, 'pulse', {}, freshReal)));
  ok('a merged tool needs an action and says which', /needs action = note \| list \| forget \| link \| route \| heard \| never_ask \| invite_text/.test(await mcp(fresh, 'contacts', {}, freshReal).catch(e => e.message)));
  const noted = await mcp(fresh, 'contacts', { action: 'note', name: 'Pat Lee', have: ['ops'], edge_score: 50 }, freshReal);
  ok('contacts(note) is note_ghost', /^Noted Pat Lee/.test(noted), noted.slice(0, 60));
  ok('contacts(list) is list_ghosts', JSON.parse(await mcp(fresh, 'contacts', { action: 'list' }, freshReal)).people.some(p => p.name === 'Pat Lee'));
  ok('cards(list) is list_known_cards', /No known cards|\[/.test(await mcp(fresh, 'cards', { action: 'list' }, freshReal)));
  ok('thread_list(kind: needs) is the old list_threads', /No threads|thread_id|\[/.test(await mcp(fresh, 'thread_list', { kind: 'needs' }, freshReal)));
  ok('thread_manage(block) is one step, and asks first', /no conversation|Not done/.test(await mcp(fresh, 'thread_manage', { action: 'block', context_id: 'x'.repeat(32) }, freshReal).catch(e => e.message)));
  ok('the merged descriptions carry the untrusted-content rule their parts did', /UNTRUSTED CONTENT/.test(listed.find(t => t.name === 'cards').description) && /UNTRUSTED CONTENT/.test(listed.find(t => t.name === 'contacts').description));
}

// ---- The installer template cannot be used as an update path ----
// A wrangler deploy from a clone binds a NEW empty mailbox: the portal comes back with no card,
// no contacts and no key, and the old namespace is left holding everything, bound to nothing.
{
  const { execFileSync } = await import('node:child_process');
  const fs = await import('node:fs');
  const root = new URL('../', import.meta.url).pathname;
  const cfg = JSON.parse(fs.readFileSync(root + 'wrangler.json', 'utf8'));
  ok('the template still carries no mailbox id, so every install gets its own', !cfg.kv_namespaces[0].id);
  ok('and a build command guards it', /deploy-guard/.test((cfg.build || {}).command || ''), JSON.stringify(cfg.build || {}));
  const fsp = await import('node:fs');
  const os = await import('node:os');
  const path = await import('node:path');
  // A machine with a wrangler login, and a build container without one.
  const withLogin = fsp.mkdtempSync(path.join(os.tmpdir(), 'mz-home-'));
  fsp.mkdirSync(path.join(withLogin, '.wrangler', 'config'), { recursive: true });
  fsp.writeFileSync(path.join(withLogin, '.wrangler', 'config', 'default.toml'), 'oauth_token = "x"\n');
  const container = fsp.mkdtempSync(path.join(os.tmpdir(), 'mz-ci-'));
  const run = (env, home) => { try { execFileSync(process.execPath, [root + 'bin/deploy-guard.mjs'], { env: { PATH: process.env.PATH, HOME: home, ...env }, stdio:['ignore','pipe','pipe'] }); return { code: 0, err: '' }; } catch (e) { return { code: e.status, err: String(e.stderr) }; } };
  const local = run({}, withLogin);
  ok('a deploy from a machine with a wrangler login is refused', local.code === 1, String(local.code));
  ok('and the refusal names the command that does work', /npx create-mazel/.test(local.err) && /empty mailbox/.test(local.err), local.err.split('\n').filter(Boolean)[0] || '');
  ok('Deploy to Cloudflare, which sets a CI variable, is not blocked', run({ WORKERS_CI: '1' }, withLogin).code === 0);
  // The one that matters: Cloudflare renames every CI variable tomorrow. A build container still
  // has no wrangler login, so the button must keep working on that alone.
  ok('and it still is not blocked if Cloudflare renames every CI variable', run({}, container).code === 0, 'a rename would break every new install');
  ok('the guard reasons from a local login, not only from variable names', /localLogin/.test(fsp.readFileSync(root + 'bin/deploy-guard.mjs', 'utf8')));
  fsp.rmSync(withLogin, { recursive: true, force: true });
  fsp.rmSync(container, { recursive: true, force: true });
  const pub = fs.readFileSync(root + 'publish-public.sh', 'utf8');
  ok('the public mirror carries the guard and points at it one level in', /portal\/bin/.test(pub) && /portal\/bin\//.test(pub), '');
}

console.log(`\ninstall: ${pass} passed, ${fail} failed`);
process.exit(fail?1:0);
