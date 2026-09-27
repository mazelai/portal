// M1.5 live-editable card: KV-backed card, POST /card, update_card with confirm-before-public and tiers.
import worker from '../src/index.js';
import { sendReq, flat } from './a2a-helpers.mjs';
const store = new Map();
const env = {
  HANDLE: 'ariel@mazel', PERSONA: 'Ariel runs Paragon.', INBOX_TOKEN: 'tok',
  NEED: 'tech-advisor-partners,pe-operating-partner-intros,mid-market-cfo-intros,ai-podcast-guests',
  HAVE: 'managed-ai-delivery,ai-help-for-tech-advisors,ai-first-project-scoping,sovereign-ai-design,ai-talk-tracks-for-sellers',
  MAILBOX: { get: async k => store.get(k) ?? null, put: async (k,v) => store.set(k,v), delete: async k => store.delete(k), list: async ({prefix}) => ({ keys: [...store.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})) }) },
};
const O = 'https://mazel.test.workers.dev';
const req = (path, method='GET', body, auth=true) => new Request(O+path, { method, headers: { 'content-type':'application/json', ...(auth?{authorization:'Bearer tok'}:{}) }, body: body?JSON.stringify(body):undefined });
const tool = async (name, args) => JSON.parse(await (await worker.fetch(req('/mcp','POST',{jsonrpc:'2.0',id:1,method:'tools/call',params:{name,arguments:args}}), env)).text()).result.content[0].text;
const card = async () => flat(JSON.parse(await (await worker.fetch(req('/card'), env)).text()));
let pass=0, fail=0; const ok=(l,c,x='')=>{ console.log((c?'PASS ':'FAIL ')+l+(x?'  -> '+String(x).replace(/\n/g,' ').slice(0,110):'')); c?pass++:fail++; };

const c0 = await card();
// A have the person put on their own card carries the owner witness, the weakest kind: public and
// on the relay for now. Naming a tool it was read from is what makes it stand on its own.
ok('seed: card served from vars', c0.handle==='ariel@mazel' && c0.need.length===4 && c0.have.length===5, JSON.stringify(c0.have));
ok('a seeded have carries the owner witness and nothing else', JSON.parse(await tool('my_card',{})).ownerAttestedOnly.length === 5);
ok('naming a tool corroborates it', (await tool('update_card',{add_have:'managed-ai-delivery', witnesses:'hubspot,gmail', confirmed:true})).startsWith('Written') && !JSON.parse(await tool('my_card',{})).ownerAttestedOnly.includes('managed-ai-delivery'));
ok('seed written to KV', store.has('config:card'));
ok('public add without confirmed is refused', (await tool('update_card',{add_need:'Fractional CFO'})).startsWith('Not written'));
ok('public add with confirmed writes', (await tool('update_card',{add_need:'Fractional CFO', confirmed:true})).startsWith('Written'));
ok('card shows normalized tag', (await card()).need.includes('fractional-cfo'));
ok('remove need', (await tool('update_card',{remove_need:'ai-podcast-guests', confirmed:true})).startsWith('Written') && !(await card()).need.includes('ai-podcast-guests'));
ok('directed need needs no confirm and never appears publicly', (await tool('update_card',{add_need:'foundry-data-engineers', need_visibility:'directed'})).startsWith('Written') && !(await card()).need.includes('foundry-data-engineers'));
ok('owner view shows held needs', JSON.parse(await tool('my_card',{})).heldNeeds.some(n=>n.tag==='foundry-data-engineers'));
ok('persona edit', (await tool('update_card',{persona:'New persona.', confirmed:true})).startsWith('Written') && (await card()).description==='New persona.');
ok('POST /card without token is 401', (await worker.fetch(req('/card','POST',{add_have:'x'},false), env)).status===401);
ok('POST /card add_have confirmed', (await worker.fetch(req('/card','POST',{add_have:'channel-partner-intros', witnesses:['hubspot'], confirmed:true}), env)).status===200 && (await card()).have.includes('channel-partner-intros'));
for (const t of ['a','b','c']) await tool('update_card',{add_need:t, confirmed:true});
ok('7th public need hits the cap', /already has 6 public tags/.test(await tool('update_card',{add_need:'d', confirmed:true})));
ok('gloss on a have appears publicly', (await tool('update_card',{gloss_tag:'managed-ai-delivery', gloss_text:'A team that runs AI in your cloud', confirmed:true})).startsWith('Written') && /cloud/.test((await card()).glosses['managed-ai-delivery']));
ok('a2a ack uses the stored handle', /ariel@mazel/.test(await (await worker.fetch(req('/a2a','POST', sendReq(1,'hi'), false), env)).text()));
console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail?1:0);
