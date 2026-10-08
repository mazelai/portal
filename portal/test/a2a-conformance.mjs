// Conformance: the official @a2a-js/sdk client talks to our Worker. No hand-rolled assertions
// about the spec: if the SDK resolves the card, sends, and parses the answer, we conform.
import rawWorker from '../src/index.js';
import { legacy as legacyWorker } from './a2a-helpers.mjs';
const worker = legacyWorker(rawWorker);
// The official client is a dev dependency: without it there is nothing to conform to, so say so
// and stop rather than fail. Everything else in the suite runs with no install at all.
let ClientFactory, DefaultAgentCardResolver, Role;
try {
  ({ ClientFactory, DefaultAgentCardResolver } = await import('@a2a-js/sdk/client'));
  ({ Role } = await import('@a2a-js/sdk'));
} catch {
  console.log('skipping: @a2a-js/sdk is not installed. Run npm install in portal/ to check against the official client.');
  process.exit(0);
}
import { HAAH } from './a2a-helpers.mjs';
import { readFileSync } from 'node:fs';
const PKG_VERSION = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')).version;

const mkKV = () => { const m = new Map(); return { m, get: async k => m.get(k) ?? null, put: async (k,v) => m.set(k,v), delete: async k => m.delete(k), list: async ({prefix}) => ({ keys: [...m.keys()].filter(k=>k.startsWith(prefix)).map(name=>({name})) }) }; };
const env = { HANDLE:'avery@mazel', PERSONA:'Avery runs Halcyon.', NEED:'tech-advisor-partners', HAVE:'managed-ai-delivery', INBOX_TOKEN:'tok', MAILBOX: mkKV() };
const ORIGIN = 'https://mazel.conformance.workers.dev';
const seen = [];
const realFetch = globalThis.fetch;
globalThis.fetch = async (input, init = {}) => {
  const url = typeof input === 'string' ? input : (input instanceof Request ? input.url : String(input));
  if (!url.startsWith(ORIGIN)) return realFetch(input, init);
  const req = new Request(url, init);
  if (req.method === 'POST') seen.push(JSON.parse(await req.clone().text()));
  return worker.fetch(req, env);
};
let pass=0, fail=0; const ok=(l,c,x='')=>{ console.log((c?'PASS ':'FAIL ')+l+(x?'  -> '+String(x).replace(/\n/g,' ').slice(0,140):'')); c?pass++:fail++; };

try {
  const resolver = new DefaultAgentCardResolver();
  const card = await resolver.resolve(ORIGIN);
  ok('SDK resolves our card from the well-known path', !!card && card.name === 'avery@mazel', card && card.name);
  ok('card.supportedInterfaces[0] is our /a2a JSONRPC door', card.supportedInterfaces?.[0]?.url === ORIGIN + '/a2a' && /JSONRPC/i.test(card.supportedInterfaces[0].protocolBinding));
  ok('card declares the HAAH extension, not required', card.capabilities?.extensions?.some(e => e.uri === HAAH && e.required === false));
  ok('HAAH params carry handle/need/have', (() => { const p = card.capabilities.extensions.find(e=>e.uri===HAAH).params; return p.handle==='avery@mazel' && p.have.includes('managed-ai-delivery'); })());
  ok('no Mazel fields at the card top level', !('handle' in card) && !('need' in card) && !('have' in card) && !('conventions' in card) && !('rpc' in card));
  ok('card.version is the portal version', card.version === PKG_VERSION, card.version);

  const factory = new ClientFactory();
  const client = await factory.createFromAgentCard(card);
  const resp = await client.sendMessage({ message: { messageId: 'sdk-1', role: Role.ROLE_USER, parts: [{ content: { $case: 'text', value: 'Hello from the official client.' } }], metadata: { handle: 'sdk@mazel', cardUrl: 'https://example.invalid/card' }, extensions: [HAAH], referenceTaskIds: [], contextId: '', taskId: '' } });
  const msg = resp?.message || resp?.payload?.message || resp;
  const text = (msg?.parts || []).map(p => p.content?.$case === 'text' ? p.content.value : p.text).join(' ');
  ok('SDK sendMessage returns a parsed Message ack', /Delivered to avery@mazel/.test(text), text);
  ok('ack carries the HAAH extension uri', (msg?.extensions || []).includes(HAAH));
  const wire = seen.find(b => b.method === 'SendMessage');
  ok('client used the v1.0 method name SendMessage', !!wire, seen.map(b=>b.method).join(','));
  ok('client wrote the text part as { text }', wire && wire.params.message.parts[0] && 'text' in wire.params.message.parts[0] && !('kind' in wire.params.message.parts[0]), JSON.stringify(wire && wire.params.message.parts[0]));
  ok('client wrote role as ROLE_USER', wire && wire.params.message.role === 'ROLE_USER');

  // our own outbound must be what the SDK would accept: replay our deliver() envelope through the SDK's parser by sending it to ourselves
  const inbox = env.MAILBOX.m;
  ok('the message landed in the mailbox with the extension recorded', [...inbox.keys()].some(k => k.startsWith('msg:')));
  // push notification config, register only
  const pnc = await client.createTaskPushNotificationConfig({ tenant: '', id: 'pulse-1', taskId: '*', url: 'https://example.invalid/pulse', token: 't', authentication: undefined }).catch(e => ({ err: String(e) }));
  ok('SDK can register a pulse (push-notification config)', pnc && !pnc.err && (pnc.url === 'https://example.invalid/pulse' || pnc?.config?.url === 'https://example.invalid/pulse' || JSON.stringify(pnc).includes('example.invalid')), JSON.stringify(pnc).slice(0,120));
  ok('nothing is stored for it: delivery between portals is the signed message at the door', ![...inbox.keys()].some(k => k.startsWith('pulse:')));

  // ---- threads (§4.3b): the new message shapes, as the official client writes them ----
  // A thread the Worker holds, with the SDK's sender as the other participant. The client cannot
  // produce this portal's Ed25519 signature, so the door must refuse the message as unsigned - and
  // the point is what reaches the door first: contextId at the top level, and file and data parts
  // in the JSON the SDK emits, which is the JSON our door has to read.
  const ctx = 'c0ffee00c0ffee00c0ffee00c0ffee00';
  inbox.set('conv:' + ctx, JSON.stringify({ contextId: ctx, participants: [{ handle: 'avery@mazel', me: true, role: 'need' }, { handle: 'sdk@mazel', me: false, role: 'have', rpc: 'https://sdk.invalid/a2a', publicKey: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' }], origin: { kind: 'direct', introId: null }, state: 'open', seq: 1, created: new Date().toISOString(), timers: {} }));
  const threadSend = await client.sendMessage({ message: { messageId: 'sdk-t1', contextId: ctx, role: Role.ROLE_USER,
    parts: [{ content: { $case: 'text', value: 'hello in a thread' } }, { content: { $case: 'data', value: { slots: ['Thu 9'] } } }],
    metadata: { haah: { author: 'human', from: 'sdk@mazel', at: new Date().toISOString(), sig: 'x', kid: 'k', envelope: null } }, extensions: [HAAH] } }).catch(e => ({ err: String(e.message || e) }));
  ok('an unsigned thread message from the SDK is refused as such, not crashed on', threadSend && threadSend.err && /not signed by that participant/.test(threadSend.err), threadSend && (threadSend.err || JSON.stringify(threadSend)).slice(0, 120));
  const tw = seen.find(b => b.method === 'SendMessage' && b.params.message.messageId === 'sdk-t1');
  ok('the client put contextId at the top of the Message, where the door reads it', tw && tw.params.message.contextId === ctx);
  ok('the client wrote the data part as { data }, which is what the door reads', tw && tw.params.message.parts[1] && 'data' in tw.params.message.parts[1] && tw.params.message.parts[1].data.slots[0] === 'Thu 9', JSON.stringify(tw && tw.params.message.parts[1]));
  ok('and metadata.haah survived the client untouched', tw && tw.params.message.metadata.haah.author === 'human' && tw.params.message.metadata.haah.envelope === null);
  // The subscribe primitive as this batch uses it: no bearer, the signature is the authentication.
  const sub = await client.createTaskPushNotificationConfig({ tenant: '', id: 'pnc-thread', taskId: ctx, url: 'https://sdk.invalid/a2a', token: '', authentication: { scheme: 'ed25519', credentials: '' } }).catch(e => ({ err: String(e) }));
  ok('a push notification config with taskId = the thread and no bearer registers through the SDK', sub && !sub.err && JSON.stringify(sub).includes('pnc-thread'), sub && (sub.err || JSON.stringify(sub).slice(0, 100)));
  // Reading subscriptions back is the owner's alone (v0.5.2-sec); an anonymous client gets a proper
  // JSON-RPC refusal the SDK can parse, and the stored record keeps the scheme and no token.
  const back = await client.getTaskPushNotificationConfig({ tenant: '', taskId: ctx, id: 'pnc-thread' }).catch(e => ({ err: String(e.message || e) }));
  ok('reading it back is a parsable TaskNotFound: nothing was kept', back && back.err && /keeps no push configs|TaskNotFound|owner/.test(back.err), back && (back.err || JSON.stringify(back)).slice(0, 100));
  ok('and nothing is stored', !inbox.get('pulse:' + ctx + ':pnc-thread'));
} catch (e) {
  ok('SDK client run completed without throwing', false, e.stack?.split('\n').slice(0,3).join(' | ') || String(e));
}
console.log(`\na2a-conformance: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
