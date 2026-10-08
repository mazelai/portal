// Shared by the suites: build an A2A v1.0 SendMessage request and read a Mazel card out of a v1.0 AgentCard.
export const HAAH = 'https://mazel.ai/ext/haah/v1';
export const sendReq = (id, text, { handle, cardUrl, messageId, action } = {}) => ({
  jsonrpc: '2.0', id, method: 'SendMessage',
  params: { message: { messageId: messageId || ('m-' + Math.random().toString(16).slice(2)), contextId: '', taskId: '', role: 'ROLE_USER',
    parts: [{ text }], metadata: { ...(handle ? { handle } : {}), ...(cardUrl ? { cardUrl } : {}), ...(action ? { action } : {}) }, extensions: [HAAH], referenceTaskIds: [] } },
});
// Flatten a v1.0 AgentCard into the Mazel view the old tests asserted on.
export const flat = (card) => {
  const ext = ((card.capabilities && card.capabilities.extensions) || []).find((e) => e.uri === HAAH) || { params: {} };
  const p = ext.params || {};
  const iface = ((card.supportedInterfaces) || [])[0] || {};
  return { handle: p.handle, description: card.description, url: p.cardUrl, rpc: iface.url, need: p.need || [], have: p.have || [], glosses: p.glosses, version: card.version, ext };
};
// The suites were written against the fifty-eight-name surface. The portal now answers twenty-seven
// (nineteen listed, eight hidden); the old names are reached by action on the merged tools. This
// wraps a Worker so a test's tools/call under an old name lands on the merged tool it became - the
// suites keep their sentences, the portal keeps its surface.
const LEGACY = {
  add_known_card: ['cards', 'add'], list_known_cards: ['cards', 'list'], remove_known_card: ['cards', 'remove'], resolve_handle: ['cards', 'add'],
  note_ghost: ['contacts', 'note'], list_ghosts: ['contacts', 'list'], forget_ghost: ['contacts', 'forget'], link_ghost: ['contacts', 'link'], invite_text: ['contacts', 'invite_text'],
  tribe_create: ['tribe', 'create'], tribe_invite: ['tribe', 'invite'], tribe_join: ['tribe', 'join'], tribe_leave: ['tribe', 'leave'], tribe_remove: ['tribe', 'remove'], tribe_status: ['tribe', 'status'], list_queue: ['tribe', 'queue'], clear_queue: ['tribe', 'clear_queue'],
  thread_close: ['thread_manage', 'close'], thread_block: ['thread_manage', 'block'], thread_delete: ['thread_manage', 'delete'], thread_export: ['thread_manage', 'export'], close_thread: ['thread_manage', 'close_need'], reopen_thread: ['thread_manage', 'reopen_need'],
};
export const legacy = (worker) => ({
  ...worker,
  scheduled: worker.scheduled ? worker.scheduled.bind(worker) : undefined,
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (!url.pathname.startsWith('/mcp') || request.method !== 'POST') return worker.fetch(request, env, ctx);
    let body; try { body = JSON.parse(await request.clone().text()); } catch { return worker.fetch(request, env, ctx); }
    const p = body && body.method === 'tools/call' && body.params ? body.params : null;
    if (!p) return worker.fetch(request, env, ctx);
    const name = p.name, args = p.arguments || {};
    let after = null;
    if (LEGACY[name]) { p.name = LEGACY[name][0]; p.arguments = { action: LEGACY[name][1], ...args }; }
    else if (name === 'list_threads') { p.name = 'thread_list'; p.arguments = { kind: 'needs', ...args }; }
    else if (name === 'clear_messages') { p.name = 'check_mailbox'; p.arguments = { clear: args.keys || [], force: args.force }; }
    else if (name === 'my_identity') { p.name = 'my_card'; p.arguments = {}; after = (text) => JSON.stringify(JSON.parse(text).identity, null, 2); }
    else return worker.fetch(request, env, ctx);
    const req = new Request(request.url, { method: 'POST', headers: request.headers, body: JSON.stringify(body) });
    const res = await worker.fetch(req, env, ctx);
    if (!after) return res;
    const j = await res.json();
    if (j.result && !j.result.isError && j.result.content && j.result.content[0]) j.result.content[0].text = after(j.result.content[0].text);
    return new Response(JSON.stringify(j), { status: res.status, headers: { 'content-type': 'application/json' } });
  },
});
export const ackText = (resp) => { const r = resp.result && (resp.result.message || resp.result.task || resp.result); return ((r && r.parts) || []).map((p) => p.text).join(' '); };

// Test fixture: a memory file with witnesses already recorded, so a suite that needs a have on the
// open card does not have to walk the whole corroboration flow to get one.
export const memoryDoc = ({ handle, persona = '', have = [], need = [], witnesses = ['fixture'] }) => [
  `# Mazel memory - ${handle}`, '',
  '## Persona', `- [public] ${persona}`, '',
  '## Have',
  ...have.map(h => typeof h === 'string'
    ? `- [public] ${h} (witnesses: ${witnesses.join(', ')})`
    : `- [${h.tier || 'public'}] ${h.tag}${h.gloss ? ` - ${h.gloss}` : ''}${(h.witnesses || witnesses).length ? ` (witnesses: ${(h.witnesses || witnesses).join(', ')})` : ''}`),
  '',
  '## Need',
  ...need.map(n => typeof n === 'string' ? `- [public] ${n}` : `- [${n.tier || 'public'}] ${n.tag}${n.gloss ? ` - ${n.gloss}` : ''}`),
  '',
].join('\n');
