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
