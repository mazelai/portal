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
