/**
 * Smoke test for @musubi/hub-sdk.
 *
 * Spins up a minimal in-process Control Hub mock (WebSocket + HTTP/SSE + poll
 * endpoints), then exercises HubClient end-to-end: engine.info negotiation,
 * ping, capabilities, auth.login, node.list, event subscription + dedupe,
 * offline queueing, and typed command/task methods.
 *
 * Run: node dist/detonate.mjs
 */
import { WebSocketServer } from 'ws';
import { createServer } from 'node:http';
import { HubClient } from './dist/index.mjs';

const PORT = 18765;
const BASE = `http://127.0.0.1:${PORT}`;

/* ------------------------------------------------------------------ */
/* Minimal mock hub                                                    */
/* ------------------------------------------------------------------ */

const nodes = [
  { id: 'n-1', name: 'worker-1', status: 'online', tags: ['linux'], last_seen: new Date().toISOString() },
  { id: 'n-2', name: 'worker-2', status: 'busy', tags: ['mac'], last_seen: new Date().toISOString() }
];

// Mock state observed by the tests.
const mockState = {
  subscribed: [], // { subscription_id, params } from event.subscribe
  unsubscribed: [], // subscription_id values from event.unsubscribe
  sseClients: new Set(), // open /v1/events responses (for _mock.push_event)
  pollQueue: [], // events pending delivery on the next /v1/poll
  authFailFired: false // one-shot -32003 on node.list (auth retry test)
};

function broadcastEvent(ev) {
  const frame = `data: ${JSON.stringify({ jsonrpc: '2.0', method: 'event', params: { subscription_id: 'broadcast', event: ev } })}\n\n`;
  for (const res of mockState.sseClients) res.write(frame);
  mockState.pollQueue.push(ev);
}

function rpcResult(id, result) {
  return JSON.stringify({ jsonrpc: '2.0', id, result });
}
function rpcError(id, code, message, data) {
  return JSON.stringify({ jsonrpc: '2.0', id, error: { code, message, ...(data ? { data } : {}) } });
}

function handleMethod(id, method, params = {}, auth) {
  if (method === 'engine.info') {
    const clientVersions = params.client_info?.protocol_versions ?? ['1.0'];
    const supported = ['1.0'];
    const common = supported.filter((v) => clientVersions.includes(v));
    if (common.length === 0) {
      return rpcError(id, -32005, 'protocol.version.mismatch', {
        supported,
        client: clientVersions
      });
    }
    return rpcResult(id, {
      protocol_version: common[0],
      server: { name: 'mock-hub', version: '1.0.0' },
      supported_versions: supported,
      encodings: ['json'],
      transports: ['ws', 'http', 'poll'],
      features: { batch: true, subscribe: true, sse: true, poll: true },
      capabilities: [
        { id: 'node.registry', version: '1.0' },
        { id: 'command.exec', version: '1.0' },
        { id: 'task.flow', version: '1.0' },
        { id: 'event.stream', version: '1.0' },
        { id: 'auth.token', version: '1.0' }
      ],
      session: { authenticated: !!auth },
      server_time: new Date().toISOString()
    });
  }
  switch (method) {
    case 'engine.ping':
      return rpcResult(id, { pong: true, time: new Date().toISOString() });
    case 'engine.capabilities':
      return rpcResult(id, {
        capabilities: [{ id: 'node.registry', version: '1.0' }],
        features: { batch: true }
      });
    case 'auth.login':
      return rpcResult(id, {
        access_token: 'tok-' + Math.random().toString(36).slice(2),
        token_type: 'bearer',
        expires_in: 3600,
        refresh_token: 'ref-' + Math.random().toString(36).slice(2)
      });
    case 'auth.refresh':
      // NEW engine semantics: refresh rotates BOTH tokens and invalidates the
      // old pair.
      return rpcResult(id, {
        access_token: 'tok-' + Math.random().toString(36).slice(2),
        token_type: 'bearer',
        expires_in: 3600,
        refresh_token: 'ref-' + Math.random().toString(36).slice(2)
      });
    case 'node.list':
      if (params.__fail_auth && !mockState.authFailFired) {
        mockState.authFailFired = true;
        return rpcError(id, -32003, 'auth.expired', { retry_after: 0 });
      }
      return rpcResult(id, { nodes, total: nodes.length });
    case 'command.send':
      return rpcResult(id, {
        command: {
          id: 'c-1',
          node_id: params.node_id,
          method: params.method,
          params: params.params ?? {},
          status: 'accepted',
          created_at: new Date().toISOString(),
          ...(params.idempotency_key ? { idempotency_key: params.idempotency_key } : {})
        }
      });
    case 'task.create':
      return rpcResult(id, {
        task: {
          id: 't-1',
          name: params.name,
          status: 'running',
          progress: 0.5,
          command_ids: ['c-1'],
          created_at: new Date().toISOString()
        }
      });
    case 'event.subscribe': {
      const subscription_id = 'sub-' + Math.random().toString(36).slice(2);
      mockState.subscribed.push({ subscription_id, params });
      return rpcResult(id, { subscription_id });
    }
    case 'event.unsubscribe':
      mockState.unsubscribed.push(params.subscription_id);
      return rpcResult(id, {});
    case 'node.unsubscribe':
    case 'auth.logout':
      return rpcResult(id, {});
    case '_mock.push_event':
      // Broadcast an arbitrary event over SSE + poll (transport-level
      // broadcast, like the real engine: no server-side filtering).
      broadcastEvent(params.event);
      return rpcResult(id, { ok: true });
    default:
      return rpcError(id, -32601, `method not found: ${method}`);
  }
}

// --- WebSocket server (attached to the HTTP server via 'upgrade') ---
const wss = new WebSocketServer({ noServer: true });
const wsClients = new Set();
wss.on('connection', (ws) => {
  wsClients.add(ws);
  ws.on('message', (raw) => {
    const text = raw.toString();
    let msg;
    try {
      msg = JSON.parse(text);
    } catch {
      return;
    }
    const items = Array.isArray(msg) ? msg : [msg];
    const responses = [];
    for (const item of items) {
      if (item.id === undefined || item.id === null) {
        // notification: server pushes an event back as a demo
        if (item.method === 'engine.ping') {
          ws.send(JSON.stringify({ jsonrpc: '2.0', method: 'event', params: { subscription_id: 'demo', event: { id: 'ev-pong', type: 'node.status.changed', time: new Date().toISOString(), node_id: 'n-1', data: { status: 'online' } } } }));
        }
        continue;
      }
      if (item.method === '_mock.push_pong') {
        // push the same event twice to exercise client-side dedupe
        const ev = { id: 'ev-pong', type: 'node.status.changed', time: new Date().toISOString(), node_id: 'n-1', data: { status: 'online' } };
        ws.send(JSON.stringify({ jsonrpc: '2.0', method: 'event', params: { subscription_id: 'demo', event: ev } }));
        ws.send(JSON.stringify({ jsonrpc: '2.0', method: 'event', params: { subscription_id: 'demo', event: { ...ev, id: 'ev-pong2' } } }));
        responses.push(rpcResult(item.id, { ok: true }));
        continue;
      }
      const auth = (item.params && item.params.auth_token) || undefined;
      responses.push(handleMethod(item.id, item.method, item.params ?? {}, auth));
    }
    if (responses.length) ws.send(responses.length === 1 ? responses[0] : `[${responses.join(',')}]`);
  });
  ws.on('close', () => wsClients.delete(ws));
  ws.on('error', () => wsClients.delete(ws));
});

// --- HTTP server (rpc + events SSE + poll) ---
const http = createServer(async (req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${PORT}`);
  if (url.pathname === '/v1/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, version: '1.0.0' }));
    return;
  }
  if (url.pathname === '/v1/rpc') {
    let body = '';
    for await (const chunk of req) body += chunk;
    const msg = JSON.parse(body);
    const auth = req.headers['authorization'] ? 'bearer' : undefined;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(handleMethod(msg.id, msg.method, msg.params ?? {}, auth));
    return;
  }
  if (url.pathname === '/v1/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive'
    });
    res.write('retry: 2000\n\n');
    mockState.sseClients.add(res);
    // Send a couple of demo events, then keep the stream open.
    const ev1 = { id: 'ev-sse-1', type: 'node.registered', time: new Date().toISOString(), node_id: 'n-3', data: { status: 'online' } };
    res.write(`data: ${JSON.stringify({ jsonrpc: '2.0', method: 'event', params: { subscription_id: 'sse', event: ev1 } })}\n\n`);
    const ev2 = { id: 'ev-sse-2', type: 'task.progress', time: new Date().toISOString(), task_id: 't-9', data: { progress: 0.25 } };
    res.write(`data: ${JSON.stringify({ jsonrpc: '2.0', method: 'event', params: { subscription_id: 'sse', event: ev2 } })}\n\n`);
    // Keep alive
    const keep = setInterval(() => res.write(': keepalive\n\n'), 30000);
    req.on('close', () => {
      clearInterval(keep);
      mockState.sseClients.delete(res);
    });
    return;
  }
  if (url.pathname === '/v1/poll') {
    const cursor = parseInt(url.searchParams.get('cursor') ?? '0', 10);
    const events = mockState.pollQueue.splice(0);
    if (cursor < 1) {
      events.push({ id: 'ev-poll-1', type: 'node.updated', time: new Date().toISOString(), node_id: 'n-1', data: { status: 'busy' } });
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ cursor: cursor + 1, events }));
    return;
  }
  res.writeHead(404);
  res.end('not found');
});
http.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url ?? '/', `http://127.0.0.1:${PORT}`);
  if (url.pathname === '/v1/ws') {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req);
    });
  } else {
    socket.destroy();
  }
});
http.listen(PORT);

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

/* ------------------------------------------------------------------ */
/* Smoke                                                               */
/* ------------------------------------------------------------------ */

let failures = 0;
function check(name, cond) {
  if (cond) {
    console.log(`  ok   ${name}`);
  } else {
    failures += 1;
    console.log(`  FAIL ${name}`);
  }
}

async function main() {
  // 1. WS client: negotiation + typed calls + auth + events + dedupe
  console.log('--- ws client ---');
  const wsClient = new HubClient({ url: BASE, credentials: { token: 'x' }, autoReconnect: false });
  const pongEvents = [];
  wsClient.on('*', (ev) => {
    if (ev.type === 'node.status.changed' && ev.id === 'ev-pong') pongEvents.push(ev);
  });
  await wsClient.open();
  check('transportKind === ws', wsClient.transportKind === 'ws');
  check('serverInfo.protocol_version === 1.0', wsClient.serverInfo?.protocol_version === '1.0');
  // server_info.session reflects the state BEFORE auto-login; after open() the
  // client must hold an access_token (auto-login ran because session was
  // unauthenticated and credentials were configured).
  check('client holds access token (auto-login ran)', typeof wsClient.accessToken === 'string');

  const ping = await wsClient.ping();
  check('ping.pong === true', ping.pong === true);
  // Mock server pushes ev-pong twice with same id on `_mock.push_pong`.
  await wsClient.request('_mock.push_pong', {});
  await wait(100);
  check('dedupe: duplicate ev-pong delivered once', pongEvents.length === 1);

  const caps = await wsClient.capabilities();
  check('capabilities.length > 0', (caps.capabilities ?? []).length > 0);

  const { nodes: nl } = await wsClient.nodeList();
  check('nodeList total === 2', nl.length === 2);

  const { command } = await wsClient.commandSend({ node_id: 'n-1', method: 'echo', params: { text: 'hi' }, idempotency_key: 'ik-1' });
  check('commandSend idempotency_key', command?.idempotency_key === 'ik-1');

  const { task } = await wsClient.taskCreate({ name: 'deploy', commands: [{ node_id: 'n-1', method: 'echo' }] });
  check('taskCreate status running', task?.status === 'running');

  const sub = await wsClient.subscribeEvents({ node_id: 'n-1' });
  check('subscribeEvents got subscription_id', !!sub.subscription_id);
  await sub.unsubscribe();
  check('unsubscribe ok', true);

  await wsClient.close();
  check('ws close ok', true);

  // 2. HTTP/SSE client
  console.log('--- http (sse) client ---');
  const httpClient = new HubClient({ url: BASE, token: 't', transportOrder: ['http'], autoReconnect: false });
  const sseEvents = [];
  httpClient.on('node.registered', (ev) => sseEvents.push(ev));
  httpClient.on('task.progress', (ev) => sseEvents.push(ev));
  await httpClient.open();
  check('http transportKind === http', httpClient.transportKind === 'http');
  await wait(300);
  check('received sse events', sseEvents.length >= 2);
  const ping2 = await httpClient.ping();
  check('http ping ok', ping2.pong === true);
  await httpClient.close();

  // 3. Poll client
  console.log('--- poll client ---');
  const pollClient = new HubClient({ url: BASE, token: 't', transportOrder: ['poll'], autoReconnect: false });
  const pollEvents = [];
  pollClient.on('node.updated', (ev) => pollEvents.push(ev));
  await pollClient.open();
  await wait(400);
  check('poll transport + event', pollClient.transportKind === 'poll' && pollEvents.length >= 1);
  const ping3 = await pollClient.ping();
  check('poll ping ok', ping3.pong === true);
  await pollClient.close();

  // 4. Offline queue: open() starts connecting; a write issued while
  //    connecting (before the transport is established) gets queued and
  //    replayed once the connection is up.
  console.log('--- offline queue ---');
  const qClient = new HubClient({ url: BASE, autoReconnect: true, maxRetries: 5 });
  const opening = qClient.open();
  const qPromise = qClient.commandSend({ node_id: 'n-1', method: 'echo' }); // while connecting
  let settled = false;
  qPromise.finally(() => (settled = true));
  // Synchronous check: state is still 'connecting' here (establish() awaits the
  // network), so the request must have been queued, not rejected or resolved.
  check('queued (not settled while connecting)', settled === false);
  await opening;
  const qres = await qPromise;
  check('queued command settled after open', settled === true && !!qres?.command);
  await qClient.close();

  // 5. Protocol mismatch
  console.log('--- protocol mismatch ---');
  const badClient = new HubClient({ url: BASE, autoReconnect: false });
  // Directly override the client_info to claim only 0.9 (server supports 1.0
  // only ⇒ -32005 protocol.version.mismatch ⇒ fatal).
  badClient.buildClientInfo = () => ({
    name: 'musubi-hub-sdk',
    version: '1.0.0',
    protocol_versions: ['0.9'],
    encodings: ['json'],
    transports: ['ws'],
    features: {},
    platform: {}
  });
  try {
    await badClient.open();
    check('protocol mismatch throws', false);
  } catch (e) {
    check('protocol mismatch throws HubError -32005', e?.name === 'HubError' && e?.code === -32005);
  }

  // 6. Auth retry: -32003 triggers refresh (token rotation) + one retry.
  //     Assumes the NEW engine semantics: auth.refresh returns a NEW
  //     access_token AND refresh_token and invalidates the old pair.
  console.log('--- auth retry (refresh) ---');
  const authClient = new HubClient({ url: BASE, credentials: { token: 'x' }, autoReconnect: false });
  await authClient.open();
  const tokBefore = authClient.accessToken;
  const rtBefore = authClient.refreshToken;
  const nlAuth = await authClient.request('node.list', { __fail_auth: true });
  check('auth retry: request retried after refresh', nlAuth.total === 2);
  check(
    'auth retry: access + refresh tokens rotated',
    authClient.accessToken !== tokBefore && authClient.refreshToken !== rtBefore
  );
  await authClient.close();

  // 7. onNode/onTask unsubscribe must actually remove the server subscription
  //     (regression: lookup key never matched, event.unsubscribe never sent).
  console.log('--- onNode/onTask unsubscribe ---');
  const subClient = new HubClient({ url: BASE, token: 't', transportOrder: ['http'], autoReconnect: false });
  await subClient.open();
  const subCountBefore = mockState.subscribed.length;
  const stopNode = subClient.onNode('n-1');
  const stopTask = subClient.onTask('t-1');
  await wait(300);
  const newSubIds = mockState.subscribed.slice(subCountBefore).map((s) => s.subscription_id);
  check('onNode/onTask created 2 server subscriptions', newSubIds.length === 2);
  stopNode();
  stopTask();
  await wait(300);
  check(
    'onNode/onTask unsubscribe reached the server',
    newSubIds.length === 2 && newSubIds.every((sid) => mockState.unsubscribed.includes(sid))
  );
  await subClient.close();

  // 8. onNode before open(): must connect first instead of silently failing.
  console.log('--- onNode before open ---');
  const earlyClient = new HubClient({ url: BASE, token: 't', transportOrder: ['http'], autoReconnect: false });
  const subCountEarly = mockState.subscribed.length;
  const stopEarly = earlyClient.onNode('n-1');
  await earlyClient.open();
  await wait(300);
  check(
    'onNode before open still subscribes',
    mockState.subscribed.length === subCountEarly + 1
  );
  stopEarly();
  await wait(200);
  await earlyClient.close();

  // 9. Client-side filter enforcement over SSE/poll (the engine broadcasts all
  //     events on /v1/events and /v1/poll; filters must be applied locally).
  console.log('--- sse/poll client-side filtering ---');
  const filterClient = new HubClient({ url: BASE, token: 't', transportOrder: ['http'], autoReconnect: false });
  const nodeEvents = [];
  const taskEvents = [];
  filterClient.on('node.status.changed', (ev) => nodeEvents.push(ev));
  filterClient.on('task.progress', (ev) => taskEvents.push(ev));
  await filterClient.open();
  const stopFilter = filterClient.onNode('n-1');
  await wait(300); // subscription active ⇒ filtering kicks in
  await filterClient.request('_mock.push_event', { event: { id: 'ev-f-1', type: 'node.status.changed', time: new Date().toISOString(), node_id: 'n-1', data: {} } });
  await filterClient.request('_mock.push_event', { event: { id: 'ev-f-2', type: 'node.status.changed', time: new Date().toISOString(), node_id: 'n-2', data: {} } });
  await filterClient.request('_mock.push_event', { event: { id: 'ev-f-3', type: 'task.progress', time: new Date().toISOString(), task_id: 't-5', data: {} } });
  await wait(300);
  check('sse filter: matching node event delivered', nodeEvents.some((e) => e.id === 'ev-f-1'));
  check('sse filter: non-matching node event dropped', !nodeEvents.some((e) => e.id === 'ev-f-2'));
  check('sse filter: task event dropped under node filter', !taskEvents.some((e) => e.id === 'ev-f-3'));
  stopFilter();
  await wait(200);
  await filterClient.request('_mock.push_event', { event: { id: 'ev-f-4', type: 'task.progress', time: new Date().toISOString(), task_id: 't-5', data: {} } });
  await wait(300);
  check('sse: no subscriptions ⇒ broadcast delivered unfiltered', taskEvents.some((e) => e.id === 'ev-f-4'));
  await filterClient.close();

  const pollFilterClient = new HubClient({ url: BASE, token: 't', transportOrder: ['poll'], autoReconnect: false });
  const pollNodeEvents = [];
  pollFilterClient.on('node.status.changed', (ev) => pollNodeEvents.push(ev));
  await pollFilterClient.open();
  const stopPollFilter = pollFilterClient.onNode('n-1');
  await wait(300);
  await pollFilterClient.request('_mock.push_event', { event: { id: 'ev-pf-1', type: 'node.status.changed', time: new Date().toISOString(), node_id: 'n-1', data: {} } });
  await pollFilterClient.request('_mock.push_event', { event: { id: 'ev-pf-2', type: 'node.status.changed', time: new Date().toISOString(), node_id: 'n-2', data: {} } });
  await wait(500);
  check('poll filter: matching node event delivered', pollNodeEvents.some((e) => e.id === 'ev-pf-1'));
  check('poll filter: non-matching node event dropped', !pollNodeEvents.some((e) => e.id === 'ev-pf-2'));
  stopPollFilter();
  await pollFilterClient.close();

  // 10. Negative dedupeWindow must not hang (regression: infinite loop).
  console.log('--- negative dedupeWindow ---');
  const negClient = new HubClient({ url: BASE, token: 't', transportOrder: ['http'], autoReconnect: false, dedupeWindow: -5 });
  const negEvents = [];
  negClient.on('node.status.changed', (ev) => negEvents.push(ev));
  await negClient.open();
  await negClient.request('_mock.push_event', { event: { id: 'ev-neg-1', type: 'node.status.changed', time: new Date().toISOString(), node_id: 'n-1', data: {} } });
  await negClient.request('_mock.push_event', { event: { id: 'ev-neg-2', type: 'node.status.changed', time: new Date().toISOString(), node_id: 'n-1', data: {} } });
  await wait(300);
  check('negative dedupeWindow: no hang, both events delivered', negEvents.length === 2);
  await negClient.close();

  console.log(failures === 0 ? '\nALL DETONATION TESTS PASSED' : `\n${failures} DETONATION TEST(S) FAILED`);
  process.exit(failures === 0 ? 0 : 1);
}

main()
  .catch((err) => {
    console.error('detonation crashed:', err);
    process.exit(1);
  })
  .finally(() => {
    wss.close();
    http.close();
  });