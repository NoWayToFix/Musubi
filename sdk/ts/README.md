# @musubi/hub-sdk

TypeScript SDK for the [Control Hub](PROTOCOL.md) protocol — JSON-RPC 2.0 over
WebSocket (primary), HTTP POST + SSE (fallback), and long polling (last resort).

Isomorphic: works in browsers, Node.js (≥18), Deno, Bun, and Electron (≥22).

## Install & build

```bash
cd sdk/ts
npm install          # devDeps: typescript ^5, tsup ^8
npm run build        # tsup → dist/index.mjs (ESM), index.cjs (CJS), index.global.js (IIFE) + .d.ts
npm run typecheck    # tsc --noEmit
```

Optional runtime dependency for old Node (18/20) / Electron (<22) WebSocket
support: `npm install ws` (the SDK falls back to it automatically).

## Usage

### ESM

```js
import { HubClient } from '@musubi/hub-sdk';

const client = new HubClient({ url: 'http://localhost:8080', token: 'secret' });
client.on('node.updated', (ev) => console.log(ev));
await client.open();
const { nodes } = await client.nodeList();
await client.close();
```

### CJS

```js
const { HubClient } = require('@musubi/hub-sdk');
const client = new HubClient({ url: 'http://localhost:8080' });
client.open().then(() => client.ping()).then(console.log);
```

### Browser (IIFE)

```html
<script src="dist/index.global.js"></script>
<script>
  const client = new Hub.HubClient({ url: 'http://localhost:8080' });
  client.open().then(() => client.capabilities()).then(console.log);
</script>
```

## Client options

| option | default | description |
|---|---|---|
| `url` | `http://localhost:8080` | base server URL |
| `token` | — | static bearer token |
| `tokenProvider` | — | dynamic token fn (`() => string \| Promise<string>`) |
| `credentials` | — | `{token}` / `{username,password}` / `{}` for auto-login |
| `transportOrder` | `['ws','http','poll']` | transport preference (ws → sse → poll) |
| `autoReconnect` | `true` | exponential backoff (base 1s, ×2, max 30s, ±20% jitter) |
| `maxRetries` | `Infinity` | reconnect attempts before giving up |
| `queueOffline` | `true` | queue writes offline (FIFO, cap 1000; engine.\*/auth.\* excluded) |
| `requestTimeoutMs` | `30000` | per-request timeout |
| `pingIntervalMs` | `30000` | heartbeat (WS ping / engine.ping) |
| `dedupeWindow` | `5000` | event.id dedupe window |
| `batch` | `false` | coalesce WS requests into JSON-RPC batch frames |
| `proxy`/`ca`/`insecure` | — | passed to transports (Node fetch honors proxy/ca via undici dispatcher; set `NODE_USE_ENV_PROXY` or custom `Dispatcher` — see note below) |

> **proxy/ca note**: the SDK threads `proxy`/`ca`/`insecure` through to
> transports but does not depend on `undici` itself. In Node you can attach a
> custom dispatcher by monkey-patching `globalThis.fetch` (e.g.
> `fetch = undici.fetch.bind(undefined, undefined, { dispatcher })`) or by
> using env vars (`HTTP_PROXY`/`HTTPS_PROXY` + `NODE_TLS_REJECT_UNAUTHORIZED=0`).
> Self-signed certs: provide the CA PEM via the `ca` option (transports pass it
> through; Node fetch needs a custom dispatcher with `ca`).

## Transport & runtime matrix

| transport | endpoint | browser | Node ≥21 | Node 18/20 | Deno/Bun | Electron ≥22 |
|---|---|---|---|---|---|---|
| ws | `/v1/ws` | `globalThis.WebSocket` | native | `ws` pkg (auto) | native | `ws` pkg (main) |
| sse (http) | `/v1/events?token=` | `EventSource` | fetch stream | fetch stream | fetch stream | fetch stream |
| poll | `/v1/poll?cursor&timeout&token` | fetch | fetch | fetch | fetch | fetch |

Fallback order is `ws → sse → poll`, chosen by `transportOrder ∩ server_info.transports`.

## API

- `open()` / `close()` — connect & negotiate (`engine.info`), teardown
- `request(method, params, opts?)` — raw JSON-RPC (id auto-increment, timeout,
  optional batch; offline queue; -32003 auth-expired auto refresh + 1 retry)
- `info(clientInfo?)`, `ping()`, `capabilities()`
- `login()` / `refresh()` / `logout()`
- `nodeList(filter?, limit?, offset?)`, `nodeGet(id)`, `nodeSubscribe(id?)`,
  `nodeUnsubscribe(subId)`
- `commandSend(spec)`, `commandBatch(specs, key?)`, `commandCancel(id)`
- `taskCreate(spec)`, `taskGet(id)`, `taskList(filter?, limit?, offset?)`,
  `taskCancel(id)`, `taskSubscribe(id?)`
- `eventSubscribe(filters?)` → `Subscription`, `eventUnsubscribe(subId)`
- `subscribeEvents(filters?)` → `Subscription` (auto-restored after reconnect),
  `onNode(nodeId?)`, `onTask(taskId?)`
- Events: `client.on(type | '*', handler)` / `off(type, handler)`

## Electron

Main process:

```js
const { app, ipcMain, BrowserWindow } = require('electron');
const { HubClient, createMainBridge } = require('@musubi/hub-sdk');
const client = new HubClient({ url: 'ws://localhost:8080' });
const bridge = createMainBridge(client);
ipcMain.handle('hub:invoke', (_e, method, params) => bridge.invoke(method, params));
bridge.onEvent((ev) => mainWindow.webContents.send('hub:event', ev));
app.on('quit', () => bridge.close());
```

Preload:

```js
const { createPreloadAPI } = require('@musubi/hub-sdk');
const api = createPreloadAPI();
contextBridge.exposeInMainWorld('hub', {
  invoke: api.invoke,
  onEvent: (cb) => api.onEvent(cb)
});
```

## Compatibility

Per PROTOCOL.md §8: unknown response/event fields and unknown event types are
ignored; all SDK types are optional-agnostic; errors are surfaced as `HubError`
(`{code, message, data?}`).