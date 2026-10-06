# Electron example — Control Hub

A minimal Electron app that consumes the **TypeScript SDK build output**
(`sdk/ts/dist/index.cjs`) from the main process and exposes the hub to a
sandboxed renderer over IPC.

Plain CommonJS JavaScript — **no build step** for the example itself. The only
build that is required is the one for `sdk/ts`.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│ main.js (Electron main / Node.js runtime)                           │
│                                                                     │
│  const { HubClient } = require('../../../sdk/ts/dist/index.cjs')    │
│  client = new HubClient({ url, token, transportOrder: [...] })      │
│  await client.open()                                                │
│  client.on('*', ev => webContents.send('hub:event', ev))            │
│                                                                     │
│  ipcMain.handle('hub:invoke', (_e, method, params) =>               │
│      client.request(method, params))                                │
└──────────▲───────────────────────────────▲──────────────────────────┘
           │ ipcRenderer.invoke            │ webContents.send
           ▼                               ▼
┌──────────────────────────────┐  ┌───────────────────────────────────┐
│ preload.js (contextBridge)   │  │ renderer/renderer.js (sandboxed)  │
│ window.hub = {               │  │ window.hub.invoke('command.send', │
│   invoke(m, p), onEvent(cb)  │  │   { node_id, method, params })    │
│   onStatus(cb) }             │  │ window.hub.onEvent(renderLog)     │
└──────────────────────────────┘  └───────────────────────────────────┘
```

Main ↔ preload ↔ renderer:

1. The renderer calls `window.hub.invoke(method, params)` (exposed by
   `preload.js` via `contextBridge`).
2. `preload.js` forwards it as `ipcRenderer.invoke('hub:invoke', ...)`.
3. `main.js`'s `ipcMain.handle` runs `client.request(method, params)` against
   the hub and returns the result (or throws the `HubError`).
4. The hub pushes events → `client.on('*', ...)` → `webContents.send('hub:event', ev)`
   → `preload` re-exposes `onEvent(cb)` → the renderer renders logs/progress.

## Run

```bash
# 1. Build the TS SDK (produces sdk/ts/dist/index.cjs)
cd ../../sdk/ts
npm install
npm run build

# 2. Start the hub engine (from the repo root; see engine/ docs)
#    e.g. ./engine/hubd or `go run ./engine/cmd/hubd -listen :8080`
#    and register at least one node so the table has entries.

# 3. Install Electron and start the example
cd ../../examples/electron
npm install          # installs electron (devDependency)
npm start            # electron .

# optional: point at a non-default hub
HUB_URL=http://127.0.0.1:8080 npm start
```

## Minimum Electron version

The example's `package.json` declares `electron ^28`. Everything it uses
(`contextBridge`, `ipcMain.handle`, `ipcRenderer.invoke`, preload) has been
stable since **Electron 22**, so you can lower the version if needed.

The TS SDK itself requires Node ≥ 18. The Electron main process is a Node
runtime, so the SDK automatically uses its Node WebSocket implementation — no
browser WebSocket differences apply.

> If the SDK's WebSocket transport is unavailable (e.g. `ws` dependency was
> not installed), the SDK falls back to its HTTP/SSE/poll transports — the
> example keeps working.

## What the example exercises

- `engine.info` (version negotiation) — boot
- `node.list` — nodes table + node selector
- `event.subscribe` — server-side subscription (all events); the renderer also
  subscribes via `client.on('*', ...)` so events are streamed regardless
- `command.send` with `echo` / `fail` / `sleep` methods
- `task.create` (echo on every known node) with live `task.progress` bar

## Files

| file | purpose |
|---|---|
| `main.js` | hub connection, IPC handlers, app lifecycle |
| `preload.js` | `contextBridge` → `window.hub` API |
| `renderer/index.html` | UI: status, nodes, command form, task, event log |
| `renderer/renderer.js` | UI logic: invokes + event rendering |

## Contract

- Protocol: [`docs/PROTOCOL.md`](../../docs/PROTOCOL.md)
- TypeScript SDK: [`sdk/ts`](../../sdk/ts)