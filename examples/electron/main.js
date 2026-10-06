'use strict';

/**
 * Electron main process for the Control Hub example.
 *
 * Consumes the TypeScript SDK build output via a relative require:
 *   require('../../sdk/ts/dist/index.cjs')
 * (run `cd sdk/ts && npm install && npm run build` first — see README.md).
 *
 * IPC layout (main <-> preload <-> renderer):
 *   renderer --invoke('hub:invoke', method, params)--> main --SDK--> hub
 *   hub --SDK events--> main --webContents.send('hub:event', ev)--> renderer
 */

const path = require('path');
const { app, BrowserWindow, ipcMain } = require('electron');

// Relative path to the TS SDK build output (repo root: sdk/ts/dist).
const SDK_PATH = path.join(__dirname, '..', '..', 'sdk', 'ts', 'dist', 'index.cjs');
const { HubClient } = require(SDK_PATH);

// Hub URL: override with HUB_URL env var; default matches the engine default.
const HUB_URL = process.env.HUB_URL || 'http://127.0.0.1:8080';
const HUB_TOKEN = process.env.HUB_TOKEN || '';

let mainWindow = null;
let client = null;

// ---------------------------------------------------------------------------
// Hub connection (main process = Node runtime; SDK auto-picks WS transport)
// ---------------------------------------------------------------------------

async function connectHub() {
  client = new HubClient({
    url: HUB_URL,
    token: HUB_TOKEN,
    transportOrder: ['ws', 'http', 'poll'], // "auto": ws -> sse(http) -> poll
  });

  // Push every hub event to the renderer as 'hub:event'.
  client.on('*', (ev) => {
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.webContents.send('hub:event', ev);
    }
  });

  await client.open(); // negotiate protocol + transports (PROTOCOL.md §3.3/§3.4)
  const info = await client.info({
    name: 'musubi-hub-electron-example',
    version: '1.0.0',
    transports: ['ws', 'http'],
  });
  console.log('[hub] connected:', HUB_URL, 'protocol', info.protocol_version ?? info.protocolVersion);

  // Open a server-side subscription for ALL events, so even events emitted
  // before a renderer handler is registered are still flowing (the SDK also
  // re-subscribes automatically on reconnect).
  try {
    const sub = await client.eventSubscribe({});
    console.log('[hub] subscribed all events:', sub.subscription_id);
  } catch (err) {
    console.warn('[hub] eventSubscribe failed (events still flow via on("*")):', err.message);
  }

  // Tell the renderer we are connected.
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send('hub:status', { connected: true, url: HUB_URL });
  }
}

async function disconnectHub() {
  if (client) {
    try {
      await client.close();
    } catch {
      /* noop */
    }
    client = null;
  }
}

// ---------------------------------------------------------------------------
// IPC handlers (renderer -> main)
// ---------------------------------------------------------------------------

/**
 * Generic JSON-RPC passthrough: renderer calls window.hub.invoke(method, params).
 * `params` may be undefined; unknown/missing fields are tolerated by the SDK.
 */
ipcMain.handle('hub:invoke', async (_event, method, params) => {
  if (!client) throw new Error('hub client is not connected');
  return client.request(method, params || {});
});

// Explicit helpers so the renderer does not need to know RPC details.
ipcMain.handle('hub:info', async () => {
  if (!client) throw new Error('hub client is not connected');
  return client.info();
});
ipcMain.handle('hub:nodes', async () => {
  if (!client) throw new Error('hub client is not connected');
  return client.nodeList({}, 100, 0);
});

// ---------------------------------------------------------------------------
// App lifecycle
// ---------------------------------------------------------------------------

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1100,
    height: 800,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });
  mainWindow.loadFile(path.join(__dirname, 'renderer', 'index.html'));

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

app.whenReady().then(async () => {
  createWindow();
  try {
    await connectHub();
  } catch (err) {
    console.error('[hub] failed to connect:', err.message);
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.webContents.send('hub:status', { connected: false, url: HUB_URL, error: String(err.message) });
    }
  }

  app.on('activate', () => {
    // macOS: re-create a window when the dock icon is clicked.
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  // macOS apps stay active until Cmd+Q.
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('before-quit', async (event) => {
  if (client) {
    event.preventDefault();
    await disconnectHub();
    app.quit();
  }
});