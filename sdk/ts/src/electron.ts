import type { HubClient } from './client';
import type { HubEvent, RpcId } from './types';

/**
 * Electron helpers.
 *
 * createMainBridge(client) — wires a HubClient into the main process:
 *   - `bridge.invoke(method, params)` → ipcMain.handle('hub:invoke', ...)
 *   - `bridge.onEvent(cb)` → forwards events to `webContents.send('hub:event', ...)`
 *
 * createPreloadAPI() — factory for the preload script:
 *   - `api.invoke(method, params)` → ipcRenderer.invoke('hub:invoke', method, params)
 *   - `api.onEvent(cb)` → ipcRenderer.on('hub:event', ...) with unsubscribe
 *
 * These helpers deliberately avoid importing the `electron` package so the
 * file compiles in plain Node/browser too. Electron types are intentionally
 * untyped (`any`) — see the comments below. Minimum Electron: >=22 (fetch /
 * globalThis WebSocket are available in the main process).
 */

/** Result of createMainBridge. */
export interface MainBridge {
  /**
   * Invoke a protocol method from the renderer via ipcMain.handle('hub:invoke').
   * Wire it yourself in the main process:
   *   ipcMain.handle('hub:invoke', (_e, method, params) => bridge.invoke(method, params));
   */
  invoke(method: string, params?: Record<string, unknown>): Promise<unknown>;
  /** Forward events to all (or a specific) renderer windows. */
  onEvent(cb: (ev: HubEvent) => void): () => void;
  /** Tear down: remove listeners and close the client. */
  close(): Promise<void>;
}

export function createMainBridge(client: HubClient): MainBridge {
  return {
    invoke: (method: string, params?: Record<string, unknown>) => client.request(method, params),
    onEvent: (cb: (ev: HubEvent) => void) => client.on('*', cb),
    close: () => client.close()
  };
}

/** Preload API factory result. */
export interface PreloadApi {
  invoke(method: string, params?: Record<string, unknown>): Promise<unknown>;
  onEvent(cb: (ev: HubEvent) => void): () => void;
}

/**
 * Create the preload API. Use inside preload.js:
 *
 *   const { createPreloadAPI } = require('@musubi/hub-sdk/electron');
 *   const api = createPreloadAPI();
 *   contextBridge.exposeInMainWorld('hub', {
 *     invoke: api.invoke,
 *     onEvent: (cb) => api.onEvent(cb)
 *   });
 *
 * `ipcRenderer` is resolved lazily from require('electron') at call time so
 * the module can be loaded in non-Electron contexts without crashing.
 */
export function createPreloadAPI(): PreloadApi {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let ipc: any = null;
  const getIpc = (): any => {
    if (ipc) return ipc;
    // Electron exposes ipcRenderer synchronously via require in the preload.
    // Guarded so this file also loads in Node/browser (returns a stub).
    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const electron = require('electron');
      ipc = electron?.ipcRenderer ?? null;
    } catch {
      ipc = null;
    }
    return ipc;
  };

  return {
    invoke: async (method: string, params?: Record<string, unknown>): Promise<unknown> => {
      const r = getIpc();
      if (!r) throw new Error('hub-sdk: ipcRenderer is unavailable (not running inside Electron preload?)');
      return r.invoke('hub:invoke', method, params);
    },
    onEvent: (cb: (ev: HubEvent) => void): (() => void) => {
      const r = getIpc();
      if (!r) return () => {};
      const handler = (_event: unknown, ev: HubEvent): void => cb(ev);
      r.on('hub:event', handler);
      return () => {
        r.removeListener('hub:event', handler);
      };
    }
  };
}

// Re-export types for convenience.
export type { HubEvent, RpcId };