'use strict';

/**
 * Preload script: bridges the sandboxed renderer to the main process.
 *
 * Exposes a tiny, safe API on window.hub:
 *   hub.invoke(method, params)  -> Promise (JSON-RPC call via main process)
 *   hub.onEvent(cb)             -> subscribe to 'hub:event'; returns unsubscribe
 *   hub.onStatus(cb)            -> subscribe to 'hub:status'; returns unsubscribe
 */

const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('hub', {
  invoke: (method, params) => ipcRenderer.invoke('hub:invoke', method, params),
  info: () => ipcRenderer.invoke('hub:info'),
  nodes: () => ipcRenderer.invoke('hub:nodes'),
  onEvent: (cb) => {
    const listener = (_event, ev) => cb(ev);
    ipcRenderer.on('hub:event', listener);
    return () => ipcRenderer.removeListener('hub:event', listener);
  },
  onStatus: (cb) => {
    const listener = (_event, status) => cb(status);
    ipcRenderer.on('hub:status', listener);
    return () => ipcRenderer.removeListener('hub:status', listener);
  },
});