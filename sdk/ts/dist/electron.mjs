var __require = /* @__PURE__ */ ((x) => typeof require !== "undefined" ? require : typeof Proxy !== "undefined" ? new Proxy(x, {
  get: (a, b) => (typeof require !== "undefined" ? require : a)[b]
}) : x)(function(x) {
  if (typeof require !== "undefined") return require.apply(this, arguments);
  throw Error('Dynamic require of "' + x + '" is not supported');
});

// src/electron.ts
function createMainBridge(client) {
  return {
    invoke: (method, params) => client.request(method, params),
    onEvent: (cb) => client.on("*", cb),
    close: () => client.close()
  };
}
function createPreloadAPI() {
  let ipc = null;
  const getIpc = () => {
    if (ipc) return ipc;
    try {
      const electron = __require("electron");
      ipc = electron?.ipcRenderer ?? null;
    } catch {
      ipc = null;
    }
    return ipc;
  };
  return {
    invoke: async (method, params) => {
      const r = getIpc();
      if (!r) throw new Error("hub-sdk: ipcRenderer is unavailable (not running inside Electron preload?)");
      return r.invoke("hub:invoke", method, params);
    },
    onEvent: (cb) => {
      const r = getIpc();
      if (!r) return () => {
      };
      const handler = (_event, ev) => cb(ev);
      r.on("hub:event", handler);
      return () => {
        r.removeListener("hub:event", handler);
      };
    }
  };
}

export { createMainBridge, createPreloadAPI };
//# sourceMappingURL=electron.mjs.map
//# sourceMappingURL=electron.mjs.map