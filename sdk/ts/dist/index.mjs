var __require = /* @__PURE__ */ ((x) => typeof require !== "undefined" ? require : typeof Proxy !== "undefined" ? new Proxy(x, {
  get: (a, b) => (typeof require !== "undefined" ? require : a)[b]
}) : x)(function(x) {
  if (typeof require !== "undefined") return require.apply(this, arguments);
  throw Error('Dynamic require of "' + x + '" is not supported');
});

// src/errors.ts
var HubError = class _HubError extends Error {
  constructor(code, message, data) {
    super(message);
    this.name = "HubError";
    this.code = code;
    this.data = data;
    Object.setPrototypeOf(this, new.target.prototype);
  }
  /** Build a HubError from a JSON-RPC error envelope. */
  static fromRpc(err) {
    return new _HubError(
      typeof err.code === "number" ? err.code : RPC_CODES.INTERNAL_ERROR,
      err.message ?? "unknown rpc error",
      err.data
    );
  }
};
function isHubError(err) {
  return err instanceof HubError;
}
var RPC_CODES = {
  PARSE_ERROR: -32700,
  INVALID_REQUEST: -32600,
  METHOD_NOT_FOUND: -32601,
  INVALID_PARAMS: -32602,
  INTERNAL_ERROR: -32603
};
var HUB_CODES = {
  AUTH_REQUIRED: -32001,
  AUTH_INVALID: -32002,
  AUTH_EXPIRED: -32003,
  AUTH_FORBIDDEN: -32004,
  PROTOCOL_VERSION_MISMATCH: -32005,
  ENCODING_NOT_SUPPORTED: -32006,
  TRANSPORT_NOT_SUPPORTED: -32007,
  NODE_NOT_FOUND: -32010,
  NODE_OFFLINE: -32011,
  NODE_BUSY: -32012,
  COMMAND_NOT_FOUND: -32013,
  COMMAND_CANCEL_REJECTED: -32014,
  TASK_NOT_FOUND: -32020,
  TASK_CANCEL_REJECTED: -32021,
  SUBSCRIPTION_NOT_FOUND: -32030,
  EVENT_TYPE_NOT_SUPPORTED: -32031,
  FEATURE_DISABLED: -32040,
  RATE_LIMITED: -32041,
  SEAT_NOT_FOUND: -32050,
  SEAT_OCCUPIED: -32051,
  SEAT_EXPECT_MISMATCH: -32052
};
var SDK_CODES = {
  /** Request timed out locally. */
  TIMEOUT: 1,
  /** Transport is not connected (or connection dropped mid-flight). */
  NOT_CONNECTED: 2,
  /** A WS frame arrived in an encoding we did not negotiate (binary frames). */
  ENCODING_NOT_SUPPORTED: 3,
  /** No transport could be established / negotiated with the server. */
  NO_TRANSPORT: 4,
  /** client.open() was not called yet. */
  NOT_OPEN: 5,
  /** client.close() was already called. */
  CLOSED: 6,
  /** Offline queue is full (cap 1000, FIFO). */
  QUEUE_FULL: 7,
  /** Authentication failed and no credentials were configured. */
  AUTH_UNAVAILABLE: 8
};

// src/transport.ts
var ListenerSet = class {
  constructor() {
    this.listeners = /* @__PURE__ */ new Set();
  }
  on(cb) {
    this.listeners.add(cb);
    return () => {
      this.listeners.delete(cb);
    };
  }
  emit(a) {
    for (const cb of [...this.listeners]) {
      try {
        cb(a);
      } catch {
      }
    }
  }
  get size() {
    return this.listeners.size;
  }
};
function normalizeBaseUrl(url) {
  const trimmed = url.trim().replace(/\/+$/, "");
  const m = trimmed.match(/^([a-zA-Z][a-zA-Z0-9+.-]*:\/\/[^/]+)/);
  return m ? m[1] : trimmed;
}
function toWsUrl(baseUrl) {
  const b = normalizeBaseUrl(baseUrl);
  if (b.startsWith("wss://") || b.startsWith("ws://")) return `${b}/v1/ws`;
  if (b.startsWith("https://")) return `${b.replace(/^https/, "wss")}/v1/ws`;
  if (b.startsWith("http://")) return `${b.replace(/^http/, "ws")}/v1/ws`;
  return `${b}/v1/ws`;
}
function resolveFetch() {
  const g = globalThis;
  if (typeof g.fetch === "function") return g.fetch;
  throw new HubError(SDK_CODES.NO_TRANSPORT, "fetch is not available in this runtime");
}
async function postRpc(baseUrl, method, params, getToken, timeoutMs) {
  const fetch = resolveFetch();
  const headers = {
    "Content-Type": "application/json",
    Accept: "application/json"
  };
  const token = getToken();
  if (token) headers["Authorization"] = `Bearer ${token}`;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  let res;
  try {
    res = await fetch(`${normalizeBaseUrl(baseUrl)}/v1/rpc`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: nextHttpId(),
        method,
        ...params !== void 0 ? { params } : {}
      }),
      signal: ctrl.signal
    });
  } catch (err) {
    const aborted = err.name === "AbortError";
    throw new HubError(aborted ? SDK_CODES.TIMEOUT : SDK_CODES.NOT_CONNECTED, `http request failed: ${String(err)}`, err);
  } finally {
    clearTimeout(timer);
  }
  if (!res.ok) {
    let body;
    try {
      body = await res.json();
    } catch {
      body = void 0;
    }
    const env = body?.error;
    if (env && typeof env.code === "number") throw HubError.fromRpc(env);
    if (res.status === 401) {
      throw new HubError(SDK_CODES.NOT_CONNECTED, `http ${res.status} unauthorized`, res.status);
    }
    throw new HubError(SDK_CODES.NOT_CONNECTED, `http ${res.status} ${res.statusText}`, body);
  }
  const payload = await res.json();
  if (payload.error && typeof payload.error.code === "number") {
    throw HubError.fromRpc(payload.error);
  }
  return payload.result;
}
var httpIdSeq = 0;
function nextHttpId() {
  httpIdSeq += 1;
  return httpIdSeq;
}
function eventsUrl(baseUrl, path, query) {
  const u = new URL(`${normalizeBaseUrl(baseUrl)}${path}`);
  for (const [k, v] of Object.entries(query)) {
    if (v !== void 0) u.searchParams.set(k, String(v));
  }
  return u.toString();
}
var pollUrl = eventsUrl;

// src/transports/websocket.ts
var WS_OPEN = 1;
async function resolveWsCtor() {
  const g = globalThis;
  if (typeof g.WebSocket === "function") return g.WebSocket;
  const nodeRequire = globalThis.require ?? (typeof __require === "function" ? __require : void 0);
  if (nodeRequire) {
    try {
      const mod = nodeRequire("ws");
      return mod.WebSocket ?? mod.default ?? null;
    } catch {
    }
  }
  try {
    const spec = "ws";
    const mod = await import(
      /* webpackIgnore: true */
      /* @vite-ignore */
      spec
    );
    return mod.WebSocket ?? mod.default ?? null;
  } catch {
    return null;
  }
}
var WebSocketTransport = class {
  constructor(options) {
    this.kind = "ws";
    this.ctor = null;
    this.ws = null;
    this.pending = /* @__PURE__ */ new Map();
    this.idSeq = 0;
    this.connectPromise = null;
    this.batchQueue = [];
    this.batchTimer = null;
    // heartbeat
    this.pingTimer = null;
    this.heartbeatType = "rpc";
    // listeners
    this.onEventL = new ListenerSet();
    this.onOpenL = new ListenerSet();
    this.onCloseL = new ListenerSet();
    this.onErrorL = new ListenerSet();
    this.baseUrl = normalizeBaseUrl(options.baseUrl);
    this.options = options;
    this.batchEnabled = options.batch === true;
  }
  connect() {
    if (this.connectPromise) return this.connectPromise;
    this.connectPromise = this.doConnect().catch((err) => {
      this.connectPromise = null;
      throw err;
    }).finally(() => {
      this.connectPromise = null;
    });
    return this.connectPromise;
  }
  async doConnect() {
    const ctor = this.ctor ?? await resolveWsCtor();
    if (!ctor) {
      throw new HubError(
        SDK_CODES.NO_TRANSPORT,
        'no WebSocket implementation available (browser/Node>=21/Deno/Bun have globalThis.WebSocket; Node 18/20 need the "ws" package installed)'
      );
    }
    this.ctor = ctor;
    const url = toWsUrl(this.baseUrl);
    const ws = new ctor(url);
    this.ws = ws;
    let settled = false;
    const opened = new Promise((resolve, reject) => {
      ws.onopen = () => {
        if (settled) return;
        settled = true;
        ws.onerror = null;
        this.heartbeatType = typeof ws.ping === "function" ? "ws" : "rpc";
        this.startPing();
        this.onOpenL.emit();
        resolve();
      };
      const fail = (err) => {
        if (settled) return;
        settled = true;
        this.onErrorL.emit(err);
        reject(err);
      };
      ws.onerror = (ev) => {
        fail(new HubError(SDK_CODES.NOT_CONNECTED, `websocket connect failed: ${String(ev)}`, ev));
      };
      ws.onclose = (ev) => {
        const code = ev.code;
        fail(new HubError(SDK_CODES.NOT_CONNECTED, `websocket closed before open (code ${code ?? "?"})`, ev));
      };
    });
    ws.onmessage = (ev) => {
      const data = ev.data;
      try {
        if (typeof data !== "string") {
          throw new HubError(
            SDK_CODES.ENCODING_NOT_SUPPORTED,
            'encoding not supported: binary websocket frame received (only "json" was negotiated)',
            data
          );
        }
        this.handleFrame(data);
      } catch (err) {
        this.onErrorL.emit(err);
        this.forceClose();
      }
    };
    ws.onclose = (ev) => {
      if (!settled) return;
      const code = ev.code;
      const reason = ev.reason;
      const err = new HubError(
        SDK_CODES.NOT_CONNECTED,
        `websocket closed (code ${code ?? "?"}${reason ? `, ${reason}` : ""})`,
        ev
      );
      this.ws = null;
      this.rejectAll(err);
      this.stopPing();
      this.onCloseL.emit(err);
    };
    ws.onerror = (ev) => {
      if (!settled) return;
      this.onErrorL.emit(new HubError(SDK_CODES.NOT_CONNECTED, `websocket error: ${String(ev)}`, ev));
    };
    return opened.catch((err) => {
      try {
        ws.close();
      } catch {
      }
      throw err;
    });
  }
  startPing() {
    if (this.pingTimer) return;
    const interval = this.options.pingIntervalMs ?? 3e4;
    this.pingTimer = setInterval(() => {
      const ws = this.ws;
      if (!ws || ws.readyState !== WS_OPEN) return;
      try {
        if (this.heartbeatType === "ws") {
          ws.ping?.();
        } else {
          ws.send(JSON.stringify({ jsonrpc: "2.0", method: "engine.ping", params: {} }));
        }
      } catch {
      }
    }, interval);
  }
  request(method, params, timeoutMs) {
    const timeout = timeoutMs ?? 3e4;
    return new Promise((resolve, reject) => {
      const ws = this.ws;
      if (!ws || ws.readyState !== WS_OPEN) {
        reject(new HubError(SDK_CODES.NOT_CONNECTED, `websocket transport not connected (${method})`));
        return;
      }
      const id = ++this.idSeq;
      const entry = {
        method,
        resolve: (v) => resolve(v),
        reject,
        timer: setTimeout(() => {
          this.pending.delete(id);
          reject(new HubError(SDK_CODES.TIMEOUT, `request timeout (${timeout}ms): ${method}`));
        }, timeout)
      };
      this.pending.set(id, entry);
      if (this.batchEnabled) {
        this.batchQueue.push({ id, method, params, entry });
        if (!this.batchTimer) {
          this.batchTimer = setTimeout(() => this.flushBatch(), 0);
        }
      } else {
        ws.send(JSON.stringify({ jsonrpc: "2.0", id, method, ...params !== void 0 ? { params } : {} }));
      }
    });
  }
  flushBatch() {
    this.batchTimer = null;
    const queue = this.batchQueue;
    this.batchQueue = [];
    if (queue.length === 0) return;
    const ws = this.ws;
    if (!ws || ws.readyState !== WS_OPEN) {
      for (const item of queue) {
        clearTimeout(item.entry.timer);
        this.pending.delete(item.id);
        item.entry.reject(new HubError(SDK_CODES.NOT_CONNECTED, `websocket transport not connected (${item.method})`));
      }
      return;
    }
    const frames = queue.map((item) => ({
      jsonrpc: "2.0",
      id: item.id,
      method: item.method,
      ...item.params !== void 0 ? { params: item.params } : {}
    }));
    ws.send(JSON.stringify(frames));
  }
  sendNotification(method, params) {
    const ws = this.ws;
    if (!ws || ws.readyState !== WS_OPEN) return;
    ws.send(JSON.stringify({ jsonrpc: "2.0", method, ...params !== void 0 ? { params } : {} }));
  }
  /**
   * Parse a text frame: single envelope or batch array. Responses are matched
   * by id against pending requests; notifications with method "event" are
   * unpacked and emitted through the event listeners. Unknown notifications
   * are ignored (compatibility rule).
   */
  handleFrame(text) {
    let msg;
    try {
      msg = JSON.parse(text);
    } catch {
      this.onErrorL.emit(new HubError(SDK_CODES.ENCODING_NOT_SUPPORTED, "encoding not supported: invalid JSON frame"));
      return;
    }
    if (Array.isArray(msg)) {
      for (const item of msg) this.handleEnvelope(item);
    } else {
      this.handleEnvelope(msg);
    }
  }
  handleEnvelope(item) {
    if (!item || typeof item !== "object" || Array.isArray(item)) return;
    const obj = item;
    if (obj.id === void 0 && typeof obj.method === "string") {
      if (obj.method === "event") {
        const ev = obj.params?.event;
        if (ev) this.onEventL.emit(ev);
      }
      return;
    }
    if (obj.id !== void 0) {
      const entry = this.pending.get(obj.id);
      if (!entry) return;
      this.pending.delete(obj.id);
      clearTimeout(entry.timer);
      if (obj.error && typeof obj.error.code === "number") {
        entry.reject(HubError.fromRpc(obj.error));
      } else {
        entry.resolve(obj.result);
      }
    }
  }
  onEvent(cb) {
    return this.onEventL.on(cb);
  }
  onOpen(cb) {
    return this.onOpenL.on(cb);
  }
  onClose(cb) {
    return this.onCloseL.on(cb);
  }
  onError(cb) {
    return this.onErrorL.on(cb);
  }
  forceClose() {
    const ws = this.ws;
    this.ws = null;
    if (ws) {
      try {
        ws.onopen = null;
        ws.onerror = null;
        ws.onclose = null;
        ws.onmessage = null;
        ws.close();
      } catch {
      }
    }
    this.rejectAll(new HubError(SDK_CODES.NOT_CONNECTED, "websocket connection lost"));
    this.stopPing();
    this.onCloseL.emit(new HubError(SDK_CODES.NOT_CONNECTED, "websocket connection lost"));
  }
  rejectAll(err) {
    for (const [id, entry] of [...this.pending]) {
      this.pending.delete(id);
      clearTimeout(entry.timer);
      entry.reject(err);
    }
  }
  stopPing() {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }
  async close() {
    this.stopPing();
    this.rejectAll(new HubError(SDK_CODES.CLOSED, "transport closed by client"));
    const ws = this.ws;
    this.ws = null;
    if (ws) {
      try {
        ws.close(1e3, "client close");
      } catch {
      }
    }
  }
};

// src/transports/http.ts
var ES_CLOSED = 2;
var HttpTransport = class {
  constructor(options) {
    this.kind = "http";
    this.sseController = null;
    this.eventSource = null;
    this.connectPromise = null;
    // listeners
    this.onEventL = new ListenerSet();
    this.onOpenL = new ListenerSet();
    this.onCloseL = new ListenerSet();
    this.onErrorL = new ListenerSet();
    this.baseUrl = normalizeBaseUrl(options.baseUrl);
    this.options = options;
  }
  connect() {
    if (this.connectPromise) return this.connectPromise;
    this.connectPromise = this.doConnect().catch((err) => {
      this.connectPromise = null;
      throw err;
    }).finally(() => {
      this.connectPromise = null;
    });
    return this.connectPromise;
  }
  request(method, params, timeoutMs) {
    return postRpc(this.baseUrl, method, params, () => this.options.getToken(), timeoutMs ?? 3e4);
  }
  sendNotification(method, params) {
    void postRpc(this.baseUrl, method, params, () => this.options.getToken(), 5e3).catch(() => {
    });
  }
  async doConnect() {
    const g = globalThis;
    if (typeof g.EventSource === "function") {
      await this.connectViaEventSource(g.EventSource);
    } else {
      await this.connectViaFetch();
    }
    this.onOpenL.emit();
  }
  /* ---------------- browser: EventSource ---------------- */
  connectViaEventSource(Ctor) {
    return new Promise((resolve, reject) => {
      const url = eventsUrl(this.baseUrl, "/v1/events", {
        token: this.options.getToken()
      });
      const es = new Ctor(url);
      this.eventSource = es;
      let settled = false;
      es.onopen = () => {
        if (settled) return;
        settled = true;
        resolve();
      };
      es.onerror = (ev) => {
        if (!settled) {
          settled = true;
          reject(new HubError(SDK_CODES.NOT_CONNECTED, `sse connect failed: ${String(ev)}`, ev));
          return;
        }
        this.onErrorL.emit(new HubError(SDK_CODES.NOT_CONNECTED, `sse error: ${String(ev)}`, ev));
        if (es.readyState === ES_CLOSED) {
          this.onCloseL.emit(new HubError(SDK_CODES.NOT_CONNECTED, "sse connection closed"));
        }
      };
      es.onmessage = (ev) => {
        const raw = ev.data;
        this.dispatchSseLine(raw ?? "");
      };
    });
  }
  /* ---------------- node: fetch ReadableStream ---------------- */
  async connectViaFetch() {
    const fetch = resolveFetch();
    const ctrl = new AbortController();
    this.sseController = ctrl;
    const url = eventsUrl(this.baseUrl, "/v1/events", { token: this.options.getToken() });
    const headers = {
      Accept: "text/event-stream",
      "Cache-Control": "no-cache"
    };
    const token = this.options.getToken();
    if (token) headers["Authorization"] = `Bearer ${token}`;
    let res;
    try {
      res = await fetch(url, {
        headers,
        signal: ctrl.signal
      });
    } catch (err) {
      throw new HubError(SDK_CODES.NOT_CONNECTED, `sse connect failed: ${String(err)}`, err);
    }
    if (!res.ok) {
      throw new HubError(SDK_CODES.NOT_CONNECTED, `sse connect failed: http ${res.status} ${res.statusText}`);
    }
    if (!res.body) {
      throw new HubError(SDK_CODES.NOT_CONNECTED, "sse connect failed: response has no body");
    }
    void this.readSseStream(res, ctrl);
  }
  /** Background SSE reader: text/event-stream → `data:` lines → JSON.parse. */
  async readSseStream(res, ctrl) {
    const reader = res.body?.getReader();
    if (!reader) return;
    const decoder = new TextDecoder();
    let buffer = "";
    while (true) {
      let chunk;
      try {
        chunk = await reader.read();
      } catch (err) {
        if (ctrl.signal.aborted) break;
        this.onErrorL.emit(new HubError(SDK_CODES.NOT_CONNECTED, `sse stream error: ${String(err)}`, err));
        this.onCloseL.emit(new HubError(SDK_CODES.NOT_CONNECTED, `sse stream closed: ${String(err)}`, err));
        break;
      }
      if (chunk.done) {
        if (!ctrl.signal.aborted) {
          this.onCloseL.emit(new HubError(SDK_CODES.NOT_CONNECTED, "sse stream ended"));
        }
        break;
      }
      const value = chunk.value;
      const text = typeof value === "string" ? value : decoder.decode(value, { stream: true });
      buffer += text;
      let idx;
      while ((idx = buffer.indexOf("\n\n")) !== -1) {
        const block = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        this.dispatchSseBlock(block);
      }
    }
  }
  /** Dispatch a single SSE block (may contain several `data:` lines). */
  dispatchSseBlock(block) {
    const dataLines = [];
    for (const line of block.split("\n")) {
      if (line.startsWith("data:")) {
        dataLines.push(line.slice(5).trimStart());
      }
    }
    if (dataLines.length === 0) return;
    this.dispatchSseLine(dataLines.join("\n"));
  }
  /** JSON.parse a `data:` payload — must be a JSON-RPC notification ("event"). */
  dispatchSseLine(raw) {
    if (!raw) return;
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      this.onErrorL.emit(new HubError(SDK_CODES.ENCODING_NOT_SUPPORTED, "encoding not supported: invalid SSE data"));
      return;
    }
    const obj = parsed;
    if (obj && typeof obj.method === "string" && obj.method === "event" && obj.params?.event) {
      this.onEventL.emit(obj.params.event);
    }
  }
  onEvent(cb) {
    return this.onEventL.on(cb);
  }
  onOpen(cb) {
    return this.onOpenL.on(cb);
  }
  onClose(cb) {
    return this.onCloseL.on(cb);
  }
  onError(cb) {
    return this.onErrorL.on(cb);
  }
  async close() {
    if (this.sseController) {
      this.sseController.abort();
      this.sseController = null;
    }
    if (this.eventSource) {
      try {
        this.eventSource.close();
      } catch {
      }
      this.eventSource = null;
    }
  }
};

// src/transports/poll.ts
var PollTransport = class {
  constructor(options) {
    this.kind = "poll";
    this.cursor = null;
    this.stopped = false;
    this.loopPromise = null;
    this.connectPromise = null;
    // listeners
    this.onEventL = new ListenerSet();
    this.onOpenL = new ListenerSet();
    this.onCloseL = new ListenerSet();
    this.onErrorL = new ListenerSet();
    this.baseUrl = normalizeBaseUrl(options.baseUrl);
    this.options = options;
  }
  connect() {
    if (this.connectPromise) return this.connectPromise;
    this.connectPromise = this.doConnect().catch((err) => {
      this.connectPromise = null;
      throw err;
    }).finally(() => {
      this.connectPromise = null;
    });
    return this.connectPromise;
  }
  async doConnect() {
    this.stopped = false;
    this.cursor = null;
    this.loopPromise = this.pollLoop();
    this.onOpenL.emit();
  }
  request(method, params, timeoutMs) {
    return postRpc(this.baseUrl, method, params, () => this.options.getToken(), timeoutMs ?? 3e4);
  }
  sendNotification(method, params) {
    void postRpc(this.baseUrl, method, params, () => this.options.getToken(), 5e3).catch(() => {
    });
  }
  async pollLoop() {
    const fetch = resolveFetch();
    let consecutiveErrors = 0;
    const pollTimeout = 35e3;
    const holdTimeout = 30;
    while (!this.stopped) {
      const params = { timeout: String(holdTimeout) };
      if (this.cursor !== null) params["cursor"] = String(this.cursor);
      const token = this.options.getToken();
      if (token) params["token"] = token;
      const url = pollUrl(this.baseUrl, "/v1/poll", params);
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), pollTimeout);
      try {
        const res = await fetch(url, { signal: ctrl.signal });
        if (this.stopped) break;
        if (res.status === 401) {
          this.onErrorL.emit(new HubError(SDK_CODES.NOT_CONNECTED, "poll 401 unauthorized"));
          this.onCloseL.emit(new HubError(SDK_CODES.NOT_CONNECTED, "poll 401 unauthorized"));
          break;
        }
        if (!res.ok) {
          throw new HubError(SDK_CODES.NOT_CONNECTED, `poll http ${res.status} ${res.statusText}`);
        }
        const payload = await res.json();
        if (typeof payload.cursor === "number") this.cursor = payload.cursor;
        for (const ev of payload.events ?? []) {
          if (ev) this.onEventL.emit(ev);
        }
        consecutiveErrors = 0;
      } catch (err) {
        if (this.stopped) break;
        const aborted = err.name === "AbortError";
        if (aborted) {
          continue;
        }
        consecutiveErrors += 1;
        this.onErrorL.emit(err);
        if (consecutiveErrors >= 2) {
          this.onCloseL.emit(err);
          break;
        }
      } finally {
        clearTimeout(timer);
      }
    }
  }
  onEvent(cb) {
    return this.onEventL.on(cb);
  }
  onOpen(cb) {
    return this.onOpenL.on(cb);
  }
  onClose(cb) {
    return this.onCloseL.on(cb);
  }
  onError(cb) {
    return this.onErrorL.on(cb);
  }
  async close() {
    this.stopped = true;
    if (this.loopPromise) {
      try {
        await this.loopPromise;
      } catch {
      }
      this.loopPromise = null;
    }
  }
};

// src/client.ts
var SDK_VERSION = "1.0.0";
var DEFAULT_TRANSPORT_ORDER = ["ws", "http", "poll"];
var QUEUE_LIMIT = 1e3;
var RECONNECT_BASE_MS = 1e3;
var RECONNECT_MAX_MS = 3e4;
var HubClient = class {
  constructor(options = {}) {
    this.state = "new";
    this.closed = false;
    this.connectPromise = null;
    this.transport = null;
    this.serverInfoValue = null;
    this.featuresValue = {};
    // event routing + dedupe
    this.listeners = /* @__PURE__ */ new Map();
    this.seenIds = [];
    this.seenSet = /* @__PURE__ */ new Set();
    // offline queue + reconnect
    this.queue = [];
    this.reconnectAttempt = 0;
    this.reconnectTimer = null;
    // subscriptions to restore after reconnect
    this.activeSubs = [];
    this.opts = {
      url: options.url ?? "http://localhost:8080",
      token: options.token,
      tokenProvider: options.tokenProvider,
      credentials: options.credentials,
      transportOrder: options.transportOrder ?? DEFAULT_TRANSPORT_ORDER,
      autoReconnect: options.autoReconnect ?? true,
      maxRetries: options.maxRetries ?? Infinity,
      queueOffline: options.queueOffline ?? true,
      requestTimeoutMs: options.requestTimeoutMs ?? 3e4,
      pingIntervalMs: options.pingIntervalMs ?? 3e4,
      // Clamp to >= 0: a negative window previously drove dedupeEvent into a
      // non-terminating loop. Non-finite values fall back to the default.
      dedupeWindow: typeof options.dedupeWindow === "number" && Number.isFinite(options.dedupeWindow) ? Math.max(0, Math.floor(options.dedupeWindow)) : 5e3,
      batch: options.batch ?? false,
      proxy: options.proxy,
      ca: options.ca,
      insecure: options.insecure
    };
  }
  /* ---------------------------------------------------------------- */
  /* Open / close                                                     */
  /* ---------------------------------------------------------------- */
  /**
   * Connect and negotiate. Throws a HubError when no transport could be
   * established (protocol mismatch -32005, or all transports failed). With
   * autoReconnect, background retries continue after a failure.
   */
  open() {
    if (this.closed) return Promise.reject(new HubError(SDK_CODES.CLOSED, "client is closed"));
    if (this.state === "open") return Promise.resolve();
    if (this.connectPromise) return this.connectPromise;
    this.connectPromise = this.initialConnect().finally(() => {
      this.connectPromise = null;
    });
    return this.connectPromise;
  }
  async initialConnect() {
    this.state = "connecting";
    try {
      await this.establish();
      this.reconnectAttempt = 0;
      this.state = "open";
      await this.ensureAuth();
      await this.restoreSubscriptions();
      await this.flushQueue();
    } catch (err) {
      if (this.isFatalError(err)) {
        this.state = "closed";
        this.closed = true;
        throw err;
      }
      this.state = "reconnecting";
      if (this.opts.autoReconnect) this.scheduleReconnect();
      throw err;
    }
  }
  async close() {
    this.closed = true;
    this.state = "closed";
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.rejectQueued(new HubError(SDK_CODES.CLOSED, "client closed"));
    const t = this.transport;
    this.transport = null;
    if (t) {
      try {
        await t.close();
      } catch {
      }
    }
  }
  /* ---------------------------------------------------------------- */
  /* Negotiation / transport                                          */
  /* ---------------------------------------------------------------- */
  /** Negotiated server_info from the last engine.info. */
  get serverInfo() {
    return this.serverInfoValue;
  }
  /** Merged features (client ∩ server). */
  get features() {
    return this.featuresValue;
  }
  /** Currently active transport kind, or null. */
  get transportKind() {
    return this.transport?.kind ?? null;
  }
  buildClientInfo() {
    return {
      name: "musubi-hub-sdk",
      version: SDK_VERSION,
      protocol_versions: ["1.0"],
      encodings: ["json"],
      transports: this.opts.transportOrder,
      features: {
        batch: this.opts.batch === true,
        subscribe: true
      },
      platform: detectPlatform()
    };
  }
  createTransport(kind) {
    const options = {
      baseUrl: this.opts.url ?? "http://localhost:8080",
      getToken: () => this.accessToken,
      pingIntervalMs: this.opts.pingIntervalMs,
      batch: this.opts.batch,
      proxy: this.opts.proxy,
      ca: this.opts.ca,
      insecure: this.opts.insecure
    };
    switch (kind) {
      case "ws":
        return new WebSocketTransport(options);
      case "http":
        return new HttpTransport(options);
      case "poll":
        return new PollTransport(options);
    }
  }
  /**
   * Establish the best transport: try transportOrder in order, use the first
   * one the server accepts (a -32007 transport.not.supported error means "try
   * the next"). Then negotiate engine.info and switch to the best intersection
   * (transportOrder ∩ server_info.transports) if the server advertises a
   * better one than the probing transport.
   */
  async establish() {
    await this.teardownTransport();
    const order = this.opts.transportOrder;
    const errors = [];
    for (const kind of order) {
      const t = this.createTransport(kind);
      this.bindTransport(t);
      try {
        await t.connect();
        this.transport = t;
        const info = await this.negotiate(t);
        const preferred = this.pickTransport(info);
        if (preferred && preferred !== kind) {
          await this.teardownTransport();
          const t2 = this.createTransport(preferred);
          this.bindTransport(t2);
          await t2.connect();
          this.transport = t2;
          await this.negotiate(t2);
        }
        return;
      } catch (err) {
        await this.teardownTransport();
        if (this.isFatalError(err)) throw err;
        if (err instanceof HubError && err.code === HUB_CODES.TRANSPORT_NOT_SUPPORTED) continue;
        errors.push(`${kind}: ${err.message}`);
      }
    }
    throw new HubError(
      SDK_CODES.NO_TRANSPORT,
      `no transport could be established: ${errors.join("; ") || "server closed all attempts"}`
    );
  }
  async negotiate(t) {
    const info = await t.request("engine.info", { client_info: this.buildClientInfo() });
    this.serverInfoValue = info;
    this.featuresValue = { ...this.buildClientInfo().features ?? {}, ...info.features ?? {} };
    return info;
  }
  isFatalError(err) {
    if (err instanceof HubError) {
      return err.code === HUB_CODES.PROTOCOL_VERSION_MISMATCH || err.code === HUB_CODES.ENCODING_NOT_SUPPORTED || err.code === SDK_CODES.NO_TRANSPORT;
    }
    return false;
  }
  pickTransport(info) {
    const server = new Set(info.transports ?? []);
    for (const kind of this.opts.transportOrder) {
      if (server.has(kind)) return kind;
    }
    return null;
  }
  bindTransport(t) {
    t.onEvent((ev) => this.dispatchEvent(ev));
    t.onClose((err) => this.handleTransportClose(err));
    t.onError(() => {
    });
  }
  async teardownTransport() {
    const t = this.transport;
    this.transport = null;
    if (t) {
      try {
        await t.close();
      } catch {
      }
    }
  }
  handleTransportClose(_err) {
    if (this.closed) return;
    if (this.state === "open") {
      this.state = "reconnecting";
    }
    if (this.state !== "reconnecting") return;
    if (this.opts.autoReconnect) this.scheduleReconnect();
  }
  /* ---------------------------------------------------------------- */
  /* Reconnect / offline queue                                        */
  /* ---------------------------------------------------------------- */
  scheduleReconnect() {
    if (this.reconnectTimer) return;
    const attempt = this.reconnectAttempt++;
    const max = this.opts.maxRetries;
    if (attempt >= max) {
      this.state = "closed";
      this.closed = true;
      return;
    }
    const delay = Math.min(RECONNECT_BASE_MS * Math.pow(2, attempt), RECONNECT_MAX_MS);
    const jitter = delay * 0.2 * (Math.random() * 2 - 1);
    const wait = Math.max(0, Math.round(delay + jitter));
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.reconnectOnce();
    }, wait);
  }
  async reconnectOnce() {
    try {
      await this.establish();
      this.reconnectAttempt = 0;
      this.state = "open";
      await this.ensureAuth();
      await this.restoreSubscriptions();
      await this.flushQueue();
    } catch (err) {
      if (!this.closed) this.scheduleReconnect();
    }
  }
  isQueueable(method) {
    return !method.startsWith("engine.") && !method.startsWith("auth.");
  }
  enqueue(method, params) {
    return new Promise((resolve, reject) => {
      if (this.queue.length >= QUEUE_LIMIT) {
        reject(new HubError(SDK_CODES.QUEUE_FULL, `offline queue full (${QUEUE_LIMIT})`));
        return;
      }
      this.queue.push({
        method,
        params,
        resolve: (v) => resolve(v),
        reject
      });
    });
  }
  async flushQueue() {
    const items = this.queue;
    this.queue = [];
    for (const item of items) {
      try {
        const res = await this.sendWithAuthRetry(item.method, item.params, { noQueue: true });
        item.resolve(res);
      } catch (err) {
        item.reject(err);
      }
    }
  }
  rejectQueued(err) {
    const items = this.queue;
    this.queue = [];
    for (const item of items) item.reject(err);
  }
  /* ---------------------------------------------------------------- */
  /* Auth                                                             */
  /* ---------------------------------------------------------------- */
  async ensureToken() {
    if (this.accessToken) return this.accessToken;
    if (this.opts.tokenProvider) {
      try {
        const t = await this.opts.tokenProvider();
        if (t) {
          this.accessToken = t;
          return t;
        }
      } catch {
      }
    }
    if (this.opts.token) {
      this.accessToken = this.opts.token;
      return this.opts.token;
    }
    return void 0;
  }
  /** Login automatically when the server requires it and we have no token. */
  async ensureAuth() {
    const token = await this.ensureToken();
    if (token) return;
    const info = this.serverInfoValue;
    if (info?.session && info.session.authenticated === false) {
      await this.login(this.opts.credentials ?? {});
    }
  }
  /* ---------------------------------------------------------------- */
  /* Core request path                                                */
  /* ---------------------------------------------------------------- */
  /**
   * Send a JSON-RPC request. When offline and queueOffline is enabled, write
   * operations (everything except engine.* and auth.*) are queued in order
   * (FIFO, cap 1000) and their promises settle after the connection recovers.
   * Requests that fail with -32003 (auth.expired) are retried once after
   * refreshing the token (or re-logging-in).
   */
  request(method, params, opts) {
    if (this.closed) return Promise.reject(new HubError(SDK_CODES.CLOSED, "client is closed"));
    if (this.state === "new") {
      return Promise.reject(new HubError(SDK_CODES.NOT_OPEN, "call client.open() before request()"));
    }
    if (this.state !== "open") {
      if (this.opts.queueOffline && this.isQueueable(method) && !opts?.noQueue) {
        return this.enqueue(method, params);
      }
      return Promise.reject(
        new HubError(SDK_CODES.NOT_CONNECTED, `client is not connected (state=${this.state}, method=${method})`)
      );
    }
    return this.sendWithAuthRetry(method, params, opts);
  }
  async sendWithAuthRetry(method, params, opts) {
    let attempts = 0;
    while (true) {
      try {
        return await this.sendOnce(method, params, opts);
      } catch (err) {
        const isAuth = err instanceof HubError && (err.code === HUB_CODES.AUTH_EXPIRED || err.code === HUB_CODES.AUTH_INVALID);
        if (isAuth && attempts === 0 && !method.startsWith("auth.")) {
          attempts += 1;
          if (await this.recoverAuth()) continue;
        }
        throw err;
      }
    }
  }
  /**
   * Recover from an auth failure: refresh the token pair first (the engine
   * rotates BOTH access_token and refresh_token and invalidates the old pair,
   * so refresh() stores the new pair), then fall back to a full re-login with
   * the configured credentials. Returns true when a fresh token is available
   * for retrying the original request exactly once.
   */
  async recoverAuth() {
    if (this.refreshToken) {
      try {
        await this.refresh();
        return true;
      } catch {
      }
    }
    if (this.opts.credentials) {
      try {
        await this.login(this.opts.credentials);
        return true;
      } catch {
      }
    }
    return false;
  }
  async sendOnce(method, params, opts) {
    const t = this.transport;
    if (!t) throw new HubError(SDK_CODES.NOT_CONNECTED, `no transport (${method})`);
    const timeout = opts?.timeoutMs ?? this.opts.requestTimeoutMs;
    let finalParams = params;
    if (t.kind === "ws" && this.accessToken && !method.startsWith("engine.") && !method.startsWith("auth.")) {
      finalParams = { ...params ?? {}, auth_token: this.accessToken };
    }
    return t.request(method, finalParams, timeout);
  }
  /* ---------------------------------------------------------------- */
  /* Events                                                           */
  /* ---------------------------------------------------------------- */
  /**
   * Register an event handler for a specific event type or '*' (all events,
   * including unknown ones). Returns an unsubscribe function.
   */
  on(type, handler) {
    let set = this.listeners.get(type);
    if (!set) {
      set = /* @__PURE__ */ new Set();
      this.listeners.set(type, set);
    }
    set.add(handler);
    return () => {
      set?.delete(handler);
    };
  }
  off(type, handler) {
    this.listeners.get(type)?.delete(handler);
  }
  dispatchEvent(ev) {
    if (!this.dedupeEvent(ev.id)) return;
    const kind = this.transport?.kind;
    if ((kind === "http" || kind === "poll") && this.activeSubs.length > 0) {
      const matched = this.activeSubs.some((s) => !s.cancelled && eventMatchesFilters(s.filters, ev));
      if (!matched) return;
    }
    const type = ev.type ?? "";
    const handlers = this.listeners.get(type);
    if (handlers) {
      for (const h of [...handlers]) {
        try {
          h(ev);
        } catch {
        }
      }
    }
    const star = this.listeners.get("*");
    if (star) {
      for (const h of [...star]) {
        try {
          h(ev);
        } catch {
        }
      }
    }
  }
  dedupeEvent(id) {
    if (!id) return true;
    if (this.seenSet.has(id)) return false;
    this.seenSet.add(id);
    this.seenIds.push(id);
    const window = Math.max(0, this.opts.dedupeWindow);
    while (this.seenIds.length > window) {
      const old = this.seenIds.shift();
      if (old !== void 0) this.seenSet.delete(old);
    }
    return true;
  }
  /* ---------------------------------------------------------------- */
  /* Subscriptions                                                    */
  /* ---------------------------------------------------------------- */
  /**
   * Subscribe to events server-side. Returns a Subscription handle whose
   * unsubscribe() removes the server subscription. Active subscriptions are
   * re-established automatically after reconnects.
   */
  async subscribeEvents(filters) {
    const filtersValue = filters ?? {};
    const entry = { filters: filtersValue, pending: true, cancelled: false };
    this.activeSubs.push(entry);
    try {
      const res = await this.eventSubscribe(filtersValue);
      entry.pending = false;
      entry.serverId = res.subscription_id ?? "";
    } catch (err) {
      this.removeActiveSub(entry);
      throw err;
    }
    if (entry.cancelled && entry.serverId) {
      void this.eventUnsubscribe(entry.serverId).catch(() => {
      });
    }
    return {
      get subscription_id() {
        return entry.serverId ?? "";
      },
      unsubscribe: async () => {
        if (entry.cancelled) return;
        entry.cancelled = true;
        this.removeActiveSub(entry);
        if (entry.serverId) {
          await this.eventUnsubscribe(entry.serverId).catch(() => {
          });
        }
      }
    };
  }
  /** Convenience: subscribe to node events; returns an unsubscribe function. */
  onNode(nodeId) {
    return this.trackSubscription({ node_id: nodeId });
  }
  /** Convenience: subscribe to task events; returns an unsubscribe function. */
  onTask(taskId) {
    return this.trackSubscription({ task_id: taskId });
  }
  /**
   * Shared helper behind onNode/onTask: subscribes (possibly while offline —
   * the request is queued and replayed on reconnect) and returns a
   * synchronous unsubscribe function that is safe to call before the
   * subscribe call has resolved.
   */
  trackSubscription(filters) {
    let sub = null;
    let cancelled = false;
    const subscribe = () => {
      void this.subscribeEvents(filters).then((s) => {
        if (cancelled) {
          void s.unsubscribe();
        } else {
          sub = s;
        }
      }).catch(() => {
      });
    };
    if (this.state === "new") {
      void this.open().then(subscribe, () => {
      });
    } else {
      subscribe();
    }
    return () => {
      cancelled = true;
      if (sub) {
        void sub.unsubscribe().catch(() => {
        });
      }
    };
  }
  removeActiveSub(entry) {
    const idx = this.activeSubs.indexOf(entry);
    if (idx !== -1) this.activeSubs.splice(idx, 1);
  }
  async restoreSubscriptions() {
    for (const sub of [...this.activeSubs]) {
      if (sub.cancelled || sub.pending) continue;
      try {
        const res = await this.eventSubscribe(sub.filters);
        if (sub.cancelled) {
          if (res.subscription_id) {
            void this.eventUnsubscribe(res.subscription_id).catch(() => {
            });
          }
          continue;
        }
        sub.serverId = res.subscription_id ?? "";
      } catch {
      }
    }
  }
  /* ---------------------------------------------------------------- */
  /* Typed methods (PROTOCOL.md §5)                                    */
  /* ---------------------------------------------------------------- */
  info(clientInfo) {
    return this.request("engine.info", { client_info: clientInfo ?? this.buildClientInfo() });
  }
  ping() {
    return this.request("engine.ping", {});
  }
  capabilities() {
    return this.request("engine.capabilities", {});
  }
  login(params = {}) {
    return this.request("auth.login", params, { noQueue: true }).then((res) => {
      if (res?.access_token) this.accessToken = res.access_token;
      if (res?.refresh_token) this.refreshToken = res.refresh_token;
      return res;
    });
  }
  refresh() {
    const rt = this.refreshToken;
    if (!rt) return Promise.reject(new HubError(SDK_CODES.AUTH_UNAVAILABLE, "no refresh_token available"));
    return this.request("auth.refresh", { refresh_token: rt }, { noQueue: true }).then(
      (res) => {
        if (res?.access_token) this.accessToken = res.access_token;
        if (res?.refresh_token) this.refreshToken = res.refresh_token;
        return res;
      },
      (err) => {
        if (err instanceof HubError && (err.code === HUB_CODES.AUTH_EXPIRED || err.code === HUB_CODES.AUTH_INVALID)) {
          this.refreshToken = void 0;
          this.accessToken = void 0;
        }
        throw err;
      }
    );
  }
  logout() {
    return this.request("auth.logout", {}, { noQueue: true }).then((res) => {
      this.accessToken = void 0;
      this.refreshToken = void 0;
      return res;
    });
  }
  nodeList(filter, limit, offset) {
    const params = {};
    if (filter) params["filter"] = filter;
    if (limit !== void 0) params["limit"] = limit;
    if (offset !== void 0) params["offset"] = offset;
    return this.request("node.list", params);
  }
  nodeGet(nodeId) {
    return this.request("node.get", { node_id: nodeId });
  }
  nodeSubscribe(nodeId) {
    const params = {};
    if (nodeId) params["node_id"] = nodeId;
    return this.request("node.subscribe", params);
  }
  nodeUnsubscribe(subscriptionId) {
    return this.request("node.unsubscribe", { subscription_id: subscriptionId });
  }
  commandSend(spec) {
    const params = {
      node_id: spec.node_id,
      method: spec.method,
      ...spec.params !== void 0 ? { params: spec.params } : {},
      ...spec.timeout !== void 0 ? { timeout: spec.timeout } : {},
      ...spec.idempotency_key !== void 0 ? { idempotency_key: spec.idempotency_key } : {}
    };
    return this.request("command.send", params);
  }
  commandBatch(specs, idempotencyKey) {
    const params = { commands: specs };
    if (idempotencyKey !== void 0) params["idempotency_key"] = idempotencyKey;
    return this.request("command.batch", params);
  }
  commandCancel(commandId) {
    const params = { command_id: commandId };
    return this.request("command.cancel", params);
  }
  taskCreate(spec) {
    return this.request("task.create", spec);
  }
  taskGet(taskId) {
    const params = { task_id: taskId };
    return this.request("task.get", params);
  }
  taskList(filter, limit, offset) {
    const params = {};
    if (filter) params["filter"] = filter;
    if (limit !== void 0) params["limit"] = limit;
    if (offset !== void 0) params["offset"] = offset;
    return this.request("task.list", params);
  }
  taskCancel(taskId) {
    const params = { task_id: taskId };
    return this.request("task.cancel", params);
  }
  taskSubscribe(taskId) {
    const params = {};
    if (taskId) params["task_id"] = taskId;
    return this.request("task.subscribe", params);
  }
  eventSubscribe(filters) {
    return this.request("event.subscribe", filters ?? {});
  }
  eventUnsubscribe(subscriptionId) {
    const params = { subscription_id: subscriptionId };
    return this.request("event.unsubscribe", params);
  }
  seatList(filter, limit, offset) {
    const params = {};
    if (filter) params["filter"] = filter;
    if (limit !== void 0) params["limit"] = limit;
    if (offset !== void 0) params["offset"] = offset;
    return this.request("seat.list", params);
  }
  seatCreate(spec = {}) {
    return this.request("seat.create", spec);
  }
  seatGet(seatId) {
    return this.request("seat.get", { seat_id: seatId });
  }
  seatDelete(seatId) {
    return this.request("seat.delete", { seat_id: seatId });
  }
  seatBind(seatId, nodeId) {
    return this.request("seat.bind", { seat_id: seatId, node_id: nodeId });
  }
  seatUnbind(seatId) {
    return this.request("seat.unbind", { seat_id: seatId });
  }
};
function eventMatchesFilters(filters, ev) {
  if (filters.event_types && filters.event_types.length > 0) {
    if (!ev.type || !filters.event_types.includes(ev.type)) return false;
  }
  if (filters.node_id && ev.node_id !== filters.node_id) return false;
  if (filters.task_id && ev.task_id !== filters.task_id) return false;
  return true;
}
function detectPlatform() {
  const g = globalThis;
  let runtime = "unknown";
  let os = "";
  let arch = "";
  const nav = g.navigator ?? void 0;
  if (nav?.userAgent) {
    if (/Electron\//.test(nav.userAgent)) runtime = "electron";
    else if (typeof g.Deno !== "undefined") runtime = "deno";
    else if (typeof g.Bun !== "undefined") runtime = "bun";
    else runtime = "browser";
    os = nav.platform ?? "";
  }
  const proc = g.process ?? void 0;
  if (proc?.versions?.node) {
    runtime = "node";
    os = proc.platform ?? "";
    arch = proc.arch ?? "";
  } else if (typeof g.Deno !== "undefined") {
    runtime = "deno";
  } else if (typeof g.Bun !== "undefined") {
    runtime = "bun";
  }
  if (!arch) {
    const ua = nav?.userAgent ?? "";
    if (ua.includes("arm64") || ua.includes("aarch64")) arch = "arm64";
    else if (ua.includes("x86_64") || ua.includes("x64")) arch = "x64";
  }
  return { os, runtime, arch };
}

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

// src/index.ts
var EVENT_TYPES = [
  "node.registered",
  "node.updated",
  "node.status.changed",
  "node.unregistered",
  "command.accepted",
  "command.started",
  "command.result",
  "command.failed",
  "command.cancelled",
  "task.created",
  "task.progress",
  "task.updated",
  "task.completed",
  "task.failed",
  "task.cancelled"
];

export { EVENT_TYPES, HUB_CODES, HttpTransport, HubClient, HubError, ListenerSet, PollTransport, RPC_CODES, SDK_CODES, SDK_VERSION, WebSocketTransport, createMainBridge, createPreloadAPI, eventsUrl, isHubError, normalizeBaseUrl, pollUrl, toWsUrl };
//# sourceMappingURL=index.mjs.map
//# sourceMappingURL=index.mjs.map