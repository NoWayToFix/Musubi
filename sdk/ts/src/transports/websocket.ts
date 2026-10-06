import {
  ListenerSet,
  normalizeBaseUrl,
  toWsUrl,
  type PendingRequest,
  type Transport,
  type TransportOptions
} from '../transport';
import { HubError, SDK_CODES } from '../errors';
import type { HubEvent, RpcId, RpcMessage, RpcNotification, RpcResponse, RpcRequest } from '../types';

/* ------------------------------------------------------------------ */
/* WebSocket constructor auto-detection                                */
/* ------------------------------------------------------------------ */

/** Minimal structural type for the WebSocket objects we accept. */
export interface WsLike {
  readyState: number;
  send(data: string | ArrayBuffer | ArrayBufferView): void;
  close(code?: number, reason?: string): void;
  ping?(data?: unknown): void;
  onopen: ((ev: unknown) => void) | null;
  onmessage: ((ev: unknown) => void) | null;
  onclose: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
}

export interface WsCtor {
  new (url: string, protocols?: string | string[]): WsLike;
  OPEN?: number;
}

const WS_OPEN = 1;

/**
 * Resolve the best available WebSocket implementation:
 *  1. globalThis.WebSocket — browsers, Node >= 21, Deno, Bun
 *  2. require('ws')        — Node 18/20 / Electron (CJS build)
 *  3. import('ws')         — Node 18/20 ESM builds
 * Returns null when nothing is available (caller throws a clear error).
 */
async function resolveWsCtor(): Promise<WsCtor | null> {
  const g = globalThis as { WebSocket?: unknown };
  if (typeof g.WebSocket === 'function') return g.WebSocket as WsCtor;

  // CJS / Electron: `require` may exist on globalThis (Electron main) or
  // lexically inside the CJS bundle. Guarded by typeof so ESM/browser stay safe.
  const nodeRequire =
    (globalThis as { require?: unknown }).require ??
    (typeof require === 'function' ? (require as (id: string) => unknown) : undefined);
  if (nodeRequire) {
    try {
      const mod = (nodeRequire as (id: string) => unknown)('ws') as {
        WebSocket?: WsCtor;
        default?: WsCtor;
      };
      return mod.WebSocket ?? mod.default ?? null;
    } catch {
      /* ws not installed — fall through */
    }
  }

  // ESM build on Node 18/20: dynamic import. The specifier is built
  // dynamically so bundlers (webpack/vite/rollup) leave it alone instead of
  // trying to resolve it at build time.
  try {
    const spec = 'w' + 's';
    const mod = (await import(/* webpackIgnore: true */ /* @vite-ignore */ spec)) as {
      WebSocket?: WsCtor;
      default?: WsCtor;
    };
    return mod.WebSocket ?? mod.default ?? null;
  } catch {
    return null;
  }
}

/* ------------------------------------------------------------------ */
/* WebSocketTransport                                                 */
/* ------------------------------------------------------------------ */

interface BatchItem {
  id: RpcId;
  method: string;
  params?: Record<string, unknown>;
  entry: PendingRequest;
}

/**
 * Primary transport: JSON-RPC frames over a WebSocket at /v1/ws.
 *
 * - Parses single-envelope and batch (array) frames.
 * - Binary frames are a protocol violation (we only negotiate "json") — throws
 *   "encoding not supported" and drops the connection.
 * - Heartbeat: uses protocol-level ws.ping() when available (the `ws` package),
 *   otherwise sends engine.ping notifications every pingIntervalMs.
 * - Optional batch coalescing: pending requests are flushed as one array frame
 *   on the next macrotask (requires server features.batch).
 */
export class WebSocketTransport implements Transport {
  readonly kind = 'ws' as const;

  private baseUrl: string;
  private options: TransportOptions;
  private ctor: WsCtor | null = null;
  private ws: WsLike | null = null;
  private pending = new Map<RpcId, PendingRequest>();
  private idSeq = 0;
  private connectPromise: Promise<void> | null = null;

  // batch coalescing
  private batchEnabled: boolean;
  private batchQueue: BatchItem[] = [];
  private batchTimer: ReturnType<typeof setTimeout> | null = null;

  // heartbeat
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private heartbeatType: 'ws' | 'rpc' = 'rpc';

  // listeners
  private onEventL = new ListenerSet<HubEvent>();
  private onOpenL = new ListenerSet<void>();
  private onCloseL = new ListenerSet<Error | undefined>();
  private onErrorL = new ListenerSet<Error>();

  constructor(options: TransportOptions) {
    this.baseUrl = normalizeBaseUrl(options.baseUrl);
    this.options = options;
    this.batchEnabled = options.batch === true;
  }

  connect(): Promise<void> {
    if (this.connectPromise) return this.connectPromise;
    this.connectPromise = this.doConnect()
      .catch((err: unknown) => {
        this.connectPromise = null;
        throw err;
      })
      .finally(() => {
        this.connectPromise = null;
      });
    return this.connectPromise;
  }

  private async doConnect(): Promise<void> {
    const ctor = this.ctor ?? (await resolveWsCtor());
    if (!ctor) {
      throw new HubError(
        SDK_CODES.NO_TRANSPORT,
        'no WebSocket implementation available (browser/Node>=21/Deno/Bun have globalThis.WebSocket; ' +
          'Node 18/20 need the "ws" package installed)'
      );
    }
    this.ctor = ctor;

    const url = toWsUrl(this.baseUrl);
    const ws = new ctor(url);
    this.ws = ws;

    let settled = false;

    // --- open promise: resolves when the handshake completes ----------
    const opened = new Promise<void>((resolve, reject) => {
      ws.onopen = (): void => {
        if (settled) return;
        settled = true;
        // Errors after open are handled by the live onerror handler.
        ws.onerror = null;
        this.heartbeatType = typeof ws.ping === 'function' ? 'ws' : 'rpc';
        this.startPing();
        this.onOpenL.emit();
        resolve();
      };
      const fail = (err: Error): void => {
        if (settled) return;
        settled = true;
        this.onErrorL.emit(err);
        reject(err);
      };
      ws.onerror = (ev: unknown): void => {
        fail(new HubError(SDK_CODES.NOT_CONNECTED, `websocket connect failed: ${String(ev)}`, ev));
      };
      ws.onclose = (ev: unknown): void => {
        const code = (ev as { code?: number }).code;
        fail(new HubError(SDK_CODES.NOT_CONNECTED, `websocket closed before open (code ${code ?? '?'})`, ev));
      };
    });

    // --- live handlers: active for the whole lifetime of the socket ----
    ws.onmessage = (ev: unknown): void => {
      const data = (ev as { data?: unknown }).data;
      try {
        if (typeof data !== 'string') {
          throw new HubError(
            SDK_CODES.ENCODING_NOT_SUPPORTED,
            'encoding not supported: binary websocket frame received (only "json" was negotiated)',
            data
          );
        }
        this.handleFrame(data);
      } catch (err) {
        this.onErrorL.emit(err as Error);
        // Protocol violation: drop the connection so the client can recover.
        this.forceClose();
      }
    };
    ws.onclose = (ev: unknown): void => {
      if (!settled) return; // pre-open close is handled by the open promise
      const code = (ev as { code?: number }).code;
      const reason = (ev as { reason?: string }).reason;
      const err = new HubError(
        SDK_CODES.NOT_CONNECTED,
        `websocket closed (code ${code ?? '?'}${reason ? `, ${reason}` : ''})`,
        ev
      );
      this.ws = null;
      this.rejectAll(err);
      this.stopPing();
      this.onCloseL.emit(err);
    };
    ws.onerror = (ev: unknown): void => {
      if (!settled) return; // pre-open errors are handled by the open promise
      this.onErrorL.emit(new HubError(SDK_CODES.NOT_CONNECTED, `websocket error: ${String(ev)}`, ev));
    };

    // Also catch sync constructor errors (e.g. invalid URL).
    return opened.catch((err: unknown) => {
      try {
        ws.close();
      } catch {
        /* noop */
      }
      throw err;
    });
  }

  private startPing(): void {
    if (this.pingTimer) return;
    const interval = this.options.pingIntervalMs ?? 30000;
    this.pingTimer = setInterval(() => {
      const ws = this.ws;
      if (!ws || ws.readyState !== WS_OPEN) return;
      try {
        if (this.heartbeatType === 'ws') {
          ws.ping?.();
        } else {
          // JSON-RPC engine.ping as a notification (fire-and-forget).
          ws.send(JSON.stringify({ jsonrpc: '2.0', method: 'engine.ping', params: {} }));
        }
      } catch {
        /* ignore heartbeat errors */
      }
    }, interval);
  }

  request<T = unknown>(method: string, params?: Record<string, unknown>, timeoutMs?: number): Promise<T> {
    const timeout = timeoutMs ?? 30000;
    return new Promise<T>((resolve, reject) => {
      const ws = this.ws;
      if (!ws || ws.readyState !== WS_OPEN) {
        reject(new HubError(SDK_CODES.NOT_CONNECTED, `websocket transport not connected (${method})`));
        return;
      }
      const id: RpcId = ++this.idSeq;
      const entry: PendingRequest = {
        method,
        resolve: (v: unknown) => resolve(v as T),
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
        ws.send(JSON.stringify({ jsonrpc: '2.0', id, method, ...(params !== undefined ? { params } : {}) }));
      }
    });
  }

  private flushBatch(): void {
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
    const frames: RpcRequest[] = queue.map((item) => ({
      jsonrpc: '2.0' as const,
      id: item.id,
      method: item.method,
      ...(item.params !== undefined ? { params: item.params } : {})
    }));
    ws.send(JSON.stringify(frames));
  }

  sendNotification(method: string, params?: Record<string, unknown>): void {
    const ws = this.ws;
    if (!ws || ws.readyState !== WS_OPEN) return;
    ws.send(JSON.stringify({ jsonrpc: '2.0', method, ...(params !== undefined ? { params } : {}) }));
  }

  /**
   * Parse a text frame: single envelope or batch array. Responses are matched
   * by id against pending requests; notifications with method "event" are
   * unpacked and emitted through the event listeners. Unknown notifications
   * are ignored (compatibility rule).
   */
  private handleFrame(text: string): void {
    let msg: RpcMessage;
    try {
      msg = JSON.parse(text) as RpcMessage;
    } catch {
      this.onErrorL.emit(new HubError(SDK_CODES.ENCODING_NOT_SUPPORTED, 'encoding not supported: invalid JSON frame'));
      return;
    }
    if (Array.isArray(msg)) {
      for (const item of msg) this.handleEnvelope(item as RpcMessage);
    } else {
      this.handleEnvelope(msg);
    }
  }

  private handleEnvelope(item: RpcMessage): void {
    if (!item || typeof item !== 'object' || Array.isArray(item)) return;
    const obj = item as RpcResponse & RpcNotification;
    // Notification: no id. Only "event" notifications matter; others ignored.
    if (obj.id === undefined && typeof obj.method === 'string') {
      if (obj.method === 'event') {
        const ev = (obj.params as { event?: HubEvent } | undefined)?.event;
        if (ev) this.onEventL.emit(ev);
      }
      return;
    }
    // Response (may arrive out of order; match by id).
    if (obj.id !== undefined) {
      const entry = this.pending.get(obj.id);
      if (!entry) return; // stale response (after timeout) — ignore
      this.pending.delete(obj.id);
      clearTimeout(entry.timer);
      if (obj.error && typeof obj.error.code === 'number') {
        entry.reject(HubError.fromRpc(obj.error as never));
      } else {
        entry.resolve(obj.result);
      }
    }
  }

  onEvent(cb: (ev: HubEvent) => void): () => void {
    return this.onEventL.on(cb);
  }
  onOpen(cb: () => void): () => void {
    return this.onOpenL.on(cb);
  }
  onClose(cb: (err?: Error) => void): () => void {
    return this.onCloseL.on(cb);
  }
  onError(cb: (err: Error) => void): () => void {
    return this.onErrorL.on(cb);
  }

  private forceClose(): void {
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
        /* noop */
      }
    }
    this.rejectAll(new HubError(SDK_CODES.NOT_CONNECTED, 'websocket connection lost'));
    this.stopPing();
    this.onCloseL.emit(new HubError(SDK_CODES.NOT_CONNECTED, 'websocket connection lost'));
  }

  private rejectAll(err: Error): void {
    for (const [id, entry] of [...this.pending]) {
      this.pending.delete(id);
      clearTimeout(entry.timer);
      entry.reject(err);
    }
  }

  private stopPing(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  async close(): Promise<void> {
    this.stopPing();
    this.rejectAll(new HubError(SDK_CODES.CLOSED, 'transport closed by client'));
    const ws = this.ws;
    this.ws = null;
    if (ws) {
      try {
        ws.close(1000, 'client close');
      } catch {
        /* noop */
      }
    }
  }
}