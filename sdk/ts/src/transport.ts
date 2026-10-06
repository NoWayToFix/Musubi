import type { HubEvent, RpcId, TransportKind } from './types';
import { HubError, SDK_CODES } from './errors';

/**
 * Transport — a stateless pipe to the Control Hub server.
 *
 * Connection, reconnection, transport fallback and offline queueing are the
 * HubClient's job; a Transport only knows how to:
 *   - connect() and close()
 *   - issue JSON-RPC requests (matched by id, timeout enforced)
 *   - send notifications (fire-and-forget, no id)
 *   - surface server-pushed events via onEvent()
 *   - report lifecycle via onOpen / onClose / onError (on/off style)
 */

/** A pending in-flight request. */
export interface PendingRequest {
  method: string;
  resolve: (value: unknown) => void;
  reject: (err: unknown) => void;
  timer: ReturnType<typeof setTimeout>;
}

/** Options handed to a Transport factory by HubClient. */
export interface TransportOptions {
  /** Base URL without path, e.g. "https://hub.example.com" (scheme normalized per transport). */
  baseUrl: string;
  /** Resolves the current bearer token (changes after login/refresh). */
  getToken: () => string | undefined;
  /** Heartbeat interval in ms. */
  pingIntervalMs?: number;
  /** Enable JSON-RPC batch coalescing (WS only). */
  batch?: boolean;
  /** Passthrough network options (proxy/ca/insecure). */
  proxy?: string;
  ca?: string;
  insecure?: boolean;
}

export interface Transport {
  readonly kind: TransportKind;
  /** Open the underlying connection (WS handshake / SSE stream / poll loop). */
  connect(): Promise<void>;
  /**
   * Send a JSON-RPC request and resolve with its `result`.
   * Rejects with HubError on RPC error, timeout or connection loss.
   */
  request<T = unknown>(method: string, params?: Record<string, unknown>, timeoutMs?: number): Promise<T>;
  /** Send a JSON-RPC notification (no id, no response). */
  sendNotification(method: string, params?: Record<string, unknown>): void;
  /** Register an event listener; returns an unsubscribe function. */
  onEvent(cb: (ev: HubEvent) => void): () => void;
  /** Register an open listener; returns an unsubscribe function. */
  onOpen(cb: () => void): () => void;
  /** Register a close listener (with optional reason); returns an unsubscribe function. */
  onClose(cb: (err?: Error) => void): () => void;
  /** Register an error listener; returns an unsubscribe function. */
  onError(cb: (err: Error) => void): () => void;
  close(): Promise<void>;
}

/**
 * Tiny listener registry implementing the on/off style.
 * Listener exceptions are isolated so one bad callback cannot break the pipe.
 */
export class ListenerSet<T> {
  private listeners = new Set<(a: T) => void>();

  on(cb: (a: T) => void): () => void {
    this.listeners.add(cb);
    return () => {
      this.listeners.delete(cb);
    };
  }

  emit(a: T): void {
    for (const cb of [...this.listeners]) {
      try {
        cb(a);
      } catch {
        /* swallow user-callback errors; never break the transport loop */
      }
    }
  }

  get size(): number {
    return this.listeners.size;
  }
}

/** Normalize a user-provided URL to a bare origin (scheme://host[:port]). */
export function normalizeBaseUrl(url: string): string {
  const trimmed = url.trim().replace(/\/+$/, '');
  // Strip any path so transports can append their own endpoints.
  const m = trimmed.match(/^([a-zA-Z][a-zA-Z0-9+.-]*:\/\/[^/]+)/);
  return m ? m[1] : trimmed;
}

/** Convert a base URL to the ws(s):// form used by /v1/ws. */
export function toWsUrl(baseUrl: string): string {
  const b = normalizeBaseUrl(baseUrl);
  if (b.startsWith('wss://') || b.startsWith('ws://')) return `${b}/v1/ws`;
  if (b.startsWith('https://')) return `${b.replace(/^https/, 'wss')}/v1/ws`;
  if (b.startsWith('http://')) return `${b.replace(/^http/, 'ws')}/v1/ws`;
  return `${b}/v1/ws`;
}

export interface FetchLike {
  (input: string | URL | Request, init?: Record<string, unknown>): Promise<ResponseLike>;
}

export interface ResponseLike {
  readonly ok: boolean;
  readonly status: number;
  readonly statusText: string;
  headers: { get(name: string): string | null };
  json(): Promise<unknown>;
  text(): Promise<string>;
  body: ReadableStreamLike | null;
}

export interface ReadableStreamLike {
  getReader(): {
    read(): Promise<{ done: boolean; value?: Uint8Array | string }>;
    cancel?(): Promise<void>;
    releaseLock?(): void;
  };
}

/** Resolve a fetch implementation (browser, Node≥18, Deno, Bun all have it). */
export function resolveFetch(): FetchLike {
  const g = globalThis as Record<string, unknown>;
  if (typeof g.fetch === 'function') return g.fetch as FetchLike;
  throw new HubError(SDK_CODES.NO_TRANSPORT, 'fetch is not available in this runtime');
}

/**
 * Shared JSON-RPC-over-HTTP request helper used by HttpTransport and
 * PollTransport: POST a single envelope to /v1/rpc with Bearer auth.
 */
export async function postRpc(
  baseUrl: string,
  method: string,
  params: Record<string, unknown> | undefined,
  getToken: () => string | undefined,
  timeoutMs: number
): Promise<unknown> {
  const fetch = resolveFetch();
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    Accept: 'application/json'
  };
  const token = getToken();
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  let res: ResponseLike;
  try {
    res = await fetch(`${normalizeBaseUrl(baseUrl)}/v1/rpc`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: nextHttpId(),
        method,
        ...(params !== undefined ? { params } : {})
      }),
      signal: ctrl.signal
    });
  } catch (err) {
    const aborted = (err as { name?: string }).name === 'AbortError';
    throw new HubError(aborted ? SDK_CODES.TIMEOUT : SDK_CODES.NOT_CONNECTED, `http request failed: ${String(err)}`, err);
  } finally {
    clearTimeout(timer);
  }

  if (!res.ok) {
    // Non-2xx: try to surface an RPC-style error if the body carries one.
    let body: unknown;
    try {
      body = await res.json();
    } catch {
      body = undefined;
    }
    const env = (body as { error?: { code?: number; message?: string; data?: unknown } })?.error;
    if (env && typeof env.code === 'number') throw HubError.fromRpc(env as never);
    if (res.status === 401) {
      throw new HubError(SDK_CODES.NOT_CONNECTED, `http ${res.status} unauthorized`, res.status);
    }
    throw new HubError(SDK_CODES.NOT_CONNECTED, `http ${res.status} ${res.statusText}`, body);
  }

  const payload = (await res.json()) as {
    result?: unknown;
    error?: { code?: number; message?: string; data?: unknown };
  };
  if (payload.error && typeof payload.error.code === 'number') {
    throw HubError.fromRpc(payload.error as never);
  }
  return payload.result;
}

let httpIdSeq = 0;
function nextHttpId(): number {
  httpIdSeq += 1;
  return httpIdSeq;
}

/** Build the event URL for SSE / long-poll endpoints. */
export function eventsUrl(baseUrl: string, path: string, query: Record<string, string | number | undefined>): string {
  const u = new URL(`${normalizeBaseUrl(baseUrl)}${path}`);
  for (const [k, v] of Object.entries(query)) {
    if (v !== undefined) u.searchParams.set(k, String(v));
  }
  return u.toString();
}

/** Alias used by PollTransport (same URL builder). */
export const pollUrl = eventsUrl;

/** Factory signature used by HubClient to create transports. */
export type TransportFactory = (options: TransportOptions) => Transport;