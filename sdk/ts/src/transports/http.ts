import {
  ListenerSet,
  eventsUrl,
  normalizeBaseUrl,
  postRpc,
  resolveFetch,
  type ResponseLike,
  type Transport,
  type TransportOptions
} from '../transport';
import { HubError, SDK_CODES } from '../errors';
import type { HubEvent } from '../types';

/* ------------------------------------------------------------------ */
/* HTTP transport: POST /v1/rpc + SSE /v1/events                       */
/* ------------------------------------------------------------------ */

/** Minimal structural type for EventSource (browser or polyfill). */
export interface EventSourceLike {
  onopen: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  onmessage: ((ev: unknown) => void) | null;
  close(): void;
  readyState: number;
}

export interface EventSourceCtor {
  new (url: string): EventSourceLike;
}

const ES_CONNECTING = 0;
const ES_OPEN = 1;
const ES_CLOSED = 2;

/**
 * HTTP transport:
 *  - request():  POST a JSON-RPC envelope to /v1/rpc with Bearer auth.
 *  - events():   SSE stream from /v1/events.
 *      * browsers: EventSource (token passed via ?token= — EventSource cannot
 *        set headers).
 *      * Node:     fetch() + ReadableStream parsing of text/event-stream
 *        (`data:` lines → JSON.parse). Bearer header is used here.
 *
 * Connection/reconnect/fallback logic lives in HubClient; this transport is a
 * stateless pipe. `connect()` establishes the SSE stream and resolves once the
 * stream is open.
 */
export class HttpTransport implements Transport {
  readonly kind = 'http' as const;

  private baseUrl: string;
  private options: TransportOptions;
  private sseController: AbortController | null = null;
  private eventSource: EventSourceLike | null = null;
  private connectPromise: Promise<void> | null = null;

  // listeners
  private onEventL = new ListenerSet<HubEvent>();
  private onOpenL = new ListenerSet<void>();
  private onCloseL = new ListenerSet<Error | undefined>();
  private onErrorL = new ListenerSet<Error>();

  constructor(options: TransportOptions) {
    this.baseUrl = normalizeBaseUrl(options.baseUrl);
    this.options = options;
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

  request<T = unknown>(method: string, params?: Record<string, unknown>, timeoutMs?: number): Promise<T> {
    return postRpc(this.baseUrl, method, params, () => this.options.getToken(), timeoutMs ?? 30000) as Promise<T>;
  }

  sendNotification(method: string, params?: Record<string, unknown>): void {
    // Fire-and-forget over the RPC endpoint; used as heartbeat on HTTP.
    void postRpc(this.baseUrl, method, params, () => this.options.getToken(), 5000).catch(() => {
      /* heartbeat failures are ignored (PROTOCOL §8.6) */
    });
  }

  private async doConnect(): Promise<void> {
    const g = globalThis as { EventSource?: unknown };
    if (typeof g.EventSource === 'function') {
      await this.connectViaEventSource(g.EventSource as EventSourceCtor);
    } else {
      await this.connectViaFetch();
    }
    this.onOpenL.emit();
  }

  /* ---------------- browser: EventSource ---------------- */

  private connectViaEventSource(Ctor: EventSourceCtor): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const url = eventsUrl(this.baseUrl, '/v1/events', {
        token: this.options.getToken()
      });
      const es = new Ctor(url);
      this.eventSource = es;
      let settled = false;

      es.onopen = (): void => {
        if (settled) return;
        settled = true;
        resolve();
      };
      es.onerror = (ev: unknown): void => {
        if (!settled) {
          settled = true;
          reject(new HubError(SDK_CODES.NOT_CONNECTED, `sse connect failed: ${String(ev)}`, ev));
          return;
        }
        this.onErrorL.emit(new HubError(SDK_CODES.NOT_CONNECTED, `sse error: ${String(ev)}`, ev));
        // EventSource auto-reconnects; only when it gives up (CLOSED) do we
        // tell the client so it can run its own reconnect/fallback logic.
        if (es.readyState === ES_CLOSED) {
          this.onCloseL.emit(new HubError(SDK_CODES.NOT_CONNECTED, 'sse connection closed'));
        }
      };
      es.onmessage = (ev: unknown): void => {
        const raw = (ev as { data?: string }).data;
        this.dispatchSseLine(raw ?? '');
      };
    });
  }

  /* ---------------- node: fetch ReadableStream ---------------- */

  private async connectViaFetch(): Promise<void> {
    const fetch = resolveFetch();
    const ctrl = new AbortController();
    this.sseController = ctrl;

    const url = eventsUrl(this.baseUrl, '/v1/events', { token: this.options.getToken() });
    const headers: Record<string, string> = {
      Accept: 'text/event-stream',
      'Cache-Control': 'no-cache'
    };
    const token = this.options.getToken();
    if (token) headers['Authorization'] = `Bearer ${token}`;

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
      throw new HubError(SDK_CODES.NOT_CONNECTED, 'sse connect failed: response has no body');
    }

    // Stream established. Run the read loop in the background — connect()
    // must resolve here, not when the stream ends.
    void this.readSseStream(res, ctrl);
  }

  /** Background SSE reader: text/event-stream → `data:` lines → JSON.parse. */
  private async readSseStream(res: ResponseLike, ctrl: AbortController): Promise<void> {
    const reader = res.body?.getReader();
    if (!reader) return;
    const decoder = new TextDecoder();
    let buffer = '';

    // eslint-disable-next-line no-constant-condition
    while (true) {
      let chunk;
      try {
        chunk = await reader.read();
      } catch (err) {
        if (ctrl.signal.aborted) break; // intentional close
        this.onErrorL.emit(new HubError(SDK_CODES.NOT_CONNECTED, `sse stream error: ${String(err)}`, err));
        this.onCloseL.emit(new HubError(SDK_CODES.NOT_CONNECTED, `sse stream closed: ${String(err)}`, err));
        break;
      }
      if (chunk.done) {
        if (!ctrl.signal.aborted) {
          this.onCloseL.emit(new HubError(SDK_CODES.NOT_CONNECTED, 'sse stream ended'));
        }
        break;
      }
      const value = chunk.value;
      const text = typeof value === 'string' ? value : decoder.decode(value, { stream: true });
      buffer += text;
      // SSE: events are separated by blank lines; dispatch complete blocks.
      let idx;
      while ((idx = buffer.indexOf('\n\n')) !== -1) {
        const block = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        this.dispatchSseBlock(block);
      }
    }
  }

  /** Dispatch a single SSE block (may contain several `data:` lines). */
  private dispatchSseBlock(block: string): void {
    const dataLines: string[] = [];
    for (const line of block.split('\n')) {
      if (line.startsWith('data:')) {
        dataLines.push(line.slice(5).trimStart());
      }
    }
    if (dataLines.length === 0) return;
    this.dispatchSseLine(dataLines.join('\n'));
  }

  /** JSON.parse a `data:` payload — must be a JSON-RPC notification ("event"). */
  private dispatchSseLine(raw: string): void {
    if (!raw) return;
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      this.onErrorL.emit(new HubError(SDK_CODES.ENCODING_NOT_SUPPORTED, 'encoding not supported: invalid SSE data'));
      return;
    }
    const obj = parsed as { method?: string; params?: { event?: HubEvent } };
    if (obj && typeof obj.method === 'string' && obj.method === 'event' && obj.params?.event) {
      this.onEventL.emit(obj.params.event);
    }
    // Unknown notification methods are ignored (compatibility rule).
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

  async close(): Promise<void> {
    if (this.sseController) {
      this.sseController.abort();
      this.sseController = null;
    }
    if (this.eventSource) {
      try {
        this.eventSource.close();
      } catch {
        /* noop */
      }
      this.eventSource = null;
    }
  }
}