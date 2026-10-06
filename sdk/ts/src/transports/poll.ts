import {
  ListenerSet,
  normalizeBaseUrl,
  pollUrl,
  postRpc,
  resolveFetch,
  type Transport,
  type TransportOptions
} from '../transport';
import { HubError, SDK_CODES } from '../errors';
import type { HubEvent } from '../types';

/**
 * Poll transport: long-polling fallback.
 *
 * - Events: GET /v1/poll?cursor=N&timeout=30&token=...
 *   Response: {"cursor": N, "events": [...]} (or empty events list).
 * - RPC: POST /v1/rpc (same as HttpTransport).
 *
 * The poll loop runs until close(); every response advances the cursor.
 * Network failures surface through onError + onClose so HubClient can run its
 * own reconnect/backoff policy.
 */
export class PollTransport implements Transport {
  readonly kind = 'poll' as const;

  private baseUrl: string;
  private options: TransportOptions;
  private cursor: number | null = null;
  private stopped = false;
  private loopPromise: Promise<void> | null = null;
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

  private async doConnect(): Promise<void> {
    this.stopped = false;
    this.cursor = null;
    // Start the poll loop in the background; connect() resolves immediately
    // because a long-poll endpoint is always reachable (first poll proves it).
    this.loopPromise = this.pollLoop();
    this.onOpenL.emit();
  }

  request<T = unknown>(method: string, params?: Record<string, unknown>, timeoutMs?: number): Promise<T> {
    return postRpc(this.baseUrl, method, params, () => this.options.getToken(), timeoutMs ?? 30000) as Promise<T>;
  }

  sendNotification(method: string, params?: Record<string, unknown>): void {
    void postRpc(this.baseUrl, method, params, () => this.options.getToken(), 5000).catch(() => {
      /* heartbeat failures are ignored */
    });
  }

  private async pollLoop(): Promise<void> {
    const fetch = resolveFetch();
    let consecutiveErrors = 0;
    const pollTimeout = 35_000; // server holds up to 30s; give it 5s slack
    const holdTimeout = 30;

    while (!this.stopped) {
      const params: Record<string, number | string> = { timeout: String(holdTimeout) };
      if (this.cursor !== null) params['cursor'] = String(this.cursor);
      const token = this.options.getToken();
      if (token) params['token'] = token;

      const url = pollUrl(this.baseUrl, '/v1/poll', params);
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), pollTimeout);

      try {
        const res = await fetch(url, { signal: ctrl.signal });
        if (this.stopped) break;
        if (res.status === 401) {
          this.onErrorL.emit(new HubError(SDK_CODES.NOT_CONNECTED, 'poll 401 unauthorized'));
          this.onCloseL.emit(new HubError(SDK_CODES.NOT_CONNECTED, 'poll 401 unauthorized'));
          break;
        }
        if (!res.ok) {
          throw new HubError(SDK_CODES.NOT_CONNECTED, `poll http ${res.status} ${res.statusText}`);
        }
        const payload = (await res.json()) as {
          cursor?: number;
          events?: HubEvent[];
        };
        if (typeof payload.cursor === 'number') this.cursor = payload.cursor;
        for (const ev of payload.events ?? []) {
          if (ev) this.onEventL.emit(ev);
        }
        consecutiveErrors = 0;
      } catch (err) {
        if (this.stopped) break;
        const aborted = (err as { name?: string }).name === 'AbortError';
        if (aborted) {
          // A single aborted long-poll is normal (server idle timeout); retry.
          continue;
        }
        consecutiveErrors += 1;
        this.onErrorL.emit(err as Error);
        if (consecutiveErrors >= 2) {
          this.onCloseL.emit(err as Error);
          break;
        }
      } finally {
        clearTimeout(timer);
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

  async close(): Promise<void> {
    this.stopped = true;
    if (this.loopPromise) {
      try {
        await this.loopPromise;
      } catch {
        /* noop */
      }
      this.loopPromise = null;
    }
  }
}