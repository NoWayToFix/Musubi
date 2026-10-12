import type {
  AuthResult,
  CapabilitiesResult,
  ClientInfo,
  CommandBatchParams,
  CommandBatchResult,
  CommandCancelParams,
  CommandCancelResult,
  CommandSendParams,
  CommandSendResult,
  CommandSpec,
  HubClientOptions,
  HubEvent,
  LoginParams,
  NodeFilter,
  NodeGetResult,
  NodeListParams,
  NodeListResult,
  NodeSubscribeParams,
  PingResult,
  PlatformInfo,
  RequestOptions,
  SeatCreateParams,
  SeatFilter,
  SeatListParams,
  SeatListResult,
  SeatResult,
  ServerInfo,
  SubscribeParams,
  SubscribeResult,
  Subscription,
  TaskCancelParams,
  TaskCancelResult,
  TaskCreateParams,
  TaskCreateResult,
  TaskGetParams,
  TaskGetResult,
  TaskListParams,
  TaskListResult,
  TaskSubscribeParams,
  TransportKind,
  UnsubscribeParams,
  UnsubscribeResult
} from './types';
import { HubError, HUB_CODES, SDK_CODES } from './errors';
import type { Transport, TransportOptions } from './transport';
import { WebSocketTransport } from './transports/websocket';
import { HttpTransport } from './transports/http';
import { PollTransport } from './transports/poll';

/** Version of this SDK (mirrors package.json). */
export const SDK_VERSION = '1.0.0';

const DEFAULT_TRANSPORT_ORDER: TransportKind[] = ['ws', 'http', 'poll'];
const QUEUE_LIMIT = 1000;
const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30_000;

type ClientState = 'new' | 'connecting' | 'open' | 'reconnecting' | 'closed';

interface QueuedRequest {
  method: string;
  params: Record<string, unknown> | undefined;
  resolve: (value: unknown) => void;
  reject: (err: unknown) => void;
}

interface ActiveSub {
  filters: SubscribeParams;
  serverId?: string;
  /** Initial event.subscribe still queued/in flight (skipped during restore). */
  pending: boolean;
  /** Set once unsubscribe() ran; must never be re-registered or restored. */
  cancelled: boolean;
}

/**
 * HubClient — the Control Hub SDK entry point.
 *
 * Responsibilities (see docs/PROTOCOL.md):
 *  - engine.info negotiation (protocol 1.0, encoding json, transport pick by
 *    transportOrder ∩ server_info.transports, features merged)
 *  - authentication (auto login with credentials, refresh on -32003)
 *  - transport fallback ws → sse (http) → poll
 *  - auto-reconnect with exponential backoff (base 1s, ×2, max 30s, ±20% jitter)
 *  - offline queue for write operations (cap 1000 FIFO; engine.* and auth.* excluded)
 *  - event dedupe by event.id (window 5000)
 *  - typed methods for every protocol method, subscription helpers, event on/off
 */
export class HubClient {
  private opts: HubClientOptions & {
    requestTimeoutMs: number;
    pingIntervalMs: number;
    dedupeWindow: number;
    queueOffline: boolean;
    autoReconnect: boolean;
    maxRetries: number;
    transportOrder: TransportKind[];
  };

  private state: ClientState = 'new';
  private closed = false;
  private connectPromise: Promise<void> | null = null;

  private transport: Transport | null = null;
  private serverInfoValue: ServerInfo | null = null;
  private featuresValue: Record<string, boolean> = {};

  private accessToken?: string;
  private refreshToken?: string;

  // event routing + dedupe
  private listeners = new Map<string, Set<(ev: HubEvent) => void>>();
  private seenIds: string[] = [];
  private seenSet = new Set<string>();

  // offline queue + reconnect
  private queue: QueuedRequest[] = [];
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  // subscriptions to restore after reconnect
  private activeSubs: ActiveSub[] = [];

  constructor(options: HubClientOptions = {}) {
    this.opts = {
      url: options.url ?? 'http://localhost:8080',
      token: options.token,
      tokenProvider: options.tokenProvider,
      credentials: options.credentials,
      transportOrder: options.transportOrder ?? DEFAULT_TRANSPORT_ORDER,
      autoReconnect: options.autoReconnect ?? true,
      maxRetries: options.maxRetries ?? Infinity,
      queueOffline: options.queueOffline ?? true,
      requestTimeoutMs: options.requestTimeoutMs ?? 30_000,
      pingIntervalMs: options.pingIntervalMs ?? 30_000,
      // Clamp to >= 0: a negative window previously drove dedupeEvent into a
      // non-terminating loop. Non-finite values fall back to the default.
      dedupeWindow:
        typeof options.dedupeWindow === 'number' && Number.isFinite(options.dedupeWindow)
          ? Math.max(0, Math.floor(options.dedupeWindow))
          : 5000,
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
  open(): Promise<void> {
    if (this.closed) return Promise.reject(new HubError(SDK_CODES.CLOSED, 'client is closed'));
    if (this.state === 'open') return Promise.resolve();
    if (this.connectPromise) return this.connectPromise;

    this.connectPromise = this.initialConnect().finally(() => {
      this.connectPromise = null;
    });
    return this.connectPromise;
  }

  private async initialConnect(): Promise<void> {
    this.state = 'connecting';
    try {
      await this.establish();
      this.reconnectAttempt = 0;
      this.state = 'open';
      await this.ensureAuth();
      await this.restoreSubscriptions();
      await this.flushQueue();
    } catch (err) {
      if (this.isFatalError(err)) {
        this.state = 'closed';
        this.closed = true;
        throw err;
      }
      // Non-fatal (server down, network): keep retrying in the background.
      this.state = 'reconnecting';
      if (this.opts.autoReconnect) this.scheduleReconnect();
      throw err;
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    this.state = 'closed';
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.rejectQueued(new HubError(SDK_CODES.CLOSED, 'client closed'));
    const t = this.transport;
    this.transport = null;
    if (t) {
      try {
        await t.close();
      } catch {
        /* noop */
      }
    }
  }

  /* ---------------------------------------------------------------- */
  /* Negotiation / transport                                          */
  /* ---------------------------------------------------------------- */

  /** Negotiated server_info from the last engine.info. */
  get serverInfo(): ServerInfo | null {
    return this.serverInfoValue;
  }

  /** Merged features (client ∩ server). */
  get features(): Record<string, boolean> {
    return this.featuresValue;
  }

  /** Currently active transport kind, or null. */
  get transportKind(): TransportKind | null {
    return this.transport?.kind ?? null;
  }

  private buildClientInfo(): ClientInfo {
    return {
      name: 'musubi-hub-sdk',
      version: SDK_VERSION,
      protocol_versions: ['1.0'],
      encodings: ['json'],
      transports: this.opts.transportOrder,
      features: {
        batch: this.opts.batch === true,
        subscribe: true
      },
      platform: detectPlatform()
    };
  }

  private createTransport(kind: TransportKind): Transport {
    const options: TransportOptions = {
      baseUrl: this.opts.url ?? 'http://localhost:8080',
      getToken: () => this.accessToken,
      pingIntervalMs: this.opts.pingIntervalMs,
      batch: this.opts.batch,
      proxy: this.opts.proxy,
      ca: this.opts.ca,
      insecure: this.opts.insecure
    };
    switch (kind) {
      case 'ws':
        return new WebSocketTransport(options);
      case 'http':
        return new HttpTransport(options);
      case 'poll':
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
  private async establish(): Promise<void> {
    await this.teardownTransport();

    const order = this.opts.transportOrder;
    const errors: string[] = [];

    for (const kind of order) {
      const t = this.createTransport(kind);
      // Bind event/close/error listeners BEFORE connect: transports may start
      // delivering events as soon as the underlying stream/socket opens (SSE
      // writes events immediately on connect), and we must not lose them.
      this.bindTransport(t);
      try {
        await t.connect();
        this.transport = t;
        const info = await this.negotiate(t);
        // If the server advertises a transport we support and it differs from
        // the one we probed with, switch (one hop max).
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
        // Server says "transport not supported" — try the next one.
        if (err instanceof HubError && err.code === HUB_CODES.TRANSPORT_NOT_SUPPORTED) continue;
        errors.push(`${kind}: ${(err as Error).message}`);
      }
    }
    throw new HubError(
      SDK_CODES.NO_TRANSPORT,
      `no transport could be established: ${errors.join('; ') || 'server closed all attempts'}`
    );
  }

  private async negotiate(t: Transport): Promise<ServerInfo> {
    const info = await t.request<ServerInfo>('engine.info', { client_info: this.buildClientInfo() });
    this.serverInfoValue = info;
    this.featuresValue = { ...(this.buildClientInfo().features ?? {}), ...(info.features ?? {}) };
    return info;
  }

  private isFatalError(err: unknown): boolean {
    if (err instanceof HubError) {
      // NOT_CONNECTED is retryable; NO_TRANSPORT / protocol / encoding are not.
      return (
        err.code === HUB_CODES.PROTOCOL_VERSION_MISMATCH ||
        err.code === HUB_CODES.ENCODING_NOT_SUPPORTED ||
        err.code === SDK_CODES.NO_TRANSPORT
      );
    }
    return false;
  }

  private pickTransport(info: ServerInfo): TransportKind | null {
    const server = new Set(info.transports ?? []);
    for (const kind of this.opts.transportOrder) {
      if (server.has(kind)) return kind;
    }
    return null;
  }

  private bindTransport(t: Transport): void {
    t.onEvent((ev) => this.dispatchEvent(ev));
    t.onClose((err) => this.handleTransportClose(err));
    t.onError(() => {
      /* surfaced through onClose / request failures */
    });
  }

  private async teardownTransport(): Promise<void> {
    const t = this.transport;
    this.transport = null;
    if (t) {
      try {
        await t.close();
      } catch {
        /* noop */
      }
    }
  }

  private handleTransportClose(_err?: Error): void {
    if (this.closed) return;
    if (this.state === 'open') {
      this.state = 'reconnecting';
    }
    if (this.state !== 'reconnecting') return;
    if (this.opts.autoReconnect) this.scheduleReconnect();
  }

  /* ---------------------------------------------------------------- */
  /* Reconnect / offline queue                                        */
  /* ---------------------------------------------------------------- */

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;
    const attempt = this.reconnectAttempt++;
    const max = this.opts.maxRetries;
    if (attempt >= max) {
      this.state = 'closed';
      this.closed = true;
      return; // gave up
    }
    const delay = Math.min(RECONNECT_BASE_MS * Math.pow(2, attempt), RECONNECT_MAX_MS);
    const jitter = delay * 0.2 * (Math.random() * 2 - 1); // ±20%
    const wait = Math.max(0, Math.round(delay + jitter));
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.reconnectOnce();
    }, wait);
  }

  private async reconnectOnce(): Promise<void> {
    try {
      await this.establish();
      this.reconnectAttempt = 0;
      this.state = 'open';
      await this.ensureAuth();
      await this.restoreSubscriptions();
      await this.flushQueue();
    } catch (err) {
      if (!this.closed) this.scheduleReconnect();
    }
  }

  private isQueueable(method: string): boolean {
    return !method.startsWith('engine.') && !method.startsWith('auth.');
  }

  private enqueue<T>(method: string, params: Record<string, unknown> | undefined): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      if (this.queue.length >= QUEUE_LIMIT) {
        reject(new HubError(SDK_CODES.QUEUE_FULL, `offline queue full (${QUEUE_LIMIT})`));
        return;
      }
      this.queue.push({
        method,
        params,
        resolve: (v: unknown) => resolve(v as T),
        reject
      });
    });
  }

  private async flushQueue(): Promise<void> {
    const items = this.queue;
    this.queue = [];
    for (const item of items) {
      try {
        const res = await this.sendWithAuthRetry<unknown>(item.method, item.params, { noQueue: true });
        item.resolve(res);
      } catch (err) {
        item.reject(err);
      }
    }
  }

  private rejectQueued(err: Error): void {
    const items = this.queue;
    this.queue = [];
    for (const item of items) item.reject(err);
  }

  /* ---------------------------------------------------------------- */
  /* Auth                                                             */
  /* ---------------------------------------------------------------- */

  private async ensureToken(): Promise<string | undefined> {
    if (this.accessToken) return this.accessToken;
    if (this.opts.tokenProvider) {
      try {
        const t = await this.opts.tokenProvider();
        if (t) {
          this.accessToken = t;
          return t;
        }
      } catch {
        /* fall through */
      }
    }
    if (this.opts.token) {
      this.accessToken = this.opts.token;
      return this.opts.token;
    }
    return undefined;
  }

  /** Login automatically when the server requires it and we have no token. */
  private async ensureAuth(): Promise<void> {
    const token = await this.ensureToken();
    if (token) return;
    const info = this.serverInfoValue;
    if (info?.session && info.session.authenticated === false) {
      // With credentials use them; otherwise assume auth mode "none"
      // (anonymous login returns a token per PROTOCOL §3.5).
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
  request<T = unknown>(method: string, params?: Record<string, unknown>, opts?: RequestOptions): Promise<T> {
    if (this.closed) return Promise.reject(new HubError(SDK_CODES.CLOSED, 'client is closed'));
    if (this.state === 'new') {
      return Promise.reject(new HubError(SDK_CODES.NOT_OPEN, 'call client.open() before request()'));
    }
    if (this.state !== 'open') {
      // Offline (connecting / reconnecting / transport lost): queue write
      // operations so they replay in order after the connection recovers.
      if (this.opts.queueOffline && this.isQueueable(method) && !opts?.noQueue) {
        return this.enqueue<T>(method, params);
      }
      return Promise.reject(
        new HubError(SDK_CODES.NOT_CONNECTED, `client is not connected (state=${this.state}, method=${method})`)
      );
    }
    return this.sendWithAuthRetry<T>(method, params, opts);
  }

  private async sendWithAuthRetry<T>(
    method: string,
    params: Record<string, unknown> | undefined,
    opts?: RequestOptions
  ): Promise<T> {
    let attempts = 0;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        return await this.sendOnce<T>(method, params, opts);
      } catch (err) {
        const isAuth =
          err instanceof HubError && (err.code === HUB_CODES.AUTH_EXPIRED || err.code === HUB_CODES.AUTH_INVALID);
        // auth.* calls (login/refresh themselves) are never retried: a failed
        // re-auth must surface to the caller, not recurse.
        if (isAuth && attempts === 0 && !method.startsWith('auth.')) {
          attempts += 1;
          if (await this.recoverAuth()) continue; // retry once with the new token
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
  private async recoverAuth(): Promise<boolean> {
    if (this.refreshToken) {
      try {
        await this.refresh();
        return true;
      } catch {
        /* refresh failed: fall through to re-login */
      }
    }
    if (this.opts.credentials) {
      try {
        await this.login(this.opts.credentials);
        return true;
      } catch {
        /* fall through */
      }
    }
    return false;
  }

  private async sendOnce<T>(
    method: string,
    params: Record<string, unknown> | undefined,
    opts?: RequestOptions
  ): Promise<T> {
    const t = this.transport;
    if (!t) throw new HubError(SDK_CODES.NOT_CONNECTED, `no transport (${method})`);
    const timeout = opts?.timeoutMs ?? this.opts.requestTimeoutMs;
    // WS has no headers: carry the token in params.auth_token (PROTOCOL §3.1).
    let finalParams = params;
    if (t.kind === 'ws' && this.accessToken && !method.startsWith('engine.') && !method.startsWith('auth.')) {
      finalParams = { ...(params ?? {}), auth_token: this.accessToken };
    }
    return t.request<T>(method, finalParams, timeout);
  }

  /* ---------------------------------------------------------------- */
  /* Events                                                           */
  /* ---------------------------------------------------------------- */

  /**
   * Register an event handler for a specific event type or '*' (all events,
   * including unknown ones). Returns an unsubscribe function.
   */
  on(type: string, handler: (ev: HubEvent) => void): () => void {
    let set = this.listeners.get(type);
    if (!set) {
      set = new Set();
      this.listeners.set(type, set);
    }
    set.add(handler);
    return () => {
      set?.delete(handler);
    };
  }

  off(type: string, handler: (ev: HubEvent) => void): void {
    this.listeners.get(type)?.delete(handler);
  }

  private dispatchEvent(ev: HubEvent): void {
    // Dedupe by event.id (window dedupeWindow, default 5000).
    if (!this.dedupeEvent(ev.id)) return;
    // SSE (/v1/events) and long-poll (/v1/poll) broadcast ALL events to every
    // connected client regardless of subscriptions, so enforce the active
    // subscriptions' filters client-side — the same way the Go and Python
    // SDKs do. Over WS the server streams per-subscription events already.
    // (No subscriptions ⇒ no filtering: on()-only usage keeps seeing the
    // broadcast stream.)
    const kind = this.transport?.kind;
    if ((kind === 'http' || kind === 'poll') && this.activeSubs.length > 0) {
      const matched = this.activeSubs.some((s) => !s.cancelled && eventMatchesFilters(s.filters, ev));
      if (!matched) return;
    }
    const type = ev.type ?? '';
    const handlers = this.listeners.get(type);
    if (handlers) {
      for (const h of [...handlers]) {
        try {
          h(ev);
        } catch {
          /* isolate user callbacks */
        }
      }
    }
    const star = this.listeners.get('*');
    if (star) {
      for (const h of [...star]) {
        try {
          h(ev);
        } catch {
          /* isolate user callbacks */
        }
      }
    }
  }

  private dedupeEvent(id: string | undefined): boolean {
    if (!id) return true;
    if (this.seenSet.has(id)) return false;
    this.seenSet.add(id);
    this.seenIds.push(id);
    // The window is clamped to >= 0 in the constructor, and shift() strictly
    // decreases seenIds.length, so this loop always terminates.
    const window = Math.max(0, this.opts.dedupeWindow);
    while (this.seenIds.length > window) {
      const old = this.seenIds.shift();
      if (old !== undefined) this.seenSet.delete(old);
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
  async subscribeEvents(filters?: SubscribeParams): Promise<Subscription> {
    const filtersValue = filters ?? {};
    // Register the entry BEFORE the server round-trip so a synchronous
    // unsubscribe() finds it even while the subscribe call is queued offline
    // or still in flight.
    const entry: ActiveSub = { filters: filtersValue, pending: true, cancelled: false };
    this.activeSubs.push(entry);
    try {
      const res = await this.eventSubscribe(filtersValue);
      entry.pending = false;
      entry.serverId = res.subscription_id ?? '';
    } catch (err) {
      // Real failure (not offline-queued): drop the placeholder so
      // restoreSubscriptions cannot resurrect a subscription that never was.
      this.removeActiveSub(entry);
      throw err;
    }
    if (entry.cancelled && entry.serverId) {
      // unsubscribe() ran while the subscribe was queued/in flight: the server
      // just registered it — tear it down again instead of leaking it.
      void this.eventUnsubscribe(entry.serverId).catch(() => {
        /* already gone */
      });
    }
    return {
      get subscription_id() {
        return entry.serverId ?? '';
      },
      unsubscribe: async () => {
        if (entry.cancelled) return;
        entry.cancelled = true;
        this.removeActiveSub(entry);
        if (entry.serverId) {
          await this.eventUnsubscribe(entry.serverId).catch(() => {
            /* already gone */
          });
        }
      }
    };
  }

  /** Convenience: subscribe to node events; returns an unsubscribe function. */
  onNode(nodeId?: string): () => void {
    return this.trackSubscription({ node_id: nodeId });
  }

  /** Convenience: subscribe to task events; returns an unsubscribe function. */
  onTask(taskId?: string): () => void {
    return this.trackSubscription({ task_id: taskId });
  }

  /**
   * Shared helper behind onNode/onTask: subscribes (possibly while offline —
   * the request is queued and replayed on reconnect) and returns a
   * synchronous unsubscribe function that is safe to call before the
   * subscribe call has resolved.
   */
  private trackSubscription(filters: SubscribeParams): () => void {
    let sub: Subscription | null = null;
    let cancelled = false;
    const subscribe = (): void => {
      void this.subscribeEvents(filters)
        .then((s) => {
          if (cancelled) {
            // unsubscribe() won the race: remove the fresh server subscription.
            void s.unsubscribe();
          } else {
            sub = s;
          }
        })
        .catch(() => {
          /* subscribe failed for real; nothing to clean up */
        });
    };
    if (this.state === 'new') {
      // open() was never called: request() would reject with NOT_OPEN and the
      // subscription would silently never exist. Connect first, then subscribe.
      void this.open().then(subscribe, () => {
        /* connect failed; nothing to clean up */
      });
    } else {
      subscribe();
    }
    return () => {
      cancelled = true;
      if (sub) {
        void sub.unsubscribe().catch(() => {
          /* noop */
        });
      }
    };
  }

  private removeActiveSub(entry: ActiveSub): void {
    const idx = this.activeSubs.indexOf(entry);
    if (idx !== -1) this.activeSubs.splice(idx, 1);
  }

  private async restoreSubscriptions(): Promise<void> {
    // Iterate a snapshot: unsubscribe() may splice entries concurrently.
    for (const sub of [...this.activeSubs]) {
      // Dead or still-queued subscriptions must not be resurrected here: the
      // queued event.subscribe is replayed by flushQueue() instead.
      if (sub.cancelled || sub.pending) continue;
      try {
        const res = await this.eventSubscribe(sub.filters);
        if (sub.cancelled) {
          // Unsubscribed while restoring: drop the fresh server subscription.
          if (res.subscription_id) {
            void this.eventUnsubscribe(res.subscription_id).catch(() => {
              /* already gone */
            });
          }
          continue;
        }
        sub.serverId = res.subscription_id ?? '';
      } catch {
        /* keep the sub for the next reconnect */
      }
    }
  }

  /* ---------------------------------------------------------------- */
  /* Typed methods (PROTOCOL.md §5)                                    */
  /* ---------------------------------------------------------------- */

  info(clientInfo?: ClientInfo): Promise<ServerInfo> {
    return this.request<ServerInfo>('engine.info', { client_info: clientInfo ?? this.buildClientInfo() });
  }

  ping(): Promise<PingResult> {
    return this.request<PingResult>('engine.ping', {});
  }

  capabilities(): Promise<CapabilitiesResult> {
    return this.request<CapabilitiesResult>('engine.capabilities', {});
  }

  login(params: LoginParams = {}): Promise<AuthResult> {
    return this.request<AuthResult>('auth.login', params, { noQueue: true }).then((res) => {
      if (res?.access_token) this.accessToken = res.access_token;
      if (res?.refresh_token) this.refreshToken = res.refresh_token;
      return res;
    });
  }

  refresh(): Promise<AuthResult> {
    const rt = this.refreshToken;
    if (!rt) return Promise.reject(new HubError(SDK_CODES.AUTH_UNAVAILABLE, 'no refresh_token available'));
    return this.request<AuthResult>('auth.refresh', { refresh_token: rt }, { noQueue: true }).then(
      (res) => {
        // The engine rotates both tokens; always store the fresh pair so the
        // next refresh uses the new refresh_token.
        if (res?.access_token) this.accessToken = res.access_token;
        if (res?.refresh_token) this.refreshToken = res.refresh_token;
        return res;
      },
      (err) => {
        // A rotated refresh_token is single-use: once the server rejects it
        // with an auth error the whole pair is dead — drop it so the retry
        // path falls back to re-login instead of replaying a dead token.
        if (
          err instanceof HubError &&
          (err.code === HUB_CODES.AUTH_EXPIRED || err.code === HUB_CODES.AUTH_INVALID)
        ) {
          this.refreshToken = undefined;
          this.accessToken = undefined;
        }
        throw err;
      }
    );
  }

  logout(): Promise<UnsubscribeResult> {
    return this.request<UnsubscribeResult>('auth.logout', {}, { noQueue: true }).then((res) => {
      this.accessToken = undefined;
      this.refreshToken = undefined;
      return res;
    });
  }

  nodeList(filter?: NodeFilter, limit?: number, offset?: number): Promise<NodeListResult> {
    const params: NodeListParams = {};
    if (filter) params['filter'] = filter;
    if (limit !== undefined) params['limit'] = limit;
    if (offset !== undefined) params['offset'] = offset;
    return this.request<NodeListResult>('node.list', params);
  }

  nodeGet(nodeId: string): Promise<NodeGetResult> {
    return this.request<NodeGetResult>('node.get', { node_id: nodeId });
  }

  nodeSubscribe(nodeId?: string): Promise<SubscribeResult> {
    const params: NodeSubscribeParams = {};
    if (nodeId) params['node_id'] = nodeId;
    return this.request<SubscribeResult>('node.subscribe', params);
  }

  nodeUnsubscribe(subscriptionId: string): Promise<UnsubscribeResult> {
    return this.request<UnsubscribeResult>('node.unsubscribe', { subscription_id: subscriptionId });
  }

  commandSend(spec: CommandSendParams): Promise<CommandSendResult> {
    const params: CommandSendParams = {
      node_id: spec.node_id,
      method: spec.method,
      ...(spec.params !== undefined ? { params: spec.params } : {}),
      ...(spec.timeout !== undefined ? { timeout: spec.timeout } : {}),
      ...(spec.idempotency_key !== undefined ? { idempotency_key: spec.idempotency_key } : {})
    };
    return this.request<CommandSendResult>('command.send', params);
  }

  commandBatch(specs: CommandSpec[], idempotencyKey?: string): Promise<CommandBatchResult> {
    const params: CommandBatchParams = { commands: specs };
    if (idempotencyKey !== undefined) params['idempotency_key'] = idempotencyKey;
    return this.request<CommandBatchResult>('command.batch', params);
  }

  commandCancel(commandId: string): Promise<CommandCancelResult> {
    const params: CommandCancelParams = { command_id: commandId };
    return this.request<CommandCancelResult>('command.cancel', params);
  }

  taskCreate(spec: TaskCreateParams): Promise<TaskCreateResult> {
    return this.request<TaskCreateResult>('task.create', spec);
  }

  taskGet(taskId: string): Promise<TaskGetResult> {
    const params: TaskGetParams = { task_id: taskId };
    return this.request<TaskGetResult>('task.get', params);
  }

  taskList(filter?: { status?: string }, limit?: number, offset?: number): Promise<TaskListResult> {
    const params: TaskListParams = {};
    if (filter) params['filter'] = filter;
    if (limit !== undefined) params['limit'] = limit;
    if (offset !== undefined) params['offset'] = offset;
    return this.request<TaskListResult>('task.list', params);
  }

  taskCancel(taskId: string): Promise<TaskCancelResult> {
    const params: TaskCancelParams = { task_id: taskId };
    return this.request<TaskCancelResult>('task.cancel', params);
  }

  taskSubscribe(taskId?: string): Promise<SubscribeResult> {
    const params: TaskSubscribeParams = {};
    if (taskId) params['task_id'] = taskId;
    return this.request<SubscribeResult>('task.subscribe', params);
  }

  eventSubscribe(filters?: SubscribeParams): Promise<SubscribeResult> {
    return this.request<SubscribeResult>('event.subscribe', filters ?? {});
  }

  eventUnsubscribe(subscriptionId: string): Promise<UnsubscribeResult> {
    const params: UnsubscribeParams = { subscription_id: subscriptionId };
    return this.request<UnsubscribeResult>('event.unsubscribe', params);
  }

  seatList(filter?: SeatFilter, limit?: number, offset?: number): Promise<SeatListResult> {
    const params: SeatListParams = {};
    if (filter) params['filter'] = filter;
    if (limit !== undefined) params['limit'] = limit;
    if (offset !== undefined) params['offset'] = offset;
    return this.request<SeatListResult>('seat.list', params);
  }

  seatCreate(spec: SeatCreateParams = {}): Promise<SeatResult> {
    return this.request<SeatResult>('seat.create', spec);
  }

  seatGet(seatId: string): Promise<SeatResult> {
    return this.request<SeatResult>('seat.get', { seat_id: seatId });
  }

  seatDelete(seatId: string): Promise<Record<string, never>> {
    return this.request<Record<string, never>>('seat.delete', { seat_id: seatId });
  }

  seatBind(seatId: string, nodeId: string): Promise<SeatResult> {
    return this.request<SeatResult>('seat.bind', { seat_id: seatId, node_id: nodeId });
  }

  seatUnbind(seatId: string): Promise<SeatResult> {
    return this.request<SeatResult>('seat.unbind', { seat_id: seatId });
  }
}

/* ------------------------------------------------------------------ */
/* Platform detection (no node: imports — isomorphic)                 */
/* ------------------------------------------------------------------ */

/**
 * Client-side subscription filter matching (PROTOCOL.md §5 event.subscribe):
 * mirrors sdk/go Subscription.matches and sdk/python Subscription._matches.
 * Empty/absent filter fields match everything.
 */
function eventMatchesFilters(filters: SubscribeParams, ev: HubEvent): boolean {
  if (filters.event_types && filters.event_types.length > 0) {
    if (!ev.type || !filters.event_types.includes(ev.type)) return false;
  }
  if (filters.node_id && ev.node_id !== filters.node_id) return false;
  if (filters.task_id && ev.task_id !== filters.task_id) return false;
  return true;
}

function detectPlatform(): PlatformInfo {
  const g = globalThis as Record<string, unknown>;
  let runtime = 'unknown';
  let os = '';
  let arch = '';

  const nav = (g.navigator ?? undefined) as { userAgent?: string; platform?: string } | undefined;
  if (nav?.userAgent) {
    if (/Electron\//.test(nav.userAgent)) runtime = 'electron';
    else if (typeof g.Deno !== 'undefined') runtime = 'deno';
    else if (typeof g.Bun !== 'undefined') runtime = 'bun';
    else runtime = 'browser';
    os = nav.platform ?? '';
  }

  const proc = (g.process ?? undefined) as
    | { versions?: { node?: string }; platform?: string; arch?: string }
    | undefined;
  if (proc?.versions?.node) {
    runtime = 'node';
    os = proc.platform ?? '';
    arch = proc.arch ?? '';
  } else if (typeof g.Deno !== 'undefined') {
    runtime = 'deno';
  } else if (typeof g.Bun !== 'undefined') {
    runtime = 'bun';
  }
  if (!arch) {
    // No node:process — approximate from userAgent data if present.
    const ua = nav?.userAgent ?? '';
    if (ua.includes('arm64') || ua.includes('aarch64')) arch = 'arm64';
    else if (ua.includes('x86_64') || ua.includes('x64')) arch = 'x64';
  }
  return { os, runtime, arch };
}