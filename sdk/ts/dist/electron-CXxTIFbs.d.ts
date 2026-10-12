/**
 * TypeScript types for the Control Hub protocol (see docs/PROTOCOL.md).
 *
 * ⚠️ COMPATIBILITY IRON LAW: every field here is OPTIONAL. Servers may add new
 * fields or omit fields; clients MUST tolerate both. We never validate unknown
 * fields at runtime, and the `[key: string]: unknown` index signatures keep the
 * door open for forward-compatible payloads.
 */
/** JSON-RPC request id: number or string. */
type RpcId = number | string;
/** ISO-8601 UTC timestamp, e.g. "2026-10-05T12:00:00.000Z". */
type Timestamp = string;
/** JSON-RPC error object. `data` is optional and must be an object. */
interface ErrorData {
    code?: number;
    message?: string;
    data?: Record<string, unknown>;
    [key: string]: unknown;
}
interface RpcRequest {
    jsonrpc?: '2.0';
    id?: RpcId;
    method?: string;
    params?: Record<string, unknown>;
}
interface RpcResponse<T = unknown> {
    jsonrpc?: '2.0';
    id?: RpcId;
    result?: T;
    error?: ErrorData;
    [key: string]: unknown;
}
interface RpcNotification {
    jsonrpc?: '2.0';
    method?: string;
    params?: Record<string, unknown>;
}
/** A single envelope or a batch (array) of envelopes. */
type RpcMessage = RpcRequest | RpcResponse | RpcNotification | RpcMessage[];
type NodeStatus = 'online' | 'busy' | 'offline' | 'unknown';
/** A controlled node. `capabilities`/`tags`/`metadata` are opaque to the engine. */
interface Node {
    id?: string;
    name?: string;
    status?: NodeStatus;
    capabilities?: string[];
    tags?: string[];
    metadata?: Record<string, unknown>;
    last_seen?: Timestamp;
    registered_at?: Timestamp;
    seat_id?: string;
    [key: string]: unknown;
}
/** Expected profile a filling node must match (PROTOCOL.md §4.7). */
interface SeatExpect {
    capabilities?: string[];
    tags?: string[];
    metadata?: Record<string, unknown>;
    [key: string]: unknown;
}
/** A reservable position in the engine (PROTOCOL.md §4.7): the extensibility unit. */
interface Seat {
    id?: string;
    kind?: 'standing' | 'vacant';
    name?: string;
    bind_policy?: 'auto' | 'manual';
    expect?: SeatExpect;
    node_id?: string | null;
    created_at?: Timestamp;
    filled_at?: Timestamp | null;
    released_at?: Timestamp | null;
    [key: string]: unknown;
}
interface SeatCreateParams {
    name?: string;
    bind_policy?: 'auto' | 'manual';
    expect?: SeatExpect;
    [key: string]: unknown;
}
interface SeatResult {
    seat?: Seat;
    [key: string]: unknown;
}
interface SeatFilter {
    kind?: 'standing' | 'vacant';
    node_id?: string;
    [key: string]: unknown;
}
interface SeatListResult {
    seats?: Seat[];
    total?: number;
    [key: string]: unknown;
}
type CommandStatus = 'pending' | 'accepted' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'timeout';
/** A single command sent to a node. Fields may be absent before completion. */
interface Command {
    id?: string;
    node_id?: string;
    method?: string;
    params?: Record<string, unknown>;
    status?: CommandStatus;
    result?: unknown;
    error?: ErrorData | null;
    timeout?: number;
    created_at?: Timestamp;
    started_at?: Timestamp | null;
    finished_at?: Timestamp | null;
    idempotency_key?: string | null;
    task_id?: string | null;
    [key: string]: unknown;
}
type TaskStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'partial';
/** A task is a collection of commands with aggregate status and progress. */
interface Task {
    id?: string;
    name?: string;
    status?: TaskStatus;
    progress?: number;
    command_ids?: string[];
    error?: ErrorData | null;
    created_at?: Timestamp;
    updated_at?: Timestamp;
    [key: string]: unknown;
}
/** Known event types (PROTOCOL.md §4.5). New types are additive — treat as string. */
type EventType = 'node.registered' | 'node.updated' | 'node.status.changed' | 'node.unregistered' | 'command.accepted' | 'command.started' | 'command.result' | 'command.failed' | 'command.cancelled' | 'task.created' | 'task.progress' | 'task.updated' | 'task.completed' | 'task.failed' | 'task.cancelled';
/** Server-pushed event, delivered as JSON-RPC notification `method: "event"`. */
interface HubEvent {
    id?: string;
    type?: string;
    time?: Timestamp;
    node_id?: string | null;
    task_id?: string | null;
    command_id?: string | null;
    data?: Record<string, unknown>;
    [key: string]: unknown;
}
interface Capability {
    id?: string;
    version?: string;
}
interface PlatformInfo {
    os?: string;
    runtime?: string;
    arch?: string;
    [key: string]: unknown;
}
interface ClientInfo {
    name?: string;
    version?: string;
    /** Omitted ⇒ treated as ["1.0"]. */
    protocol_versions?: string[];
    encodings?: string[];
    transports?: string[];
    features?: Record<string, boolean>;
    platform?: PlatformInfo;
    [key: string]: unknown;
}
interface SessionInfo {
    authenticated?: boolean;
    expires_at?: Timestamp;
}
interface ServerInfo {
    protocol_version?: string;
    server?: {
        name?: string;
        version?: string;
    };
    supported_versions?: string[];
    encodings?: string[];
    transports?: string[];
    features?: Record<string, boolean>;
    capabilities?: Capability[];
    session?: SessionInfo;
    server_time?: Timestamp;
    [key: string]: unknown;
}
interface PingResult {
    pong?: boolean;
    time?: Timestamp;
    [key: string]: unknown;
}
interface CapabilitiesResult {
    capabilities?: Capability[];
    features?: Record<string, boolean>;
    [key: string]: unknown;
}
/** auth.login params depend on mode: token / basic / none. */
interface LoginParams {
    token?: string;
    username?: string;
    password?: string;
    [key: string]: unknown;
}
interface AuthResult {
    access_token?: string;
    token_type?: string;
    expires_in?: number;
    refresh_token?: string;
    [key: string]: unknown;
}
interface NodeFilter {
    status?: string;
    tags?: string[];
    [key: string]: unknown;
}
interface NodeListParams {
    filter?: NodeFilter;
    limit?: number;
    offset?: number;
    [key: string]: unknown;
}
interface NodeListResult {
    nodes?: Node[];
    total?: number;
    [key: string]: unknown;
}
interface NodeGetResult {
    node?: Node;
    [key: string]: unknown;
}
interface NodeSubscribeParams {
    node_id?: string;
    [key: string]: unknown;
}
/** Result of any *subscribe / event.subscribe call. */
interface SubscribeResult {
    subscription_id?: string;
    [key: string]: unknown;
}
interface UnsubscribeParams {
    subscription_id?: string;
    [key: string]: unknown;
}
interface UnsubscribeResult {
    [key: string]: unknown;
}
/** A command to dispatch (command.send / command.batch / task.create). */
interface CommandSpec {
    node_id?: string;
    method?: string;
    params?: Record<string, unknown>;
    timeout?: number;
    idempotency_key?: string;
    [key: string]: unknown;
}
interface CommandSendParams extends CommandSpec {
}
interface CommandSendResult {
    command?: Command;
    [key: string]: unknown;
}
interface CommandBatchParams {
    commands?: CommandSpec[];
    idempotency_key?: string;
    [key: string]: unknown;
}
interface CommandBatchResult {
    commands?: Command[];
    task_id?: string | null;
    [key: string]: unknown;
}
interface CommandCancelParams {
    command_id?: string;
    [key: string]: unknown;
}
interface CommandCancelResult {
    command?: Command;
    [key: string]: unknown;
}
interface TaskCreateParams {
    name?: string;
    commands?: CommandSpec[];
    parallel?: boolean;
    [key: string]: unknown;
}
interface TaskCreateResult {
    task?: Task;
    [key: string]: unknown;
}
interface TaskGetParams {
    task_id?: string;
    [key: string]: unknown;
}
interface TaskGetResult {
    task?: Task;
    [key: string]: unknown;
}
interface TaskListParams {
    filter?: {
        status?: string;
        [key: string]: unknown;
    };
    limit?: number;
    offset?: number;
    [key: string]: unknown;
}
interface TaskListResult {
    tasks?: Task[];
    total?: number;
    [key: string]: unknown;
}
interface TaskCancelParams {
    task_id?: string;
    [key: string]: unknown;
}
interface TaskCancelResult {
    task?: Task;
    [key: string]: unknown;
}
interface TaskSubscribeParams {
    task_id?: string;
    [key: string]: unknown;
}
/** Filters for event.subscribe / node.subscribe / task.subscribe. */
interface SubscribeParams {
    event_types?: string[];
    node_id?: string;
    task_id?: string;
    subscription_id?: string;
    [key: string]: unknown;
}
interface EventSubscribeParams extends SubscribeParams {
}
/** Handle returned by subscribeEvents() / onNode() / onTask(). */
interface Subscription {
    readonly subscription_id: string;
    unsubscribe(): Promise<void>;
}
type TransportKind = 'ws' | 'http' | 'poll';
/** Options accepted by HubClient (all optional). */
interface HubClientOptions {
    /** Base server URL, e.g. "https://hub.example.com" or "ws://localhost:8080". */
    url?: string;
    /** Static bearer token. */
    token?: string;
    /** Dynamic token provider (async supported); overrides `token` when set. */
    tokenProvider?: () => string | Promise<string>;
    /** Credentials used for automatic auth.login when not authenticated. */
    credentials?: LoginParams;
    /** Transport preference order; default ["ws", "http", "poll"]. */
    transportOrder?: TransportKind[];
    /** Auto-reconnect with exponential backoff; default true. */
    autoReconnect?: boolean;
    /** Max reconnect attempts (Infinity = forever); default Infinity. */
    maxRetries?: number;
    /** Queue write operations while offline (FIFO, cap 1000); default true. */
    queueOffline?: boolean;
    /** Per-request timeout in ms; default 30000. */
    requestTimeoutMs?: number;
    /** Heartbeat interval in ms (WS ping / engine.ping); default 30000. */
    pingIntervalMs?: number;
    /** Event dedupe window (max tracked event ids); default 5000. */
    dedupeWindow?: number;
    /** Enable JSON-RPC batch coalescing on WS (requires server features.batch). */
    batch?: boolean;
    /** Proxy URL — passed to transports; Node fetch honors it via undici dispatcher (see README). */
    proxy?: string;
    /** CA certificate / PEM — passed to transports for self-signed TLS. */
    ca?: string;
    /** Skip TLS verification — passed to transports (self-signed). */
    insecure?: boolean;
}
/** Per-request options for client.request(). */
interface RequestOptions {
    timeoutMs?: number;
    /** Skip offline queueing (used for engine.* / auth.* internally). */
    noQueue?: boolean;
    /** Coalesce with other pending requests into one WS batch frame. */
    batch?: boolean;
}

/** Version of this SDK (mirrors package.json). */
declare const SDK_VERSION = "1.0.0";
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
declare class HubClient {
    private opts;
    private state;
    private closed;
    private connectPromise;
    private transport;
    private serverInfoValue;
    private featuresValue;
    private accessToken?;
    private refreshToken?;
    private listeners;
    private seenIds;
    private seenSet;
    private queue;
    private reconnectAttempt;
    private reconnectTimer;
    private activeSubs;
    constructor(options?: HubClientOptions);
    /**
     * Connect and negotiate. Throws a HubError when no transport could be
     * established (protocol mismatch -32005, or all transports failed). With
     * autoReconnect, background retries continue after a failure.
     */
    open(): Promise<void>;
    private initialConnect;
    close(): Promise<void>;
    /** Negotiated server_info from the last engine.info. */
    get serverInfo(): ServerInfo | null;
    /** Merged features (client ∩ server). */
    get features(): Record<string, boolean>;
    /** Currently active transport kind, or null. */
    get transportKind(): TransportKind | null;
    private buildClientInfo;
    private createTransport;
    /**
     * Establish the best transport: try transportOrder in order, use the first
     * one the server accepts (a -32007 transport.not.supported error means "try
     * the next"). Then negotiate engine.info and switch to the best intersection
     * (transportOrder ∩ server_info.transports) if the server advertises a
     * better one than the probing transport.
     */
    private establish;
    private negotiate;
    private isFatalError;
    private pickTransport;
    private bindTransport;
    private teardownTransport;
    private handleTransportClose;
    private scheduleReconnect;
    private reconnectOnce;
    private isQueueable;
    private enqueue;
    private flushQueue;
    private rejectQueued;
    private ensureToken;
    /** Login automatically when the server requires it and we have no token. */
    private ensureAuth;
    /**
     * Send a JSON-RPC request. When offline and queueOffline is enabled, write
     * operations (everything except engine.* and auth.*) are queued in order
     * (FIFO, cap 1000) and their promises settle after the connection recovers.
     * Requests that fail with -32003 (auth.expired) are retried once after
     * refreshing the token (or re-logging-in).
     */
    request<T = unknown>(method: string, params?: Record<string, unknown>, opts?: RequestOptions): Promise<T>;
    private sendWithAuthRetry;
    /**
     * Recover from an auth failure: refresh the token pair first (the engine
     * rotates BOTH access_token and refresh_token and invalidates the old pair,
     * so refresh() stores the new pair), then fall back to a full re-login with
     * the configured credentials. Returns true when a fresh token is available
     * for retrying the original request exactly once.
     */
    private recoverAuth;
    private sendOnce;
    /**
     * Register an event handler for a specific event type or '*' (all events,
     * including unknown ones). Returns an unsubscribe function.
     */
    on(type: string, handler: (ev: HubEvent) => void): () => void;
    off(type: string, handler: (ev: HubEvent) => void): void;
    private dispatchEvent;
    private dedupeEvent;
    /**
     * Subscribe to events server-side. Returns a Subscription handle whose
     * unsubscribe() removes the server subscription. Active subscriptions are
     * re-established automatically after reconnects.
     */
    subscribeEvents(filters?: SubscribeParams): Promise<Subscription>;
    /** Convenience: subscribe to node events; returns an unsubscribe function. */
    onNode(nodeId?: string): () => void;
    /** Convenience: subscribe to task events; returns an unsubscribe function. */
    onTask(taskId?: string): () => void;
    /**
     * Shared helper behind onNode/onTask: subscribes (possibly while offline —
     * the request is queued and replayed on reconnect) and returns a
     * synchronous unsubscribe function that is safe to call before the
     * subscribe call has resolved.
     */
    private trackSubscription;
    private removeActiveSub;
    private restoreSubscriptions;
    info(clientInfo?: ClientInfo): Promise<ServerInfo>;
    ping(): Promise<PingResult>;
    capabilities(): Promise<CapabilitiesResult>;
    login(params?: LoginParams): Promise<AuthResult>;
    refresh(): Promise<AuthResult>;
    logout(): Promise<UnsubscribeResult>;
    nodeList(filter?: NodeFilter, limit?: number, offset?: number): Promise<NodeListResult>;
    nodeGet(nodeId: string): Promise<NodeGetResult>;
    nodeSubscribe(nodeId?: string): Promise<SubscribeResult>;
    nodeUnsubscribe(subscriptionId: string): Promise<UnsubscribeResult>;
    commandSend(spec: CommandSendParams): Promise<CommandSendResult>;
    commandBatch(specs: CommandSpec[], idempotencyKey?: string): Promise<CommandBatchResult>;
    commandCancel(commandId: string): Promise<CommandCancelResult>;
    taskCreate(spec: TaskCreateParams): Promise<TaskCreateResult>;
    taskGet(taskId: string): Promise<TaskGetResult>;
    taskList(filter?: {
        status?: string;
    }, limit?: number, offset?: number): Promise<TaskListResult>;
    taskCancel(taskId: string): Promise<TaskCancelResult>;
    taskSubscribe(taskId?: string): Promise<SubscribeResult>;
    eventSubscribe(filters?: SubscribeParams): Promise<SubscribeResult>;
    eventUnsubscribe(subscriptionId: string): Promise<UnsubscribeResult>;
    seatList(filter?: SeatFilter, limit?: number, offset?: number): Promise<SeatListResult>;
    seatCreate(spec?: SeatCreateParams): Promise<SeatResult>;
    seatGet(seatId: string): Promise<SeatResult>;
    seatDelete(seatId: string): Promise<Record<string, never>>;
    seatBind(seatId: string, nodeId: string): Promise<SeatResult>;
    seatUnbind(seatId: string): Promise<SeatResult>;
}

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
interface MainBridge {
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
declare function createMainBridge(client: HubClient): MainBridge;
/** Preload API factory result. */
interface PreloadApi {
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
declare function createPreloadAPI(): PreloadApi;

export { type TaskStatus as $, type AuthResult as A, type RpcRequest as B, type CapabilitiesResult as C, type RpcResponse as D, type ErrorData as E, type ServerInfo as F, type SessionInfo as G, type HubEvent as H, type SubscribeParams as I, type SubscribeResult as J, type Subscription as K, type LoginParams as L, type MainBridge as M, type Node as N, type Task as O, type PingResult as P, type TaskCancelParams as Q, type RequestOptions as R, SDK_VERSION as S, type TransportKind as T, type TaskCancelResult as U, type TaskCreateParams as V, type TaskCreateResult as W, type TaskGetParams as X, type TaskGetResult as Y, type TaskListParams as Z, type TaskListResult as _, type Capability as a, type TaskSubscribeParams as a0, type Timestamp as a1, type UnsubscribeParams as a2, type UnsubscribeResult as a3, createMainBridge as a4, createPreloadAPI as a5, type ClientInfo as b, type Command as c, type CommandBatchParams as d, type CommandBatchResult as e, type CommandCancelParams as f, type CommandCancelResult as g, type CommandSendParams as h, type CommandSendResult as i, type CommandSpec as j, type CommandStatus as k, type EventSubscribeParams as l, type EventType as m, HubClient as n, type HubClientOptions as o, type NodeFilter as p, type NodeGetResult as q, type NodeListParams as r, type NodeListResult as s, type NodeStatus as t, type NodeSubscribeParams as u, type PlatformInfo as v, type PreloadApi as w, type RpcId as x, type RpcMessage as y, type RpcNotification as z };
