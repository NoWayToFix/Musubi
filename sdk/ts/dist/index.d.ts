import { H as HubClientOptions, a as HubClient, N as NodePlatform, C as Command, b as Node, E as ErrorData, T as TransportKind, c as HubEvent } from './electron-BPu69-ry.js';
export { A as AuthResult, d as CapabilitiesResult, e as Capability, f as ClientInfo, g as CommandBatchParams, h as CommandBatchResult, i as CommandCancelParams, j as CommandCancelResult, k as CommandSendParams, l as CommandSendResult, m as CommandSpec, n as CommandStatus, o as EventSubscribeParams, p as EventType, L as LoginParams, M as MainBridge, q as NodeFilter, r as NodeGetResult, s as NodeHeartbeatParams, t as NodeListParams, u as NodeListResult, v as NodeRegisterParams, w as NodeRegisterResult, x as NodeStatus, y as NodeSubscribeParams, z as NodeUnregisterParams, P as PingResult, B as PlatformInfo, D as PreloadApi, R as RequestOptions, F as RpcId, G as RpcMessage, I as RpcNotification, J as RpcRequest, K as RpcResponse, S as SDK_VERSION, O as ServerInfo, Q as SessionInfo, U as SubscribeParams, V as SubscribeResult, W as Subscription, X as Task, Y as TaskCancelParams, Z as TaskCancelResult, _ as TaskCreateParams, $ as TaskCreateResult, a0 as TaskGetParams, a1 as TaskGetResult, a2 as TaskListParams, a3 as TaskListResult, a4 as TaskStatus, a5 as TaskSubscribeParams, a6 as Timestamp, a7 as UnsubscribeParams, a8 as UnsubscribeResult, a9 as createMainBridge, aa as createPreloadAPI } from './electron-BPu69-ry.js';

/**
 * Agent — the reference worker runtime for a real node (PROTOCOL.md §5.3).
 *
 * One platform, one SDK: create an Agent in a Node/Deno/Bun/Electron process,
 * register command handlers, call run(). The Agent registers itself with a
 * platform descriptor, heartbeats, long-polls command.pull, executes handlers,
 * reports command.complete and unregisters on shutdown.
 *
 * This module is browser-safe at import time: no Node built-ins are imported
 * statically, and platform detection reads `globalThis` defensively.
 */

/** Executes one command method. Throwing marks the command failed. */
type AgentHandler = (params: Record<string, unknown>, ctx: {
    command: Command;
    signal: AbortSignal;
}) => unknown | Promise<unknown>;
/** Handler error carrying a protocol error code/message. */
declare class AgentHandlerError extends Error {
    readonly code: number;
    readonly data?: Record<string, unknown>;
    constructor(code: number, message: string, data?: Record<string, unknown>);
}
interface AgentOptions extends HubClientOptions {
    /** Reuse an existing client (it is not closed by the Agent). */
    client?: HubClient;
    /** Node id; the engine generates one when omitted. */
    nodeId?: string;
    name?: string;
    capabilities?: string[];
    tags?: string[];
    metadata?: Record<string, unknown>;
    /** Overrides auto-detection. */
    platform?: NodePlatform;
    /** Heartbeat interval; default 10000 ms (core watchdog is 15s). */
    heartbeatMs?: number;
    /** command.pull long-poll; default 5000 ms. */
    pullWaitMs?: number;
    /** Parallel pull loops; default 1. */
    concurrency?: number;
    /** Per-command handler timeout; 0 = none. */
    handlerTimeoutMs?: number;
    onError?: (err: unknown) => void;
}
/** Detects the platform of the current JavaScript runtime. */
declare function detectPlatform(): NodePlatform;
/** A registered node driven by the TypeScript SDK. */
declare class Agent {
    private readonly client;
    private readonly ownClient;
    private readonly nodeIdOpt?;
    private readonly name?;
    private readonly capabilities;
    private readonly tags;
    private readonly metadata?;
    private readonly platform;
    private readonly heartbeatMs;
    private readonly pullWaitMs;
    private readonly concurrency;
    private readonly handlerTimeoutMs;
    private readonly onError?;
    private readonly handlers;
    private node?;
    private running;
    private controller?;
    private regPromise?;
    constructor(options?: AgentOptions);
    /** Registers a handler. Built-ins echo/platform.info can be overridden. */
    handle(method: string, handler: AgentHandler): this;
    get clientRef(): HubClient;
    /** Current registered node id (undefined before registration). */
    get nodeID(): string | undefined;
    /** Snapshot of the registered node. */
    get nodeInfo(): Node | undefined;
    /** Stops a running agent (run() resolves after cleanup). */
    stop(): void;
    /**
     * Registers and serves commands until the signal aborts (or stop() runs).
     * Initial registration retries with exponential backoff.
     */
    run(signal?: AbortSignal): Promise<void>;
    private report;
    private registerParams;
    private registerOnce;
    private registerWithRetry;
    /** Single-flight re-registration after node.not_found / node.offline. */
    private ensureRegistered;
    private heartbeatLoop;
    private pullLoop;
    private execute;
    private complete;
    private unregister;
}

/**
 * HubError — the single error type raised by the SDK.
 *
 * RPC-level failures (error envelope from the server) carry the protocol
 * error code (PROTOCOL.md §7). SDK-local failures (timeouts, not connected,
 * protocol violations) use small positive codes documented below.
 */
declare class HubError extends Error {
    readonly code: number;
    readonly data?: unknown;
    constructor(code: number, message: string, data?: unknown);
    /** Build a HubError from a JSON-RPC error envelope. */
    static fromRpc(err: ErrorData): HubError;
}
declare function isHubError(err: unknown): err is HubError;
/** JSON-RPC standard error codes (PROTOCOL.md §7.1). */
declare const RPC_CODES: {
    readonly PARSE_ERROR: -32700;
    readonly INVALID_REQUEST: -32600;
    readonly METHOD_NOT_FOUND: -32601;
    readonly INVALID_PARAMS: -32602;
    readonly INTERNAL_ERROR: -32603;
};
/** Hub domain error codes (PROTOCOL.md §7.2). */
declare const HUB_CODES: {
    readonly AUTH_REQUIRED: -32001;
    readonly AUTH_INVALID: -32002;
    readonly AUTH_EXPIRED: -32003;
    readonly AUTH_FORBIDDEN: -32004;
    readonly PROTOCOL_VERSION_MISMATCH: -32005;
    readonly ENCODING_NOT_SUPPORTED: -32006;
    readonly TRANSPORT_NOT_SUPPORTED: -32007;
    readonly NODE_NOT_FOUND: -32010;
    readonly NODE_OFFLINE: -32011;
    readonly NODE_BUSY: -32012;
    readonly COMMAND_NOT_FOUND: -32013;
    readonly COMMAND_CANCEL_REJECTED: -32014;
    readonly TASK_NOT_FOUND: -32020;
    readonly TASK_CANCEL_REJECTED: -32021;
    readonly SUBSCRIPTION_NOT_FOUND: -32030;
    readonly EVENT_TYPE_NOT_SUPPORTED: -32031;
    readonly FEATURE_DISABLED: -32040;
    readonly RATE_LIMITED: -32041;
    readonly SEAT_NOT_FOUND: -32050;
    readonly SEAT_OCCUPIED: -32051;
    readonly SEAT_EXPECT_MISMATCH: -32052;
    readonly SEAT_VACANT: -32053;
};
/**
 * SDK-local error codes (positive; never sent to the server, never collide
 * with protocol codes which are all negative).
 */
declare const SDK_CODES: {
    /** Request timed out locally. */
    readonly TIMEOUT: 1;
    /** Transport is not connected (or connection dropped mid-flight). */
    readonly NOT_CONNECTED: 2;
    /** A WS frame arrived in an encoding we did not negotiate (binary frames). */
    readonly ENCODING_NOT_SUPPORTED: 3;
    /** No transport could be established / negotiated with the server. */
    readonly NO_TRANSPORT: 4;
    /** client.open() was not called yet. */
    readonly NOT_OPEN: 5;
    /** client.close() was already called. */
    readonly CLOSED: 6;
    /** Offline queue is full (cap 1000, FIFO). */
    readonly QUEUE_FULL: 7;
    /** Authentication failed and no credentials were configured. */
    readonly AUTH_UNAVAILABLE: 8;
};

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
interface PendingRequest {
    method: string;
    resolve: (value: unknown) => void;
    reject: (err: unknown) => void;
    timer: ReturnType<typeof setTimeout>;
}
/** Options handed to a Transport factory by HubClient. */
interface TransportOptions {
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
interface Transport {
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
declare class ListenerSet<T> {
    private listeners;
    on(cb: (a: T) => void): () => void;
    emit(a: T): void;
    get size(): number;
}
/** Normalize a user-provided URL to a bare origin (scheme://host[:port]). */
declare function normalizeBaseUrl(url: string): string;
/** Convert a base URL to the ws(s):// form used by /v1/ws. */
declare function toWsUrl(baseUrl: string): string;
/** Build the event URL for SSE / long-poll endpoints. */
declare function eventsUrl(baseUrl: string, path: string, query: Record<string, string | number | undefined>): string;
/** Alias used by PollTransport (same URL builder). */
declare const pollUrl: typeof eventsUrl;

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
declare class WebSocketTransport implements Transport {
    readonly kind: "ws";
    private baseUrl;
    private options;
    private ctor;
    private ws;
    private pending;
    private idSeq;
    private connectPromise;
    private batchEnabled;
    private batchQueue;
    private batchTimer;
    private pingTimer;
    private heartbeatType;
    private onEventL;
    private onOpenL;
    private onCloseL;
    private onErrorL;
    constructor(options: TransportOptions);
    connect(): Promise<void>;
    private doConnect;
    private startPing;
    request<T = unknown>(method: string, params?: Record<string, unknown>, timeoutMs?: number): Promise<T>;
    private flushBatch;
    sendNotification(method: string, params?: Record<string, unknown>): void;
    /**
     * Parse a text frame: single envelope or batch array. Responses are matched
     * by id against pending requests; notifications with method "event" are
     * unpacked and emitted through the event listeners. Unknown notifications
     * are ignored (compatibility rule).
     */
    private handleFrame;
    private handleEnvelope;
    onEvent(cb: (ev: HubEvent) => void): () => void;
    onOpen(cb: () => void): () => void;
    onClose(cb: (err?: Error) => void): () => void;
    onError(cb: (err: Error) => void): () => void;
    private forceClose;
    private rejectAll;
    private stopPing;
    close(): Promise<void>;
}

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
declare class HttpTransport implements Transport {
    readonly kind: "http";
    private baseUrl;
    private options;
    private sseController;
    private eventSource;
    private connectPromise;
    private onEventL;
    private onOpenL;
    private onCloseL;
    private onErrorL;
    constructor(options: TransportOptions);
    connect(): Promise<void>;
    request<T = unknown>(method: string, params?: Record<string, unknown>, timeoutMs?: number): Promise<T>;
    sendNotification(method: string, params?: Record<string, unknown>): void;
    private doConnect;
    private connectViaEventSource;
    private connectViaFetch;
    /** Background SSE reader: text/event-stream → `data:` lines → JSON.parse. */
    private readSseStream;
    /** Dispatch a single SSE block (may contain several `data:` lines). */
    private dispatchSseBlock;
    /** JSON.parse a `data:` payload — must be a JSON-RPC notification ("event"). */
    private dispatchSseLine;
    onEvent(cb: (ev: HubEvent) => void): () => void;
    onOpen(cb: () => void): () => void;
    onClose(cb: (err?: Error) => void): () => void;
    onError(cb: (err: Error) => void): () => void;
    close(): Promise<void>;
}

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
declare class PollTransport implements Transport {
    readonly kind: "poll";
    private baseUrl;
    private options;
    private cursor;
    private stopped;
    private loopPromise;
    private connectPromise;
    private onEventL;
    private onOpenL;
    private onCloseL;
    private onErrorL;
    constructor(options: TransportOptions);
    connect(): Promise<void>;
    private doConnect;
    request<T = unknown>(method: string, params?: Record<string, unknown>, timeoutMs?: number): Promise<T>;
    sendNotification(method: string, params?: Record<string, unknown>): void;
    private pollLoop;
    onEvent(cb: (ev: HubEvent) => void): () => void;
    onOpen(cb: () => void): () => void;
    onClose(cb: (err?: Error) => void): () => void;
    onError(cb: (err: Error) => void): () => void;
    close(): Promise<void>;
}

/**
 * @musubi/hub-sdk — TypeScript Control Hub SDK
 *
 * Public API surface. Everything else under src/ is internal.
 */

declare const EVENT_TYPES: readonly ["node.registered", "node.updated", "node.status.changed", "node.unregistered", "command.accepted", "command.started", "command.result", "command.failed", "command.cancelled", "task.created", "task.progress", "task.updated", "task.completed", "task.failed", "task.cancelled"];

export { Agent, type AgentHandler, AgentHandlerError, type AgentOptions, Command, EVENT_TYPES, ErrorData, HUB_CODES, HttpTransport, HubClient, HubClientOptions, HubError, HubEvent, ListenerSet, Node, NodePlatform, type PendingRequest, PollTransport, RPC_CODES, SDK_CODES, type Transport, TransportKind, type TransportOptions, WebSocketTransport, detectPlatform, eventsUrl, isHubError, normalizeBaseUrl, pollUrl, toWsUrl };
