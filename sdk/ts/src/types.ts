/**
 * TypeScript types for the Control Hub protocol (see docs/PROTOCOL.md).
 *
 * ⚠️ COMPATIBILITY IRON LAW: every field here is OPTIONAL. Servers may add new
 * fields or omit fields; clients MUST tolerate both. We never validate unknown
 * fields at runtime, and the `[key: string]: unknown` index signatures keep the
 * door open for forward-compatible payloads.
 */

/* ------------------------------------------------------------------ */
/* JSON-RPC 2.0 envelope (schema/common.json)                          */
/* ------------------------------------------------------------------ */

/** JSON-RPC request id: number or string. */
export type RpcId = number | string;

/** ISO-8601 UTC timestamp, e.g. "2026-10-05T12:00:00.000Z". */
export type Timestamp = string;

/** JSON-RPC error object. `data` is optional and must be an object. */
export interface ErrorData {
  code?: number;
  message?: string;
  data?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface RpcRequest {
  jsonrpc?: '2.0';
  id?: RpcId;
  method?: string;
  params?: Record<string, unknown>;
}

export interface RpcResponse<T = unknown> {
  jsonrpc?: '2.0';
  id?: RpcId;
  result?: T;
  error?: ErrorData;
  [key: string]: unknown;
}

export interface RpcNotification {
  jsonrpc?: '2.0';
  method?: string;
  params?: Record<string, unknown>;
}

/** A single envelope or a batch (array) of envelopes. */
export type RpcMessage = RpcRequest | RpcResponse | RpcNotification | RpcMessage[];

/* ------------------------------------------------------------------ */
/* Data models (schema/node.json, command.json, task.json, event.json) */
/* ------------------------------------------------------------------ */

export type NodeStatus = 'online' | 'busy' | 'offline' | 'unknown';

/** A controlled node. `capabilities`/`tags`/`metadata` are opaque to the engine. */
export interface Node {
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
export interface SeatExpect {
  capabilities?: string[];
  tags?: string[];
  metadata?: Record<string, unknown>;
  [key: string]: unknown;
}

/** A reservable position in the engine (PROTOCOL.md §4.7): the extensibility unit. */
export interface Seat {
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

export interface SeatCreateParams {
  name?: string;
  bind_policy?: 'auto' | 'manual';
  expect?: SeatExpect;
  [key: string]: unknown;
}

export interface SeatResult {
  seat?: Seat;
  [key: string]: unknown;
}

export interface SeatFilter {
  kind?: 'standing' | 'vacant';
  node_id?: string;
  [key: string]: unknown;
}

export interface SeatListParams {
  filter?: SeatFilter;
  limit?: number;
  offset?: number;
  [key: string]: unknown;
}

export interface SeatListResult {
  seats?: Seat[];
  total?: number;
  [key: string]: unknown;
}

export type CommandStatus =
  | 'pending'
  | 'accepted'
  | 'running'
  | 'succeeded'
  | 'failed'
  | 'cancelled'
  | 'timeout';

/** A single command sent to a node. Fields may be absent before completion. */
export interface Command {
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

export type TaskStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'partial';

/** A task is a collection of commands with aggregate status and progress. */
export interface Task {
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
export type EventType =
  | 'node.registered'
  | 'node.updated'
  | 'node.status.changed'
  | 'node.unregistered'
  | 'command.accepted'
  | 'command.started'
  | 'command.result'
  | 'command.failed'
  | 'command.cancelled'
  | 'task.created'
  | 'task.progress'
  | 'task.updated'
  | 'task.completed'
  | 'task.failed'
  | 'task.cancelled';

/** Server-pushed event, delivered as JSON-RPC notification `method: "event"`. */
export interface HubEvent {
  id?: string;
  type?: string;
  time?: Timestamp;
  node_id?: string | null;
  task_id?: string | null;
  command_id?: string | null;
  data?: Record<string, unknown>;
  [key: string]: unknown;
}

/* ------------------------------------------------------------------ */
/* Capability / negotiation (schema/capability.json)                   */
/* ------------------------------------------------------------------ */

export interface Capability {
  id?: string;
  version?: string;
}

export interface PlatformInfo {
  os?: string;
  runtime?: string;
  arch?: string;
  [key: string]: unknown;
}

export interface ClientInfo {
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

export interface SessionInfo {
  authenticated?: boolean;
  expires_at?: Timestamp;
}

export interface ServerInfo {
  protocol_version?: string;
  server?: { name?: string; version?: string };
  supported_versions?: string[];
  encodings?: string[];
  transports?: string[];
  features?: Record<string, boolean>;
  capabilities?: Capability[];
  session?: SessionInfo;
  server_time?: Timestamp;
  [key: string]: unknown;
}

/* ------------------------------------------------------------------ */
/* Method params / results (schema/methods.json)                       */
/* ------------------------------------------------------------------ */

export interface EngineInfoParams {
  client_info?: ClientInfo;
  [key: string]: unknown;
}

export interface PingResult {
  pong?: boolean;
  time?: Timestamp;
  [key: string]: unknown;
}

export interface CapabilitiesResult {
  capabilities?: Capability[];
  features?: Record<string, boolean>;
  [key: string]: unknown;
}

/** auth.login params depend on mode: token / basic / none. */
export interface LoginParams {
  token?: string;
  username?: string;
  password?: string;
  [key: string]: unknown;
}

export interface AuthResult {
  access_token?: string;
  token_type?: string;
  expires_in?: number;
  refresh_token?: string;
  [key: string]: unknown;
}

export interface NodeFilter {
  status?: string;
  tags?: string[];
  [key: string]: unknown;
}

export interface NodeListParams {
  filter?: NodeFilter;
  limit?: number;
  offset?: number;
  [key: string]: unknown;
}

export interface NodeListResult {
  nodes?: Node[];
  total?: number;
  [key: string]: unknown;
}

export interface NodeGetResult {
  node?: Node;
  [key: string]: unknown;
}

export interface NodeSubscribeParams {
  node_id?: string;
  [key: string]: unknown;
}

/** Result of any *subscribe / event.subscribe call. */
export interface SubscribeResult {
  subscription_id?: string;
  [key: string]: unknown;
}

export interface UnsubscribeParams {
  subscription_id?: string;
  [key: string]: unknown;
}

export interface UnsubscribeResult {
  [key: string]: unknown;
}

/** A command to dispatch (command.send / command.batch / task.create). */
export interface CommandSpec {
  node_id?: string;
  method?: string;
  params?: Record<string, unknown>;
  timeout?: number;
  idempotency_key?: string;
  [key: string]: unknown;
}

export interface CommandSendParams extends CommandSpec {}

export interface CommandSendResult {
  command?: Command;
  [key: string]: unknown;
}

export interface CommandBatchParams {
  commands?: CommandSpec[];
  idempotency_key?: string;
  [key: string]: unknown;
}

export interface CommandBatchResult {
  commands?: Command[];
  task_id?: string | null;
  [key: string]: unknown;
}

export interface CommandCancelParams {
  command_id?: string;
  [key: string]: unknown;
}

export interface CommandCancelResult {
  command?: Command;
  [key: string]: unknown;
}

export interface TaskCreateParams {
  name?: string;
  commands?: CommandSpec[];
  parallel?: boolean;
  [key: string]: unknown;
}

export interface TaskCreateResult {
  task?: Task;
  [key: string]: unknown;
}

export interface TaskGetParams {
  task_id?: string;
  [key: string]: unknown;
}

export interface TaskGetResult {
  task?: Task;
  [key: string]: unknown;
}

export interface TaskListParams {
  filter?: { status?: string; [key: string]: unknown };
  limit?: number;
  offset?: number;
  [key: string]: unknown;
}

export interface TaskListResult {
  tasks?: Task[];
  total?: number;
  [key: string]: unknown;
}

export interface TaskCancelParams {
  task_id?: string;
  [key: string]: unknown;
}

export interface TaskCancelResult {
  task?: Task;
  [key: string]: unknown;
}

export interface TaskSubscribeParams {
  task_id?: string;
  [key: string]: unknown;
}

/** Filters for event.subscribe / node.subscribe / task.subscribe. */
export interface SubscribeParams {
  event_types?: string[];
  node_id?: string;
  task_id?: string;
  subscription_id?: string;
  [key: string]: unknown;
}

export interface EventSubscribeParams extends SubscribeParams {}

/** Handle returned by subscribeEvents() / onNode() / onTask(). */
export interface Subscription {
  readonly subscription_id: string;
  unsubscribe(): Promise<void>;
}

/* ------------------------------------------------------------------ */
/* SDK-level types                                                     */
/* ------------------------------------------------------------------ */

export type TransportKind = 'ws' | 'http' | 'poll';

/** Options accepted by HubClient (all optional). */
export interface HubClientOptions {
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
export interface RequestOptions {
  timeoutMs?: number;
  /** Skip offline queueing (used for engine.* / auth.* internally). */
  noQueue?: boolean;
  /** Coalesce with other pending requests into one WS batch frame. */
  batch?: boolean;
}

/**
 * A queued write operation held while offline. Mirrors the HubClient-internal
 * QueuedRequest (client.ts); kept for typing public helpers only.
 */
export interface QueuedRequest {
  method: string;
  params: Record<string, unknown> | undefined;
  resolve: (value: unknown) => void;
  reject: (err: unknown) => void;
  timeoutMs: number;
}
