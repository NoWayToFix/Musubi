/**
 * @musubi/hub-sdk — TypeScript Control Hub SDK
 *
 * Public API surface. Everything else under src/ is internal.
 */
export { HubClient, SDK_VERSION } from './client';
export { HubError, isHubError, RPC_CODES, HUB_CODES, SDK_CODES } from './errors';
export { createMainBridge, createPreloadAPI } from './electron';
export type { MainBridge, PreloadApi } from './electron';

// transports
export { WebSocketTransport } from './transports/websocket';
export { HttpTransport } from './transports/http';
export { PollTransport } from './transports/poll';
export type { Transport, TransportOptions, PendingRequest } from './transport';
export { ListenerSet, normalizeBaseUrl, toWsUrl, eventsUrl, pollUrl } from './transport';

// types
export type {
  // envelope
  RpcId,
  RpcRequest,
  RpcResponse,
  RpcNotification,
  RpcMessage,
  ErrorData,
  Timestamp,
  // models
  Node,
  NodeStatus,
  Command,
  CommandStatus,
  Task,
  TaskStatus,
  HubEvent,
  EventType,
  Capability,
  ClientInfo,
  ServerInfo,
  SessionInfo,
  PlatformInfo,
  AuthResult,
  // filters / params / results
  NodeFilter,
  CommandSpec,
  CommandSendParams,
  CommandSendResult,
  CommandBatchParams,
  CommandBatchResult,
  CommandCancelParams,
  CommandCancelResult,
  SubscribeParams,
  SubscribeResult,
  UnsubscribeParams,
  UnsubscribeResult,
  NodeListResult,
  NodeListParams,
  NodeGetResult,
  NodeSubscribeParams,
  TaskCreateParams,
  TaskCreateResult,
  TaskGetParams,
  TaskGetResult,
  TaskListParams,
  TaskListResult,
  TaskCancelParams,
  TaskCancelResult,
  TaskSubscribeParams,
  EventSubscribeParams,
  LoginParams,
  PingResult,
  CapabilitiesResult,
  Subscription,
  // SDK-level
  TransportKind,
  HubClientOptions,
  RequestOptions
} from './types';

// Convenience list of known event types (for subscriptions / filters).
export const EVENT_TYPES = [
  'node.registered',
  'node.updated',
  'node.status.changed',
  'node.unregistered',
  'command.accepted',
  'command.started',
  'command.result',
  'command.failed',
  'command.cancelled',
  'task.created',
  'task.progress',
  'task.updated',
  'task.completed',
  'task.failed',
  'task.cancelled'
] as const;