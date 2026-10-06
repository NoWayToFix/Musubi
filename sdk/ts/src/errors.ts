import type { ErrorData } from './types';

/**
 * HubError — the single error type raised by the SDK.
 *
 * RPC-level failures (error envelope from the server) carry the protocol
 * error code (PROTOCOL.md §7). SDK-local failures (timeouts, not connected,
 * protocol violations) use small positive codes documented below.
 */
export class HubError extends Error {
  readonly code: number;
  readonly data?: unknown;

  constructor(code: number, message: string, data?: unknown) {
    super(message);
    this.name = 'HubError';
    this.code = code;
    this.data = data;
    // Restore prototype chain when targeting ES5-style transpiled environments.
    Object.setPrototypeOf(this, new.target.prototype);
  }

  /** Build a HubError from a JSON-RPC error envelope. */
  static fromRpc(err: ErrorData): HubError {
    return new HubError(
      typeof err.code === 'number' ? err.code : RPC_CODES.INTERNAL_ERROR,
      err.message ?? 'unknown rpc error',
      err.data
    );
  }
}

export function isHubError(err: unknown): err is HubError {
  return err instanceof HubError;
}

/** JSON-RPC standard error codes (PROTOCOL.md §7.1). */
export const RPC_CODES = {
  PARSE_ERROR: -32700,
  INVALID_REQUEST: -32600,
  METHOD_NOT_FOUND: -32601,
  INVALID_PARAMS: -32602,
  INTERNAL_ERROR: -32603
} as const;

/** Hub domain error codes (PROTOCOL.md §7.2). */
export const HUB_CODES = {
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
  RATE_LIMITED: -32041
} as const;

/**
 * SDK-local error codes (positive; never sent to the server, never collide
 * with protocol codes which are all negative).
 */
export const SDK_CODES = {
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
} as const;