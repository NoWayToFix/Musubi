"""musubi-hub-sdk: Python SDK for the Control Hub (JSON-RPC 2.0).

Zero third-party dependencies by default (urllib.request only). Events are
delivered over SSE (or long-poll when the server does not advertise SSE),
writes made while disconnected are queued and replayed on reconnect
(PROTOCOL.md §6.2). If the optional ``websocket-client`` package is installed
and the server advertises it, calls may use the WebSocket transport;
otherwise they degrade gracefully to HTTP.

Contract anchor: docs/PROTOCOL.md (see repository root).
"""

__version__ = "1.0.0"

from .errors import (
    ERROR_AUTH_REQUIRED,
    ERROR_AUTH_INVALID,
    ERROR_AUTH_EXPIRED,
    ERROR_AUTH_FORBIDDEN,
    ERROR_PROTOCOL_VERSION_MISMATCH,
    ERROR_ENCODING_NOT_SUPPORTED,
    ERROR_TRANSPORT_NOT_SUPPORTED,
    ERROR_NODE_NOT_FOUND,
    ERROR_NODE_OFFLINE,
    ERROR_NODE_BUSY,
    ERROR_COMMAND_NOT_FOUND,
    ERROR_COMMAND_CANCEL_REJECTED,
    ERROR_TASK_NOT_FOUND,
    ERROR_TASK_CANCEL_REJECTED,
    ERROR_SUBSCRIPTION_NOT_FOUND,
    ERROR_EVENT_TYPE_NOT_SUPPORTED,
    ERROR_FEATURE_DISABLED,
    ERROR_RATE_LIMITED,
    HubError,
)
from .client import HubClient, Subscription

__all__ = [
    "__version__",
    "HubClient",
    "Subscription",
    "HubError",
    "ERROR_AUTH_REQUIRED",
    "ERROR_AUTH_INVALID",
    "ERROR_AUTH_EXPIRED",
    "ERROR_AUTH_FORBIDDEN",
    "ERROR_PROTOCOL_VERSION_MISMATCH",
    "ERROR_ENCODING_NOT_SUPPORTED",
    "ERROR_TRANSPORT_NOT_SUPPORTED",
    "ERROR_NODE_NOT_FOUND",
    "ERROR_NODE_OFFLINE",
    "ERROR_NODE_BUSY",
    "ERROR_COMMAND_NOT_FOUND",
    "ERROR_COMMAND_CANCEL_REJECTED",
    "ERROR_TASK_NOT_FOUND",
    "ERROR_TASK_CANCEL_REJECTED",
    "ERROR_SUBSCRIPTION_NOT_FOUND",
    "ERROR_EVENT_TYPE_NOT_SUPPORTED",
    "ERROR_FEATURE_DISABLED",
    "ERROR_RATE_LIMITED",
]