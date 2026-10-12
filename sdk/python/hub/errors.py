"""Hub error types and error-code constants (PROTOCOL.md §7)."""

# Standard JSON-RPC 2.0 errors (§7.1).
ERROR_PARSE_ERROR = -32700
ERROR_INVALID_REQUEST = -32600
ERROR_METHOD_NOT_FOUND = -32601
ERROR_INVALID_PARAMS = -32602
ERROR_INTERNAL_ERROR = -32603

# Domain errors (§7.2, additive only — never remove or repurpose).
ERROR_AUTH_REQUIRED = -32001
ERROR_AUTH_INVALID = -32002
ERROR_AUTH_EXPIRED = -32003
ERROR_AUTH_FORBIDDEN = -32004
ERROR_PROTOCOL_VERSION_MISMATCH = -32005
ERROR_ENCODING_NOT_SUPPORTED = -32006
ERROR_TRANSPORT_NOT_SUPPORTED = -32007
ERROR_NODE_NOT_FOUND = -32010
ERROR_NODE_OFFLINE = -32011
ERROR_NODE_BUSY = -32012
ERROR_COMMAND_NOT_FOUND = -32013
ERROR_COMMAND_CANCEL_REJECTED = -32014
ERROR_TASK_NOT_FOUND = -32020
ERROR_TASK_CANCEL_REJECTED = -32021
ERROR_SUBSCRIPTION_NOT_FOUND = -32030
ERROR_EVENT_TYPE_NOT_SUPPORTED = -32031
ERROR_FEATURE_DISABLED = -32040
ERROR_RATE_LIMITED = -32041
ERROR_SEAT_NOT_FOUND = -32050
ERROR_SEAT_OCCUPIED = -32051
ERROR_SEAT_EXPECT_MISMATCH = -32052


class HubError(Exception):
    """A JSON-RPC error returned by the hub.

    Attributes:
        code:    JSON-RPC / domain error code (see constants above).
        message: human-readable message.
        data:    optional per-error payload (dict); may contain extra fields
                 from newer hub versions — never assume its exact shape.
    """

    def __init__(self, code, message, data=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data if data is not None else {}

    def __str__(self):
        if self.data:
            return "hub error %d: %s (data=%r)" % (self.code, self.message, self.data)
        return "hub error %d: %s" % (self.code, self.message)

    def is_auth(self):
        """True for authentication-related errors (-32001..-32004)."""
        return self.code in (
            ERROR_AUTH_REQUIRED,
            ERROR_AUTH_INVALID,
            ERROR_AUTH_EXPIRED,
            ERROR_AUTH_FORBIDDEN,
        )

    def is_version_mismatch(self):
        """True for protocol version mismatch (-32005)."""
        return self.code == ERROR_PROTOCOL_VERSION_MISMATCH