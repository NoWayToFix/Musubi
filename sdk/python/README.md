# musubi-hub-sdk — Python SDK

Python SDK for the Control Hub. **Zero third-party dependencies by default**
(Python 3.8+): JSON-RPC calls use `urllib.request` over HTTP POST `/v1/rpc`;
events arrive over SSE (`GET /v1/events`, preferred) or long-polling on
`/v1/poll` (fallback) in a background thread.

If the optional [`websocket-client`](https://pypi.org/project/websocket-client/)
package is installed and the server advertises the `ws` transport, calls are
negotiated over the WebSocket endpoint `/v1/ws` (or forced with
`transport="ws"`). An import failure degrades gracefully to HTTP —
`websocket-client` is never a hard dependency.

The single source of truth is [`docs/PROTOCOL.md`](../../docs/PROTOCOL.md) at
the repository root.

## Install

```bash
# from this directory
pip install .            # core (no dependencies)
pip install .[ws]        # + optional websocket-client for WS transport
```

Or just copy the `hub/` package into your project.

## Usage

```python
from hub import HubClient

client = HubClient("http://127.0.0.1:8080", transport="auto")
info = client.info({"name": "py-demo", "version": "1.0.0"})
print("protocol:", info["protocol_version"], "hub:", info["server"])

# one-shot calls
nodes = client.node_list(limit=100)["nodes"]
print("nodes:", [(n["id"], n["status"]) for n in nodes])

# subscribe to all events (SSE, or long-poll fallback, in a background thread)
with client.subscribe() as sub:
    client.command_send("n-1", "echo", {"text": "hi"})
    ev = sub.get(timeout=5)
    print("event:", ev["type"], ev.get("data"))
```

## Error handling

All JSON-RPC errors raise `HubError`:

```python
from hub import HubClient, HubError

try:
    client.node_get("nope")
except HubError as e:
    if e.code == -32010:        # node.not_found
        pass
    if e.is_auth():             # -32001..-32004
        pass
    if e.is_version_mismatch(): # -32005
        pass
```

Error-code constants are exported from `hub.errors` (`ERROR_NODE_NOT_FOUND`,
`ERROR_AUTH_REQUIRED`, ...) — see [`hub/errors.py`](hub/errors.py).

## Events & subscriptions

```python
sub = client.subscribe({"event_types": ["task.progress", "task.completed"]})
# sub.id            subscription id (local, see below)
# sub.events        queue.Queue of event dicts
# sub.on_event      optional callback (ev) -> None
# sub.error         terminal delivery error, or None
# sub.close() / sub.unsubscribe()
```

- Events arrive over the transport-level stream (SSE, or long-poll when SSE is
  unavailable — §6.1), so subscriptions are matched **client-side**: no
  `event.subscribe` round-trip is made and `sub.id` is a local id
  (`"local-N"`).
- Delivery failures (terminal auth errors, retry exhaustion) surface via
  `client.poll_error` and `sub.error`; a later `subscribe()` (e.g. after
  re-login) restarts delivery.
- Events are deduplicated by `event.id` (sliding window of 5000, §6.2).
- Unknown event types are delivered as-is; consumers must ignore them (§4.5).
- Unknown/missing fields in responses and events are always tolerated.
- The delivery thread reconnects with exponential backoff (base 1s, factor 2,
  max 30s, ±20% jitter). After a failed iteration the poll cursor resets to 0,
  so a restarted engine (in-memory cursor) is re-read instead of starving the
  client; the dedup window absorbs replays, but consumers should stay
  idempotent (§6.2).

## Transport & TLS options

```python
HubClient(
    url="https://hub.example.com",   # https supported
    transport="auto",                # "auto" | "ws" | "http" | "poll"
    timeout=30.0,                    # per-request timeout
    max_retries=None,                # None = unlimited backoff retries
    reconnect_base=1.0,              # backoff base (seconds)
    verify=False,                    # disable TLS cert verification
    ca_bundle="/path/to/ca.pem",     # custom CA bundle
    proxy="http://proxy:8080",       # explicit proxy (default: env)
)
```

## Offline queue

Eligible writes (everything except `engine.*`, `auth.*` and `event.*`) that
fail with a **connect-phase transport error** are buffered in an in-memory FIFO
(cap 1000) and replayed in order once connectivity works again
(PROTOCOL.md §6.2). This is on by default (`queue_offline=True`); the raised
error carries `data["queued"] = True` — the request was NOT executed yet.
Pass `queue_offline=False` for strict at-most-once semantics (the transport
error surfaces instead). Server-returned JSON-RPC errors mean the server saw
the write: those queue items are dropped, not retried.
`HubClient.queue_write({"method": ..., "params": ...})` buffers a write
manually.

## Contract

- Protocol: [`docs/PROTOCOL.md`](../../docs/PROTOCOL.md)
- Schemas: [`schema/*.json`](../../schema/)