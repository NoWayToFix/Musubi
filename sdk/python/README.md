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

## Seats and real-node dispatch

```python
# Address a seat instead of a node (mutually exclusive with node_id).
client.command_send(seat_id="s-...", method="echo", params={"text": "hi"})
client.command_list({"status": "succeeded"}, limit=20)
client.node_list({"runtime": "python"})      # node.list filters platform runtime/os

# Real-node worker loop: claim, execute, complete.
for cmd in client.command_pull("w-1", limit=4, wait=5000)["commands"]:
    try:
        result = run_locally(cmd["method"], cmd.get("params", {}))
        client.command_complete("w-1", cmd["id"], "succeeded", result=result)
    except Exception as exc:
        client.command_complete("w-1", cmd["id"], "failed",
                                error={"code": -32000, "message": str(exc)})
```

## Agent（节点运行时）

`Agent` 实现 PROTOCOL.md §5.4 的完整生命周期，是"一个平台一份 SDK"的接入入口：

```python
from hub import Agent, AgentHandlerError

agent = Agent(url="http://127.0.0.1:8080", node_id="py-app-1",
              capabilities=["echo", "sys.info"], tags=["python", "prod"],
              concurrency=2)

@agent.handle("sys.info")
def sys_info(_params):
    return collect_sys_info()

@agent.handle("fail")
def fail(params):
    raise AgentHandlerError(-32603, params.get("message", "failed"))

agent.start()          # 后台注册 → 心跳 → pull → 执行 → complete
agent.wait()           # 阻塞直到 stop()
```

- **自动平台探测**：`detect_platform()` 上报 `os/arch/runtime=python/runtime_version/agent_version`；
  可用 `platform=` 覆盖。
- **内置方法**：`echo`、`platform.info`。
- **失败语义**：handler 抛异常 → `failed`；`AgentHandlerError(code, message)` 指定错误码；
  未注册方法 → `-32601`。
- **自愈**：注册指数退避；`-32010`/`-32011` 自动重新注册；`stop()` 触发注销。
- 线程安全：handler 注册、心跳与多个 pull 线程互不阻塞；`handler_timeout=` 可限制单条执行时长。

示例：[`examples/agents/python`](../../examples/agents/python/)。

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