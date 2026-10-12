# musubi/hub/sdk — Go SDK

Go SDK for the Control Hub. **Zero third-party dependencies** — only the Go
standard library, including the minimal RFC6455 WebSocket client in
[`wsclient.go`](wsclient.go).

The single source of truth is [`docs/PROTOCOL.md`](../../docs/PROTOCOL.md) at
the repository root. All method names, error codes, transports and event types
follow that document.

## Features

- JSON-RPC 2.0 over WebSocket (`/v1/ws`) or HTTP POST (`/v1/rpc`), with the
  protocol fallback order `ws -> sse -> poll` (§6.1). The WS→HTTP fallback
  happens only for transport-level failures; server-returned `*HubError`s are
  never resent.
- Per-call timeout applies to WS calls too (`Options.Timeout`); a context
  deadline always wins when shorter.
- Transport selection: `auto` (negotiated via `engine.info`), `ws`, `http`,
  `poll`.
- Automatic version negotiation (`engine.info`); `-32005` `HubError` on no
  overlap.
- Events: WebSocket notifications, SSE stream or long-poll — with reconnect +
  exponential backoff (base 1s, factor 2, max 30s, ±20% jitter) and automatic
  re-subscription. Poll responses are classified: 2xx proceeds, 401/403 stop
  the loop (surfaced via `Options.OnError`), other errors retry with backoff.
  After a reconnect the poll cursor resets to 0 so a restarted engine
  (in-memory cursor) is re-read instead of starving the client.
- Event dedup by `event.id` with a 5000-entry sliding window.
- Auth: token / basic login, refresh, logout; stores the rotating access +
  refresh token pair and auto-refreshes once on `-32003`/`-32002`.
- Compatibility: unknown response/event fields are ignored, missing fields are
  tolerated, unknown event types are delivered as-is (consumers must ignore
  them, §4.5).

## Usage

```go
import "musubi/hub/sdk"

func main() {
    c := sdk.New(sdk.Options{URL: "http://127.0.0.1:8080", Transport: "auto"})
    defer c.Close()

    ctx := context.Background()
    si, err := c.Info(ctx, &sdk.ClientInfo{Name: "go-demo", Version: "1.0.0"})
    if err != nil {
        var he *sdk.HubError
        if errors.As(err, &he) && he.IsVersionMismatch() {
            log.Fatal("protocol mismatch:", he.Data)
        }
        log.Fatal(err)
    }
    log.Printf("hub %s/%s negotiated protocol %s",
        si.Server.Name, si.Server.Version, si.ProtocolVersion)

    nodes, err := c.NodeList(ctx, nil, 100, 0)
    if err != nil {
        log.Fatal(err)
    }
    for _, n := range nodes.Nodes {
        log.Printf("node %s (%s) status=%s", n.ID, n.Name, n.Status)
    }

    sub, err := c.EventSubscribe(ctx, nil) // all events
    if err != nil {
        log.Fatal(err)
    }
    defer sub.Close()
    go func() {
        for ev := range sub.Events() {
            log.Printf("event %s %s node=%s task=%s data=%v",
                ev.ID, ev.Type, ev.NodeID, ev.TaskID, ev.Data)
        }
    }()

    cmd, err := c.CommandSend(ctx, sdk.CommandSpec{
        NodeID: "n-1", Method: "echo", Params: map[string]any{"text": "hi"},
    })
    if err != nil {
        var he *sdk.HubError
        if errors.As(err, &he) {
            log.Printf("hub error %d: %s", he.Code, he.Message)
        }
    } else {
        log.Printf("command %s status=%s result=%v", cmd.ID, cmd.Status, cmd.Result)
    }
}
```

## Error handling

All RPC failures are returned as `*sdk.HubError` (see [`errors.go`](errors.go)
for the `-32001..-32031` constants). Use `errors.As` to inspect them:

```go
_, err := c.NodeGet(ctx, "nope")
var he *sdk.HubError
if errors.As(err, &he) {
    switch he.Code {
    case sdk.ErrNodeNotFound:
        // -32010
    }
    if he.IsAuth() { /* -32001..-32004 */ }
    if he.IsVersionMismatch() { /* -32005 */ }
}
```

## Offline queue

Write calls (everything except `engine.*` and `auth.*`) that fail with a
transport-level error are buffered in an in-memory FIFO (cap 1000) and
replayed in order once the connection works again (PROTOCOL.md §6.2). Such a
call returns `sdk.ErrQueued` — the request was NOT executed yet:

```go
_, err := c.CommandSend(ctx, spec)
if errors.Is(err, sdk.ErrQueued) {
    // buffered; it replays automatically after reconnect
}
```

Set `Options.DisableOfflineQueue` for strict at-most-once semantics (the
transport error surfaces instead). `HubClient.QueueWrite(method, params)` can
buffer a write manually. Server-returned `*HubError`s are authoritative and
never queued nor resent on another transport.

## Auth / token rotation

`Login` and `Refresh` store the whole token pair. Under rotation the engine
returns a NEW access_token AND a NEW refresh_token and invalidates the old
pair; both are stored together. A call rejected with `-32003`/`-32002` is
retried once after an automatic `auth.refresh` (single-flighted, so
concurrent calls share one rotation instead of burning the rotated-away
token). `Refresh(ctx, "")` uses the stored refresh token; a refresh token
rejected by the server is dropped.

## Contract

- Protocol: [`docs/PROTOCOL.md`](../../docs/PROTOCOL.md)
- Schemas: [`schema/*.json`](../../schema/)