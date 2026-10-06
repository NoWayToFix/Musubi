# musubi/hub/sdk — Go SDK

Go SDK for the Control Hub. **Zero third-party dependencies** — only the Go
standard library, including the minimal RFC6455 WebSocket client in
[`wsclient.go`](wsclient.go).

The single source of truth is [`docs/PROTOCOL.md`](../../docs/PROTOCOL.md) at
the repository root. All method names, error codes, transports and event types
follow that document.

## Features

- JSON-RPC 2.0 over WebSocket (`/v1/ws`) or HTTP POST (`/v1/rpc`), with the
  protocol fallback order `ws -> sse -> poll` (§6.1).
- Transport selection: `auto` (negotiated via `engine.info`), `ws`, `http`,
  `poll`.
- Automatic version negotiation (`engine.info`); `-32005` `HubError` on no
  overlap.
- Events: WebSocket notifications, SSE stream or long-poll — with reconnect +
  exponential backoff (base 1s, factor 2, max 30s, ±20% jitter) and automatic
  re-subscription.
- Event dedup by `event.id` with a 5000-entry sliding window.
- Auth: token / basic login, refresh, logout.
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

The offline write queue is intentionally left as a `TODO` scaffold
(`HubClient.QueueWrite`): the default behavior is "at-most-once" — transport
errors surface to the caller. See `docs/PROTOCOL.md` §6.2 for the full
semantics.

## Contract

- Protocol: [`docs/PROTOCOL.md`](../../docs/PROTOCOL.md)
- Schemas: [`schema/*.json`](../../schema/)