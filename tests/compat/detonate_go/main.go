// Command-line compatibility smoke test (Go SDK version).
//
// Runs the same assertion sequence as smoke_py.py and smoke_ts.mjs against a
// running hub, for two transports (long-poll and HTTP/SSE), to prove the Go
// SDK behaves identically to the Python and TypeScript SDKs.
//
// Usage: HUB_URL=http://127.0.0.1:8080 go run .
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	sdk "musubi/hub/sdk"
)

var passed int

func check(name string, cond bool, detail string) {
	if !cond {
		fmt.Fprintf(os.Stderr, "FAIL %s: %s\n", name, detail)
		os.Exit(1)
	}
	passed++
	fmt.Println("PASS", name)
}

// waitFor blocks until an event satisfying pred arrives on the subscription.
func waitFor(ctx context.Context, sub *sdk.Subscription, pred func(sdk.HubEvent) bool) (sdk.HubEvent, error) {
	t := time.NewTimer(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case ev := <-sub.Events():
			if pred(ev) {
				return ev, nil
			}
		case <-t.C:
			return sdk.HubEvent{}, errors.New("等待事件超时")
		case <-ctx.Done():
			return sdk.HubEvent{}, ctx.Err()
		}
	}
}

func cmdResultText(ev sdk.HubEvent) string {
	m, ok := ev.Data["result"].(map[string]any)
	if !ok {
		return ""
	}
	s, _ := m["text"].(string)
	return s
}

func run(url, transport string) {
	tag := strings.ToUpper(transport)
	ctx := context.Background()
	c := sdk.New(sdk.Options{URL: url, Transport: transport, Timeout: 10 * time.Second})
	defer c.Close()

	// 1. 版本协商与能力发现
	info, err := c.Info(ctx, &sdk.ClientInfo{
		Name: "smoke-go", Version: "1.0.0",
		ProtocolVersions: []string{"1.0"}, Encodings: []string{"json"},
		Transports: []string{"ws", "http"},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL %s engine.info 协商: %v\n", tag, err)
		os.Exit(1)
	}
	check(tag+" engine.info 协商", info.ProtocolVersion == "1.0", info.ProtocolVersion+"@"+info.Server.Name)
	check(tag+" 传输能力含 ws/http", has(info.Transports, "ws") && has(info.Transports, "http"), fmt.Sprint(info.Transports))

	// 2. engine.ping
	pong, err := c.Ping(ctx)
	check(tag+" engine.ping", err == nil && pong["pong"] == true, fmt.Sprint(pong, err))

	// 3. 节点
	nodes, err := c.NodeList(ctx, nil, 0, 0)
	check(tag+" node.list 返回 3 个节点", err == nil && len(nodes.Nodes) == 3, fmt.Sprint(len(nodes.Nodes), err))
	node, err := c.NodeGet(ctx, "n-1")
	check(tag+" node.get", err == nil && node.ID == "n-1", fmt.Sprint(err))
	check(tag+" node.get 能力非空", len(node.Capabilities) >= 1, fmt.Sprint(node.Capabilities))

	// 4. command.send 异步：订阅先行，等待 command.result
	sub, err := c.EventSubscribe(ctx, &sdk.EventFilters{
		EventTypes: []string{"command.result", "command.failed"},
		NodeID:     "n-1",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "FAIL subscribe:", err)
		os.Exit(1)
	}
	cmd, err := c.CommandSend(ctx, sdk.CommandSpec{
		NodeID: "n-1", Method: "echo", Params: map[string]any{"text": "hi"}, Timeout: 5000,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "FAIL command.send:", err)
		os.Exit(1)
	}
	ev, err := waitFor(ctx, sub, func(e sdk.HubEvent) bool {
		return e.Type == "command.result" && e.CommandID == cmd.ID
	})
	check(tag+" command.send 事件终态 succeeded",
		err == nil && ev.Data["status"] == "succeeded" && cmdResultText(ev) == "hi",
		fmt.Sprint(ev, err))

	// 5. 幂等键：同键二次下发复用首次结果
	ik := "smoke-ik-" + tag + "-" + strconv.Itoa(os.Getpid())
	first, err := c.CommandSend(ctx, sdk.CommandSpec{
		NodeID: "n-1", Method: "echo", Params: map[string]any{"text": "dedupe"}, IdempotencyKey: ik,
	})
	_, _ = waitFor(ctx, sub, func(e sdk.HubEvent) bool {
		return e.Type == "command.result" && e.CommandID == first.ID
	})
	dup, err := c.CommandSend(ctx, sdk.CommandSpec{
		NodeID: "n-1", Method: "echo", Params: map[string]any{"text": "changed"}, IdempotencyKey: ik,
	})
	dupText := ""
	if m, ok := dup.Result.(map[string]any); ok {
		dupText, _ = m["text"].(string)
	}
	check(tag+" command.send 幂等去重",
		err == nil && dup.ID == first.ID && dup.Status == "succeeded" && dupText == "dedupe",
		fmt.Sprint(dup, err))

	// 6. command.batch 同步返回终态（含失败项，不抛异常）
	batch, err := c.CommandBatch(ctx, []sdk.CommandSpec{
		{NodeID: "n-2", Method: "echo", Params: map[string]any{"text": "b"}},
		{NodeID: "n-2", Method: "fail"},
	}, "")
	st := []string{}
	for _, cm := range batch.Commands {
		st = append(st, cm.Status)
	}
	check(tag+" command.batch 混合终态", err == nil && fmt.Sprint(st) == "[succeeded failed]", fmt.Sprint(st, err))

	// 7. task.create 异步：等待 task.completed
	tsub, err := c.EventSubscribe(ctx, &sdk.EventFilters{EventTypes: []string{"task.completed", "task.failed"}})
	if err != nil {
		fmt.Fprintln(os.Stderr, "FAIL task subscribe:", err)
		os.Exit(1)
	}
	task, err := c.TaskCreate(ctx, sdk.TaskSpec{
		Name:     "smoke-task-" + tag,
		Commands: []sdk.CommandSpec{{NodeID: "n-3", Method: "echo"}},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "FAIL task.create:", err)
		os.Exit(1)
	}
	tev, err := waitFor(ctx, tsub, func(e sdk.HubEvent) bool {
		return e.TaskID == task.ID && (e.Type == "task.completed" || e.Type == "task.failed")
	})
	check(tag+" task.create 终态 succeeded", err == nil && tev.Data["status"] == "succeeded", fmt.Sprint(tev, err))
	got, err := c.TaskGet(ctx, task.ID)
	check(tag+" task.get 与事件一致", err == nil && got.Status == "succeeded", fmt.Sprint(got, err))

	// 8. 错误码：未知节点
	_, err = c.NodeGet(ctx, "n-999")
	var he *sdk.HubError
	check(tag+" node.not_found -32010", errors.As(err, &he) && he.Code == sdk.ErrNodeNotFound, fmt.Sprint(err))

	// 9. 订阅释放
	err = sub.Close()
	check(tag+" event.unsubscribe", err == nil, fmt.Sprint(err))
	_ = tsub.Close()

	c.Close()
}

func has(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

func main() {
	url := os.Getenv("HUB_URL")
	if url == "" {
		url = "http://127.0.0.1:8080"
	}
	run(url, "poll")
	run(url, "http")
	fmt.Printf("GO SDK SMOKE: ALL PASS (%d)\n", passed)
}
