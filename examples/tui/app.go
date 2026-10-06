package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	sdk "musubi/hub/sdk"
)

// Pane identifiers; also the order of the Tab cycle and the 1..4 hot keys.
const (
	paneNodes = iota
	paneCmds
	paneTasks
	paneEvents
	paneCount
)

var paneNames = [paneCount]string{"NODES", "COMMANDS", "TASKS", "EVENTS"}

// Retention caps: the console keeps a bounded window so long sessions never
// grow without limit.
const (
	maxCommands = 200
	maxEvents   = 400
)

type appConfig struct {
	url, token, user, pass string
	transport              string
	refresh                time.Duration
	color                  bool
}

// app holds all console state. The hub event stream mutates it from a
// background goroutine, the UI reads it under mu — that is the whole
// concurrency model.
type app struct {
	cfg appConfig
	c   *sdk.HubClient
	sub *sdk.Subscription

	mu     sync.Mutex
	info   *sdk.ServerInfo
	nodes  []sdk.Node
	cmds   []sdk.Command // newest first
	tasks  []sdk.Task    // newest first
	events []sdk.HubEvent
	status string
	input  string
	pane   int
	sel    [paneCount]int
	scroll [paneCount]int
	help   bool
	quit   bool
}

func newApp(cfg appConfig) *app {
	a := &app{cfg: cfg}
	a.status = "starting..."
	return a
}

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------

// start negotiates the protocol, logs in when credentials were given, loads the
// initial snapshot and subscribes to the event stream.
func (a *app) start(ctx context.Context) error {
	a.c = sdk.New(a.cfg.options())

	info, err := a.c.Info(ctx, &sdk.ClientInfo{
		Name:             "hubtui",
		Version:          "1.0.0",
		ProtocolVersions: []string{"1.0"},
		Encodings:        []string{"json"},
		Transports:       []string{"ws", "http", "poll"},
	})
	if err != nil {
		return fmt.Errorf("engine.info 失败: %w", err)
	}
	a.mu.Lock()
	a.info = info
	a.mu.Unlock()

	switch {
	case a.cfg.token != "":
		if _, err := a.c.LoginToken(ctx, a.cfg.token); err != nil {
			// An engine started with -auth none issues anonymous tokens and
			// rejects the secret; that is fine, keep going anonymously.
			a.setStatus("token 登录被拒（引擎可能为 -auth none），按匿名继续")
		} else {
			a.setStatus("token 登录成功")
		}
	case a.cfg.user != "":
		if _, err := a.c.LoginBasic(ctx, a.cfg.user, a.cfg.pass); err != nil {
			return fmt.Errorf("basic 登录失败: %w", err)
		}
		a.setStatus("basic 登录成功")
	}

	if err := a.refresh(ctx); err != nil {
		a.setStatus("初始拉取失败: " + err.Error())
	}

	sub, err := a.c.EventSubscribe(ctx, nil) // nil = every event type
	if err != nil {
		return fmt.Errorf("event.subscribe 失败: %w", err)
	}
	a.sub = sub

	go a.consumeEvents(sub)
	go a.tick(ctx)

	a.setStatus(fmt.Sprintf("已连接 %s  协议 %s", a.cfg.url, info.ProtocolVersion))
	return nil
}

func (a *app) stop() {
	if a.sub != nil {
		_ = a.sub.Close()
	}
	if a.c != nil {
		_ = a.c.Close()
	}
}

// consumeEvents is the only writer of node/command/task state: the hub tells us
// what changed, exactly like a real console should behave.
func (a *app) consumeEvents(sub *sdk.Subscription) {
	for {
		select {
		case <-sub.Done():
			return
		case ev, ok := <-sub.Events():
			if !ok {
				return
			}
			a.applyEvent(ev)
		}
	}
}

// tick is a safety net: events may be dropped by a full buffer, so the lists are
// re-read periodically (PROTOCOL.md §6.2 delivery is at-least-once/best-effort).
func (a *app) tick(ctx context.Context) {
	iv := a.cfg.refresh
	if iv <= 0 {
		iv = 2 * time.Second
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = a.refresh(ctx)
		}
	}
}

// refresh re-reads node.list and task.list.
func (a *app) refresh(ctx context.Context) error {
	nl, err := a.c.NodeList(ctx, nil, 0, 0)
	if err != nil {
		return err
	}
	tl, err := a.c.TaskList(ctx, nil, 0, 0)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.nodes = nl.Nodes
	a.tasks = newestFirst(len(tl.Tasks), func(i int) sdk.Task { return tl.Tasks[i] })
	a.mu.Unlock()
	return nil
}

// snapshot forces a refresh and is used by the headless modes.
func (a *app) snapshot(ctx context.Context) { _ = a.refresh(ctx) }

// ---------------------------------------------------------------------------
// event -> state
// ---------------------------------------------------------------------------

func (a *app) applyEvent(ev sdk.HubEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.events = append([]sdk.HubEvent{ev}, a.events...)
	if len(a.events) > maxEvents {
		a.events = a.events[:maxEvents]
	}

	switch ev.Type {
	case "node.registered", "node.updated":
		var n sdk.Node
		if decode(ev.Data, &n) == nil && n.ID != "" {
			a.upsertNode(n)
		}
	case "node.status.changed":
		status, _ := ev.Data["status"].(string)
		for i := range a.nodes {
			if a.nodes[i].ID == ev.NodeID {
				a.nodes[i].Status = status
			}
		}
	case "node.unregistered":
		out := a.nodes[:0]
		for _, n := range a.nodes {
			if n.ID != ev.NodeID {
				out = append(out, n)
			}
		}
		a.nodes = out
	case "command.accepted", "command.result", "command.failed", "command.cancelled":
		var c sdk.Command
		if decode(ev.Data, &c) == nil && c.ID != "" {
			a.upsertCommand(c)
		}
	case "command.started":
		id := ev.CommandID
		if id == "" {
			id, _ = ev.Data["command_id"].(string)
		}
		a.setCommandStatus(id, "running")
	case "task.created", "task.progress", "task.updated", "task.completed", "task.failed", "task.cancelled":
		var t sdk.Task
		if decode(ev.Data, &t) == nil && t.ID != "" {
			a.upsertTask(t)
		}
	default:
		// Unknown event types are logged but must never break the console
		// (PROTOCOL.md §4.5 forward compatibility).
	}
}

func (a *app) upsertNode(n sdk.Node) {
	for i := range a.nodes {
		if a.nodes[i].ID == n.ID {
			a.nodes[i] = n
			return
		}
	}
	a.nodes = append(a.nodes, n)
	sort.Slice(a.nodes, func(i, j int) bool { return a.nodes[i].ID < a.nodes[j].ID })
}

func (a *app) upsertCommand(c sdk.Command) {
	for i := range a.cmds {
		if a.cmds[i].ID == c.ID {
			a.cmds[i] = c
			return
		}
	}
	a.cmds = append([]sdk.Command{c}, a.cmds...)
	if len(a.cmds) > maxCommands {
		a.cmds = a.cmds[:maxCommands]
	}
}

func (a *app) setCommandStatus(id, status string) {
	if id == "" {
		return
	}
	for i := range a.cmds {
		if a.cmds[i].ID == id && !isCmdTerminal(a.cmds[i].Status) {
			a.cmds[i].Status = status
			return
		}
	}
}

func (a *app) upsertTask(t sdk.Task) {
	for i := range a.tasks {
		if a.tasks[i].ID == t.ID {
			a.tasks[i] = t
			return
		}
	}
	a.tasks = append([]sdk.Task{t}, a.tasks...)
}

func isCmdTerminal(s string) bool {
	switch s {
	case "succeeded", "failed", "cancelled", "timeout":
		return true
	}
	return false
}

// newestFirst returns a copy of the task list ordered newest first.
func newestFirst[T any](n int, get func(int) T) []T {
	out := make([]T, 0, n)
	for i := n - 1; i >= 0; i-- {
		out = append(out, get(i))
	}
	return out
}

// decode converts an event's free-form data object into a typed struct.
func decode(data map[string]any, dst any) error {
	if data == nil {
		return fmt.Errorf("empty data")
	}
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}

// ---------------------------------------------------------------------------
// selection helpers
// ---------------------------------------------------------------------------

// selectedNode returns the highlighted node id (empty when the pane is empty).
func (a *app) selectedNode() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := a.sel[paneNodes]
	if a.scroll[paneNodes]+i < 0 || a.scroll[paneNodes]+i >= len(a.nodes) {
		return ""
	}
	return a.nodes[a.scroll[paneNodes]+i].ID
}

func (a *app) selectedCommand() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := a.sel[paneCmds]
	if a.scroll[paneCmds]+i < 0 || a.scroll[paneCmds]+i >= len(a.cmds) {
		return ""
	}
	return a.cmds[a.scroll[paneCmds]+i].ID
}

func (a *app) selectedTask() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := a.sel[paneTasks]
	if a.scroll[paneTasks]+i < 0 || a.scroll[paneTasks]+i >= len(a.tasks) {
		return ""
	}
	return a.tasks[a.scroll[paneTasks]+i].ID
}

func (a *app) paneLen(p int) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch p {
	case paneNodes:
		return len(a.nodes)
	case paneCmds:
		return len(a.cmds)
	case paneTasks:
		return len(a.tasks)
	}
	return len(a.events)
}

func (a *app) setStatus(s string) {
	a.mu.Lock()
	a.status = s
	a.mu.Unlock()
}

// ---------------------------------------------------------------------------
// text rendering helpers (headless modes)
// ---------------------------------------------------------------------------

// dumpText renders the whole console state as plain text, used by the
// headless script/line modes.
func (a *app) dumpText() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "NODES (%d)\n", len(a.nodes))
	for _, n := range a.nodes {
		caps := strings.Join(n.Capabilities, ",")
		fmt.Fprintf(&b, "  %-8s %-8s %-14s %s\n", n.ID, n.Status, caps, n.Tags)
	}
	fmt.Fprintf(&b, "COMMANDS (%d)\n", len(a.cmds))
	for _, c := range a.cmds {
		line := fmt.Sprintf("  %-6s %-6s %-10s %-10s", c.ID, c.NodeID, c.Method, c.Status)
		switch {
		case c.Error != nil:
			line += fmt.Sprintf(" err=%d %s", c.Error.Code, c.Error.Message)
		case c.Result != nil:
			if s := compact(c.Result); s != "" {
				line += " " + s
			}
		}
		fmt.Fprintln(&b, line)
	}
	fmt.Fprintf(&b, "TASKS (%d)\n", len(a.tasks))
	for _, t := range a.tasks {
		fmt.Fprintf(&b, "  %-6s %-14s %-10s %3d%%  %d cmds\n", t.ID, t.Name, t.Status,
			int(t.Progress*100), len(t.CommandIDs))
	}
	fmt.Fprintf(&b, "EVENTS (latest %d of stream)\n", min(len(a.events), 15))
	for i, ev := range a.events {
		if i >= 15 {
			break
		}
		fmt.Fprintf(&b, "  %-20s %-22s %s\n", ev.Type, ev.ID, ev.NodeID+ev.TaskID+ev.CommandID)
	}
	return strings.TrimRight(b.String(), "\n")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// compact renders a command result as a short one-line string.
func compact(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case map[string]any, []any:
		b, err := json.Marshal(t)
		if err != nil {
			return ""
		}
		s := string(b)
		if len(s) > 60 {
			s = s[:60] + "…"
		}
		return s
	default:
		return fmt.Sprint(t)
	}
}
