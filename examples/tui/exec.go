package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	sdk "musubi/hub/sdk"
)

// execute runs one console command line and returns its textual output.
// It is shared by the interactive input line, --script and piped stdin so all
// three entry points behave identically.
func (a *app) execute(ctx context.Context, line string) (string, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", nil
	}
	args := splitArgs(line)
	if len(args) == 0 {
		return "", nil
	}
	cmd, args := strings.ToLower(args[0]), args[1:]

	switch cmd {
	case "help", "?":
		return helpText, nil
	case "quit", "exit", "q":
		a.mu.Lock()
		a.quit = true
		a.mu.Unlock()
		return "bye", nil
	case "wait":
		ms := 500
		if len(args) > 0 {
			if v, err := parseInt(args[0]); err == nil {
				ms = v
			}
		}
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
		case <-ctx.Done():
		}
		return fmt.Sprintf("waited %dms", ms), nil
	case "nodes":
		if err := a.refreshNodes(ctx); err != nil {
			return "", err
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		var b strings.Builder
		for _, n := range a.nodes {
			fmt.Fprintf(&b, "%s %s %s\n", n.ID, n.Status, strings.Join(n.Capabilities, ","))
		}
		return strings.TrimRight(b.String(), "\n"), nil
	case "tasks":
		if err := a.refreshTasks(ctx); err != nil {
			return "", err
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		var b strings.Builder
		for _, t := range a.tasks {
			fmt.Fprintf(&b, "%s %s %s %.0f%%\n", t.ID, t.Name, t.Status, t.Progress*100)
		}
		return strings.TrimRight(b.String(), "\n"), nil
	case "refresh":
		if err := a.refresh(ctx); err != nil {
			return "", err
		}
		return "refreshed", nil
	case "dump":
		return a.dumpText(), nil
	case "send":
		return a.cmdSend(ctx, args)
	case "batch":
		return a.cmdBatch(ctx, args)
	case "task":
		return a.cmdTask(ctx, args)
	case "cancel":
		if len(args) == 0 {
			return "", fmt.Errorf("用法: cancel <command_id>")
		}
		c, err := a.c.CommandCancel(ctx, args[0])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s %s", c.ID, c.Status), nil
	case "taskcancel":
		if len(args) == 0 {
			return "", fmt.Errorf("用法: taskcancel <task_id>")
		}
		t, err := a.c.TaskCancel(ctx, args[0])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s %s", t.ID, t.Status), nil
	}
	return "", fmt.Errorf("未知命令 %q（输入 help 查看用法）", cmd)
}

// cmdSend: send <node_id> <method> [json] [key=<idempotency_key>]
func (a *app) cmdSend(ctx context.Context, args []string) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("用法: send <node_id> <method> [json参数] [key=<幂等键>]")
	}
	spec := sdk.CommandSpec{NodeID: args[0], Method: args[1]}
	for _, extra := range args[2:] {
		if k, v, ok := strings.Cut(extra, "="); ok && k == "key" {
			spec.IdempotencyKey = v
			continue
		}
		params, err := parseParams(extra)
		if err != nil {
			return "", err
		}
		spec.Params = params
	}
	if spec.Params == nil {
		spec.Params = map[string]any{}
	}
	c, err := a.c.CommandSend(ctx, spec)
	if err != nil {
		return "", err
	}
	// command.send is asynchronous: report the snapshot and let the event
	// stream deliver the terminal state.
	return fmt.Sprintf("%s %s/%s status=%s（终态见 EVENTS 面板）", c.ID, c.NodeID, c.Method, c.Status), nil
}

// cmdBatch: batch <node_id>:<method>[,<node_id>:<method>...] — synchronous.
func (a *app) cmdBatch(ctx context.Context, args []string) (string, error) {
	specs, err := parseSpecs(args)
	if err != nil {
		return "", err
	}
	if len(specs) == 0 {
		return "", fmt.Errorf("用法: batch <node_id>:<method>[,...]")
	}
	res, err := a.c.CommandBatch(ctx, specs, "")
	if err != nil {
		return "", err
	}
	parts := make([]string, 0, len(res.Commands))
	for _, c := range res.Commands {
		if c.Error != nil {
			parts = append(parts, fmt.Sprintf("%s=%s(%d)", c.ID, c.Status, c.Error.Code))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s", c.ID, c.Status))
	}
	return strings.Join(parts, " "), nil
}

// cmdTask: task <name> <node_id>:<method>[,...] [parallel] — asynchronous.
func (a *app) cmdTask(ctx context.Context, args []string) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("用法: task <名称> <node_id>:<method>[,<...>] [parallel]")
	}
	spec := sdk.TaskSpec{Name: args[0]}
	for _, extra := range args[1:] {
		if strings.EqualFold(extra, "parallel") {
			spec.Parallel = true
			continue
		}
		cs, err := parseSpecs([]string{extra})
		if err != nil {
			return "", err
		}
		spec.Commands = append(spec.Commands, cs...)
	}
	if len(spec.Commands) == 0 {
		return "", fmt.Errorf("任务至少需要一条指令")
	}
	t, err := a.c.TaskCreate(ctx, spec)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s %s status=%s cmds=%d（终态见 TASKS 面板）", t.ID, t.Name, t.Status, len(t.CommandIDs)), nil
}

// parseSpecs turns "n-1:echo,n-2:fail" into command specs.
func parseSpecs(args []string) ([]sdk.CommandSpec, error) {
	var out []sdk.CommandSpec
	for _, group := range args {
		for _, item := range strings.Split(group, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			node, method, ok := strings.Cut(item, ":")
			if !ok {
				return nil, fmt.Errorf("无法解析 %q，应为 <node_id>:<method>", item)
			}
			spec := sdk.CommandSpec{NodeID: node, Method: method, Params: map[string]any{}}
			// Optional third field: "n-1:sleep:500".
			if extra := strings.Split(method, ":"); len(extra) == 2 {
				spec.Method, spec.Params = extra[0], map[string]any{"ms": extra[1]}
			}
			out = append(out, spec)
		}
	}
	return out, nil
}

// parseParams accepts a JSON object; an empty argument yields an empty map.
func parseParams(s string) (map[string]any, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, fmt.Errorf("参数不是合法 JSON 对象 %q: %w", s, err)
	}
	return m, nil
}

// splitArgs splits on spaces while keeping quoted strings and JSON braces
// together: send n-1 echo {"text":"a b"} → ["send","n-1","echo","{...}"]
func splitArgs(s string) []string {
	var (
		out   []string
		cur   strings.Builder
		depth int
		quote bool
	)
	flush := func() {
		if cur.Len() > 0 {
			out = append(out, cur.String())
			cur.Reset()
		}
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case quote:
			cur.WriteByte(ch)
			if ch == '"' {
				quote = false
			}
		case ch == '"':
			quote = true
			cur.WriteByte(ch)
		case ch == '{' || ch == '[':
			depth++
			cur.WriteByte(ch)
		case ch == '}' || ch == ']':
			depth--
			cur.WriteByte(ch)
		case (ch == ' ' || ch == '\t') && depth == 0:
			flush()
		default:
			cur.WriteByte(ch)
		}
	}
	flush()
	return out
}

func parseInt(s string) (int, error) {
	var v int
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}

func (a *app) refreshNodes(ctx context.Context) error {
	nl, err := a.c.NodeList(ctx, nil, 0, 0)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.nodes = nl.Nodes
	a.mu.Unlock()
	return nil
}

func (a *app) refreshTasks(ctx context.Context) error {
	tl, err := a.c.TaskList(ctx, nil, 0, 0)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.tasks = newestFirst(len(tl.Tasks), func(i int) sdk.Task { return tl.Tasks[i] })
	a.mu.Unlock()
	return nil
}

const helpText = `命令: nodes | tasks | refresh | dump | wait <ms> | help | quit
      send <node_id> <method> [json] [key=<幂等键>]
      batch <node_id>:<method>[,<node_id>:<method>...]
      task <名称> <node_id>:<method>[,...] [parallel]
      cancel <command_id> | taskcancel <task_id>`

// runScript executes a ';'-separated script (used by --script).
func (a *app) runScript(ctx context.Context, lines []string) int {
	code := 0
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		fmt.Printf("$ %s\n", l)
		out, err := a.execute(ctx, l)
		if err != nil {
			fmt.Println("  error:", err)
			code = 1
			continue
		}
		if out != "" {
			for _, ln := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
				fmt.Println(" ", ln)
			}
		}
		a.mu.Lock()
		quit := a.quit
		a.mu.Unlock()
		if quit {
			break
		}
	}
	return code
}

// runLineMode reads commands from a non-TTY stdin (pipes, CI logs).
func (a *app) runLineMode(ctx context.Context, in io.Reader) int {
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 256)
	for {
		n, err := in.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			for {
				idx := strings.IndexByte(string(buf), '\n')
				if idx < 0 {
					break
				}
				line := string(buf[:idx])
				buf = buf[idx+1:]
				line = strings.TrimSpace(line)
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				fmt.Printf("> %s\n", line)
				out, execErr := a.execute(ctx, line)
				if execErr != nil {
					fmt.Println("  error:", execErr)
					continue
				}
				if out != "" {
					fmt.Println(indent(out))
				}
				a.mu.Lock()
				quit := a.quit
				a.mu.Unlock()
				if quit {
					return 0
				}
			}
		}
		if err != nil {
			return 0
		}
	}
}

func indent(s string) string {
	var b strings.Builder
	for _, l := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		b.WriteString("  ")
		b.WriteString(l)
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}
