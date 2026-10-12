package main

import (
	"fmt"
	"strings"
	"time"

	sdk "musubi/hub/sdk"
)

// ---------------------------------------------------------------------------
// ANSI helpers (basic 16 colours + SGR attributes: universally supported)
// ---------------------------------------------------------------------------

const (
	ansiReset   = "\x1b[0m"
	ansiBold    = "\x1b[1m"
	ansiDim     = "\x1b[2m"
	ansiReverse = "\x1b[7m"
)

// Colour codes used by the palette below.
const (
	cReset = "\x1b[0m"
	cBold  = "\x1b[1m"
	cDim   = "\x1b[90m"
	cRed   = "\x1b[31m"
	cGreen = "\x1b[32m"
	cAmber = "\x1b[33m"
	cBlue  = "\x1b[36m"
	cWhite = "\x1b[97m"
)

// colorize wraps s in a colour when colours are enabled.
func (a *app) colorize(c, s string) string {
	if !a.cfg.color || c == "" {
		return s
	}
	return c + s + cReset
}

// visibleWidth counts display cells, treating every rune as width 1. The demo
// stays ASCII-only inside panes, so this is enough and keeps the code simple.
func visibleWidth(s string) int {
	return len([]rune(stripANSI(s)))
}

// stripANSI removes SGR sequences so widths can be measured.
func stripANSI(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] == 0x1b {
			for i < len(s) && s[i] != 'm' {
				i++
			}
			i++ // skip 'm'
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

// pad right-pads s with spaces to exactly w display cells.
func pad(s string, w int) string {
	n := w - visibleWidth(s)
	if n <= 0 {
		return s
	}
	return s + strings.Repeat(" ", n)
}

// ellipsis truncates s to w cells, appending "…" when it does not fit.
func ellipsis(s string, w int) string {
	r := []rune(s)
	if len(r) <= w {
		return s
	}
	if w <= 1 {
		return string(r[:w])
	}
	return string(r[:w-1]) + "…"
}

// ---------------------------------------------------------------------------
// frame
// ---------------------------------------------------------------------------

// frame renders the whole console into a string of exactly termH lines.
func (a *app) frame() string {
	w, h := termSize()
	a.mu.Lock()
	info := a.info
	status := a.status
	input := a.input
	help := a.help
	nodes := append([]sdk.Node(nil), a.nodes...)
	cmds := append([]sdk.Command(nil), a.cmds...)
	tasks := append([]sdk.Task(nil), a.tasks...)
	events := append([]sdk.HubEvent(nil), a.events...)
	sel := a.sel
	scroll := a.scroll
	pane := a.pane
	a.mu.Unlock()

	if h < 10 {
		h = 10
	}
	if w < 40 {
		w = 40
	}
	body := h - 4 // header + input + status + bottom padding

	var lines []string
	lines = append(lines, a.headerLine(w, info))
	if help {
		lines = append(lines, a.helpLines(w, body)...)
	} else {
		lines = append(lines, a.paneLines(w, body, pane, nodes, cmds, tasks, events, sel, scroll)...)
	}
	lines = append(lines, a.inputLine(w, input, pane))
	lines = append(lines, a.statusLine(w, status))
	for len(lines) < h {
		lines = append(lines, "")
	}
	return strings.Join(lines[:h], "\n")
}

func (a *app) headerLine(w int, info *sdk.ServerInfo) string {
	name, ver, proto, transports := "hub", "", "", ""
	if info != nil {
		name = "hubd"
		if info.Server != nil {
			if info.Server.Name != "" {
				name = info.Server.Name
			}
			ver = info.Server.Version
		}
		proto = info.ProtocolVersion
		transports = strings.Join(info.Transports, "|")
	}
	ts := time.Now().Format("15:04:05")
	left := fmt.Sprintf(" %s %s %s", a.colorize(cBold+cBlue, "HUB"), a.colorize(cBold, name), a.colorize(cDim, ver))
	mid := fmt.Sprintf(" proto %s ", proto)
	right := fmt.Sprintf(" %s %s ", ts, a.colorize(cDim, transports))
	gap := w - visibleWidth(left) - visibleWidth(mid) - visibleWidth(right)
	if gap < 1 {
		return ellipsis(left, w)
	}
	return left + strings.Repeat(" ", gap/2) + mid + strings.Repeat(" ", gap-gap/2) + right
}

// paneLines renders the four panels. Wide terminals get the classic 3-column
// layout; narrow ones (or a focused pane) fall back to a single full-width
// pane so nothing is ever squeezed into unreadable slivers.
func (a *app) paneLines(w, h, active int, nodes []sdk.Node, cmds []sdk.Command,
	tasks []sdk.Task, events []sdk.HubEvent, sel, scroll [paneCount]int) []string {

	if w < 108 {
		// Single pane mode: show the focused panel only.
		title, rows := "", []string{}
		switch active {
		case paneNodes:
			title, rows = "NODES", a.nodeRows(nodes, sel[paneNodes], scroll[paneNodes], w-4)
		case paneCmds:
			title, rows = "COMMANDS", a.commandRows(cmds, sel[paneCmds], scroll[paneCmds], w-4)
		case paneTasks:
			title, rows = "TASKS", a.taskRows(tasks, sel[paneTasks], scroll[paneTasks], w-4)
		default:
			title, rows = "EVENTS", a.eventRows(events, sel[paneEvents], scroll[paneEvents], w-4)
		}
		return a.box(title, w, rows, h, sel[active], scroll[active], true)
	}

	// 3-column layout: nodes | commands | events, with tasks under nodes.
	colW := (w - 3) / 3
	if colW < 24 {
		colW = 24
	}
	leftH := h / 2
	rightH := h - leftH

	leftTop := a.box("NODES", colW, a.nodeRows(nodes, sel[paneNodes], scroll[paneNodes], colW-4), leftH, sel[paneNodes], scroll[paneNodes], active == paneNodes)
	leftBot := a.box("TASKS", colW, a.taskRows(tasks, sel[paneTasks], scroll[paneTasks], colW-4), rightH, sel[paneTasks], scroll[paneTasks], active == paneTasks)
	mid := a.box("COMMANDS", colW, a.commandRows(cmds, sel[paneCmds], scroll[paneCmds], colW-4), h, sel[paneCmds], scroll[paneCmds], active == paneCmds)
	right := a.box("EVENTS", colW, a.eventRows(events, sel[paneEvents], scroll[paneEvents], colW-4), h, sel[paneEvents], scroll[paneEvents], active == paneEvents)

	var out []string
	for i := 0; i < h; i++ {
		l := leftTop[i]
		if i >= leftH {
			l = leftBot[i-leftH]
		}
		out = append(out, l+mid[i]+right[i])
	}
	return out
}

// box wraps rows in a titled frame of the given size, padding with blanks.
// Only the visible window of rows is rendered (h-2 body lines), positioned so
// the highlighted row (scroll+sel) always stays on screen.
func (a *app) box(title string, w int, rows []string, h, sel, scroll int, focused bool) []string {
	border := a.colorize(cDim, "│")
	head := " " + title
	if focused {
		head = a.colorize(cBold+cWhite, "▌"+title)
	}
	out := []string{a.boxTop(w, head, focused)}
	rows = visibleRows(rows, sel, scroll, h-2)
	for i := 0; i < h-2; i++ {
		row := ""
		if i < len(rows) {
			row = rows[i]
		}
		out = append(out, border+pad(" "+row, w-2)+border)
	}
	out = append(out, a.boxBottom(w))
	return out
}

func (a *app) boxTop(w int, title string, focused bool) string {
	t := " " + title
	if focused {
		t = a.colorize(cBold+cWhite, "▌"+title)
	}
	dashes := w - visibleWidth(t) - 3
	if dashes < 0 {
		dashes = 0
	}
	return a.colorize(cDim, "┌") + t + a.colorize(cDim, strings.Repeat("─", dashes)+"┐")
}

func (a *app) boxBottom(w int) string {
	return a.colorize(cDim, "└"+strings.Repeat("─", w-2)+"┘")
}

func (a *app) inputLine(w int, input string, pane int) string {
	prompt := a.colorize(cBold+cGreen, "▌ ")
	prompt += a.colorize(cDim, "command ❯ ")
	if pane == paneNodes || pane == paneTasks || pane == paneCmds {
		prompt += a.colorize(cDim, hintFor(pane, input))
	}
	used := visibleWidth(prompt) + visibleWidth(input)
	if used > w-1 {
		prompt = a.colorize(cBold+cGreen, "▌ ")
	}
	return prompt + ellipsis(input, w-visibleWidth(prompt)-1)
}

func hintFor(pane int, input string) string {
	if input != "" {
		return ""
	}
	switch pane {
	case paneNodes:
		return "(Enter=send echo · s=echo · f=fail · 1-4 切面板)"
	case paneCmds:
		return "(Enter=cancel · c=取消所选 · r=刷新)"
	case paneTasks:
		return "(Enter=新建任务 · t=任务模板)"
	}
	return "(Tab/1-4 面板 · ↑↓ 选择 · ? 帮助 · q 退出)"
}

func (a *app) statusLine(w int, status string) string {
	keys := a.colorize(cDim, " Tab面板 ↑↓选择 Enter执行 s发送 f失败 t任务 c取消 r刷新 ?帮助 q退出 ")
	left := " " + status
	gap := w - visibleWidth(left) - visibleWidth(keys)
	if gap < 1 {
		return ellipsis(a.colorize(cAmber, left), w)
	}
	return a.colorize(cAmber, left) + strings.Repeat(" ", gap) + keys
}

func (a *app) helpLines(w, h int) []string {
	help := []string{
		" 键盘操作",
		"   ↑ / ↓ / k / j   在当前面板上下选择",
		"   PgUp / PgDn      翻页",
		"   Tab 或 1..4      切换 NODES / COMMANDS / TASKS / EVENTS",
		"   Enter            在 NODES 上填入 send，在 COMMANDS 上填入 cancel",
		"   s / f / t / c    快速模板：echo / fail / task / cancel",
		"   r                立即刷新节点与任务",
		"   ?                打开/关闭本帮助    q 或 Ctrl-C 退出",
		"",
		" 命令行语法（输入框，回车执行）",
		"   nodes | tasks | refresh | dump | help | quit",
		"   send <node_id> <method> [json参数] [key=<幂等键>]",
		"   batch <node_id>:<method>[,<node_id>:<method>...]",
		"   task <名称> <node_id>:<method>[,<...>] [parallel]",
		"   cancel <command_id>",
		"   taskcancel <task_id>",
		"   wait <毫秒>",
		"",
		" 说明：command.send 与 task.create 是异步接口（立即返回 accepted/running），",
		"       终态由事件推送，本界面完全依赖事件流实时更新。",
	}
	out := make([]string, 0, len(help))
	for _, l := range help {
		out = append(out, a.colorize(cDim, l))
	}
	for len(out) < h {
		out = append(out, "")
	}
	return out
}

// ---------------------------------------------------------------------------
// row builders
// ---------------------------------------------------------------------------

func (a *app) nodeRows(nodes []sdk.Node, sel, scroll, w int) []string {
	if len(nodes) == 0 {
		return []string{a.colorize(cDim, "(no nodes — 用 hubd -demo 3 启动演示节点)")}
	}
	out := make([]string, 0, len(nodes))
	for i, n := range nodes {
		row := fmt.Sprintf("%-4s %-8s %s", n.ID, statusColor(a, n.Status), ellipsis(strings.Join(n.Capabilities, ","), maxInt(0, w-16)))
		out = append(out, highlight(a, i, scroll, sel, row))
	}
	return out
}

func (a *app) commandRows(cmds []sdk.Command, sel, scroll, w int) []string {
	if len(cmds) == 0 {
		return []string{a.colorize(cDim, "(no commands yet — 输入 send n-1 echo {\"text\":\"hi\"})")}
	}
	out := make([]string, 0, len(cmds))
	for i, c := range cmds {
		st := a.colorize(cmdStatusColor(c.Status), c.Status)
		row := fmt.Sprintf("%-5s %-4s %-8s %-9s", c.ID, c.NodeID, ellipsis(c.Method, 8), st)
		switch {
		case c.Error != nil:
			row += " " + a.colorize(cRed, fmt.Sprintf("%d %s", c.Error.Code, c.Error.Message))
		case c.Result != nil:
			row += " " + a.colorize(cDim, ellipsis(compact(c.Result), maxInt(0, w-38)))
		}
		out = append(out, highlight(a, i, scroll, sel, row))
	}
	return out
}

func (a *app) taskRows(tasks []sdk.Task, sel, scroll, w int) []string {
	if len(tasks) == 0 {
		return []string{a.colorize(cDim, "(no tasks)")}
	}
	out := make([]string, 0, len(tasks))
	for i, t := range tasks {
		bar := progressBar(t.Progress, 10)
		row := fmt.Sprintf("%-5s %-10s %s %3d%%", t.ID, ellipsis(t.Name, 10), bar, int(t.Progress*100))
		out = append(out, highlight(a, i, scroll, sel, row+" "+a.colorize(taskStatusColor(t.Status), t.Status)))
	}
	return out
}

func (a *app) eventRows(events []sdk.HubEvent, sel, scroll, w int) []string {
	if len(events) == 0 {
		return []string{a.colorize(cDim, "(waiting for events…)")}
	}
	out := make([]string, 0, len(events))
	for i, ev := range events {
		ts := ""
		if len(ev.Time) >= 19 {
			ts = ev.Time[11:19]
		}
		who := ev.NodeID
		if who == "" {
			who = ev.TaskID
		}
		if who == "" {
			who = ev.CommandID
		}
		row := fmt.Sprintf("%-8s %-20s %-6s", ts, a.colorize(evtColor(ev.Type), ev.Type), ellipsis(who, 6))
		out = append(out, highlight(a, i, scroll, sel, row))
	}
	return out
}

// visibleRows windows the full row list down to the inner body of a pane.
// scroll is the absolute index of the first visible row and sel the cursor
// offset inside that window; both are re-clamped here because lists can
// shrink (or the state-side height estimate can drift) between key presses,
// and the highlighted row scroll+sel must always stay on screen.
func visibleRows(rows []string, sel, scroll, inner int) []string {
	if inner < 1 {
		inner = 1
	}
	if len(rows) == 0 {
		return rows
	}
	abs := scroll + sel
	if abs > len(rows)-1 {
		abs = len(rows) - 1
	}
	if abs < 0 {
		abs = 0
	}
	top := abs - sel // keep the cursor at its offset inside the window
	if sel >= inner || top < 0 {
		top = abs - inner + 1 // pin the cursor to the last visible row
	}
	if top > len(rows)-inner {
		top = len(rows) - inner
	}
	if top < 0 {
		top = 0
	}
	if end := top + inner; end < len(rows) {
		return rows[top:end]
	}
	return rows[top:]
}

// highlight renders the selection cursor for the visible row index i.
func highlight(a *app, i, scroll, sel int, row string) string {
	if i == scroll+sel {
		return a.colorize(cBold+cWhite, "▸"+strings.TrimPrefix(row, ""))
	}
	return " " + row
}

func progressBar(p float64, width int) string {
	filled := int(p * float64(width))
	if filled < 0 {
		filled = 0
	}
	if filled > width {
		filled = width
	}
	return "[" + strings.Repeat("█", filled) + strings.Repeat("░", width-filled) + "]"
}

func statusColor(a *app, s string) string {
	switch s {
	case "online":
		return a.colorize(cGreen, s)
	case "busy":
		return a.colorize(cAmber, s)
	case "offline":
		return a.colorize(cRed, s)
	}
	return a.colorize(cDim, s)
}

func cmdStatusColor(s string) string {
	switch s {
	case "succeeded":
		return cGreen
	case "failed", "timeout":
		return cRed
	case "cancelled":
		return cDim
	case "running":
		return cBlue
	}
	return cAmber
}

func taskStatusColor(s string) string {
	switch s {
	case "succeeded":
		return cGreen
	case "failed":
		return cRed
	case "partial":
		return cAmber
	case "cancelled":
		return cDim
	}
	return cBlue
}

func evtColor(t string) string {
	switch {
	case strings.HasPrefix(t, "node."):
		return cBlue
	case strings.HasPrefix(t, "command."):
		return cAmber
	case strings.HasPrefix(t, "task."):
		return cGreen
	}
	return cDim
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
