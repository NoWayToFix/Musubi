package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// key is a decoded key press. Runes are printable characters; special keys use
// the Key* constants.
type key struct {
	rune rune
	name string
}

const (
	keyUp     = "up"
	keyDown   = "down"
	keyLeft   = "left"
	keyRight  = "right"
	keyHome   = "home"
	keyEnd    = "end"
	keyPgUp   = "pgup"
	keyPgDn   = "pgdn"
	keyEnter  = "enter"
	keyBacksp = "backspace"
	keyTab    = "tab"
	keyEsc    = "esc"
	keyCtrlC  = "ctrl-c"
	keyCtrlU  = "ctrl-u"
	keyDelete = "delete"
)

// readKey decodes one key press from raw terminal bytes (see term_linux.go).
func readKey(r *bufio.Reader) (key, error) {
	b, err := r.ReadByte()
	if err != nil {
		return key{}, err
	}
	switch b {
	case 0x03:
		return key{name: keyCtrlC}, nil
	case 0x09:
		return key{name: keyTab}, nil
	case 0x0d, 0x0a:
		return key{name: keyEnter}, nil
	case 0x7f, 0x08:
		return key{name: keyBacksp}, nil
	case 0x15:
		return key{name: keyCtrlU}, nil
	case 0x1b:
		// Escape sequence (CSI) or a bare ESC.
		if r.Buffered() == 0 {
			return key{name: keyEsc}, nil
		}
		next, err := r.ReadByte()
		if err != nil {
			return key{name: keyEsc}, nil
		}
		if next != '[' && next != 'O' {
			return key{name: keyEsc}, nil
		}
		var seq strings.Builder
		for r.Buffered() > 0 {
			c, err := r.ReadByte()
			if err != nil {
				break
			}
			seq.WriteByte(c)
			if (c >= 'A' && c <= 'Z') || c == '~' {
				break
			}
		}
		switch s := seq.String(); s {
		case "A":
			return key{name: keyUp}, nil
		case "B":
			return key{name: keyDown}, nil
		case "C":
			return key{name: keyRight}, nil
		case "D":
			return key{name: keyLeft}, nil
		case "H":
			return key{name: keyHome}, nil
		case "F":
			return key{name: keyEnd}, nil
		case "1~", "7~":
			return key{name: keyHome}, nil
		case "4~", "8~":
			return key{name: keyEnd}, nil
		case "5~":
			return key{name: keyPgUp}, nil
		case "6~":
			return key{name: keyPgDn}, nil
		case "3~":
			return key{name: keyDelete}, nil
		}
		return key{name: keyEsc}, nil
	}
	if b < 0x20 {
		return key{rune: rune(b)}, nil
	}
	// UTF-8 continuation bytes: pull the rest of the rune.
	if b >= 0x80 {
		extra := 0
		switch {
		case b&0xE0 == 0xC0:
			extra = 1
		case b&0xF0 == 0xE0:
			extra = 2
		default:
			extra = 3
		}
		buf := []byte{b}
		for i := 0; i < extra; i++ {
			c, err := r.ReadByte()
			if err != nil {
				break
			}
			buf = append(buf, c)
		}
		rs := []rune(string(buf))
		if len(rs) == 0 {
			return key{}, nil
		}
		return key{rune: rs[0]}, nil
	}
	return key{rune: rune(b)}, nil
}

// runInteractive is the main event loop: keys go in, frames come out.
func (a *app) runInteractive(ctx context.Context) int {
	state, err := enterRaw()
	if err != nil {
		fmt.Fprintln(os.Stderr, "hubtui: 无法进入 raw 模式:", err)
		return 2
	}
	defer restore(state)

	// Hide cursor + alternate screen so the console owns the terminal.
	fmt.Print("\x1b[?1049h\x1b[?25l")
	defer fmt.Print("\x1b[?25h\x1b[?1049l")

	keys := make(chan key, 64)
	go readKeys(ctx, os.Stdin, keys)

	ticker := time.NewTicker(250 * time.Millisecond) // ~4 fps redraw
	defer ticker.Stop()
	draw := func() { paint(a.frame()) }

	draw()
	for {
		select {
		case <-ctx.Done():
			return 0
		case <-ticker.C:
			draw()
		case k, ok := <-keys:
			if !ok {
				return 0
			}
			if a.handleKey(ctx, k) {
				return 0
			}
			draw()
		}
	}
}

// paint repaints in place: home + per-line clear avoids flicker.
func paint(frame string) {
	lines := strings.Split(frame, "\n")
	var b strings.Builder
	b.WriteString("\x1b[H")
	for i, l := range lines {
		b.WriteString(l)
		b.WriteString("\x1b[K")
		if i < len(lines)-1 {
			b.WriteString("\r\n")
		}
	}
	fmt.Print(b.String())
}

// readKeys decodes keys until the reader is closed or ctx is done.
func readKeys(ctx context.Context, in io.Reader, out chan<- key) {
	r := bufio.NewReaderSize(in, 256)
	for {
		k, err := readKey(r)
		if err != nil {
			close(out)
			return
		}
		select {
		case out <- k:
		case <-ctx.Done():
			return
		}
	}
}

// handleKey applies one key press; it returns true when the console must exit.
func (a *app) handleKey(ctx context.Context, k key) bool {
	a.mu.Lock()
	pane := a.pane
	a.mu.Unlock()

	// Global keys.
	switch k.name {
	case keyCtrlC:
		return true
	case keyCtrlU:
		a.mu.Lock()
		a.input = ""
		a.mu.Unlock()
		return false
	case keyTab:
		a.setPane((pane + 1) % paneCount)
		return false
	case keyEsc:
		a.mu.Lock()
		if a.input != "" {
			a.input = "" // clear the line; otherwise nothing
		} else {
			a.help = false
		}
		a.mu.Unlock()
		return false
	case keyEnter:
		a.submit(ctx)
		return false
	case keyUp, keyDown, keyPgUp, keyPgDn, keyLeft, keyRight, keyHome, keyEnd, keyDelete:
		a.moveSel(k)
		return false
	}

	switch k.rune {
	case 'q':
		return true
	case '?':
		a.mu.Lock()
		a.help = !a.help
		a.mu.Unlock()
		return false
	case 'r':
		go func() {
			if err := a.refresh(ctx); err != nil {
				a.setStatus("刷新失败: " + err.Error())
				return
			}
			a.setStatus("已刷新 " + time.Now().Format("15:04:05"))
		}()
		return false
	case '1', '2', '3', '4':
		a.setPane(int(k.rune - '1'))
		return false
	case 'j':
		a.moveSel(key{name: keyDown})
		return false
	case 'k':
		a.moveSel(key{name: keyUp})
		return false
	case 'g':
		a.setSel(0)
		return false
	case 'G':
		a.setSel(1 << 20)
		return false
	case 's':
		a.setInput(fmt.Sprintf("send %s echo {\"text\":\"hello\"}", orUnknown(a.selectedNode())))
		return false
	case 'f':
		a.setInput(fmt.Sprintf("send %s fail", orUnknown(a.selectedNode())))
		return false
	case 't':
		node := orUnknown(a.selectedNode())
		a.setInput(fmt.Sprintf("task demo %s:echo,%s:echo", node, node))
		return false
	case 'c':
		id := a.selectedCommand()
		if id == "" {
			id = a.selectedTask()
			if id != "" {
				a.setInput("taskcancel " + id)
				return false
			}
			a.setStatus("没有可取消的指令或任务")
			return false
		}
		a.setInput("cancel " + id)
		return false
	}

	// Printable character: append to the input line (and dismiss the help).
	if k.rune >= 0x20 && k.name == "" {
		a.mu.Lock()
		a.help = false
		a.input += string(k.rune)
		a.mu.Unlock()
	}
	return false
}

// submit runs the current input line.
func (a *app) submit(ctx context.Context) {
	a.mu.Lock()
	line := strings.TrimSpace(a.input)
	a.input = ""
	a.mu.Unlock()
	if line == "" {
		// Enter on a list pane prefills a command for the highlighted row.
		switch a.paneSnapshot() {
		case paneNodes:
			a.setInput(fmt.Sprintf("send %s echo {\"text\":\"hello\"}", orUnknown(a.selectedNode())))
		case paneCmds:
			if id := a.selectedCommand(); id != "" {
				a.setInput("cancel " + id)
			}
		case paneTasks:
			node := orUnknown(a.selectedNode())
			a.setInput(fmt.Sprintf("task demo %s:echo,%s:echo", node, node))
		}
		return
	}
	out, err := a.execute(ctx, line)
	switch {
	case err != nil:
		a.setStatus("✗ " + err.Error())
	case out != "":
		a.setStatus(firstLine(out))
	default:
		a.setStatus("✓ " + line)
	}
}

func (a *app) paneSnapshot() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.pane
}

func (a *app) setPane(p int) {
	a.mu.Lock()
	if p < 0 || p >= paneCount {
		p = 0
	}
	a.pane = p
	a.sel[p] = 0
	a.scroll[p] = 0
	a.mu.Unlock()
}

func (a *app) setSel(i int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	p := a.pane
	max := a.paneLenLocked(p) - 1
	if max < 0 {
		a.sel[p], a.scroll[p] = 0, 0
		return
	}
	if i > max {
		i = max
	}
	if i < 0 {
		i = 0
	}
	a.sel[p] = i
	a.visibleLocked(p)
}

// moveSel handles arrows, page keys and Home/End.
func (a *app) moveSel(k key) {
	a.mu.Lock()
	p := a.pane
	page := 5
	switch k.name {
	case keyUp:
		a.sel[p]--
	case keyDown:
		a.sel[p]++
	case keyPgUp:
		a.sel[p] -= page
	case keyPgDn:
		a.sel[p] += page
	case keyHome, keyLeft:
		a.sel[p] = 0
	case keyEnd, keyRight:
		a.sel[p] = 1 << 20
	case keyDelete:
		// Remove the highlighted event from the local log.
		if p == paneEvents {
			idx := a.scroll[p] + a.sel[p]
			if idx >= 0 && idx < len(a.events) {
				a.events = append(a.events[:idx], a.events[idx+1:]...)
				if a.sel[p] >= len(a.events) && a.sel[p] > 0 {
					a.sel[p]--
				}
			}
		}
	}
	max := a.paneLenLocked(p) - 1
	if max < 0 {
		a.sel[p], a.scroll[p] = 0, 0
		return
	}
	if a.sel[p] > max {
		a.sel[p] = max
	}
	if a.sel[p] < 0 {
		a.sel[p] = 0
	}
	a.visibleLocked(p)
	a.mu.Unlock()
}

// visibleLocked keeps the selection inside the visible window of the pane.
// Caller must hold a.mu.
func (a *app) visibleLocked(p int) {
	_, h := termSize()
	h -= 4
	if a.pane == paneNodes && h > 6 {
		h /= 2 // nodes pane is half height in the 3-column layout
	}
	if h < 3 {
		h = 3
	}
	if a.scroll[p] > a.sel[p] {
		a.scroll[p] = a.sel[p]
	}
	if a.scroll[p]+h <= a.sel[p] {
		a.scroll[p] = a.sel[p] - h + 1
	}
	if a.scroll[p] < 0 {
		a.scroll[p] = 0
	}
}

// paneLenLocked returns the item count of a pane; caller must hold a.mu.
func (a *app) paneLenLocked(p int) int {
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

func (a *app) setInput(s string) {
	a.mu.Lock()
	a.input = s
	a.help = false
	a.mu.Unlock()
}

func orUnknown(s string) string {
	if s == "" {
		return "n-1"
	}
	return s
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
