//go:build !linux

package main

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// Non-Linux platforms (macOS/BSD use stty; Windows has no stty and simply keeps
// line mode). The demo stays dependency-free by driving the shell tool instead
// of linking platform terminal libraries.

// termState holds the stty settings captured before entering raw mode.
type termState struct {
	saved string
}

func stty(args ...string) (string, error) {
	cmd := exec.Command("stty", args...)
	cmd.Stdin = os.Stdin
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// enterRaw switches the tty into raw mode.
func enterRaw() (*termState, error) {
	saved, err := stty("-g")
	if err != nil {
		return nil, err
	}
	if _, err := stty("raw", "-echo"); err != nil {
		return nil, err
	}
	return &termState{saved: saved}, nil
}

// restore puts the saved settings back.
func restore(st *termState) {
	if st == nil || st.saved == "" {
		return
	}
	_, _ = stty(st.saved)
}

// termIsTTY reports whether stdin is an interactive terminal.
func termIsTTY() bool {
	cmd := exec.Command("stty", "-a")
	cmd.Stdin = os.Stdin
	return cmd.Run() == nil
}

// termSize returns the terminal size in cells, defaulting to 80x24.
func termSize() (int, int) {
	out, err := stty("size") // "rows cols"
	if err != nil {
		return 80, 24
	}
	f := strings.Fields(out)
	if len(f) < 2 {
		return 80, 24
	}
	h, err1 := strconv.Atoi(f[0])
	w, err2 := strconv.Atoi(f[1])
	if err1 != nil || err2 != nil || w <= 0 || h <= 0 {
		return 80, 24
	}
	if w < 40 {
		w = 40
	}
	if h < 10 {
		h = 10
	}
	return w, h
}
