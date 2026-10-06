//go:build linux

package main

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// termState holds the terminal attributes captured before entering raw mode.
type termState struct {
	fd    uintptr
	saved syscall.Termios
}

// enterRaw switches the tty into raw mode (no echo, no line buffering,
// unbuffered reads) so single key presses arrive immediately.
func enterRaw() (*termState, error) {
	fd := os.Stdin.Fd()
	var t syscall.Termios
	if err := ioctlTermios(fd, syscall.TCGETS, &t); err != nil {
		return nil, err
	}
	st := &termState{fd: fd, saved: t}

	raw := t
	raw.Iflag &^= syscall.IGNBRK | syscall.BRKINT | syscall.PARMRK | syscall.ISTRIP |
		syscall.INLCR | syscall.IGNCR | syscall.ICRNL | syscall.IXON
	raw.Oflag &^= syscall.OPOST
	raw.Lflag &^= syscall.ECHO | syscall.ECHONL | syscall.ICANON | syscall.ISIG | syscall.IEXTEN
	raw.Cflag &^= syscall.CSIZE | syscall.PARENB
	raw.Cflag |= syscall.CS8
	raw.Cc[syscall.VMIN] = 1
	raw.Cc[syscall.VTIME] = 0

	if err := ioctlTermios(fd, syscall.TCSETS, &raw); err != nil {
		return nil, err
	}
	return st, nil
}

// restore puts the saved terminal attributes back.
func restore(st *termState) {
	if st == nil {
		return
	}
	saved := st.saved
	_ = ioctlTermios(st.fd, syscall.TCSETS, &saved)
}

func ioctlTermios(fd uintptr, req uintptr, t *syscall.Termios) error {
	_, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, fd, req,
		uintptr(unsafe.Pointer(t)), 0, 0, 0)
	if errno != 0 {
		return fmt.Errorf("ioctl 0x%x: %w", req, errno)
	}
	return nil
}

// termIsTTY reports whether stdin is an interactive terminal.
func termIsTTY() bool {
	var t syscall.Termios
	return ioctlTermios(os.Stdin.Fd(), syscall.TCGETS, &t) == nil
}

// termSize returns the terminal size in cells, defaulting to 80x24.
func termSize() (int, int) {
	var ws struct {
		Row, Col, Xpixel, Ypixel uint16
	}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, os.Stdout.Fd(),
		uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&ws)))
	if errno != 0 || ws.Col == 0 {
		return 80, 24
	}
	w, h := int(ws.Col), int(ws.Row)
	if w < 40 {
		w = 40
	}
	if h < 10 {
		h = 10
	}
	return w, h
}
