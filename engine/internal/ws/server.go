// Package ws implements a minimal RFC6455 WebSocket server using only the
// standard library: opening handshake (SHA1 + base64 accept key), frame
// encoding/decoding (lengths 7/16/64-bit, masking), ping/pong, close frames
// and concurrent writes guarded by a mutex. It intentionally does not cover
// the full RFC (extensions, permessage-deflate, etc.).
package ws

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Opcodes.
const (
	OpContinuation = 0x0
	OpText         = 0x1
	OpBinary       = 0x2
	OpClose        = 0x8
	OpPing         = 0x9
	OpPong         = 0xA
)

// Errors.
var (
	ErrClosed      = errors.New("ws: connection closed")
	ErrFrameTooBig = errors.New("ws: frame too large")
	ErrUnsupported = errors.New("ws: unsupported opcode")
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// maxFrameSize caps a single frame payload (16 MiB), a safety valve only.
const maxFrameSize = 16 * 1024 * 1024

// Conn is one upgraded WebSocket connection.
type Conn struct {
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer

	writeMu   sync.Mutex
	closeOnce sync.Once
	closeCh   chan struct{}
	pongCh    chan struct{}
}

// Upgrade performs the RFC6455 opening handshake by hijacking the HTTP
// connection and responding with a 101 Switching Protocols.
func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(w, "websocket upgrade required", http.StatusBadRequest)
		return nil, errors.New("ws: not a websocket upgrade")
	}
	if !strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
		http.Error(w, "websocket upgrade required", http.StatusBadRequest)
		return nil, errors.New("ws: connection header missing upgrade")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "missing Sec-WebSocket-Key", http.StatusBadRequest)
		return nil, errors.New("ws: missing Sec-WebSocket-Key")
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return nil, errors.New("ws: hijack not supported")
	}
	netConn, brw, err := hj.Hijack()
	if err != nil {
		http.Error(w, "hijack failed", http.StatusInternalServerError)
		return nil, err
	}
	sum := sha1.Sum([]byte(key + wsGUID))
	accept := base64.StdEncoding.EncodeToString(sum[:])
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := brw.WriteString(resp); err != nil {
		netConn.Close()
		return nil, err
	}
	if err := brw.Flush(); err != nil {
		netConn.Close()
		return nil, err
	}
	return &Conn{
		conn:    netConn,
		br:      brw.Reader,
		bw:      brw.Writer,
		closeCh: make(chan struct{}),
		pongCh:  make(chan struct{}, 1),
	}, nil
}

// ReadMessage reads the next message. Data messages (text/binary) may span
// multiple frames (continuation); control frames (ping/pong/close) are
// returned as they arrive.
func (c *Conn) ReadMessage() (opcode byte, payload []byte, err error) {
	var first byte
	var buf []byte
	for {
		fin, op, p, err := c.readFrame()
		if err != nil {
			return 0, nil, err
		}
		if op&0x8 != 0 { // control frame: never fragmented, return immediately
			return op, p, nil
		}
		if first == 0 {
			first = op
		}
		buf = append(buf, p...)
		if fin {
			return first, buf, nil
		}
	}
}

func (c *Conn) readFrame() (fin bool, opcode byte, payload []byte, err error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	var hdr [2]byte
	if _, err := io.ReadFull(c.br, hdr[:]); err != nil {
		return false, 0, nil, err
	}
	fin = hdr[0]&0x80 != 0
	opcode = hdr[0] & 0x0f
	masked := hdr[1]&0x80 != 0
	length := uint64(hdr[1] & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(c.br, mask[:]); err != nil {
			return false, 0, nil, err
		}
	}
	if length > maxFrameSize {
		return false, 0, nil, ErrFrameTooBig
	}
	payload = make([]byte, length)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return false, 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return fin, opcode, payload, nil
}

// WriteMessage writes one unmasked (server-side) message frame.
func (c *Conn) WriteMessage(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.closeCh:
		return ErrClosed
	default:
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	var hdr [10]byte
	hdr[0] = 0x80 | opcode // FIN
	n := 2
	l := len(payload)
	switch {
	case l <= 125:
		hdr[1] = byte(l)
	case l <= 0xFFFF:
		hdr[1] = 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(l))
		n = 4
	default:
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(l))
		n = 10
	}
	if _, err := c.bw.Write(hdr[:n]); err != nil {
		return err
	}
	if _, err := c.bw.Write(payload); err != nil {
		return err
	}
	return c.bw.Flush()
}

// WriteClose sends a close frame (echoing code/reason) and closes the TCP
// connection.
func (c *Conn) WriteClose(code uint16, reason string) error {
	var payload []byte
	if code > 0 {
		payload = make([]byte, 2+len(reason))
		binary.BigEndian.PutUint16(payload[:2], code)
		copy(payload[2:], reason)
	}
	err := c.WriteMessage(OpClose, payload)
	c.Close()
	return err
}

// Close terminates the connection and unblocks any pending read.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeCh)
		_ = c.conn.SetReadDeadline(time.Now())
		_ = c.conn.Close()
	})
	return nil
}

// KeepAlive runs the server-side heartbeat: a ping every pingInterval; if no
// pong arrives within pongTimeout the connection is closed. The read loop must
// call NotePong for incoming pong frames.
func (c *Conn) KeepAlive(pingInterval, pongTimeout time.Duration) {
	go func() {
		t := time.NewTicker(pingInterval)
		defer t.Stop()
		for {
			select {
			case <-c.closeCh:
				return
			case <-t.C:
				if err := c.WriteMessage(OpPing, nil); err != nil {
					c.Close()
					return
				}
				select {
				case <-c.pongCh:
				case <-time.After(pongTimeout):
					c.Close()
					return
				case <-c.closeCh:
					return
				}
			}
		}
	}()
}

// NotePong records an incoming pong frame.
func (c *Conn) NotePong() {
	select {
	case c.pongCh <- struct{}{}:
	default:
	}
}

// Done is closed when the connection is closed. Event-forwarding goroutines
// select on it so they never outlive the socket that carries them.
func (c *Conn) Done() <-chan struct{} { return c.closeCh }
