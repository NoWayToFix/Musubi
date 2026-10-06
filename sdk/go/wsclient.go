package sdk

// Minimal RFC6455 WebSocket client built on the standard library only.
// Supports the opening handshake, text frames (client masking), ping/pong and
// close. No extensions, no compression, no fragmented control frames.

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// WS opcodes (RFC6455 §5.2).
const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// ErrWSClosed is returned when the peer sent a close frame.
var ErrWSClosed = errors.New("websocket: connection closed")

// WSConn is a minimal WebSocket client connection.
type WSConn struct {
	conn net.Conn
	r    *bufio.Reader

	wmu       sync.Mutex // serializes all writes (including control frames)
	closeOnce sync.Once
	closeErr  error
}

// DialWS performs the RFC6455 opening handshake against rawURL
// (ws:// or wss://) and returns a connected WSConn.
func DialWS(rawURL string, headers http.Header, timeout time.Duration) (*WSConn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "ws":
	case "wss":
	default:
		return nil, fmt.Errorf("unsupported websocket scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("websocket url missing host")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	path := u.RequestURI()
	if path == "" {
		path = "/"
	}

	var conn net.Conn
	dialer := &net.Dialer{Timeout: timeout}
	if u.Scheme == "wss" {
		conn, err = tls.DialWithDialer(dialer, "tcp", u.Host, &tls.Config{ServerName: hostOnly(u.Host), MinVersion: tls.VersionTLS12})
	} else {
		conn, err = dialer.Dial("tcp", u.Host)
	}
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
	}

	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		conn.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	var b strings.Builder
	b.WriteString("GET " + path + " HTTP/1.1\r\n")
	b.WriteString("Host: " + u.Host + "\r\n")
	b.WriteString("Upgrade: websocket\r\n")
	b.WriteString("Connection: Upgrade\r\n")
	b.WriteString("Sec-WebSocket-Version: 13\r\n")
	b.WriteString("Sec-WebSocket-Key: " + key + "\r\n")
	for k, vs := range headers {
		for _, v := range vs {
			b.WriteString(k + ": " + v + "\r\n")
		}
	}
	b.WriteString("\r\n")
	if _, err := conn.Write([]byte(b.String())); err != nil {
		conn.Close()
		return nil, err
	}

	br := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(timeout))
	resp, err := http.ReadResponse(br, &http.Request{Method: "GET"})
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		conn.Close()
		return nil, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		return nil, fmt.Errorf("websocket handshake failed: %s", resp.Status)
	}
	accept := resp.Header.Get("Sec-WebSocket-Accept")
	sum := sha1.Sum([]byte(key + wsGUID))
	expect := base64.StdEncoding.EncodeToString(sum[:])
	if accept != expect {
		conn.Close()
		return nil, errors.New("websocket handshake failed: bad Sec-WebSocket-Accept")
	}
	return &WSConn{conn: conn, r: br}, nil
}

func hostOnly(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return h
	}
	return hostport
}

// WriteText sends a single masked text frame.
func (c *WSConn) WriteText(payload []byte) error {
	return c.writeFrame(opText, payload)
}

// Ping sends a ping control frame.
func (c *WSConn) Ping(payload []byte) error {
	if len(payload) > 125 {
		payload = payload[:125]
	}
	return c.writeFrame(opPing, payload)
}

// Close sends a close frame and closes the underlying connection.
func (c *WSConn) Close() error {
	c.closeOnce.Do(func() {
		_ = c.writeFrame(opClose, nil)
		c.closeErr = c.conn.Close()
	})
	return c.closeErr
}

// SetReadDeadline is a passthrough used by the client heartbeat.
func (c *WSConn) SetReadDeadline(t time.Time) error { return c.conn.SetReadDeadline(t) }

func (c *WSConn) writeFrame(opcode byte, payload []byte) error {
	maskKey := make([]byte, 4)
	if _, err := rand.Read(maskKey); err != nil {
		return err
	}
	masked := make([]byte, len(payload))
	for i, b := range payload {
		masked[i] = b ^ maskKey[i%4]
	}

	header := []byte{0x80 | opcode} // FIN=1
	n := len(masked)
	switch {
	case n < 126:
		header = append(header, 0x80|byte(n))
	case n <= 0xFFFF:
		header = append(header, 0x80|126, byte(n>>8), byte(n))
	default:
		header = append(header, 0x80|127)
		var l [8]byte
		binary.BigEndian.PutUint64(l[:], uint64(n))
		header = append(header, l[:]...)
	}
	header = append(header, maskKey...)

	c.wmu.Lock()
	defer c.wmu.Unlock()
	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err := c.conn.Write(masked)
	return err
}

// readFrame reads one frame, automatically answering pings and close frames.
// Control frames with an empty payload produce a zero-length payload.
func (c *WSConn) readFrame() (opcode byte, payload []byte, err error) {
	for {
		b0, err := c.r.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		b1, err := c.r.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		opcode = b0 & 0x0F
		masked := b1&0x80 != 0
		length := uint64(b1 & 0x7F)
		switch length {
		case 126:
			var b [2]byte
			if _, err := io.ReadFull(c.r, b[:]); err != nil {
				return 0, nil, err
			}
			length = uint64(binary.BigEndian.Uint16(b[:]))
		case 127:
			var b [8]byte
			if _, err := io.ReadFull(c.r, b[:]); err != nil {
				return 0, nil, err
			}
			length = binary.BigEndian.Uint64(b[:])
		}
		if length > 16<<20 { // 16 MiB sanity cap
			return 0, nil, errors.New("websocket: frame too large")
		}
		var maskKey [4]byte
		if masked {
			if _, err := io.ReadFull(c.r, maskKey[:]); err != nil {
				return 0, nil, err
			}
		}
		payload = make([]byte, int(length))
		if _, err := io.ReadFull(c.r, payload); err != nil {
			return 0, nil, err
		}
		if masked {
			for i := range payload {
				payload[i] ^= maskKey[i%4]
			}
		}

		switch opcode {
		case opPing:
			_ = c.writeFrame(opPong, payload) // answer pings (server heartbeat)
			continue
		case opPong:
			continue
		case opClose:
			_ = c.writeFrame(opClose, nil)
			_ = c.conn.Close()
			return opClose, payload, ErrWSClosed
		default:
			return opcode, payload, nil // data frames (text/binary/continuation)
		}
	}
}

// ReadMessage reads a full text message, joining continuation frames.
func (c *WSConn) ReadMessage() ([]byte, error) {
	opcode, payload, err := c.readFrame()
	if err != nil {
		return nil, err
	}
	buf := append([]byte(nil), payload...)
	// Join any continuation frames until the FIN message completes.
	for opcode == opContinuation {
		opcode, payload, err = c.readFrame()
		if err != nil {
			return nil, err
		}
		buf = append(buf, payload...)
	}
	return buf, nil
}
