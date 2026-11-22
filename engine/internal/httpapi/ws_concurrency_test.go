package httpapi

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"musubi/engine/internal/hub"
)

// TestWSLongPollDoesNotBlockOtherCalls pins the per-connection concurrency
// fix: command.pull is a long-poll, and while it is waiting, a heartbeat or
// command.complete on the same socket must still be served. Before the fix
// the WS read loop handled frames serially and a 5s pull starved them.
func TestWSLongPollDoesNotBlockOtherCalls(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := hub.NewHub(hub.Config{Logger: logger})
	if _, rerr := h.RegisterNode(hub.RegisterRequest{ID: "w-1"}); rerr != nil {
		t.Fatalf("RegisterNode: %+v", rerr)
	}
	srv := httptest.NewServer(New(h, logger).Handler())
	defer srv.Close()

	conn, br := dialWS(t, srv.URL)
	defer conn.Close()

	waitMS := 800
	sendRPC(t, conn, 1, "command.pull",
		map[string]any{"node_id": "w-1", "wait": waitMS})
	start := time.Now()
	sendRPC(t, conn, 2, "engine.ping", nil)

	// The ping must be answered while the long-poll is still parked.
	id, _ := readRPCResponse(t, br, 2*time.Second)
	if id != 2 {
		t.Fatalf("first response id = %d, want 2 (ping must not wait for the pull)", id)
	}
	if elapsed := time.Since(start); elapsed >= time.Duration(waitMS)*time.Millisecond {
		t.Fatalf("ping answered after %s, i.e. after the long-poll finished", elapsed)
	}

	// The pull then completes normally with an empty batch.
	id, result := readRPCResponse(t, br, 2*time.Second)
	if id != 1 {
		t.Fatalf("second response id = %d, want 1", id)
	}
	if cmd, _ := result["commands"].([]any); len(cmd) != 0 {
		t.Fatalf("pull returned %d commands, want 0", len(cmd))
	}
}

func dialWS(t *testing.T, base string) (net.Conn, *bufio.Reader) {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	_, err = fmt.Fprintf(conn,
		"GET /v1/ws HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", u.Host, key)
	if err != nil {
		conn.Close()
		t.Fatalf("handshake write: %v", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		conn.Close()
		t.Fatalf("handshake read: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		t.Fatalf("handshake status = %d, want 101", resp.StatusCode)
	}
	return conn, br
}

func sendRPC(t *testing.T, conn net.Conn, id int, method string, params map[string]any) {
	t.Helper()
	req := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if params != nil {
		req["params"] = params
	}
	b, _ := json.Marshal(req)
	if err := writeWSFrame(conn, 0x1, b); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func readRPCResponse(t *testing.T, br *bufio.Reader, timeout time.Duration) (int, map[string]any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		op, payload, err := readWSFrame(br)
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		if op != 0x1 {
			continue
		}
		var msg struct {
			ID     int            `json:"id"`
			Result map[string]any `json:"result"`
			Error  map[string]any `json:"error"`
		}
		if err := json.Unmarshal(payload, &msg); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if msg.Error != nil {
			t.Fatalf("rpc error: %v", msg.Error)
		}
		return msg.ID, msg.Result
	}
	t.Fatal("timed out waiting for a WS response")
	return 0, nil
}

func writeWSFrame(w io.Writer, opcode byte, payload []byte) error {
	mask := [4]byte{1, 2, 3, 4}
	var hdr []byte
	hdr = append(hdr, 0x80|opcode)
	n := len(payload)
	switch {
	case n < 126:
		hdr = append(hdr, 0x80|byte(n))
	case n < 1<<16:
		hdr = append(hdr, 0x80|126, byte(n>>8), byte(n))
	default:
		hdr = append(hdr, 0x80|127)
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(n))
		hdr = append(hdr, ext[:]...)
	}
	hdr = append(hdr, mask[:]...)
	masked := make([]byte, n)
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	_, err := w.Write(append(hdr, masked...))
	return err
}

func readWSFrame(r *bufio.Reader) (byte, []byte, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	op := hdr[0] & 0x0f
	masked := hdr[1]&0x80 != 0
	length := uint64(hdr[1] & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(r, mask[:]); err != nil {
			return 0, nil, err
		}
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return op, payload, nil
}
