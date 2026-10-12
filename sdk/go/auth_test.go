package sdk

// Tests for auth token rotation: auth.refresh returns a NEW access_token AND
// a NEW refresh_token (invalidating the old pair); the SDK must store both,
// and a call failing with an auth error must auto-refresh and retry once.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type authTestServer struct {
	mu sync.Mutex

	// validRefresh is the currently valid refresh token; older ones were
	// rotated away and are rejected (-32002).
	validRefresh string
	// authByToken maps the bearer token to validity.
	methodsSeen []string
	refreshWith []string // refresh_token values received
	authWith    []string // Authorization headers on command.send
}

func newAuthTestServer() (*authTestServer, http.Handler) {
	a := &authTestServer{validRefresh: "r0"}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/rpc", func(w http.ResponseWriter, r *http.Request) {
		rec := decodeRPC(r)
		a.mu.Lock()
		a.methodsSeen = append(a.methodsSeen, rec.Method)
		a.mu.Unlock()

		var params struct {
			RefreshToken string `json:"refresh_token"`
		}
		_ = json.Unmarshal(rec.Params, &params)

		switch rec.Method {
		case "auth.login":
			a.mu.Lock()
			a.validRefresh = "r1"
			a.mu.Unlock()
			writeRPCResult(w, rec.ID, map[string]any{
				"access_token": "a1", "token_type": "bearer",
				"refresh_token": "r1", "expires_in": 60,
			})
		case "auth.refresh":
			a.mu.Lock()
			valid := params.RefreshToken == a.validRefresh
			if valid {
				// Rotate both tokens; the old pair is dead now.
				next := map[string]string{"r1": "r2", "r2": "r3"}[params.RefreshToken]
				a.validRefresh = next
				a.refreshWith = append(a.refreshWith, params.RefreshToken)
				a.mu.Unlock()
				writeRPCResult(w, rec.ID, map[string]any{
					"access_token":  map[string]string{"r1": "a2", "r2": "a3"}[params.RefreshToken],
					"token_type":    "bearer",
					"refresh_token": next,
					"expires_in":    60,
				})
				return
			}
			a.mu.Unlock()
			writeRPCTestErr(w, rec.ID, ErrAuthInvalid, "auth.invalid")
		case "command.send":
			auth := r.Header.Get("Authorization")
			a.mu.Lock()
			a.authWith = append(a.authWith, auth)
			a.mu.Unlock()
			if auth == "Bearer a1" {
				// The access token was rotated away / expired.
				writeRPCTestErr(w, rec.ID, ErrAuthExpired, "auth.expired")
				return
			}
			writeRPCResult(w, rec.ID, map[string]any{"command": map[string]any{"id": "c1", "status": "pending"}})
		default:
			writeRPCResult(w, rec.ID, map[string]any{})
		}
	})
	return a, mux
}

// A call failing with -32003 must refresh (with the CURRENT refresh token,
// receiving the rotated pair) and retry exactly once with the new token.
func TestAuthRetryRefreshesRotatedPair(t *testing.T) {
	a, mux := newAuthTestServer()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(Options{URL: srv.URL, Transport: "http"})
	defer c.Close()

	if _, err := c.LoginBasic(context.Background(), "u", "p"); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	rt := c.refreshToken
	tok := c.token
	c.mu.Unlock()
	if rt != "r1" || tok != "a1" {
		t.Fatalf("login stored (%s, %s), want (a1, r1)", tok, rt)
	}

	res, err := c.Call(context.Background(), "command.send", map[string]any{"node_id": "n1", "method": "x"})
	if err != nil {
		t.Fatalf("call should succeed after auto-refresh, got %v", err)
	}
	if res["command"] == nil {
		t.Fatalf("unexpected result %v", res)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.refreshWith) != 1 || a.refreshWith[0] != "r1" {
		t.Fatalf("refresh calls = %v, want exactly one with r1", a.refreshWith)
	}
	if len(a.authWith) != 2 || a.authWith[0] != "Bearer a1" || a.authWith[1] != "Bearer a2" {
		t.Fatalf("command.send auth headers = %v, want [a1, a2]", a.authWith)
	}

	c.mu.Lock()
	rt2, tok2 := c.refreshToken, c.token
	c.mu.Unlock()
	if rt2 != "r2" || tok2 != "a2" {
		t.Fatalf("stored pair after refresh = (%s, %s), want (a2, r2)", tok2, rt2)
	}
}

// Public Refresh with an empty argument uses the stored refresh token, stores
// the rotated pair, and rejects the now-invalidated old token.
func TestPublicRefreshRotatesBothTokens(t *testing.T) {
	a, mux := newAuthTestServer()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(Options{URL: srv.URL, Transport: "http"})
	defer c.Close()

	if _, err := c.LoginBasic(context.Background(), "u", "p"); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.token, c.refreshToken = "a1", "r1"
	c.mu.Unlock()

	ar, err := c.Refresh(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if ar.AccessToken != "a2" || ar.RefreshToken != "r2" {
		t.Fatalf("refresh result = (%s, %s), want (a2, r2)", ar.AccessToken, ar.RefreshToken)
	}
	c.mu.Lock()
	tok, rt := c.token, c.refreshToken
	c.mu.Unlock()
	if tok != "a2" || rt != "r2" {
		t.Fatalf("stored pair = (%s, %s), want (a2, r2)", tok, rt)
	}

	// The old refresh token is invalidated by rotation: refreshing with it
	// fails and clears the stored pair.
	c.mu.Lock()
	c.refreshToken = "r1"
	c.mu.Unlock()
	if _, err := c.Refresh(context.Background(), "r1"); err == nil {
		t.Fatal("refresh with a rotated-away token must fail")
	}
	c.mu.Lock()
	cleared := c.refreshToken == ""
	c.mu.Unlock()
	if !cleared {
		t.Fatal("rejected refresh token must be dropped from the client")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	// Only the first refresh (with the then-valid r1) reaches the refresh
	// endpoint; the invalidated r1 attempt is rejected before recording.
	if len(a.refreshWith) != 1 || a.refreshWith[0] != "r1" {
		t.Fatalf("server-side refresh calls = %v, want one valid refresh with r1", a.refreshWith)
	}
}

// Refresh without any refresh token must not hit the network.
func TestRefreshWithoutToken(t *testing.T) {
	c := New(Options{URL: "http://127.0.0.1:1", Transport: "http"})
	defer c.Close()
	if _, err := c.Refresh(context.Background(), ""); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("refresh without a token must fail fast, got %v", err)
	}
}
