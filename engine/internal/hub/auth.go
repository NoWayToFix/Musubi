package hub

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"sync"
	"time"

	"musubi/engine/internal/jsonrpc"
)

// Token lifetimes (PROTOCOL.md §3.5: access tokens TTL 1h, refreshable).
const (
	authTokenTTL    = time.Hour
	authRefreshTTL  = 24 * time.Hour
	authLoginTTLMsg = 3600 // seconds, reported in auth.login expires_in
)

// Token is an issued access token stored in memory.
type Token struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    time.Time
	Anonymous    bool
	User         string
}

// Session is the resolved authentication for one request.
type Session struct {
	Authenticated bool
	Anonymous     bool
	Token         *Token
}

type authManager struct {
	mode        string
	staticToken string
	user        string
	pass        string

	mu            sync.RWMutex
	tokens        map[string]*Token // access token -> token
	refreshTokens map[string]string // refresh token -> access token
}

func newAuthManager(cfg Config) *authManager {
	return &authManager{
		mode:          cfg.AuthMode,
		staticToken:   cfg.Token,
		user:          cfg.User,
		pass:          cfg.Pass,
		tokens:        map[string]*Token{},
		refreshTokens: map[string]string{},
	}
}

// authenticate resolves a bearer token into a Session.
//
// none:  no token -> anonymous, allowed; unknown/expired token -> anonymous.
// token/basic: no token -> -32001; unknown -> -32002; expired -> -32003.
func (a *authManager) authenticate(token string) (*Session, *jsonrpc.Error) {
	switch a.mode {
	case "none":
		if token == "" {
			return &Session{Anonymous: true}, nil
		}
		if t, ok := a.lookup(token); ok && time.Now().Before(t.ExpiresAt) {
			return &Session{Authenticated: true, Token: t}, nil
		}
		return &Session{Anonymous: true}, nil
	case "token", "basic":
		if token == "" {
			return nil, jsonrpc.NewError(jsonrpc.CodeAuthRequired, "auth.required", map[string]any{})
		}
		t, ok := a.lookup(token)
		if !ok {
			return nil, jsonrpc.NewError(jsonrpc.CodeAuthInvalid, "auth.invalid", map[string]any{})
		}
		if time.Now().After(t.ExpiresAt) {
			return nil, jsonrpc.NewError(jsonrpc.CodeAuthExpired, "auth.expired",
				map[string]any{"retry_after": 0})
		}
		return &Session{Authenticated: true, Token: t}, nil
	}
	return &Session{Anonymous: true}, nil
}

// authenticateBasic verifies HTTP Basic credentials directly (basic mode).
func (a *authManager) authenticateBasic(user, pass string) (*Session, bool) {
	if a.mode != "basic" {
		return nil, false
	}
	if constEq(user, a.user) && constEq(pass, a.pass) {
		return &Session{Authenticated: true, Token: &Token{User: user, ExpiresAt: time.Now().Add(authTokenTTL)}}, true
	}
	return nil, false
}

func (a *authManager) lookup(access string) (*Token, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	t, ok := a.tokens[access]
	return t, ok
}

// login implements auth.login per mode (PROTOCOL.md §3.5).
func (a *authManager) login(p map[string]any) (map[string]any, *jsonrpc.Error) {
	switch a.mode {
	case "token":
		secret := strParam(p["token"])
		if secret == "" || !constEq(secret, a.staticToken) {
			return nil, jsonrpc.NewError(jsonrpc.CodeAuthInvalid, "auth.invalid", map[string]any{})
		}
		return a.issue("", false)
	case "basic":
		username := strParam(p["username"])
		password := strParam(p["password"])
		if !constEq(username, a.user) || !constEq(password, a.pass) {
			return nil, jsonrpc.NewError(jsonrpc.CodeAuthInvalid, "auth.invalid", map[string]any{})
		}
		return a.issue(username, false)
	case "none":
		// Anonymous login: returns a bearer token for session tracking.
		return a.issue("", true)
	}
	return nil, jsonrpc.NewError(jsonrpc.CodeInternalError, "internal error", nil)
}

func (a *authManager) issue(user string, anonymous bool) (map[string]any, *jsonrpc.Error) {
	access, err := randomToken()
	if err != nil {
		return nil, jsonrpc.NewError(jsonrpc.CodeInternalError, "internal error", nil)
	}
	refresh, err := randomToken()
	if err != nil {
		return nil, jsonrpc.NewError(jsonrpc.CodeInternalError, "internal error", nil)
	}
	t := &Token{
		AccessToken:  access,
		RefreshToken: refresh,
		ExpiresAt:    time.Now().Add(authTokenTTL),
		Anonymous:    anonymous,
		User:         user,
	}
	a.mu.Lock()
	a.tokens[access] = t
	a.refreshTokens[refresh] = access
	a.mu.Unlock()
	return map[string]any{
		"access_token":  access,
		"token_type":    "bearer",
		"expires_in":    authLoginTTLMsg,
		"refresh_token": refresh,
	}, nil
}

// refresh implements auth.refresh: rotates the access token's expiry.
func (a *authManager) refresh(refreshToken string) (map[string]any, *jsonrpc.Error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	access, ok := a.refreshTokens[refreshToken]
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeAuthInvalid, "auth.invalid", map[string]any{})
	}
	t, ok := a.tokens[access]
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeAuthInvalid, "auth.invalid", map[string]any{})
	}
	t.ExpiresAt = time.Now().Add(authTokenTTL)
	return map[string]any{
		"access_token":  access,
		"token_type":    "bearer",
		"expires_in":    authLoginTTLMsg,
		"refresh_token": refreshToken,
	}, nil
}

// logout revokes the session's access token (and its refresh token).
func (a *authManager) logout(sess *Session) {
	if sess == nil || sess.Token == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if t, ok := a.tokens[sess.Token.AccessToken]; ok {
		delete(a.refreshTokens, t.RefreshToken)
		delete(a.tokens, sess.Token.AccessToken)
	}
}

// revoke invalidates a specific access token and its refresh token pair,
// independently of the caller's own session.
func (a *authManager) revoke(access string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if t, ok := a.tokens[access]; ok {
		delete(a.refreshTokens, t.RefreshToken)
		delete(a.tokens, access)
	}
}

func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// constEq compares strings in constant time (length short-circuits).
func constEq(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// ---------------------------------------------------------------------------
// auth.* RPC handlers

func (h *Hub) authLogin(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	return h.auth.login(p)
}

func (h *Hub) authRefresh(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	rt := strParam(p["refresh_token"])
	if rt == "" {
		return nil, jsonrpc.ErrInvalidParams("refresh_token required")
	}
	return h.auth.refresh(rt)
}

func (h *Hub) authLogout(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	// An explicit access_token param revokes that exact token (and its
	// refresh pair), so contract tests can verify invalidation without
	// depending on the caller's own session header.
	if tok := strParam(p["access_token"]); tok != "" {
		h.auth.revoke(tok)
		return map[string]any{}, nil
	}
	if cc != nil {
		h.auth.logout(cc.Session)
	}
	return map[string]any{}, nil
}
