package httpapi

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"

	"musubi/engine/internal/hub"
)

func testAPI(allowedOrigins ...string) *API {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(hub.NewHub(hub.Config{AllowedOrigins: allowedOrigins, Logger: logger}), logger)
}

func req(origin string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "http://hub.example.com/v1/rpc", nil)
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	return httptest.NewRecorder()
}

func TestOriginAllowed(t *testing.T) {
	cases := []struct {
		name    string
		allowed []string
		origin  string
		want    bool
	}{
		{"absent origin (non-browser client)", nil, "", true},
		{"same origin", nil, "http://hub.example.com", true},
		{"same origin, different case", nil, "HTTP://Hub.Example.com", true},
		{"foreign origin", nil, "http://evil.example", false},
		{"null origin (file:// page)", nil, "null", false},
		{"explicitly allow-listed", []string{"http://app.local"}, "http://app.local", true},
		{"allow-list does not leak", []string{"http://app.local"}, "http://evil.example", false},
		{"wildcard allow-list", []string{"*"}, "http://evil.example", true},
		{"null origin when explicitly allowed", []string{"null"}, "null", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := testAPI(tc.allowed...)
			r := httptest.NewRequest("POST", "http://hub.example.com/v1/rpc", nil)
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if got := a.originAllowed(r); got != tc.want {
				t.Fatalf("originAllowed(%q) = %v, want %v", tc.origin, got, tc.want)
			}
		})
	}
}

// An unparseable Origin must be rejected, not treated as same-origin.
func TestOriginMalformedRejected(t *testing.T) {
	a := testAPI()
	r := httptest.NewRequest("POST", "http://hub.example.com/v1/rpc", nil)
	r.Header.Set("Origin", "://not a url")
	if a.originAllowed(r) {
		t.Fatal("malformed Origin was allowed, want rejected")
	}
}
