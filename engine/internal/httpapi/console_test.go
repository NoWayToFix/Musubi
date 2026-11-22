package httpapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConsoleServed(t *testing.T) {
	srv := httptest.NewServer(testAPI().Handler())
	defer srv.Close()

	res, err := http.Get(srv.URL + "/console/")
	if err != nil {
		t.Fatalf("GET /console/ failed: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if got := res.Header.Get("Content-Security-Policy"); got == "" {
		t.Fatal("missing Content-Security-Policy header")
	}
	body, _ := io.ReadAll(res.Body)
	html := string(body)
	if !strings.Contains(html, "Musubi Control Center") {
		t.Fatalf("console HTML does not contain its title: %q", html[:min(len(html), 200)])
	}
	for _, want := range []string{`data-tab="platforms"`, `id="platform-cards"`, `id="nodes-filter-runtime"`} {
		if !strings.Contains(html, want) {
			t.Fatalf("console HTML missing %s", want)
		}
	}
}

func TestConsoleRedirects(t *testing.T) {
	srv := httptest.NewServer(testAPI().Handler())
	defer srv.Close()

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, path := range []string{"/console", "/"} {
		res, err := client.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s failed: %v", path, err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusMovedPermanently && res.StatusCode != http.StatusFound {
			t.Fatalf("%s status = %d, want a redirect", path, res.StatusCode)
		}
		if loc := res.Header.Get("Location"); loc != "/console/" {
			t.Fatalf("%s Location = %q, want /console/", path, loc)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
