// Package console serves the embedded, build-free web management console at
// /console/. The console is static: it talks to the same origin's JSON-RPC and
// WebSocket endpoints, so the engine stays a single binary with no asset
// pipeline. Authentication still happens at the API layer; the console merely
// remembers a token for its own requests.
package console

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed index.html app.js style.css
var assets embed.FS

// Handler serves the console assets with hardened headers.
func Handler() http.Handler {
	sub, err := fs.Sub(assets, ".")
	if err != nil {
		panic("console: embedded assets unavailable: " + err.Error())
	}
	files := http.StripPrefix("/console", http.FileServer(http.FS(sub)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		// The console only needs same-origin HTTP/WS (including ws:// to the
		// same host) and its own CSS/JS.
		w.Header().Set("Content-Security-Policy",
			"default-src 'self'; connect-src 'self' ws: wss:; img-src 'self' data:; style-src 'self'; script-src 'self'; base-uri 'none'; form-action 'self'")
		files.ServeHTTP(w, r)
	})
}
