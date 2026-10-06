// Command hubtui is a terminal console for the Control Hub.
//
// It talks to any hub engine through the Go SDK (docs/PROTOCOL.md) and renders
// four live panels — nodes, commands, tasks and the event stream — updated by
// the hub's own event subscription. Everything is standard library only, so the
// demo cross-compiles to a single static binary (CGO_ENABLED=0).
//
// Interactive (needs a TTY):
//
//	go run . --url http://127.0.0.1:18080
//
// Headless / scripted (piped stdin or CI):
//
//	go run . --url http://127.0.0.1:18080 --script 'nodes;send n-1 echo {"text":"hi"};wait 500;dump'
//	cat cmds.txt | go run . --url http://127.0.0.1:18080
//
// Press ? inside the TUI for the key bindings.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	sdk "musubi/hub/sdk"
)

func main() {
	var (
		url       = flag.String("url", envOr("HUB_URL", "http://127.0.0.1:18080"), "hub base URL")
		token     = flag.String("token", os.Getenv("HUB_TOKEN"), "static token (engine -auth token)")
		user      = flag.String("user", os.Getenv("HUB_USER"), "basic auth user (engine -auth basic)")
		pass      = flag.String("pass", os.Getenv("HUB_PASS"), "basic auth password")
		transport = flag.String("transport", "auto", "transport: auto | ws | http | poll")
		script    = flag.String("script", "", "run ';'-separated commands and exit (no TTY needed)")
		render    = flag.Bool("render-once", false, "render one frame to stdout and exit")
		noColor   = flag.Bool("no-color", false, "disable ANSI colours")
		refresh   = flag.Duration("refresh", 2*time.Second, "polling fallback interval for node/task lists")
	)
	flag.Parse()

	a := newApp(appConfig{
		url:       *url,
		token:     *token,
		user:      *user,
		pass:      *pass,
		transport: *transport,
		refresh:   *refresh,
		color:     !*noColor && os.Getenv("NO_COLOR") == "" && termIsTTY(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	if err := a.start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "hubtui:", err)
		os.Exit(1)
	}
	defer a.stop()

	// Headless paths first: they need no terminal at all.
	switch {
	case *script != "":
		os.Exit(a.runScript(ctx, strings.Split(*script, ";")))
	case *render:
		a.snapshot(ctx)
		fmt.Print(a.frame())
		return
	case !termIsTTY():
		os.Exit(a.runLineMode(ctx, os.Stdin))
	}
	os.Exit(a.runInteractive(ctx))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// options builds the SDK client options shared by every path.
func (cfg appConfig) options() sdk.Options {
	return sdk.Options{URL: cfg.url, Transport: cfg.transport, Timeout: 10 * time.Second}
}
