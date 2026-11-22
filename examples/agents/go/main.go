// Command go-agent joins the Musubi control center as a real node using the
// Go SDK Agent runtime. It is the smallest "one platform, one SDK" example:
//
//	go run . -url http://127.0.0.1:8080 -id go-1
//
// The engine sees a node with platform.runtime="go". Send it a command:
//
//	curl -s -X POST http://127.0.0.1:8080/v1/rpc -H 'Content-Type: application/json' \
//	  -d '{"jsonrpc":"2.0","id":1,"method":"command.send",
//	       "params":{"node_id":"go-1","method":"sys.info"}}'
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	sdk "musubi/hub/sdk"
)

func main() {
	var (
		url         = flag.String("url", "http://127.0.0.1:8080", "control center base URL")
		token       = flag.String("token", "", "bearer token (token/basic mode)")
		id          = flag.String("id", "go-agent-1", "node id")
		name        = flag.String("name", "", "display name (default: id)")
		caps        = flag.String("capabilities", "echo,sys.info,sleep,fail", "comma-separated capabilities")
		tags        = flag.String("tags", "agent,go", "comma-separated tags")
		labels      = flag.String("labels", "", "platform labels k=v,k=v")
		pullWait    = flag.Int64("pull-wait", 5000, "command.pull long-poll milliseconds")
		concurrency = flag.Int("concurrency", 2, "parallel pull loops")
		heartbeat   = flag.Duration("heartbeat", 10*time.Second, "heartbeat interval")
	)
	flag.Parse()

	nodeName := *name
	if nodeName == "" {
		nodeName = *id
	}
	agent := sdk.NewAgent(sdk.AgentOptions{
		URL:          *url,
		Token:        *token,
		NodeID:       *id,
		Name:         nodeName,
		Capabilities: splitCSV(*caps),
		Tags:         splitCSV(*tags),
		Labels:       parseLabels(*labels),
		Heartbeat:    *heartbeat,
		PullWait:     *pullWait,
		Concurrency:  *concurrency,
		OnError:      func(err error) { log.Printf("agent: %v", err) },
	})

	agent.Handle("sys.info", func(_ context.Context, _ map[string]any) (any, error) {
		host, _ := os.Hostname()
		return map[string]any{
			"node_id":    agent.NodeID(),
			"host":       host,
			"pid":        os.Getpid(),
			"os":         runtime.GOOS,
			"arch":       runtime.GOARCH,
			"go_version": runtime.Version(),
			"num_cpu":    runtime.NumCPU(),
			"goroutines": runtime.NumGoroutine(),
		}, nil
	})
	agent.Handle("sleep", func(ctx context.Context, params map[string]any) (any, error) {
		ms := int64(0)
		if v, ok := params["ms"].(float64); ok {
			ms = int64(v)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(ms) * time.Millisecond):
		}
		return map[string]any{"slept_ms": ms}, nil
	})
	agent.Handle("fail", func(_ context.Context, params map[string]any) (any, error) {
		msg, _ := params["message"].(string)
		if msg == "" {
			msg = "requested failure"
		}
		return nil, &sdk.HandlerError{Code: sdk.ErrInternalError, Message: msg}
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("go agent starting: id=%s url=%s capabilities=%s", *id, *url, *caps)
	if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("agent: %v", err)
	}
	fmt.Println("agent stopped")
}

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseLabels(s string) map[string]string {
	out := map[string]string{}
	for _, part := range splitCSV(s) {
		if k, v, ok := strings.Cut(part, "="); ok {
			out[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return out
}
