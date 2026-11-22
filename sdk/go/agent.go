package sdk

// Agent is the reference worker runtime for a real node (PROTOCOL.md §5.3).
//
// It is the "one platform, one SDK" integration path: an application (a
// desktop app, a server daemon, an embedded device, a CI runner — any Go
// program) creates an Agent, registers command handlers, and calls Run. The
// Agent then:
//
//	1. registers itself as a node (node.register) with a platform descriptor,
//	2. heartbeats (node.heartbeat) to stay online,
//	3. long-polls for work (command.pull),
//	4. executes the registered handler for each command,
//	5. reports the terminal result (command.complete),
//	6. unregisters on shutdown (node.unregister).
//
// Everything travels over the regular JSON-RPC transports, so the core never
// needs to call back into the platform.

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// Version is the Go SDK/agent version reported in platform.agent_version.
const Version = "1.1.0"

// AgentHandler executes one command method. Returning an error marks the
// command failed; use *HandlerError to control the reported error code.
type AgentHandler func(ctx context.Context, params map[string]any) (any, error)

// HandlerError lets a handler report a protocol error code/message.
type HandlerError struct {
	Code    int
	Message string
	Data    map[string]any
}

func (e *HandlerError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("handler error %d", e.Code)
}

// AgentOptions configures an Agent. Zero values select safe defaults.
type AgentOptions struct {
	// Connection. If Client is non-nil it is reused and never closed by the
	// Agent; otherwise a HubClient is created from URL/Token/Transport.
	Client    *HubClient
	URL       string
	Token     string
	Transport string // "auto" | "ws" | "http" | "poll"

	// Identity. NodeID empty asks the engine to generate one.
	NodeID       string
	Name         string
	Capabilities []string
	Tags         []string
	Metadata     map[string]any
	Labels       map[string]string

	// Platform overrides auto-detection when non-nil.
	Platform *NodePlatform

	Heartbeat      time.Duration // default 10s (core watchdog is 15s)
	PullWait       int64         // long-poll milliseconds, default 5000
	Concurrency    int           // parallel pull loops, default 1
	HandlerTimeout time.Duration // per-command execution timeout, 0 = none
	OnError        func(error)   // non-fatal background errors
}

// Agent is a registered node driven by the Go SDK.
type Agent struct {
	opts     AgentOptions
	client   *HubClient
	own      bool
	handlers map[string]AgentHandler

	mu   sync.RWMutex
	node *Node

	regMu sync.Mutex // serializes re-registration
}

// NewAgent creates an Agent and installs the built-in handlers
// (echo, platform.info). User handlers with the same names override them.
func NewAgent(opts AgentOptions) *Agent {
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = 10 * time.Second
	}
	if opts.PullWait <= 0 {
		opts.PullWait = 5000
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1
	}
	if opts.Platform == nil {
		opts.Platform = DetectPlatform()
	}
	if opts.Platform.Labels == nil && len(opts.Labels) > 0 {
		opts.Platform.Labels = make(map[string]string, len(opts.Labels))
		for k, v := range opts.Labels {
			opts.Platform.Labels[k] = v
		}
	}
	a := &Agent{opts: opts, handlers: map[string]AgentHandler{}}
	if opts.Client != nil {
		a.client = opts.Client
	} else {
		a.client = New(Options{URL: opts.URL, Token: opts.Token, Transport: opts.Transport})
		a.own = true
	}
	a.Handle("echo", func(_ context.Context, params map[string]any) (any, error) {
		return params, nil
	})
	a.Handle("platform.info", func(_ context.Context, _ map[string]any) (any, error) {
		p := *a.opts.Platform
		return map[string]any{
			"node_id":  a.NodeID(),
			"platform": p,
			"agent":    map[string]any{"runtime": "go", "version": Version},
		}, nil
	})
	return a
}

// DetectPlatform returns the platform descriptor of the current process.
func DetectPlatform() *NodePlatform {
	return &NodePlatform{
		OS:             runtime.GOOS,
		Arch:           runtime.GOARCH,
		Runtime:        "go",
		RuntimeVersion: runtime.Version(),
		AgentVersion:   Version,
	}
}

// Handle registers a handler for a command method. Thread-safe; call before
// or during Run.
func (a *Agent) Handle(method string, h AgentHandler) {
	a.mu.Lock()
	a.handlers[method] = h
	a.mu.Unlock()
}

// NodeID returns the registered node id ("" before the first registration).
func (a *Agent) NodeID() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.node == nil {
		return a.opts.NodeID
	}
	return a.node.ID
}

// Node returns a snapshot of the registered node, or nil before registration.
func (a *Agent) Node() *Node {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.node == nil {
		return nil
	}
	c := *a.node
	return &c
}

// Client exposes the underlying SDK client.
func (a *Agent) Client() *HubClient { return a.client }

func (a *Agent) handler(method string) AgentHandler {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.handlers[method]
}

func (a *Agent) setNode(n *Node) {
	a.mu.Lock()
	a.node = n
	a.mu.Unlock()
}

func (a *Agent) report(err error) {
	if err == nil || a.opts.OnError == nil || errors.Is(err, context.Canceled) {
		return
	}
	a.opts.OnError(err)
}

func (a *Agent) registerParams() NodeRegisterParams {
	return NodeRegisterParams{
		NodeID:       a.NodeID(),
		Name:         a.opts.Name,
		Capabilities: a.opts.Capabilities,
		Tags:         a.opts.Tags,
		Metadata:     a.opts.Metadata,
		Platform:     a.opts.Platform,
	}
}

// registerOnce performs one registration attempt.
func (a *Agent) registerOnce(ctx context.Context) error {
	n, err := a.client.NodeRegister(ctx, a.registerParams())
	if err != nil {
		return err
	}
	a.setNode(n)
	return nil
}

// ensureRegistered re-registers once, serialized across pull/heartbeat loops.
func (a *Agent) ensureRegistered(ctx context.Context) {
	a.regMu.Lock()
	defer a.regMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	if err := a.registerOnce(ctx); err != nil {
		a.report(err)
	}
}

// Run registers the node and serves commands until ctx is cancelled. The
// initial registration is retried with exponential backoff; transient pull
// and heartbeat failures trigger re-registration.
func (a *Agent) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	delay := time.Second
	for {
		err := a.registerOnce(ctx)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		a.report(err)
		if !sleepCtx(ctx, delay) {
			return ctx.Err()
		}
		if delay < 30*time.Second {
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.heartbeatLoop(runCtx)
	}()
	for i := 0; i < a.opts.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.pullLoop(runCtx)
		}()
	}
	<-runCtx.Done()
	cancel()
	wg.Wait()

	a.unregister()
	if a.own {
		_ = a.client.Close()
	}
	return nil
}

func (a *Agent) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(a.opts.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			id := a.NodeID()
			if id == "" {
				continue
			}
			if _, err := a.client.NodeHeartbeat(ctx, id); err != nil {
				if ctx.Err() != nil {
					return
				}
				if isNodeGone(err) {
					a.ensureRegistered(ctx)
				} else {
					a.report(err)
				}
			}
		}
	}
}

func (a *Agent) pullLoop(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		id := a.NodeID()
		if id == "" {
			if !sleepCtx(ctx, backoff) {
				return
			}
			continue
		}
		cmds, err := a.client.CommandPull(ctx, id, 1, a.opts.PullWait)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isNodeGone(err) {
				// The core offlined or forgot us: re-register brings us back.
				a.ensureRegistered(ctx)
				backoff = time.Second
				continue
			}
			a.report(err)
			if !sleepCtx(ctx, backoff) {
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		if len(cmds) == 0 {
			// A conforming core holds the long-poll for PullWait, so this
			// only guards against a peer that returns empty immediately.
			sleepCtx(ctx, 50*time.Millisecond)
			continue
		}
		for i := range cmds {
			a.execute(ctx, cmds[i])
		}
	}
}

func (a *Agent) execute(ctx context.Context, cmd Command) {
	h := a.handler(cmd.Method)
	if h == nil {
		a.complete(ctx, cmd, "failed", nil,
			&HandlerError{Code: ErrMethodNotFound, Message: "method not supported: " + cmd.Method})
		return
	}

	execCtx := ctx
	var cancel context.CancelFunc
	if a.opts.HandlerTimeout > 0 {
		execCtx, cancel = context.WithTimeout(ctx, a.opts.HandlerTimeout)
		defer cancel()
	}

	var (
		result any
		err    error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = &HandlerError{Code: ErrInternalError, Message: fmt.Sprintf("handler panic: %v", r)}
			}
		}()
		result, err = h(execCtx, cmd.Params)
	}()
	if err != nil {
		a.complete(ctx, cmd, "failed", nil, err)
		return
	}
	a.complete(ctx, cmd, "succeeded", result, nil)
}

// complete reports a terminal state. During shutdown the caller's context may
// already be cancelled; a short background context still lands the report,
// while the engine's unregister reclamation covers the rest.
func (a *Agent) complete(ctx context.Context, cmd Command, status string, result any, execErr error) {
	cctx := ctx
	var cancel context.CancelFunc
	if ctx.Err() != nil {
		cctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
	}
	p := CommandCompleteParams{
		NodeID:    cmd.NodeID,
		CommandID: cmd.ID,
		Status:    status,
		Result:    result,
	}
	if execErr != nil {
		p.Error = commandErrorFrom(execErr)
	}
	if _, err := a.client.CommandComplete(cctx, p); err != nil && cctx.Err() == nil {
		a.report(err)
	}
}

func commandErrorFrom(err error) *CommandError {
	var he *HandlerError
	if errors.As(err, &he) {
		return &CommandError{Code: he.Code, Message: he.Message, Data: he.Data}
	}
	code := ErrInternalError
	var hube *HubError
	if errors.As(err, &hube) {
		code = hube.Code
	}
	return &CommandError{Code: code, Message: err.Error()}
}

// isNodeGone reports whether the core no longer accepts this node (removed or
// offlined); the agent answers by re-registering.
func isNodeGone(err error) bool {
	var he *HubError
	if !errors.As(err, &he) {
		return false
	}
	return he.Code == ErrNodeNotFound || he.Code == ErrNodeOffline
}

func (a *Agent) unregister() {
	id := a.NodeID()
	if id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := a.client.NodeUnregister(ctx, id); err != nil {
		a.report(err)
	}
}

// sleepCtx sleeps for d or until ctx is done. It reports whether the full
// sleep elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
