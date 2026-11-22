package hub

import "testing"

// registerPlatform registers a real node over JSON-RPC with a platform
// descriptor and returns the resulting node snapshot.
func registerPlatform(t *testing.T, h *Hub, id string) *Node {
	t.Helper()
	res, rerr := h.nodeRegister(anonCC(), map[string]any{
		"node_id":      id,
		"name":         "platform-" + id,
		"capabilities": []any{"echo", "sys.info"},
		"tags":         []any{"edge"},
		"metadata":     map[string]any{"region": "lab"},
		"platform": map[string]any{
			"os":              "linux",
			"arch":            "arm64",
			"runtime":         "python",
			"runtime_version": "3.12.4",
			"agent_version":   "1.1.0",
			"labels":          map[string]any{"env": "lab", "zone": "a"},
		},
	})
	if rerr != nil {
		t.Fatalf("node.register failed: %+v", rerr)
	}
	return res.(map[string]any)["node"].(*Node)
}

func TestNodeRegisterOverRPC(t *testing.T) {
	h := newSeatHub(t)

	n := registerPlatform(t, h, "w-py")
	if n.ID != "w-py" || n.Name != "platform-w-py" || n.Status != StatusOnline {
		t.Fatalf("unexpected node: %#v", n)
	}
	if n.Platform == nil {
		t.Fatal("platform descriptor was not stored")
	}
	if n.Platform.OS != "linux" || n.Platform.Arch != "arm64" ||
		n.Platform.Runtime != "python" || n.Platform.RuntimeVersion != "3.12.4" ||
		n.Platform.AgentVersion != "1.1.0" {
		t.Fatalf("unexpected platform: %#v", n.Platform)
	}
	if n.Platform.Labels["env"] != "lab" {
		t.Fatalf("unexpected labels: %#v", n.Platform.Labels)
	}

	// node.list filters by runtime/os and the "platform" alias.
	for _, filter := range []map[string]any{
		{"runtime": "python"},
		{"platform": "python"},
		{"os": "linux", "runtime": "python"},
	} {
		res, rerr := h.nodeList(anonCC(), map[string]any{"filter": filter})
		if rerr != nil {
			t.Fatalf("node.list(%v) failed: %+v", filter, rerr)
		}
		nodes := res.(map[string]any)["nodes"].([]*Node)
		if len(nodes) != 1 || nodes[0].ID != "w-py" {
			t.Fatalf("node.list(%v) = %d nodes, want w-py", filter, len(nodes))
		}
	}

	res, rerr := h.nodeList(anonCC(), map[string]any{"filter": map[string]any{"runtime": "go"}})
	if rerr != nil {
		t.Fatalf("node.list failed: %+v", rerr)
	}
	if got := res.(map[string]any)["nodes"].([]*Node); len(got) != 0 {
		t.Fatalf("runtime=go matched %d nodes, want 0", len(got))
	}
}

func TestNodeRegisterGeneratedIDAndReregistration(t *testing.T) {
	h := newSeatHub(t)

	res, rerr := h.nodeRegister(anonCC(), map[string]any{
		"platform": map[string]any{"runtime": "go", "os": "linux"},
	})
	if rerr != nil {
		t.Fatalf("node.register failed: %+v", rerr)
	}
	n := res.(map[string]any)["node"].(*Node)
	if n.ID == "" {
		t.Fatal("engine did not generate a node id")
	}

	// Re-registering the same id updates the identity and stays online.
	res, rerr = h.nodeRegister(anonCC(), map[string]any{
		"node_id": n.ID, "name": "renamed",
		"platform": map[string]any{"runtime": "go", "os": "linux", "arch": "amd64"},
	})
	if rerr != nil {
		t.Fatalf("re-register failed: %+v", rerr)
	}
	upd := res.(map[string]any)["node"].(*Node)
	if upd.Name != "renamed" || upd.Platform.Arch != "amd64" {
		t.Fatalf("re-register did not update: %#v", upd)
	}
}

// Anonymous tokens (auth.login {} in none mode) track sessions, not owners:
// a restarted agent must be able to reclaim its own node id.
func TestNodeRegisterAnonymousTakeover(t *testing.T) {
	h := newSeatHub(t)
	anonA := &CallCtx{Session: &Session{Token: &Token{AccessToken: "anon-A", Anonymous: true}}}
	anonB := &CallCtx{Session: &Session{Token: &Token{AccessToken: "anon-B", Anonymous: true}}}

	p := map[string]any{"node_id": "w-anon", "platform": map[string]any{"runtime": "go"}}
	if _, rerr := h.nodeRegister(anonA, p); rerr != nil {
		t.Fatalf("first register failed: %+v", rerr)
	}
	if owner, _ := h.nodes.ownerOf("w-anon"); owner != "" {
		t.Fatalf("anonymous registration recorded owner %q, want none", owner)
	}
	if _, rerr := h.nodeRegister(anonB, p); rerr != nil {
		t.Fatalf("second anonymous session could not reclaim the node: %+v", rerr)
	}
	if _, rerr := h.cmdPull(anonB, map[string]any{"node_id": "w-anon"}); rerr != nil {
		t.Fatalf("anonymous pull failed: %+v", rerr)
	}
}

func TestNodePlatformPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	h := NewHub(Config{AuthMode: "none", DemoNodes: 1, DataDir: dir, Logger: discardLogger()})
	n := registerPlatform(t, h, "w-persist")
	h.Stop()

	h2 := NewHub(Config{AuthMode: "none", DemoNodes: 1, DataDir: dir, Logger: discardLogger()})
	defer h2.Stop()
	got, ok := h2.nodes.get("w-persist")
	if !ok {
		t.Fatal("node was not restored")
	}
	if got.Status != StatusOffline {
		t.Fatalf("restored node status = %s, want offline", got.Status)
	}
	if got.Platform == nil || got.Platform.Runtime != "python" || got.Platform.Labels["env"] != "lab" {
		t.Fatalf("platform descriptor was not restored: %#v", got.Platform)
	}
	if got.Capabilities == nil || len(got.Capabilities) != 2 {
		t.Fatalf("capabilities were not restored: %#v", got.Capabilities)
	}
	_ = n
}

func TestDemoNodesAdvertiseDemoPlatform(t *testing.T) {
	h := newSeatHub(t)
	n, ok := h.nodes.get("n-1")
	if !ok {
		t.Fatal("demo node missing")
	}
	if n.Platform == nil || n.Platform.Runtime != "demo" || n.Platform.OS == "" {
		t.Fatalf("demo platform missing: %#v", n.Platform)
	}
}
