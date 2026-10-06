package hub

import (
	"strconv"
	"strings"

	"musubi/engine/internal/jsonrpc"
)

// Protocol constants (PROTOCOL.md §3.3 / §4.6).
var (
	supportedProtocolVersions = []string{"1.0"}

	hubCapabilities = []map[string]any{
		{"id": "node.registry", "version": "1.0"},
		{"id": "command.exec", "version": "1.0"},
		{"id": "task.flow", "version": "1.0"},
		{"id": "event.stream", "version": "1.0"},
		{"id": "auth.token", "version": "1.0"},
		{"id": "transport.ws", "version": "1.0"},
		{"id": "transport.http", "version": "1.0"},
		{"id": "encoding.json", "version": "1.0"},
	}

	hubFeatures = map[string]any{
		"batch":     true,
		"subscribe": true,
		"sse":       true,
		"poll":      true,
	}
)

// negotiate returns the highest protocol version common to the client's
// protocol_versions and the server's supported set (§3.4). Missing client
// protocol_versions is treated as ["1.0"] by callers.
func negotiate(clientVersions []string) (string, bool) {
	best := ""
	for _, v := range clientVersions {
		v = strings.TrimSpace(v)
		for _, sv := range supportedProtocolVersions {
			if v == sv && versionGreater(v, best) {
				best = v
			}
		}
	}
	return best, best != ""
}

// versionGreater compares "MAJOR.MINOR" version strings.
func versionGreater(a, b string) bool {
	am, amin := parseVersion(a)
	bm, bmin := parseVersion(b)
	if am != bm {
		return am > bm
	}
	return amin > bmin
}

func parseVersion(v string) (major, minor int) {
	parts := strings.Split(v, ".")
	if len(parts) == 0 {
		return 0, 0
	}
	major, _ = strconv.Atoi(parts[0])
	if len(parts) > 1 {
		minor, _ = strconv.Atoi(parts[1])
	}
	return major, minor
}

// engineInfo implements engine.info: version negotiation (§3.4) plus the
// server_info discovery payload (§3.3). Never requires authentication (§8.5).
func (h *Hub) engineInfo(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	versions := []string{"1.0"}
	if ci, ok := p["client_info"].(map[string]any); ok {
		if vs, ok := ci["protocol_versions"].([]any); ok && len(vs) > 0 {
			versions = versions[:0]
			for _, v := range vs {
				if s, ok := v.(string); ok {
					versions = append(versions, s)
				}
			}
		}
	}
	ver, ok := negotiate(versions)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeVersionMismatch, "protocol.version.mismatch",
			map[string]any{"supported": supportedProtocolVersions, "client": versions})
	}
	return h.serverInfo(cc, ver), nil
}

func (h *Hub) serverInfo(cc *CallCtx, ver string) map[string]any {
	session := map[string]any{"authenticated": false, "expires_at": nil}
	if cc != nil && cc.Session != nil && cc.Session.Authenticated && cc.Session.Token != nil {
		session = map[string]any{
			"authenticated": true,
			"expires_at":    cc.Session.Token.ExpiresAt.UTC().Format(tsLayout),
		}
	}
	return map[string]any{
		"protocol_version":   ver,
		"server":             map[string]any{"name": "hubd", "version": Version},
		"supported_versions": supportedProtocolVersions,
		"encodings":          []string{"json"},
		"transports":         []string{"ws", "http", "sse", "poll"},
		"features":           hubFeatures,
		"capabilities":       hubCapabilities,
		"session":            session,
		"server_time":        nowTS(),
	}
}

// enginePing implements engine.ping.
func (h *Hub) enginePing(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	return map[string]any{"pong": true, "time": nowTS()}, nil
}

// engineCapabilities implements engine.capabilities.
func (h *Hub) engineCapabilities(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	return map[string]any{"capabilities": hubCapabilities, "features": hubFeatures}, nil
}
