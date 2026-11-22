package hub

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"musubi/engine/internal/jsonrpc"
)

// Durable state (PROTOCOL.md §9): when -data-dir is set the hub snapshots the
// entities that must survive a restart to <dir>/hub-state.json.
//
// Design notes:
//   - the file is written with 0600 permissions because it contains issued
//     access tokens (resource ownership) and node owners;
//   - writes are debounced: mutations set a dirty flag and a 1s loop writes one
//     consistent snapshot, so a burst of commands costs one disk write;
//   - writes are atomic (temp file + rename), never leaving a torn JSON file;
//   - restart semantics: registered nodes come back offline (they must
//     re-register/heartbeat), standing seats return to vacant shells with their
//     expect profile preserved, and commands/tasks that were in flight are
//     terminalised as failed ("engine restart") rather than replayed. Dispatch
//     is at-most-once; no command is executed twice because of a restart.
const (
	stateFileName          = "hub-state.json"
	stateVersion           = 1
	persistInterval        = time.Second
	persistedCommandLimit  = 2000
	restartInterruptedCode = -32000
)

// persistedState is the on-disk snapshot. The embedded runtime structs keep
// their protocol JSON shape; unexported fields (owners, channels) are handled
// explicitly via the Owner fields below.
type persistedState struct {
	Version  int                `json:"version"`
	SavedAt  string             `json:"saved_at"`
	Nodes    []persistedNode    `json:"nodes"`
	Seats    []persistedSeat    `json:"seats"`
	Commands []persistedCommand `json:"commands"`
	Tasks    []persistedTask    `json:"tasks"`
	Auth     persistedAuth      `json:"auth"`
}

type persistedNode struct {
	Node
	Owner string `json:"owner,omitempty"`
}

type persistedSeat struct {
	Seat
	Owner string `json:"owner,omitempty"`
}

type persistedCommand struct {
	Command
	Owner string `json:"owner,omitempty"`
}

// persistedTask is an explicit projection of Task: Task carries sync primitives
// that must not be copied (go vet copylocks) or serialised.
type persistedTask struct {
	ID         string         `json:"id"`
	Name       string         `json:"name,omitempty"`
	Status     string         `json:"status"`
	Progress   float64        `json:"progress"`
	CommandIDs []string       `json:"command_ids"`
	Error      *jsonrpc.Error `json:"error,omitempty"`
	CreatedAt  string         `json:"created_at"`
	UpdatedAt  string         `json:"updated_at"`
	Owner      string         `json:"owner,omitempty"`
}

type persistedToken struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token,omitempty"`
	ExpiresAt    string `json:"expires_at"`
	Anonymous    bool   `json:"anonymous,omitempty"`
	User         string `json:"user,omitempty"`
}

type persistedAuth struct {
	Tokens []persistedToken `json:"tokens,omitempty"`
}

type fileStore struct {
	dir string
	mu  sync.Mutex
}

func newFileStore(dir string) (*fileStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &fileStore{dir: dir}, nil
}

func (s *fileStore) path() string { return filepath.Join(s.dir, stateFileName) }

// load reads the snapshot. A missing file is (nil, nil): a fresh data dir.
func (s *fileStore) load() (*persistedState, error) {
	b, err := os.ReadFile(s.path())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var st persistedState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("decode %s: %w", s.path(), err)
	}
	if st.Version != stateVersion {
		return nil, fmt.Errorf("unsupported state version %d (want %d)", st.Version, stateVersion)
	}
	return &st, nil
}

// save writes the snapshot atomically with 0600 permissions.
func (s *fileStore) save(st *persistedState) error {
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tmp := s.path() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, s.path())
}

// ---------------------------------------------------------------------------
// Snapshot (locks are taken per registry, never while marshalling)

func (h *Hub) snapshotState() *persistedState {
	return &persistedState{
		Version:  stateVersion,
		SavedAt:  nowTS(),
		Nodes:    h.nodes.persistSnapshot(),
		Seats:    h.seats.persistSnapshot(),
		Commands: h.cmds.persistSnapshot(persistedCommandLimit),
		Tasks:    h.tasks.persistSnapshot(),
		Auth:     h.auth.persistSnapshot(),
	}
}

func (r *nodeRegistry) persistSnapshot() []persistedNode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]persistedNode, 0, len(r.nodes))
	for _, id := range r.order {
		n := r.nodes[id]
		if n == nil || n.demo {
			continue // simulated nodes are recreated from -demo on every boot
		}
		out = append(out, persistedNode{Node: *cloneNode(n), Owner: n.owner})
	}
	return out
}

func (r *seatRegistry) persistSnapshot() []persistedSeat {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]persistedSeat, 0, len(r.seats))
	for _, id := range r.order {
		s := r.seats[id]
		if s == nil {
			continue
		}
		out = append(out, persistedSeat{Seat: *cloneSeat(s), Owner: s.owner})
	}
	return out
}

func (r *commandRegistry) persistSnapshot(limit int) []persistedCommand {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := r.order
	if limit > 0 && len(ids) > limit {
		ids = ids[len(ids)-limit:]
	}
	out := make([]persistedCommand, 0, len(ids))
	for _, id := range ids {
		c := r.cmds[id]
		if c == nil {
			continue
		}
		out = append(out, persistedCommand{Command: *cloneCommand(c), Owner: c.owner})
	}
	return out
}

func (r *taskRegistry) persistSnapshot() []persistedTask {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]persistedTask, 0, len(r.tasks))
	for _, id := range r.order {
		t := r.tasks[id]
		if t == nil {
			continue
		}
		out = append(out, persistedTask{
			ID:         t.ID,
			Name:       t.Name,
			Status:     t.Status,
			Progress:   t.Progress,
			CommandIDs: append([]string(nil), t.CommandIDs...),
			Error:      cloneError(t.Error),
			CreatedAt:  t.CreatedAt,
			UpdatedAt:  t.UpdatedAt,
			Owner:      t.owner,
		})
	}
	return out
}

func (a *authManager) persistSnapshot() persistedAuth {
	a.mu.RLock()
	defer a.mu.RUnlock()
	now := time.Now()
	out := persistedAuth{}
	for _, t := range a.tokens {
		if t == nil || !now.Before(t.ExpiresAt) {
			continue
		}
		out.Tokens = append(out.Tokens, persistedToken{
			AccessToken:  t.AccessToken,
			RefreshToken: t.RefreshToken,
			ExpiresAt:    t.ExpiresAt.UTC().Format(tsLayout),
			Anonymous:    t.Anonymous,
			User:         t.User,
		})
	}
	return out
}

// ---------------------------------------------------------------------------
// Restore

// restoreState installs a loaded snapshot. It never replays work: in-flight
// commands and tasks are terminalised as failed.
func (h *Hub) restoreState(st *persistedState) {
	savedAt := st.SavedAt
	if savedAt == "" {
		savedAt = nowTS()
	}
	h.restoreNodes(st.Nodes)
	h.restoreSeats(st.Seats, savedAt)
	h.restoreCommands(st.Commands, savedAt)
	h.restoreTasks(st.Tasks, savedAt)
	h.auth.restoreSnapshot(st.Auth)
}

func (h *Hub) restoreNodes(nodes []persistedNode) {
	for _, pn := range nodes {
		n := pn.Node
		if n.ID == "" || h.nodes.demoCollision(n.ID) {
			continue
		}
		// The occupant is not connected after a restart: restore the node as
		// offline and let registration/heartbeat bring it back online.
		n.Status = StatusOffline
		n.SeatID = nil
		n.demo = false
		n.owner = pn.Owner
		n.misses = 0
		n.busyRefs = 0
		h.nodes.restore(&n)
		h.bumpNodeSeq(n.ID)
	}
}

func (h *Hub) restoreSeats(seats []persistedSeat, savedAt string) {
	for _, ps := range seats {
		s := ps.Seat
		if s.ID == "" {
			continue
		}
		if s.Kind == SeatKindStanding {
			// Position is durable, the occupant is transient (PROTOCOL §4.7):
			// the shell comes back vacant.
			s.Kind = SeatKindVacant
			s.NodeID = nil
			s.ReleasedAt = strPtr(savedAt)
		}
		s.owner = ps.Owner
		h.seats.restore(&s)
	}
}

func (h *Hub) restoreCommands(cmds []persistedCommand, savedAt string) {
	for _, pc := range cmds {
		c := pc.Command
		if c.ID == "" {
			continue
		}
		c.owner = pc.Owner
		c.cancelCh = make(chan struct{})
		c.done = make(chan struct{})
		if !isTerminal(c.Status) {
			c.Status = cmdFailed
			c.FinishedAt = savedAt
			c.Result = nil
			c.Error = restartError()
		}
		c.signalDone()
		h.cmds.restore(&c)
	}
}

func (h *Hub) restoreTasks(tasks []persistedTask, savedAt string) {
	for _, pt := range tasks {
		if pt.ID == "" {
			continue
		}
		status := pt.Status
		rerr := cloneError(pt.Error)
		updated := pt.UpdatedAt
		if !isTaskTerminal(status) {
			status = taskFailed
			updated = savedAt
			rerr = restartError()
		}
		t := &Task{
			ID:         pt.ID,
			Name:       pt.Name,
			Status:     status,
			Progress:   pt.Progress,
			CommandIDs: append([]string(nil), pt.CommandIDs...),
			Error:      rerr,
			CreatedAt:  pt.CreatedAt,
			UpdatedAt:  updated,
			owner:      pt.Owner,
			done:       make(chan struct{}),
		}
		close(t.done)
		h.tasks.restore(t)
	}
}

func cloneError(e *jsonrpc.Error) *jsonrpc.Error {
	if e == nil {
		return nil
	}
	c := *e
	return &c
}

// restartError marks work that was in flight when the process stopped. It is
// terminal and explicit: the engine never replays a command across a restart.
func restartError() *jsonrpc.Error {
	return jsonrpc.NewError(restartInterruptedCode, "command interrupted by engine restart",
		map[string]any{"reason": "engine.restarted", "replayed": false})
}

// bumpNodeSeq keeps generated node ids from colliding with restored ids of
// the form n-<number>.
func (h *Hub) bumpNodeSeq(id string) {
	if !strings.HasPrefix(id, "n-") {
		return
	}
	n, err := strconv.ParseInt(strings.TrimPrefix(id, "n-"), 10, 64)
	if err != nil || n <= 0 {
		return
	}
	for {
		cur := h.nodeSeq.Load()
		if n <= cur || h.nodeSeq.CompareAndSwap(cur, n) {
			return
		}
	}
}

func (r *nodeRegistry) restore(n *Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[n.ID]; ok {
		return
	}
	r.nodes[n.ID] = n
	r.order = append(r.order, n.ID)
}

func (r *seatRegistry) restore(s *Seat) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.seats[s.ID]; ok {
		return
	}
	r.seats[s.ID] = s
	r.order = append(r.order, s.ID)
}

func (r *commandRegistry) restore(c *Command) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.cmds[c.ID]; ok {
		return
	}
	r.cmds[c.ID] = c
	r.order = append(r.order, c.ID)
}

func (r *taskRegistry) restore(t *Task) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.tasks[t.ID]; ok {
		return
	}
	r.tasks[t.ID] = t
	r.order = append(r.order, t.ID)
}

func (a *authManager) restoreSnapshot(st persistedAuth) {
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, pt := range st.Tokens {
		exp, err := time.Parse(tsLayout, pt.ExpiresAt)
		if err != nil || !now.Before(exp) || pt.AccessToken == "" {
			continue
		}
		t := &Token{
			AccessToken:  pt.AccessToken,
			RefreshToken: pt.RefreshToken,
			ExpiresAt:    exp,
			Anonymous:    pt.Anonymous,
			User:         pt.User,
		}
		a.tokens[t.AccessToken] = t
		if t.RefreshToken != "" {
			a.refreshTokens[t.RefreshToken] = t.AccessToken
		}
	}
}
