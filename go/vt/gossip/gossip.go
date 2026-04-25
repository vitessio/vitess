/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package gossip provides a minimal membership and liveness gossip layer.
package gossip

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type (
	// NodeID is the stable identifier of a gossip participant. Production
	// callers use the tablet alias for vttablets and the listen address
	// (host:port) for VTOrc. Typed as a distinct string so callers can't
	// accidentally pass a raw address where an ID is expected.
	NodeID string

	// Status is the liveness verdict a peer holds for another peer. It
	// is intentionally separate from any higher-level MySQL / tablet
	// health concept — this layer only reports whether the remote gossip
	// agent appears to be running.
	Status int

	// Member is a directory entry: one row per known peer. Addr is where
	// we dial for gossip RPCs. Meta carries scope metadata (keyspace,
	// shard, tablet_alias) used both for peer-selection (same-shard
	// tablets gossip with each other) and for downstream quorum analysis
	// in VTOrc. Meta is merged on update (not replaced), so a partial
	// wire message can never erase keys a richer peer already propagated.
	// The json tags let /debug/gossip emit a stable human-readable shape
	// without a parallel Debug* type.
	Member struct {
		ID   NodeID            `json:"id"`
		Addr string            `json:"addr"`
		Meta map[string]string `json:"meta,omitempty"`
	}

	// HealthSnapshot refreshes the local node's entry in the gossip
	// network. Today gossip tracks process liveness only — a running
	// agent is Alive; peers detect process death through the absence of
	// new timestamps. There is no separate MySQL or tablet-health bit;
	// if you need those, feed them into a higher-level analysis layer.
	HealthSnapshot struct {
		NodeID    NodeID
		Timestamp time.Time
	}

	// State is the in-memory liveness record this agent holds for one
	// peer. Phi is the current phi-accrual suspicion value for that peer
	// (recomputed on every tick). LastUpdate is the timestamp carried by
	// the authoritative self-refresh from the peer, NOT the local wall
	// clock — that lets it propagate unchanged through multi-hop gossip
	// and gives the merge rule a meaningful "newer" comparison.
	// The json tags (plus Status.MarshalJSON and State's own
	// MarshalJSON) let /debug/gossip emit State directly instead of via
	// a parallel Debug* type.
	State struct {
		Status     Status    `json:"status"`
		Phi        float64   `json:"phi"`
		LastUpdate time.Time `json:"last_update"`
	}

	// Message is the wire payload of a push-pull exchange. Epoch is a
	// monotonic change counter (bumped on every internal state mutation)
	// useful for debugging/convergence observability; the merge itself
	// is timestamp-driven, not epoch-driven.
	Message struct {
		Members []Member
		States  []StateDigest
		Epoch   uint64
	}

	// StateDigest is the wire-format counterpart of State. The two types
	// are kept distinct so the wire layout stays stable even if the
	// in-memory State ever grows additional fields (e.g. local-only
	// bookkeeping).
	StateDigest struct {
		NodeID     NodeID
		Status     Status
		Phi        float64
		LastUpdate time.Time
	}

	// JoinRequest is the payload for the bootstrap RPC. Seeds lets the
	// joiner share its configured seed list so the responder can learn
	// about peers it might not otherwise discover.
	JoinRequest struct {
		Member Member
		Seeds  []Member
	}

	// JoinResponse returns the responder's scoped view to the joiner:
	// Members for directory population and Initial for the first state
	// snapshot (so the joiner starts with useful state instead of having
	// to wait for the first push-pull tick).
	JoinResponse struct {
		Members []Member
		Initial Message
	}

	// Transport abstracts the network layer. Production uses a gRPC
	// implementation with a per-target *grpc.ClientConn cache; tests use
	// an in-process transport. Close is invoked from Gossip.Stop so the
	// implementation can tear down those cached connections.
	Transport interface {
		PushPull(ctx context.Context, addr string, msg *Message) (*Message, error)
		Join(ctx context.Context, addr string, req *JoinRequest) (*JoinResponse, error)
		Close()
	}

	// Clock abstracts wall time. Production uses realClock; tests inject
	// a controllable clock to exercise time-dependent behavior (phi
	// accrual, MaxUpdateAge, future-timestamp clamping) deterministically.
	Clock interface {
		Now() time.Time
	}

	// Config is the tuning the agent runs with. Only a subset
	// (PhiThreshold, PingInterval, MaxUpdateAge) is applied by
	// Reconfigure — identity/transport/seeds are fixed for the lifetime
	// of an agent, which matches the one-shot Start semantic.
	Config struct {
		NodeID       NodeID
		BindAddr     string
		Seeds        []Member
		Meta         map[string]string
		PhiThreshold float64
		PingInterval time.Duration
		ProbeTimeout time.Duration
		MaxUpdateAge time.Duration
	}

	// Gossip is the top-level agent. One instance per process (one per
	// vttablet, one per VTOrc). All methods are safe for concurrent use.
	// Internal state (members/states/detectors/epoch) is guarded by mu;
	// reconfigure state and lifecycle flags use their own synchronization
	// so they don't contend with the gossip hot path.
	Gossip struct {
		// cfg, transport, clock, rng are set at construction and never
		// reassigned, so they're safe to read without mu.
		cfg       Config
		transport Transport
		clock     Clock
		rng       *rand.Rand

		// mu guards the in-memory directory (members), liveness view
		// (states), per-peer heartbeat trackers (detectors), and the
		// change counter (epoch). Hot-path methods like gossipOnce and
		// applyMessageLocked briefly hold this to mutate state.
		mu        sync.Mutex
		members   map[NodeID]Member
		states    map[NodeID]State
		detectors map[NodeID]*phiAccrual
		epoch     uint64

		// reconfigMu guards pendingConfig. reconfigCh is a size-1 signal
		// channel that wakes the gossip loop when a new pending config
		// is set. Multiple concurrent Reconfigure calls are safe because
		// the latest write to pendingConfig always wins (we don't queue
		// configs — we only care about the newest one).
		reconfigMu    sync.Mutex
		pendingConfig Config
		reconfigCh    chan struct{}

		// Lifecycle flags. stop is created in New and closed exactly
		// once by Stop(). Start captures it locally so the gossip loop
		// never reads g.stop directly — this keeps the stop channel
		// read out of any future field-level race analysis.
		//
		// started enforces one-shot Start semantics (a stopped agent
		// cannot be restarted; create a new one instead).
		//
		// stopInvoked gates the external Stop() path (and cleanup of
		// transport resources) independently of loopExited, so Stop()
		// always closes the transport even if the gossip loop already
		// exited because the caller-supplied ctx was cancelled first.
		stop        chan struct{}
		started     atomic.Bool
		stopInvoked atomic.Bool
		loopExited  atomic.Bool
	}
)

const (
	// StatusUnknown means we've registered the peer (e.g. as a seed) but
	// have not yet observed any gossip from it. Unknown peers do NOT age
	// to Down via MaxUpdateAge — they have to become Alive first.
	StatusUnknown Status = iota
	// StatusAlive means the peer is gossiping normally.
	StatusAlive
	// StatusSuspect means phi has crossed PhiThreshold but the last
	// observation is still recent enough (within MaxUpdateAge) that we
	// don't want to commit to Down yet.
	StatusSuspect
	// StatusDown means the peer's last observed update is older than
	// MaxUpdateAge. This is the verdict VTOrc's quorum analysis looks
	// for on the primary before triggering ERS.
	StatusDown
)

// Meta keys used by higher layers (vttablet, VTOrc) to tag members with
// topology identity. The agent itself only cares about Keyspace+Shard
// for peer-selection scoping; TabletAlias is carried through for
// downstream quorum analysis.
const (
	MetaKeyKeyspace    = "keyspace"
	MetaKeyShard       = "shard"
	MetaKeyTabletAlias = "tablet_alias"
)

type (
	// phiAccrual is a per-peer heartbeat-interval tracker. It keeps a
	// bounded ring of recent inter-arrival times and computes a phi
	// suspicion value from their mean/stddev — the idea being that a
	// peer whose heartbeats have been ~1s apart should look fine at
	// 1.2s of silence but increasingly suspicious at 5s, adapting to
	// however often that particular peer actually gossips.
	phiAccrual struct {
		last      time.Time
		intervals []time.Duration
		maxSize   int
	}

	// realClock is the default Clock for production (tests inject a
	// controllable clock via the Clock interface).
	realClock struct{}
)

// Now returns the current wall time — the default Clock production uses.
func (r realClock) Now() time.Time {
	return time.Now()
}

// New creates a gossip agent with the given configuration, transport, and clock.
func New(cfg Config, transport Transport, clock Clock) *Gossip {
	if clock == nil {
		clock = realClock{}
	}

	g := &Gossip{
		cfg:        cfg,
		transport:  transport,
		clock:      clock,
		rng:        rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano())+1)),
		members:    make(map[NodeID]Member),
		states:     make(map[NodeID]State),
		detectors:  make(map[NodeID]*phiAccrual),
		reconfigCh: make(chan struct{}, 1),
		stop:       make(chan struct{}),
	}

	if cfg.NodeID != "" {
		g.members[cfg.NodeID] = Member{ID: cfg.NodeID, Addr: cfg.BindAddr, Meta: cfg.Meta}
		g.states[cfg.NodeID] = State{Status: StatusAlive, LastUpdate: g.clock.Now()}
		g.detectors[cfg.NodeID] = newPhiAccrual(50)
	}

	for _, seed := range cfg.Seeds {
		if seed.ID == cfg.NodeID {
			continue // Don't overwrite self-entry with a metadata-less seed.
		}
		g.members[seed.ID] = seed
		g.states[seed.ID] = State{Status: StatusUnknown}
		g.detectors[seed.ID] = newPhiAccrual(50)
	}

	return g
}

// ErrAlreadyStarted is returned by Start when the agent has already been
// started. Gossip agents are single-shot: create a new one to restart.
var ErrAlreadyStarted = errors.New("gossip: agent already started")

// Start begins the periodic gossip loop in a background goroutine. Start
// is one-shot per agent instance — a stopped agent cannot be restarted;
// create a new one instead.
func (g *Gossip) Start(ctx context.Context) error {
	if g.cfg.PingInterval <= 0 {
		return nil
	}
	if !g.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}

	// Capture stop locally so the goroutine never reads g.stop directly —
	// this is a defense-in-depth measure to keep the stop channel read
	// out of any future field-level race analysis.
	stopCh := g.stop

	ticker := time.NewTicker(g.cfg.PingInterval)
	go func() {
		defer g.loopExited.Store(true)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-g.reconfigCh:
				g.applyPendingConfigLocked(ticker)
			case <-ticker.C:
				now := g.clock.Now()
				g.gossipOnce(ctx, now)
				g.updateSuspicion(now)
			}
		}
	}()

	go g.bootstrapSeeds(ctx)

	return nil
}

// Stop halts the gossip loop and releases any resources held by the
// transport (e.g., cached gRPC connections). Safe to call multiple
// times and safe to call after the loop has already exited due to
// context cancellation.
func (g *Gossip) Stop() {
	if !g.stopInvoked.CompareAndSwap(false, true) {
		return
	}
	// If Start was never called (PingInterval<=0), g.started is false —
	// the channel was never observed by any goroutine, but close is
	// still safe because it's created unconditionally in New.
	close(g.stop)
	if g.transport != nil {
		g.transport.Close()
	}
}

// Reconfigure records updated tuning parameters. The latest values always
// win: concurrent callers do not race or drop values. Changes to
// PhiThreshold, PingInterval, and MaxUpdateAge take effect on the next
// gossip tick. Safe to call concurrently.
func (g *Gossip) Reconfigure(cfg Config) {
	g.reconfigMu.Lock()
	g.pendingConfig = cfg
	g.reconfigMu.Unlock()
	// Non-blocking wake — if a previous signal is already pending, the
	// loop will see the latest pendingConfig when it wakes.
	select {
	case g.reconfigCh <- struct{}{}:
	default:
	}
}

// applyPendingConfigLocked reads the latest pendingConfig and applies it.
// Called from the gossip loop goroutine.
func (g *Gossip) applyPendingConfigLocked(ticker *time.Ticker) {
	g.reconfigMu.Lock()
	cfg := g.pendingConfig
	g.reconfigMu.Unlock()
	g.applyConfig(cfg, ticker)
}

// applyConfig updates the gossip agent's tuning parameters from the
// given config. Called from within the gossip loop goroutine.
func (g *Gossip) applyConfig(cfg Config, ticker *time.Ticker) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if cfg.PhiThreshold > 0 {
		g.cfg.PhiThreshold = cfg.PhiThreshold
	}
	if cfg.MaxUpdateAge > 0 {
		g.cfg.MaxUpdateAge = cfg.MaxUpdateAge
	}
	if cfg.PingInterval > 0 && cfg.PingInterval != g.cfg.PingInterval {
		g.cfg.PingInterval = cfg.PingInterval
		ticker.Reset(cfg.PingInterval)
	}
}

// UpdateLocal refreshes the local node's health state in the gossip network.
func (g *Gossip) UpdateLocal(snapshot HealthSnapshot) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := snapshot.Timestamp
	if now.IsZero() {
		now = g.clock.Now()
	}
	if snapshot.NodeID == "" {
		snapshot.NodeID = g.cfg.NodeID
	}

	state := State{Status: StatusAlive, LastUpdate: now}
	if existing, ok := g.states[snapshot.NodeID]; ok {
		state.Phi = existing.Phi
	}

	g.states[snapshot.NodeID] = state
	g.addMemberLocked(Member{ID: snapshot.NodeID, Addr: g.cfg.BindAddr})
	g.bumpEpochLocked()
	g.observeLocked(snapshot.NodeID, now)
}

// DebugState is a JSON-serializable snapshot of the gossip agent's
// full internal state, intended for the /debug/gossip HTTP endpoint.
// Members and States reuse the core Member and State types directly —
// the json tags on those types plus custom MarshalJSON on Status and
// State produce the human-readable shape operators expect, so we don't
// need a parallel set of Debug* structs.
type DebugState struct {
	NodeID   NodeID           `json:"node_id"`
	BindAddr string           `json:"bind_addr"`
	Epoch    uint64           `json:"epoch"`
	Members  []Member         `json:"members"`
	States   map[NodeID]State `json:"states"`
}

// Debug returns a JSON-serializable snapshot of the full gossip state.
// Used by the /debug/gossip HTTP endpoint on vttablet and VTOrc so
// operators can inspect live convergence without attaching a debugger.
func (g *Gossip) Debug() *DebugState {
	g.mu.Lock()
	defer g.mu.Unlock()

	members := make([]Member, 0, len(g.members))
	for _, m := range g.members {
		members = append(members, m)
	}

	states := make(map[NodeID]State, len(g.states))
	maps.Copy(states, g.states)

	return &DebugState{
		NodeID:   g.cfg.NodeID,
		BindAddr: g.cfg.BindAddr,
		Epoch:    g.epoch,
		Members:  members,
		States:   states,
	}
}

// MarshalJSON makes Status emit its human-readable name ("alive",
// "suspect", ...) whenever JSON-encoded. The /debug/gossip endpoint
// relies on this; it also benefits anything else that happens to
// JSON-marshal a State or a digest (e.g. log lines).
func (s Status) MarshalJSON() ([]byte, error) {
	return json.Marshal(statusString(s))
}

// MarshalJSON overrides the default time.Time encoding for
// State.LastUpdate so /debug/gossip consistently emits millisecond
// precision — the format operator dashboards have parsed since this
// endpoint was first added. Status and Phi fall through to the normal
// struct encoding (Status uses its own MarshalJSON above).
func (s State) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Status     Status  `json:"status"`
		Phi        float64 `json:"phi"`
		LastUpdate string  `json:"last_update"`
	}{
		Status:     s.Status,
		Phi:        s.Phi,
		LastUpdate: s.LastUpdate.UTC().Format("2006-01-02T15:04:05.000Z"),
	})
}

// statusString returns the human-readable name for a gossip status.
// Used by Status.MarshalJSON so both machine-readable JSON output and
// any string-context formatters share one mapping.
func statusString(s Status) string {
	switch s {
	case StatusAlive:
		return "alive"
	case StatusSuspect:
		return "suspect"
	case StatusDown:
		return "down"
	default:
		return "unknown"
	}
}

// Snapshot returns a copy of the current gossip state for all known nodes.
func (g *Gossip) Snapshot() map[NodeID]State {
	g.mu.Lock()
	defer g.mu.Unlock()

	result := make(map[NodeID]State, len(g.states))
	maps.Copy(result, g.states)
	return result
}

// Members returns a copy of all known gossip members.
func (g *Gossip) Members() []Member {
	g.mu.Lock()
	defer g.mu.Unlock()

	result := make([]Member, 0, len(g.members))
	for _, member := range g.members {
		result = append(result, member)
	}
	return result
}

// HandleJoin processes an incoming join request and returns the current
// cluster state. Returns an error when the request is malformed so
// operators can see the reason in server logs.
func (g *Gossip) HandleJoin(req *JoinRequest) (*JoinResponse, error) {
	if req == nil {
		return nil, errors.New("gossip: join request is nil")
	}
	if req.Member.ID == "" {
		return nil, errors.New("gossip: join request missing member id")
	}
	if req.Member.Addr == "" {
		return nil, errors.New("gossip: join request missing member addr")
	}

	now := g.clock.Now()
	g.mu.Lock()
	g.addMemberLocked(req.Member)
	g.states[req.Member.ID] = State{Status: StatusAlive, LastUpdate: now}
	g.observeLocked(req.Member.ID, now)
	g.bumpEpochLocked()
	scope := g.responseScopeForMemberLocked(req.Member)
	response := &JoinResponse{
		Members: g.membersSliceLocked(scope),
		Initial: g.snapshotMessageLocked(scope),
	}
	g.mu.Unlock()

	return response, nil
}

// HandlePushPull processes an incoming push-pull exchange and returns the local state.
func (g *Gossip) HandlePushPull(msg *Message) *Message {
	now := g.clock.Now()
	g.mu.Lock()
	g.applyMessageLocked(now, msg)
	response := g.snapshotMessageLocked(g.responseScopeForMessageLocked(msg))
	g.mu.Unlock()
	return &response
}

// Join sends a join request to the given seed address, registering this node in the cluster.
func (g *Gossip) Join(ctx context.Context, seedAddr string) (*JoinResponse, error) {
	if g.transport == nil || seedAddr == "" {
		return nil, nil
	}
	self := Member{ID: g.cfg.NodeID, Addr: g.cfg.BindAddr, Meta: g.cfg.Meta}
	ctx, cancel := g.withProbeTimeout(ctx)
	defer cancel()
	return g.transport.Join(ctx, seedAddr, &JoinRequest{Member: self, Seeds: g.cfg.Seeds})
}

// bootstrapSeeds runs once at Start, Joining each configured seed so
// the agent discovers peers immediately instead of waiting for the
// first periodic tick. Without this, enabling gossip right before a
// primary dies could leave that primary as Unknown (never observed)
// instead of converging to Down.
func (g *Gossip) bootstrapSeeds(ctx context.Context) {
	if g.transport == nil {
		return
	}

	seeds := append([]Member(nil), g.cfg.Seeds...)
	for _, seed := range seeds {
		if seed.ID == "" || seed.ID == g.cfg.NodeID || seed.Addr == "" {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-g.stop:
			return
		default:
		}

		response, err := g.Join(ctx, seed.Addr)
		if err != nil || response == nil {
			continue
		}

		now := g.clock.Now()
		g.mu.Lock()
		for _, member := range response.Members {
			g.addMemberLocked(member)
		}
		g.applyMessageLocked(now, &response.Initial)
		g.mu.Unlock()
	}
}

// gossipOnce runs one push-pull exchange with a randomly-picked peer.
// It also refreshes the local node's own timestamp before sending — a
// live agent's absence of refresh is precisely the signal peers use to
// detect that its process has died.
func (g *Gossip) gossipOnce(ctx context.Context, now time.Time) {
	peer, scope := g.pickPeer()
	if peer == nil || g.transport == nil {
		return
	}

	g.mu.Lock()
	// Refresh self state so peers see a current timestamp on every
	// gossip round. Without this, the local node's LastUpdate goes
	// stale and peers mark it Down via MaxUpdateAge.
	if g.cfg.NodeID != "" {
		state := g.states[g.cfg.NodeID]
		state.Status = StatusAlive
		state.LastUpdate = now
		g.states[g.cfg.NodeID] = state
		g.observeLocked(g.cfg.NodeID, now)
	}
	msg := g.snapshotMessageLocked(scope)
	g.mu.Unlock()

	ctx, cancel := g.withProbeTimeout(ctx)
	defer cancel()
	response, err := g.transport.PushPull(ctx, peer.Addr, &msg)
	if err != nil || response == nil {
		return
	}

	g.mu.Lock()
	g.applyMessageLocked(now, response)
	g.mu.Unlock()
}

// pickPeer selects a random peer and the scope to use for the exchange.
// Scoped nodes (vttablets, which advertise keyspace/shard) only gossip
// within their own shard so traffic stays local to the shard. Unscoped
// nodes (VTOrc) pick a shard first, then a member within it, so VTOrc
// fairly interleaves with every shard it knows about.
func (g *Gossip) pickPeer() (*Member, string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	selfScope := g.localScopeLocked()
	if selfScope != "" {
		peers := make([]Member, 0, len(g.members))
		for id, member := range g.members {
			if id == g.cfg.NodeID {
				continue
			}
			scope := memberScope(member.Meta)
			if scope != "" && scope != selfScope {
				continue
			}
			peers = append(peers, member)
		}

		if len(peers) == 0 {
			return nil, ""
		}

		picked := peers[g.rng.IntN(len(peers))]
		return &picked, selfScope
	}

	scopePeers := make(map[string][]Member)
	peers := make([]Member, 0, len(g.members))
	for id, member := range g.members {
		if id == g.cfg.NodeID {
			continue
		}
		scope := memberScope(member.Meta)
		if scope == "" {
			peers = append(peers, member)
			continue
		}
		scopePeers[scope] = append(scopePeers[scope], member)
	}

	if len(scopePeers) > 0 {
		scopes := make([]string, 0, len(scopePeers))
		for scope := range scopePeers {
			scopes = append(scopes, scope)
		}
		sort.Strings(scopes)
		scope := scopes[g.rng.IntN(len(scopes))]
		scopeMembers := scopePeers[scope]
		picked := scopeMembers[g.rng.IntN(len(scopeMembers))]
		return &picked, scope
	}

	if len(peers) == 0 {
		return nil, ""
	}

	picked := peers[g.rng.IntN(len(peers))]
	return &picked, ""
}

// updateSuspicion runs every tick to translate per-peer phi values into
// Status transitions (Alive → Suspect when phi exceeds threshold, or
// Suspect → Alive when it drops back). Peers that haven't refreshed
// within MaxUpdateAge escalate to Down — the key signal VTOrc's quorum
// analysis keys off of.
//
// Note: this function never MUTATES LastUpdate. Status-only changes stay
// local to this agent and do not propagate through gossip, which is what
// makes a single-observer false-positive non-contagious.
func (g *Gossip) updateSuspicion(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for id, state := range g.states {
		if id == g.cfg.NodeID {
			continue
		}
		detector := g.detectors[id]
		if detector == nil {
			continue
		}
		phi := detector.Phi(now)
		state.Phi = phi
		if phi >= g.cfg.PhiThreshold {
			if state.Status == StatusAlive {
				state.Status = StatusSuspect
			}
		} else if state.Status == StatusSuspect {
			state.Status = StatusAlive
		}
		// Only apply MaxUpdateAge to nodes we have actually heard from.
		// Seeds start with zero LastUpdate and must remain Unknown until
		// their first gossip exchange, not age directly to Down.
		if g.cfg.MaxUpdateAge > 0 && !state.LastUpdate.IsZero() && now.Sub(state.LastUpdate) > g.cfg.MaxUpdateAge {
			state.Status = StatusDown
		}
		g.states[id] = state
	}
}

// addMemberLocked upserts a directory entry. Both address and metadata
// are merged (not replaced wholesale) so a partial message from one
// peer can't erase richer data another peer already propagated — this
// matters because seed entries start without metadata and get enriched
// through multi-hop gossip.
func (g *Gossip) addMemberLocked(member Member) {
	if member.ID == "" {
		return
	}
	if existing, ok := g.members[member.ID]; ok {
		updated := false
		// Update address independently of metadata so corrected
		// addresses are never ignored due to missing metadata.
		if member.Addr != "" && member.Addr != existing.Addr {
			existing.Addr = member.Addr
			updated = true
		}
		// Merge metadata keys so partial updates cannot overwrite
		// previously-known keys. A sender with only {keyspace:X} must
		// not erase shard/tablet_alias that a richer peer already
		// propagated.
		for key, value := range member.Meta {
			if value == "" {
				continue
			}
			if existing.Meta == nil {
				existing.Meta = make(map[string]string, len(member.Meta))
			}
			if existing.Meta[key] != value {
				existing.Meta[key] = value
				updated = true
			}
		}
		if updated {
			g.members[member.ID] = existing
		}
		return
	}
	g.members[member.ID] = member
	if _, ok := g.detectors[member.ID]; !ok {
		g.detectors[member.ID] = newPhiAccrual(50)
	}
}

// applyMessageLocked merges an incoming wire message into local state
// using timestamp-based last-writer-wins. The tricky rules:
//   - digests for our own NodeID are ignored (only gossipOnce's self
//     refresh may update our own state);
//   - future timestamps are clamped to now so clock-skewed peers can't
//     pin a node's freshness beyond any real observation;
//   - on equal timestamps, Alive beats Down/Suspect so a late-joining
//     observer doesn't latch onto a stale verdict when a fresher Alive
//     at the same timestamp is reachable elsewhere in the mesh.
func (g *Gossip) applyMessageLocked(now time.Time, msg *Message) {
	if msg == nil {
		return
	}

	for _, member := range msg.Members {
		g.addMemberLocked(member)
	}

	for _, digest := range msg.States {
		if digest.NodeID == "" {
			continue
		}
		// Never let peers overwrite our own state — the local node is
		// the authoritative source for its own liveness. The self
		// refresh in gossipOnce is the only thing that can update it.
		if digest.NodeID == g.cfg.NodeID {
			continue
		}
		// Clamp future timestamps to local time to prevent clock-skewed
		// peers from pinning a node's freshness beyond actual observation.
		if digest.LastUpdate.After(now) {
			digest.LastUpdate = now
		}
		current := g.states[digest.NodeID]
		isNewer := digest.LastUpdate.After(current.LastUpdate)
		isEqual := digest.LastUpdate.Equal(current.LastUpdate) && !current.LastUpdate.IsZero()
		isEmpty := current.LastUpdate.IsZero()
		if isNewer || isEmpty {
			current.Status = digest.Status
			current.Phi = digest.Phi
			current.LastUpdate = digest.LastUpdate
			g.states[digest.NodeID] = current
			g.observeLocked(digest.NodeID, now)
			g.bumpEpochLocked()
		} else if isEqual && digest.Status == StatusAlive && current.Status != StatusAlive {
			// On equal timestamps, prefer Alive over Down/Suspect.
			// This prevents a late-starting observer from permanently
			// latching onto a Down verdict when an Alive at the same
			// timestamp is also available in the gossip network.
			current.Status = digest.Status
			current.Phi = digest.Phi
			g.states[digest.NodeID] = current
			g.bumpEpochLocked()
		}
	}
}

// observeLocked records a fresh heartbeat arrival for a peer, feeding
// the per-peer phi-accrual detector so its suspicion value decays.
func (g *Gossip) observeLocked(nodeID NodeID, now time.Time) {
	detector := g.detectors[nodeID]
	if detector == nil {
		detector = newPhiAccrual(50)
		g.detectors[nodeID] = detector
	}
	detector.Observe(now)
}

// snapshotMessageLocked builds the wire payload we'll send to a peer,
// filtered to scope (keyspace/shard) so per-shard gossip traffic stays
// per-shard. The epoch carried in the payload is purely observational.
func (g *Gossip) snapshotMessageLocked(scope string) Message {
	members := g.membersSliceLocked(scope)
	states := make([]StateDigest, 0, len(g.states))
	for id, state := range g.states {
		member, ok := g.members[id]
		if scope != "" && (!ok || memberScope(member.Meta) != scope) {
			continue
		}
		states = append(states, StateDigest{
			NodeID:     id,
			Status:     state.Status,
			Phi:        state.Phi,
			LastUpdate: state.LastUpdate,
		})
	}

	return Message{
		Members: members,
		States:  states,
		Epoch:   g.epoch,
	}
}

// membersSliceLocked returns the directory entries matching scope.
// Passing "" returns all members (used when responding to an unscoped
// requester like VTOrc).
func (g *Gossip) membersSliceLocked(scope string) []Member {
	members := make([]Member, 0, len(g.members))
	for _, member := range g.members {
		if scope != "" && memberScope(member.Meta) != scope {
			continue
		}
		members = append(members, member)
	}
	return members
}

// localScopeLocked is this agent's own scope (empty for VTOrc; a
// "keyspace/shard" string for vttablets).
func (g *Gossip) localScopeLocked() string {
	return memberScope(g.cfg.Meta)
}

// responseScopeForMemberLocked picks the scope to respond with on an
// incoming Join: the requester's scope when it has one (so a scoped
// vttablet only learns about its shard), otherwise our own.
func (g *Gossip) responseScopeForMemberLocked(member Member) string {
	if scope := memberScope(member.Meta); scope != "" {
		return scope
	}
	return g.localScopeLocked()
}

// responseScopeForMessageLocked picks the scope for a PushPull response.
// We prefer our own scope; failing that we fall back to any scoped
// member in the incoming message so an unscoped peer talking to an
// unscoped peer can still converge in practice.
func (g *Gossip) responseScopeForMessageLocked(msg *Message) string {
	if scope := g.localScopeLocked(); scope != "" {
		return scope
	}
	if msg == nil {
		return ""
	}
	for _, member := range msg.Members {
		if scope := memberScope(member.Meta); scope != "" {
			return scope
		}
	}
	return ""
}

// memberScope computes the compound "keyspace/shard" key used to
// partition gossip traffic, returning "" if either is missing so
// partially-populated members stay unscoped.
func memberScope(meta map[string]string) string {
	if meta == nil {
		return ""
	}
	keyspace := meta[MetaKeyKeyspace]
	shard := meta[MetaKeyShard]
	if keyspace == "" || shard == "" {
		return ""
	}
	return keyspace + "/" + shard
}

// bumpEpochLocked advances a monotonic change counter — observability
// only; the merge logic is timestamp-driven, not epoch-driven.
func (g *Gossip) bumpEpochLocked() {
	g.epoch++
}

// withProbeTimeout caps one gossip RPC at ProbeTimeout when configured.
// Short deadlines keep a single slow peer from stalling the tick.
func (g *Gossip) withProbeTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if g.cfg.ProbeTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, g.cfg.ProbeTimeout)
}

// newPhiAccrual constructs a detector with a bounded sample window.
// The sample window limits how much historical jitter influences the
// current phi value — too small is noisy, too large is slow to react.
func newPhiAccrual(maxSize int) *phiAccrual {
	if maxSize <= 0 {
		maxSize = 1
	}
	return &phiAccrual{maxSize: maxSize}
}

// Observe records that we just saw activity from the peer. Inter-arrival
// intervals accumulate in a ring up to maxSize, providing the raw data
// Phi summarizes into a suspicion value.
func (p *phiAccrual) Observe(now time.Time) {
	if !p.last.IsZero() {
		interval := now.Sub(p.last)
		if interval > 0 {
			p.intervals = append(p.intervals, interval)
			if len(p.intervals) > p.maxSize {
				p.intervals = p.intervals[len(p.intervals)-p.maxSize:]
			}
		}
	}
	p.last = now
}

// Phi is the current suspicion value for the peer: the negative log10
// of the probability that the current silence is "still within normal
// jitter." Callers compare this against a threshold (the canonical phi
// accrual technique); a threshold of 4 means "probability of false
// positive is roughly 1e-4."
func (p *phiAccrual) Phi(now time.Time) float64 {
	if p.last.IsZero() || len(p.intervals) < 2 {
		return 0
	}

	elapsed := now.Sub(p.last)
	mean, stddev := p.stats()
	if stddev == 0 {
		if elapsed <= mean {
			return 0
		}
		return 100
	}

	x := elapsed.Seconds()
	m := mean.Seconds()
	s := stddev.Seconds()
	cdf := 0.5 * (1 + math.Erf((x-m)/(s*math.Sqrt2)))
	if cdf >= 1 {
		return 100
	}
	if cdf <= 0 {
		return 0
	}
	return -math.Log10(1 - cdf)
}

// stats returns the mean and stddev of the observed inter-arrival
// intervals, the two moments Phi needs to compute the suspicion value.
func (p *phiAccrual) stats() (time.Duration, time.Duration) {
	var sum float64
	for _, interval := range p.intervals {
		sum += interval.Seconds()
	}
	mean := sum / float64(len(p.intervals))

	var variance float64
	for _, interval := range p.intervals {
		delta := interval.Seconds() - mean
		variance += delta * delta
	}
	variance /= float64(len(p.intervals))

	return time.Duration(mean * float64(time.Second)), time.Duration(math.Sqrt(variance) * float64(time.Second))
}
