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
	"maps"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

type (
	NodeID string

	Status int

	Member struct {
		ID   NodeID
		Addr string
		Meta map[string]string
	}

	HealthSnapshot struct {
		NodeID      NodeID
		TabletAlias string
		MysqlAlive  bool
		TabletAlive bool
		Timestamp   time.Time
	}

	State struct {
		Status     Status
		Phi        float64
		LastUpdate time.Time
	}

	Message struct {
		Members []Member
		States  []StateDigest
		Epoch   uint64
	}

	StateDigest struct {
		NodeID     NodeID
		Status     Status
		Phi        float64
		LastUpdate time.Time
	}

	JoinRequest struct {
		Member Member
		Seeds  []Member
	}

	JoinResponse struct {
		Members []Member
		Initial Message
	}

	Transport interface {
		PushPull(ctx context.Context, addr string, msg *Message) (*Message, error)
		Join(ctx context.Context, addr string, req *JoinRequest) (*JoinResponse, error)
	}

	Clock interface {
		Now() time.Time
	}

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

	Gossip struct {
		cfg       Config
		transport Transport
		clock     Clock
		rng       *rand.Rand

		mu         sync.Mutex
		members    map[NodeID]Member
		states     map[NodeID]State
		detectors  map[NodeID]*phiAccrual
		epoch      uint64
		reconfigCh chan Config

		stop    chan struct{}
		stopped atomic.Bool
	}
)

const (
	StatusUnknown Status = iota
	StatusAlive
	StatusSuspect
	StatusDown
)

const (
	MetaKeyKeyspace    = "keyspace"
	MetaKeyShard       = "shard"
	MetaKeyTabletAlias = "tablet_alias"
)

type (
	phiAccrual struct {
		last      time.Time
		intervals []time.Duration
		maxSize   int
	}

	realClock struct{}
)

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
		reconfigCh: make(chan Config, 1),
		stop:       make(chan struct{}),
		stopped:    atomic.Bool{},
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

// Start begins the periodic gossip loop in a background goroutine.
func (g *Gossip) Start(ctx context.Context) error {
	if g.cfg.PingInterval <= 0 {
		return nil
	}

	g.mu.Lock()
	g.stop = make(chan struct{})
	g.stopped.Store(false)
	g.mu.Unlock()

	ticker := time.NewTicker(g.cfg.PingInterval)
	go func() {
		defer g.stopped.Store(true)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-g.stop:
				return
			case newCfg := <-g.reconfigCh:
				g.applyConfig(newCfg, ticker)
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

// Stop halts the gossip loop. Safe to call multiple times.
func (g *Gossip) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped.Load() {
		return
	}
	close(g.stop)
	g.stopped.Store(true)
}

// Reconfigure sends updated tuning parameters to the running gossip loop.
// Changes to PhiThreshold, PingInterval, and MaxUpdateAge take effect on
// the next gossip tick. If a previous config update is pending, it is
// replaced by the latest one. This is safe to call concurrently.
func (g *Gossip) Reconfigure(cfg Config) {
	// Drain any pending config to make room for the latest.
	select {
	case <-g.reconfigCh:
	default:
	}
	select {
	case g.reconfigCh <- cfg:
	default:
	}
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
type DebugState struct {
	NodeID   NodeID                 `json:"node_id"`
	BindAddr string                 `json:"bind_addr"`
	Epoch    uint64                 `json:"epoch"`
	Members  []DebugMember          `json:"members"`
	States   map[NodeID]*DebugEntry `json:"states"`
}

type DebugMember struct {
	ID   NodeID            `json:"id"`
	Addr string            `json:"addr"`
	Meta map[string]string `json:"meta,omitempty"`
}

type DebugEntry struct {
	Status     string  `json:"status"`
	Phi        float64 `json:"phi"`
	LastUpdate string  `json:"last_update"`
}

func (g *Gossip) Debug() *DebugState {
	g.mu.Lock()
	defer g.mu.Unlock()

	members := make([]DebugMember, 0, len(g.members))
	for _, m := range g.members {
		members = append(members, DebugMember(m))
	}

	states := make(map[NodeID]*DebugEntry, len(g.states))
	for id, s := range g.states {
		states[id] = &DebugEntry{
			Status:     statusString(s.Status),
			Phi:        s.Phi,
			LastUpdate: s.LastUpdate.UTC().Format("2006-01-02T15:04:05.000Z"),
		}
	}

	return &DebugState{
		NodeID:   g.cfg.NodeID,
		BindAddr: g.cfg.BindAddr,
		Epoch:    g.epoch,
		Members:  members,
		States:   states,
	}
}

// statusString returns the human-readable name for a gossip status.
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

// HandleJoin processes an incoming join request and returns the current cluster state.
func (g *Gossip) HandleJoin(req *JoinRequest) *JoinResponse {
	if req == nil || req.Member.ID == "" {
		return nil
	}

	now := g.clock.Now()
	g.mu.Lock()
	g.addMemberLocked(req.Member)
	g.states[req.Member.ID] = State{Status: StatusAlive, LastUpdate: now}
	g.observeLocked(req.Member.ID, now)
	g.bumpEpochLocked()
	response := &JoinResponse{
		Members: g.membersSliceLocked(),
		Initial: g.snapshotMessageLocked(),
	}
	g.mu.Unlock()

	return response
}

// HandlePushPull processes an incoming push-pull exchange and returns the local state.
func (g *Gossip) HandlePushPull(msg *Message) *Message {
	now := g.clock.Now()
	g.mu.Lock()
	g.applyMessageLocked(now, msg)
	response := g.snapshotMessageLocked()
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

func (g *Gossip) gossipOnce(ctx context.Context, now time.Time) {
	peer := g.pickPeer()
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
	msg := g.snapshotMessageLocked()
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

func (g *Gossip) pickPeer() *Member {
	g.mu.Lock()
	defer g.mu.Unlock()

	peers := make([]Member, 0, len(g.members))
	for id, member := range g.members {
		if id == g.cfg.NodeID {
			continue
		}
		peers = append(peers, member)
	}

	if len(peers) == 0 {
		return nil
	}

	picked := peers[g.rng.IntN(len(peers))]
	return &picked
}

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
		if len(member.Meta) > 0 {
			existing.Meta = member.Meta
			updated = true
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

func (g *Gossip) observeLocked(nodeID NodeID, now time.Time) {
	detector := g.detectors[nodeID]
	if detector == nil {
		detector = newPhiAccrual(50)
		g.detectors[nodeID] = detector
	}
	detector.Observe(now)
}

func (g *Gossip) snapshotMessageLocked() Message {
	members := g.membersSliceLocked()
	states := make([]StateDigest, 0, len(g.states))
	for id, state := range g.states {
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

func (g *Gossip) membersSliceLocked() []Member {
	members := make([]Member, 0, len(g.members))
	for _, member := range g.members {
		members = append(members, member)
	}
	return members
}

func (g *Gossip) bumpEpochLocked() {
	g.epoch++
}

func (g *Gossip) withProbeTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if g.cfg.ProbeTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, g.cfg.ProbeTimeout)
}

func newPhiAccrual(maxSize int) *phiAccrual {
	if maxSize <= 0 {
		maxSize = 1
	}
	return &phiAccrual{maxSize: maxSize}
}

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
