// Package gossip provides a minimal membership and liveness gossip layer.
package gossip

import (
	"context"
	"math"
	"math/rand/v2"
	"sync"
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

		mu        sync.Mutex
		members   map[NodeID]Member
		states    map[NodeID]State
		detectors map[NodeID]*phiAccrual
		epoch     uint64

		stop    chan struct{}
		stopped chan struct{}
	}
)

const (
	StatusUnknown Status = iota
	StatusAlive
	StatusSuspect
	StatusDown
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

func New(cfg Config, transport Transport, clock Clock) *Gossip {
	if clock == nil {
		clock = realClock{}
	}

	g := &Gossip{
		cfg:       cfg,
		transport: transport,
		clock:     clock,
		rng:       rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano())+1)),
		members:   make(map[NodeID]Member),
		states:    make(map[NodeID]State),
		detectors: make(map[NodeID]*phiAccrual),
		stop:      make(chan struct{}),
		stopped:   make(chan struct{}),
	}

	if cfg.NodeID != "" {
		g.members[cfg.NodeID] = Member{ID: cfg.NodeID, Addr: cfg.BindAddr}
		g.states[cfg.NodeID] = State{Status: StatusAlive, LastUpdate: g.clock.Now()}
		g.detectors[cfg.NodeID] = newPhiAccrual(50)
	}

	for _, seed := range cfg.Seeds {
		g.members[seed.ID] = seed
		g.states[seed.ID] = State{Status: StatusUnknown}
		g.detectors[seed.ID] = newPhiAccrual(50)
	}

	return g
}

func (g *Gossip) Start(ctx context.Context) error {
	if g.cfg.PingInterval <= 0 {
		return nil
	}

	ticker := time.NewTicker(g.cfg.PingInterval)
	go func() {
		defer close(g.stopped)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-g.stop:
				return
			case <-ticker.C:
				now := g.clock.Now()
				g.gossipOnce(ctx, now)
				g.updateSuspicion(now)
			}
		}
	}()

	return nil
}

func (g *Gossip) Stop() {
	select {
	case <-g.stopped:
		return
	default:
		close(g.stop)
		<-g.stopped
	}
}

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

func (g *Gossip) Snapshot() map[NodeID]State {
	g.mu.Lock()
	defer g.mu.Unlock()

	result := make(map[NodeID]State, len(g.states))
	for id, state := range g.states {
		result[id] = state
	}
	return result
}

func (g *Gossip) Members() []Member {
	g.mu.Lock()
	defer g.mu.Unlock()

	result := make([]Member, 0, len(g.members))
	for _, member := range g.members {
		result = append(result, member)
	}
	return result
}

func (g *Gossip) HandleJoin(req *JoinRequest) *JoinResponse {
	if req == nil {
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

func (g *Gossip) HandlePushPull(msg *Message) *Message {
	now := g.clock.Now()
	g.mu.Lock()
	g.applyMessageLocked(now, msg)
	response := g.snapshotMessageLocked()
	g.mu.Unlock()
	return &response
}

func (g *Gossip) Join(ctx context.Context, member Member) (*JoinResponse, error) {
	if g.transport == nil {
		return nil, nil
	}
	ctx, cancel := g.withProbeTimeout(ctx)
	defer cancel()
	return g.transport.Join(ctx, member.Addr, &JoinRequest{Member: member, Seeds: g.cfg.Seeds})
}

func (g *Gossip) gossipOnce(ctx context.Context, now time.Time) {
	peer := g.pickPeer()
	if peer == nil || g.transport == nil {
		return
	}

	g.mu.Lock()
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
		if phi > state.Phi {
			state.Phi = phi
		}
		if state.Status == StatusAlive && phi >= g.cfg.PhiThreshold {
			state.Status = StatusSuspect
		}
		if g.cfg.MaxUpdateAge > 0 && now.Sub(state.LastUpdate) > g.cfg.MaxUpdateAge {
			state.Status = StatusDown
		}
		g.states[id] = state
	}
}

func (g *Gossip) addMemberLocked(member Member) {
	if member.ID == "" {
		return
	}
	if _, ok := g.members[member.ID]; ok {
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
		current := g.states[digest.NodeID]
		if digest.LastUpdate.After(current.LastUpdate) || current.LastUpdate.IsZero() {
			current.Status = digest.Status
			current.Phi = digest.Phi
			current.LastUpdate = digest.LastUpdate
			g.states[digest.NodeID] = current
			g.observeLocked(digest.NodeID, now)
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
