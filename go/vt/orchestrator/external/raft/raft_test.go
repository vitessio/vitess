package raft

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
)

// MockFSM is an implementation of the FSM interface, and just stores
// the logs sequentially.
type MockFSM struct {
	sync.Mutex
	logs [][]byte
}

type MockSnapshot struct {
	logs     [][]byte
	maxIndex int
}

func (m *MockFSM) Apply(log *Log) interface{} {
	m.Lock()
	defer m.Unlock()
	m.logs = append(m.logs, log.Data)
	return len(m.logs)
}

func (m *MockFSM) Snapshot() (FSMSnapshot, error) {
	m.Lock()
	defer m.Unlock()
	return &MockSnapshot{m.logs, len(m.logs)}, nil
}

func (m *MockFSM) Restore(inp io.ReadCloser) error {
	m.Lock()
	defer m.Unlock()
	defer inp.Close()
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(inp, &hd)

	m.logs = nil
	return dec.Decode(&m.logs)
}

func (m *MockSnapshot) Persist(sink SnapshotSink) error {
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(sink, &hd)
	if err := enc.Encode(m.logs[:m.maxIndex]); err != nil {
		sink.Cancel()
		return err
	}
	sink.Close()
	return nil
}

func (m *MockSnapshot) Release() {
}

// Return configurations optimized for in-memory
func inmemConfig(t *testing.T) *Config {
	conf := DefaultConfig()
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	conf.Logger = newTestLogger(t)
	return conf
}

// This can be used as the destination for a logger and it'll
// map them into calls to testing.T.Log, so that you only see
// the logging for failed tests.
type testLoggerAdapter struct {
	t      *testing.T
	prefix string
}

func (a *testLoggerAdapter) Write(d []byte) (int, error) {
	if d[len(d)-1] == '\n' {
		d = d[:len(d)-1]
	}
	if a.prefix != "" {
		l := a.prefix + ": " + string(d)
		a.t.Log(l)
		return len(l), nil
	}

	a.t.Log(string(d))
	return len(d), nil
}

func newTestLogger(t *testing.T) *log.Logger {
	return log.New(&testLoggerAdapter{t: t}, "", log.Lmicroseconds)
}

func newTestLoggerWithPrefix(t *testing.T, prefix string) *log.Logger {
	return log.New(&testLoggerAdapter{t: t, prefix: prefix}, "", log.Lmicroseconds)
}

type cluster struct {
	dirs             []string
	stores           []*InmemStore
	fsms             []*MockFSM
	snaps            []*FileSnapshotStore
	trans            []LoopbackTransport
	rafts            []*Raft
	t                *testing.T
	observationCh    chan Observation
	conf             *Config
	propagateTimeout time.Duration
	longstopTimeout  time.Duration
	logger           *log.Logger
	startTime        time.Time

	failedLock sync.Mutex
	failedCh   chan struct{}
	failed     bool
}

func (c *cluster) Merge(other *cluster) {
	c.dirs = append(c.dirs, other.dirs...)
	c.stores = append(c.stores, other.stores...)
	c.fsms = append(c.fsms, other.fsms...)
	c.snaps = append(c.snaps, other.snaps...)
	c.trans = append(c.trans, other.trans...)
	c.rafts = append(c.rafts, other.rafts...)
}

// notifyFailed will close the failed channel which can signal the goroutine
// running the test that another goroutine has detected a failure in order to
// terminate the test.
func (c *cluster) notifyFailed() {
	c.failedLock.Lock()
	defer c.failedLock.Unlock()
	if !c.failed {
		c.failed = true
		close(c.failedCh)
	}
}

// Failf provides a logging function that fails the tests, prints the output
// with microseconds, and does not mysteriously eat the string. This can be
// safely called from goroutines but won't immediately halt the test. The
// failedCh will be closed to allow blocking functions in the main thread to
// detect the failure and react. Note that you should arrange for the main
// thread to block until all goroutines have completed in order to reliably
// fail tests using this function.
func (c *cluster) Failf(format string, args ...interface{}) {
	c.logger.Printf(format, args...)
	c.t.Fail()
	c.notifyFailed()
}

// FailNowf provides a logging function that fails the tests, prints the output
// with microseconds, and does not mysteriously eat the string. FailNowf must be
// called from the goroutine running the test or benchmark function, not from
// other goroutines created during the test. Calling FailNowf does not stop
// those other goroutines.
func (c *cluster) FailNowf(format string, args ...interface{}) {
	c.logger.Printf(format, args...)
	c.t.FailNow()
}

// Close shuts down the cluster and cleans up.
func (c *cluster) Close() {
	var futures []Future
	for _, r := range c.rafts {
		futures = append(futures, r.Shutdown())
	}

	// Wait for shutdown
	limit := time.AfterFunc(c.longstopTimeout, func() {
		// We can't FailNowf here, and c.Failf won't do anything if we
		// hang, so panic.
		panic("timed out waiting for shutdown")
	})
	defer limit.Stop()

	for _, f := range futures {
		if err := f.Error(); err != nil {
			c.FailNowf("[ERR] shutdown future err: %v", err)
		}
	}

	for _, d := range c.dirs {
		os.RemoveAll(d)
	}
}

// WaitEventChan returns a channel which will signal if an observation is made
// or a timeout occurs. It is possible to set a filter to look for specific
// observations. Setting timeout to 0 means that it will wait forever until a
// non-filtered observation is made.
func (c *cluster) WaitEventChan(filter FilterFn, timeout time.Duration) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		var timeoutCh <-chan time.Time
		if timeout > 0 {
			timeoutCh = time.After(timeout)
		}
		for {
			select {
			case <-timeoutCh:
				return

			case o, ok := <-c.observationCh:
				if !ok || filter == nil || filter(&o) {
					return
				}
			}
		}
	}()
	return ch
}

// WaitEvent waits until an observation is made, a timeout occurs, or a test
// failure is signaled. It is possible to set a filter to look for specific
// observations. Setting timeout to 0 means that it will wait forever until a
// non-filtered observation is made or a test failure is signaled.
func (c *cluster) WaitEvent(filter FilterFn, timeout time.Duration) {
	select {
	case <-c.failedCh:
		c.t.FailNow()

	case <-c.WaitEventChan(filter, timeout):
	}
}

// WaitForReplication blocks until every FSM in the cluster has the given
// length, or the long sanity check timeout expires.
func (c *cluster) WaitForReplication(fsmLength int) {
	limitCh := time.After(c.longstopTimeout)

CHECK:
	for {
		ch := c.WaitEventChan(nil, c.conf.CommitTimeout)
		select {
		case <-c.failedCh:
			c.t.FailNow()

		case <-limitCh:
			c.FailNowf("[ERR] Timeout waiting for replication")

		case <-ch:
			for _, fsm := range c.fsms {
				fsm.Lock()
				num := len(fsm.logs)
				fsm.Unlock()
				if num != fsmLength {
					continue CHECK
				}
			}
			return
		}
	}
}

// pollState takes a snapshot of the state of the cluster. This might not be
// stable, so use GetInState() to apply some additional checks when waiting
// for the cluster to achieve a particular state.
func (c *cluster) pollState(s RaftState) ([]*Raft, uint64) {
	var highestTerm uint64
	in := make([]*Raft, 0, 1)
	for _, r := range c.rafts {
		if r.State() == s {
			in = append(in, r)
		}
		term := r.getCurrentTerm()
		if term > highestTerm {
			highestTerm = term
		}
	}
	return in, highestTerm
}

// GetInState polls the state of the cluster and attempts to identify when it has
// settled into the given state.
func (c *cluster) GetInState(s RaftState) []*Raft {
	c.logger.Printf("[INFO] Starting stability test for raft state: %+v", s)
	limitCh := time.After(c.longstopTimeout)

	// An election should complete after 2 * max(HeartbeatTimeout, ElectionTimeout)
	// because of the randomised timer expiring in 1 x interval ... 2 x interval.
	// We add a bit for propagation delay. If the election fails (e.g. because
	// two elections start at once), we will have got something through our
	// observer channel indicating a different state (i.e. one of the nodes
	// will have moved to candidate state) which will reset the timer.
	//
	// Because of an implementation peculiarity, it can actually be 3 x timeout.
	timeout := c.conf.HeartbeatTimeout
	if timeout < c.conf.ElectionTimeout {
		timeout = c.conf.ElectionTimeout
	}
	timeout = 2*timeout + c.conf.CommitTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// Wait until we have a stable instate slice. Each time we see an
	// observation a state has changed, recheck it and if it has changed,
	// restart the timer.
	var pollStartTime = time.Now()
	for {
		inState, highestTerm := c.pollState(s)
		inStateTime := time.Now()

		// Sometimes this routine is called very early on before the
		// rafts have started up. We then timeout even though no one has
		// even started an election. So if the highest term in use is
		// zero, we know there are no raft processes that have yet issued
		// a RequestVote, and we set a long time out. This is fixed when
		// we hear the first RequestVote, at which point we reset the
		// timer.
		if highestTerm == 0 {
			timer.Reset(c.longstopTimeout)
		} else {
			timer.Reset(timeout)
		}

		// Filter will wake up whenever we observe a RequestVote.
		filter := func(ob *Observation) bool {
			switch ob.Data.(type) {
			case RaftState:
				return true
			case RequestVoteRequest:
				return true
			default:
				return false
			}
		}

		select {
		case <-c.failedCh:
			c.t.FailNow()

		case <-limitCh:
			c.FailNowf("[ERR] Timeout waiting for stable %s state", s)

		case <-c.WaitEventChan(filter, 0):
			c.logger.Printf("[DEBUG] Resetting stability timeout")

		case t, ok := <-timer.C:
			if !ok {
				c.FailNowf("[ERR] Timer channel errored")
			}
			c.logger.Printf("[INFO] Stable state for %s reached at %s (%d nodes), %s from start of poll, %s from cluster start. Timeout at %s, %s after stability",
				s, inStateTime, len(inState), inStateTime.Sub(pollStartTime), inStateTime.Sub(c.startTime), t, t.Sub(inStateTime))
			return inState
		}
	}
}

// Leader waits for the cluster to elect a leader and stay in a stable state.
func (c *cluster) Leader() *Raft {
	leaders := c.GetInState(Leader)
	if len(leaders) != 1 {
		c.FailNowf("[ERR] expected one leader: %v", leaders)
	}
	return leaders[0]
}

// Followers waits for the cluster to have N-1 followers and stay in a stable
// state.
func (c *cluster) Followers() []*Raft {
	expFollowers := len(c.rafts) - 1
	followers := c.GetInState(Follower)
	if len(followers) != expFollowers {
		c.FailNowf("[ERR] timeout waiting for %d followers (followers are %v)", expFollowers, followers)
	}
	return followers
}

// FullyConnect connects all the transports together.
func (c *cluster) FullyConnect() {
	c.logger.Printf("[DEBUG] Fully Connecting")
	for i, t1 := range c.trans {
		for j, t2 := range c.trans {
			if i != j {
				t1.Connect(t2.LocalAddr(), t2)
				t2.Connect(t1.LocalAddr(), t1)
			}
		}
	}
}

// Disconnect disconnects all transports from the given address.
func (c *cluster) Disconnect(a string) {
	c.logger.Printf("[DEBUG] Disconnecting %v", a)
	for _, t := range c.trans {
		if t.LocalAddr() == a {
			t.DisconnectAll()
		} else {
			t.Disconnect(a)
		}
	}
}

// IndexOf returns the index of the given raft instance.
func (c *cluster) IndexOf(r *Raft) int {
	for i, n := range c.rafts {
		if n == r {
			return i
		}
	}
	return -1
}

// EnsureLeader checks that ALL the nodes think the leader is the given expected
// leader.
func (c *cluster) EnsureLeader(t *testing.T, expect string) {
	// We assume c.Leader() has been called already; now check all the rafts
	// think the leader is correct
	fail := false
	for _, r := range c.rafts {
		leader := r.Leader()
		if leader != expect {
			if leader == "" {
				leader = "[none]"
			}
			if expect == "" {
				c.logger.Printf("[ERR] Peer %s sees leader %v expected [none]", r, leader)
			} else {
				c.logger.Printf("[ERR] Peer %s sees leader %v expected %v", r, leader, expect)
			}
			fail = true
		}
	}
	if fail {
		c.FailNowf("[ERR] At least one peer has the wrong notion of leader")
	}
}

// EnsureSame makes sure all the FSMs have the same contents.
func (c *cluster) EnsureSame(t *testing.T) {
	limit := time.Now().Add(c.longstopTimeout)
	first := c.fsms[0]

CHECK:
	first.Lock()
	for i, fsm := range c.fsms {
		if i == 0 {
			continue
		}
		fsm.Lock()

		if len(first.logs) != len(fsm.logs) {
			fsm.Unlock()
			if time.Now().After(limit) {
				c.FailNowf("[ERR] FSM log length mismatch: %d %d",
					len(first.logs), len(fsm.logs))
			} else {
				goto WAIT
			}
		}

		for idx := 0; idx < len(first.logs); idx++ {
			if bytes.Compare(first.logs[idx], fsm.logs[idx]) != 0 {
				fsm.Unlock()
				if time.Now().After(limit) {
					c.FailNowf("[ERR] FSM log mismatch at index %d", idx)
				} else {
					goto WAIT
				}
			}
		}
		fsm.Unlock()
	}

	first.Unlock()
	return

WAIT:
	first.Unlock()
	c.WaitEvent(nil, c.conf.CommitTimeout)
	goto CHECK
}

// raftToPeerSet returns the set of peers as a map.
func raftToPeerSet(r *Raft) map[string]struct{} {
	peers := make(map[string]struct{})
	peers[r.localAddr] = struct{}{}

	raftPeers, _ := r.peerStore.Peers()
	for _, p := range raftPeers {
		peers[p] = struct{}{}
	}
	return peers
}

// EnsureSamePeers makes sure all the rafts have the same set of peers.
func (c *cluster) EnsureSamePeers(t *testing.T) {
	limit := time.Now().Add(c.longstopTimeout)
	peerSet := raftToPeerSet(c.rafts[0])

CHECK:
	for i, raft := range c.rafts {
		if i == 0 {
			continue
		}

		otherSet := raftToPeerSet(raft)
		if !reflect.DeepEqual(peerSet, otherSet) {
			if time.Now().After(limit) {
				c.FailNowf("[ERR] peer mismatch: %v %v", peerSet, otherSet)
			} else {
				goto WAIT
			}
		}
	}
	return

WAIT:
	c.WaitEvent(nil, c.conf.CommitTimeout)
	goto CHECK
}

// makeCluster will return a cluster with the given config and number of peers.
// If addPeers is true, they will be added into the peer store before starting,
// otherwise their transports will be wired up but they won't yet have configured
// each other.
func makeCluster(n int, addPeers bool, t *testing.T, conf *Config) *cluster {
	if conf == nil {
		conf = inmemConfig(t)
	}

	c := &cluster{
		observationCh: make(chan Observation, 1024),
		conf:          conf,
		// Propagation takes a maximum of 2 heartbeat timeouts (time to
		// get a new heartbeat that would cause a commit) plus a bit.
		propagateTimeout: conf.HeartbeatTimeout*2 + conf.CommitTimeout,
		longstopTimeout:  5 * time.Second,
		logger:           newTestLoggerWithPrefix(t, "cluster"),
		failedCh:         make(chan struct{}),
	}
	c.t = t
	peers := make([]string, 0, n)

	// Setup the stores and transports
	for i := 0; i < n; i++ {
		dir, err := ioutil.TempDir("", "raft")
		if err != nil {
			c.FailNowf("[ERR] err: %v ", err)
		}

		store := NewInmemStore()
		c.dirs = append(c.dirs, dir)
		c.stores = append(c.stores, store)
		c.fsms = append(c.fsms, &MockFSM{})

		dir2, snap := FileSnapTest(t)
		c.dirs = append(c.dirs, dir2)
		c.snaps = append(c.snaps, snap)

		addr, trans := NewInmemTransport("")
		c.trans = append(c.trans, trans)
		peers = append(peers, addr)
	}

	// Wire the transports together
	c.FullyConnect()

	// Create all the rafts
	c.startTime = time.Now()
	for i := 0; i < n; i++ {
		if n == 1 {
			conf.EnableSingleNode = true
		}

		logs := c.stores[i]
		store := c.stores[i]
		snap := c.snaps[i]
		trans := c.trans[i]

		peerStore := &StaticPeers{}
		if addPeers {
			peerStore.StaticPeers = peers
		}
		peerConf := conf
		peerConf.Logger = newTestLoggerWithPrefix(t, peers[i])

		raft, err := NewRaft(peerConf, c.fsms[i], logs, store, snap, peerStore, trans)
		if err != nil {
			c.FailNowf("[ERR] NewRaft failed: %v", err)
		}

		raft.RegisterObserver(NewObserver(c.observationCh, false, nil))
		if err != nil {
			c.FailNowf("[ERR] RegisterObserver failed: %v", err)
		}
		c.rafts = append(c.rafts, raft)
	}

	return c
}

// See makeCluster. This adds the peers initially to the peer store.
func MakeCluster(n int, t *testing.T, conf *Config) *cluster {
	return makeCluster(n, true, t, conf)
}

// See makeCluster. This doesn't add the peers initially to the peer store.
func MakeClusterNoPeers(n int, t *testing.T, conf *Config) *cluster {
	return makeCluster(n, false, t, conf)
}

func TestRaft_StartStop(t *testing.T) {
	c := MakeCluster(1, t, nil)
	c.Close()
}

func TestRaft_AfterShutdown(t *testing.T) {
	c := MakeCluster(1, t, nil)
	c.Close()
	raft := c.rafts[0]

	// Everything should fail now
	if f := raft.Apply(nil, 0); f.Error() != ErrRaftShutdown {
		c.FailNowf("[ERR] should be shutdown: %v", f.Error())
	}
	if f := raft.AddPeer(NewInmemAddr()); f.Error() != ErrRaftShutdown {
		c.FailNowf("[ERR] should be shutdown: %v", f.Error())
	}
	if f := raft.RemovePeer(NewInmemAddr()); f.Error() != ErrRaftShutdown {
		c.FailNowf("[ERR] should be shutdown: %v", f.Error())
	}
	if f := raft.Snapshot(); f.Error() != ErrRaftShutdown {
		c.FailNowf("[ERR] should be shutdown: %v", f.Error())
	}

	// Should be idempotent
	if f := raft.Shutdown(); f.Error() != nil {
		c.FailNowf("[ERR] shutdown should be idempotent")
	}

}

func TestRaft_SingleNode(t *testing.T) {
	conf := inmemConfig(t)
	c := MakeCluster(1, t, conf)
	defer c.Close()
	raft := c.rafts[0]

	// Watch leaderCh for change
	select {
	case v := <-raft.LeaderCh():
		if !v {
			c.FailNowf("[ERR] should become leader")
		}
	case <-time.After(conf.HeartbeatTimeout * 3):
		c.FailNowf("[ERR] timeout becoming leader")
	}

	// Should be leader
	if s := raft.State(); s != Leader {
		c.FailNowf("[ERR] expected leader: %v", s)
	}

	// Should be able to apply
	future := raft.Apply([]byte("test"), c.conf.HeartbeatTimeout)
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Check the response
	if future.Response().(int) != 1 {
		c.FailNowf("[ERR] bad response: %v", future.Response())
	}

	// Check the index
	if idx := future.Index(); idx == 0 {
		c.FailNowf("[ERR] bad index: %d", idx)
	}

	// Check that it is applied to the FSM
	if len(c.fsms[0].logs) != 1 {
		c.FailNowf("[ERR] did not apply to FSM!")
	}
}

func TestRaft_TripleNode(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Should be one leader
	c.Followers()
	leader := c.Leader()
	c.EnsureLeader(t, leader.localAddr)

	// Should be able to apply
	future := leader.Apply([]byte("test"), c.conf.CommitTimeout)
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}
	c.WaitForReplication(1)
}

func TestRaft_LeaderFail(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Should be one leader
	c.Followers()
	leader := c.Leader()

	// Should be able to apply
	future := leader.Apply([]byte("test"), c.conf.CommitTimeout)
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}
	c.WaitForReplication(1)

	// Disconnect the leader now
	t.Logf("[INFO] Disconnecting %v", leader)
	leaderTerm := leader.getCurrentTerm()
	c.Disconnect(leader.localAddr)

	// Wait for new leader
	limit := time.Now().Add(c.longstopTimeout)
	var newLead *Raft
	for time.Now().Before(limit) && newLead == nil {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		leaders := c.GetInState(Leader)
		if len(leaders) == 1 && leaders[0] != leader {
			newLead = leaders[0]
		}
	}
	if newLead == nil {
		c.FailNowf("[ERR] expected new leader")
	}

	// Ensure the term is greater
	if newLead.getCurrentTerm() <= leaderTerm {
		c.FailNowf("[ERR] expected newer term! %d %d (%v, %v)", newLead.getCurrentTerm(), leaderTerm, newLead, leader)
	}

	// Apply should work not work on old leader
	future1 := leader.Apply([]byte("fail"), c.conf.CommitTimeout)

	// Apply should work on newer leader
	future2 := newLead.Apply([]byte("apply"), c.conf.CommitTimeout)

	// Future2 should work
	if err := future2.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Reconnect the networks
	t.Logf("[INFO] Reconnecting %v", leader)
	c.FullyConnect()

	// Future1 should fail
	if err := future1.Error(); err != ErrLeadershipLost && err != ErrNotLeader {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Wait for log replication
	c.EnsureSame(t)

	// Check two entries are applied to the FSM
	for _, fsm := range c.fsms {
		fsm.Lock()
		if len(fsm.logs) != 2 {
			c.FailNowf("[ERR] did not apply both to FSM! %v", fsm.logs)
		}
		if bytes.Compare(fsm.logs[0], []byte("test")) != 0 {
			c.FailNowf("[ERR] first entry should be 'test'")
		}
		if bytes.Compare(fsm.logs[1], []byte("apply")) != 0 {
			c.FailNowf("[ERR] second entry should be 'apply'")
		}
		fsm.Unlock()
	}
}

func TestRaft_BehindFollower(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Disconnect one follower
	leader := c.Leader()
	followers := c.Followers()
	behind := followers[0]
	c.Disconnect(behind.localAddr)

	// Commit a lot of things
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	} else {
		t.Logf("[INFO] Finished apply without behind follower")
	}

	// Check that we have a non zero last contact
	if behind.LastContact().IsZero() {
		c.FailNowf("[ERR] expected previous contact")
	}

	// Reconnect the behind node
	c.FullyConnect()

	// Ensure all the logs are the same
	c.EnsureSame(t)

	// Ensure one leader
	leader = c.Leader()
	c.EnsureLeader(t, leader.localAddr)
}

func TestRaft_ApplyNonLeader(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Wait for a leader
	c.Leader()

	// Try to apply to them
	followers := c.GetInState(Follower)
	if len(followers) != 2 {
		c.FailNowf("[ERR] Expected 2 followers")
	}
	follower := followers[0]

	// Try to apply
	future := follower.Apply([]byte("test"), c.conf.CommitTimeout)
	if future.Error() != ErrNotLeader {
		c.FailNowf("[ERR] should not apply on follower")
	}

	// Should be cached
	if future.Error() != ErrNotLeader {
		c.FailNowf("[ERR] should not apply on follower")
	}
}

func TestRaft_ApplyConcurrent(t *testing.T) {
	// Make the cluster
	conf := inmemConfig(t)
	conf.HeartbeatTimeout = 2 * conf.HeartbeatTimeout
	conf.ElectionTimeout = 2 * conf.ElectionTimeout
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Wait for a leader
	leader := c.Leader()

	// Create a wait group
	const sz = 100
	var group sync.WaitGroup
	group.Add(sz)

	applyF := func(i int) {
		defer group.Done()
		future := leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
		if err := future.Error(); err != nil {
			c.Failf("[ERR] err: %v", err)
		}
	}

	// Concurrently apply
	for i := 0; i < sz; i++ {
		go applyF(i)
	}

	// Wait to finish
	doneCh := make(chan struct{})
	go func() {
		group.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(c.longstopTimeout):
		c.FailNowf("[ERR] timeout")
	}

	// If anything failed up to this point then bail now, rather than do a
	// confusing compare.
	if t.Failed() {
		c.FailNowf("[ERR] One or more of the apply operations failed")
	}

	// Check the FSMs
	c.EnsureSame(t)
}

func TestRaft_ApplyConcurrent_Timeout(t *testing.T) {
	// Make the cluster
	conf := inmemConfig(t)
	conf.CommitTimeout = 1 * time.Millisecond
	conf.HeartbeatTimeout = 2 * conf.HeartbeatTimeout
	conf.ElectionTimeout = 2 * conf.ElectionTimeout
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// Wait for a leader
	leader := c.Leader()

	// Enough enqueues should cause at least one timeout...
	var didTimeout int32
	for i := 0; (i < 5000) && (atomic.LoadInt32(&didTimeout) == 0); i++ {
		go func(i int) {
			future := leader.Apply([]byte(fmt.Sprintf("test%d", i)), time.Microsecond)
			if future.Error() == ErrEnqueueTimeout {
				atomic.StoreInt32(&didTimeout, 1)
			}
		}(i)

		// Give the leader loop some other things to do in order to
		// increase the odds of a timeout.
		if i%5 == 0 {
			leader.VerifyLeader()
		}
	}

	// Loop until we see a timeout, or give up.
	limit := time.Now().Add(c.longstopTimeout)
	for time.Now().Before(limit) {
		if atomic.LoadInt32(&didTimeout) != 0 {
			return
		}
		c.WaitEvent(nil, c.propagateTimeout)
	}
	c.FailNowf("[ERR] Timeout waiting to detect apply timeouts")
}

func TestRaft_JoinNode(t *testing.T) {
	// Make a cluster
	c := MakeCluster(2, t, nil)
	defer c.Close()

	// Apply a log to this cluster to ensure it is 'newer'
	var future Future
	leader := c.Leader()
	future = leader.Apply([]byte("first"), 0)
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	} else {
		t.Logf("[INFO] Applied log")
	}

	// Make a new cluster of 1
	c1 := MakeCluster(1, t, nil)

	// Merge clusters
	c.Merge(c1)
	c.FullyConnect()

	// Wait until we have 2 leaders
	limit := time.Now().Add(c.longstopTimeout)
	var leaders []*Raft
	for time.Now().Before(limit) && len(leaders) != 2 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		leaders = c.GetInState(Leader)
	}
	if len(leaders) != 2 {
		c.FailNowf("[ERR] expected two leader: %v", leaders)
	}

	// Join the new node in
	future = leader.AddPeer(c1.rafts[0].localAddr)
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Wait until we have 2 followers
	limit = time.Now().Add(c.longstopTimeout)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 2 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		c.FailNowf("[ERR] expected two followers: %v", followers)
	}

	// Check the FSMs
	c.EnsureSame(t)

	// Check the peers
	c.EnsureSamePeers(t)

	// Ensure one leader
	leader = c.Leader()
	c.EnsureLeader(t, leader.localAddr)
}

func TestRaft_RemoveFollower(t *testing.T) {
	// Make a cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Wait until we have 2 followers
	limit := time.Now().Add(c.longstopTimeout)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 2 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		c.FailNowf("[ERR] expected two followers: %v", followers)
	}

	// Remove a follower
	follower := followers[0]
	future := leader.RemovePeer(follower.localAddr)
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Wait a while
	time.Sleep(c.propagateTimeout)

	// Other nodes should have fewer peers
	if peers, _ := leader.peerStore.Peers(); len(peers) != 2 {
		c.FailNowf("[ERR] too many peers")
	}
	if peers, _ := followers[1].peerStore.Peers(); len(peers) != 2 {
		c.FailNowf("[ERR] too many peers")
	}
}

func TestRaft_RemoveLeader(t *testing.T) {
	// Make a cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Wait until we have 2 followers
	limit := time.Now().Add(c.longstopTimeout)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 2 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		c.FailNowf("[ERR] expected two followers: %v", followers)
	}

	// Remove the leader
	leader.RemovePeer(leader.localAddr)

	// Wait a while
	time.Sleep(c.propagateTimeout)

	// Should have a new leader
	newLeader := c.Leader()

	// Wait a bit for log application
	time.Sleep(c.propagateTimeout)

	// Other nodes should have fewer peers
	if peers, _ := newLeader.peerStore.Peers(); len(peers) != 2 {
		c.FailNowf("[ERR] too many peers")
	}

	// Old leader should be shutdown
	if leader.State() != Shutdown {
		c.FailNowf("[ERR] leader should be shutdown")
	}

	// Old leader should have no peers
	if peers, _ := leader.peerStore.Peers(); len(peers) != 1 {
		c.FailNowf("[ERR] leader should have no peers")
	}
}

func TestRaft_RemoveLeader_NoShutdown(t *testing.T) {
	// Make a cluster
	conf := inmemConfig(t)
	conf.ShutdownOnRemove = false
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Get the leader
	c.Followers()
	leader := c.Leader()

	// Remove the leader
	var removeFuture Future
	for i := byte(0); i < 100; i++ {
		future := leader.Apply([]byte{i}, 0)
		if i == 80 {
			removeFuture = leader.RemovePeer(leader.localAddr)
		}
		if i > 80 {
			if err := future.Error(); err == nil || err != ErrNotLeader {
				c.FailNowf("[ERR] err: %v, future entries should fail", err)
			}
		}
	}

	if err := removeFuture.Error(); err != nil {
		c.FailNowf("[ERR] RemovePeer failed with error %v", err)
	}

	// Wait a while
	time.Sleep(c.propagateTimeout)

	// Should have a new leader
	newLeader := c.Leader()

	// Wait a bit for log application
	time.Sleep(c.propagateTimeout)

	// Other nodes should have fewer peers
	if peers, _ := newLeader.peerStore.Peers(); len(peers) != 2 {
		c.FailNowf("[ERR] too many peers")
	}

	// Old leader should be a follower
	if leader.State() != Follower {
		c.FailNowf("[ERR] leader should be shutdown")
	}

	// Old leader should have no peers
	if peers, _ := leader.peerStore.Peers(); len(peers) != 1 {
		c.FailNowf("[ERR] leader should have no peers")
	}

	// Other nodes should have the same state
	c.EnsureSame(t)
}

func TestRaft_RemoveLeader_SplitCluster(t *testing.T) {
	// Enable operation after a remove
	conf := inmemConfig(t)
	conf.EnableSingleNode = true
	conf.ShutdownOnRemove = false
	conf.DisableBootstrapAfterElect = false

	// Make a cluster
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Get the leader
	c.Followers()
	leader := c.Leader()

	// Remove the leader
	leader.RemovePeer(leader.localAddr)

	// Wait until we have 2 leaders
	limit := time.Now().Add(c.longstopTimeout)
	var leaders []*Raft
	for time.Now().Before(limit) && len(leaders) != 2 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		leaders = c.GetInState(Leader)
	}
	if len(leaders) != 2 {
		c.FailNowf("[ERR] expected two leader: %v", leaders)
	}

	// Old leader should have no peers
	if len(leader.peers) != 0 {
		c.FailNowf("[ERR] leader should have no peers")
	}
}

func TestRaft_AddKnownPeer(t *testing.T) {
	// Make a cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()
	followers := c.GetInState(Follower)

	// Add a follower
	future := leader.AddPeer(followers[0].localAddr)

	// Should be already added
	if err := future.Error(); err != ErrKnownPeer {
		c.FailNowf("[ERR] err: %v", err)
	}
}

func TestRaft_RemoveUnknownPeer(t *testing.T) {
	// Make a cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Remove unknown
	future := leader.RemovePeer(NewInmemAddr())

	// Should be already added
	if err := future.Error(); err != ErrUnknownPeer {
		c.FailNowf("[ERR] err: %v", err)
	}
}

func TestRaft_SnapshotRestore(t *testing.T) {
	// Make the cluster
	conf := inmemConfig(t)
	conf.TrailingLogs = 10
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// Commit a lot of things
	leader := c.Leader()
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Take a snapshot
	snapFuture := leader.Snapshot()
	if err := snapFuture.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Check for snapshot
	if snaps, _ := leader.snapshots.List(); len(snaps) != 1 {
		c.FailNowf("[ERR] should have a snapshot")
	}

	// Logs should be trimmed
	if idx, _ := leader.logs.FirstIndex(); idx != 92 {
		c.FailNowf("[ERR] should trim logs to 92: %d", idx)
	}

	// Shutdown
	shutdown := leader.Shutdown()
	if err := shutdown.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Restart the Raft
	r := leader
	// Can't just reuse the old transport as it will be closed
	_, trans2 := NewInmemTransport(r.trans.LocalAddr())
	r, err := NewRaft(r.conf, r.fsm, r.logs, r.stable,
		r.snapshots, r.peerStore, trans2)
	if err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}
	c.rafts[0] = r

	// We should have restored from the snapshot!
	if last := r.getLastApplied(); last != 101 {
		c.FailNowf("[ERR] bad last: %v", last)
	}
}

func TestRaft_SnapshotRestore_PeerChange(t *testing.T) {
	// Make the cluster
	conf := inmemConfig(t)
	conf.TrailingLogs = 10
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Commit a lot of things
	leader := c.Leader()
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Take a snapshot
	snapFuture := leader.Snapshot()
	if err := snapFuture.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Shutdown
	shutdown := leader.Shutdown()
	if err := shutdown.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Make a separate cluster
	c2 := MakeClusterNoPeers(2, t, conf)
	defer c2.Close()

	// Kill the old cluster
	for _, sec := range c.rafts {
		if sec != leader {
			sec.Shutdown()
		}
	}

	// Change the peer addresses
	peers := []string{leader.trans.LocalAddr()}
	for _, sec := range c2.rafts {
		peers = append(peers, sec.trans.LocalAddr())
	}

	// Restart the Raft with new peers
	r := leader
	peerStore := &StaticPeers{StaticPeers: peers}
	// Can't just reuse the old transport as it will be closed
	_, trans2 := NewInmemTransport(r.trans.LocalAddr())
	r, err := NewRaft(r.conf, r.fsm, r.logs, r.stable,
		r.snapshots, peerStore, trans2)
	if err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}
	c.rafts[0] = r
	c2.rafts = append(c2.rafts, r)
	c2.trans = append(c2.trans, r.trans.(*InmemTransport))
	c2.fsms = append(c2.fsms, r.fsm.(*MockFSM))
	c2.FullyConnect()

	// Wait a while
	time.Sleep(c.propagateTimeout)

	// Ensure we elect a leader, and that we replicate
	// to our new followers
	c2.EnsureSame(t)

	// We should have restored from the snapshot!
	if last := r.getLastApplied(); last != 102 {
		c.FailNowf("[ERR] bad last: %v", last)
	}
}

func TestRaft_AutoSnapshot(t *testing.T) {
	// Make the cluster
	conf := inmemConfig(t)
	conf.SnapshotInterval = conf.CommitTimeout * 2
	conf.SnapshotThreshold = 50
	conf.TrailingLogs = 10
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// Commit a lot of things
	leader := c.Leader()
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Wait for a snapshot to happen
	time.Sleep(c.propagateTimeout)

	// Check for snapshot
	if snaps, _ := leader.snapshots.List(); len(snaps) == 0 {
		c.FailNowf("[ERR] should have a snapshot")
	}
}

func TestRaft_ManualSnapshot(t *testing.T) {
	// Make the cluster
	conf := inmemConfig(t)
	conf.SnapshotThreshold = 50
	conf.TrailingLogs = 10
	c := MakeCluster(1, t, conf)
	defer c.Close()

	leader := c.Leader()
	// with nothing commited, asking for a snapshot should return an error
	ssErr := leader.Snapshot().Error()
	if ssErr != ErrNothingNewToSnapshot {
		t.Errorf("Attempt to manualy create snapshot should of errored because there's nothing to do: %v", ssErr)
	}
	// commit some things
	var future Future
	for i := 0; i < 10; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test %d", i)), 0)
	}
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] Error Apply new log entries: %v", err)
	}
	// now we should be able to ask for a snapshot without getting an error
	ssErr = leader.Snapshot().Error()
	if ssErr != nil {
		t.Errorf("Request for Snapshot failed: %v", ssErr)
	}
}

func TestRaft_SendSnapshotFollower(t *testing.T) {
	// Make the cluster
	conf := inmemConfig(t)
	conf.TrailingLogs = 10
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Disconnect one follower
	followers := c.Followers()
	leader := c.Leader()
	behind := followers[0]
	c.Disconnect(behind.localAddr)

	// Commit a lot of things
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	} else {
		t.Logf("[INFO] Finished apply without behind follower")
	}

	// Snapshot, this will truncate logs!
	for _, r := range c.rafts {
		future = r.Snapshot()
		// the disconnected node will have nothing to snapshot, so that's expected
		if err := future.Error(); err != nil && err != ErrNothingNewToSnapshot {
			c.FailNowf("[ERR] err: %v", err)
		}
	}

	// Reconnect the behind node
	c.FullyConnect()

	// Ensure all the logs are the same
	c.EnsureSame(t)
}

func TestRaft_ReJoinFollower(t *testing.T) {
	// Enable operation after a remove
	conf := inmemConfig(t)
	conf.ShutdownOnRemove = false

	// Make a cluster
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Wait until we have 2 followers
	limit := time.Now().Add(c.longstopTimeout)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 2 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		c.FailNowf("[ERR] expected two followers: %v", followers)
	}

	// Remove a follower
	follower := followers[0]
	future := leader.RemovePeer(follower.localAddr)
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Wait a while
	time.Sleep(c.propagateTimeout)

	// Other nodes should have fewer peers
	if peers, _ := leader.peerStore.Peers(); len(peers) != 2 {
		c.FailNowf("[ERR] too many peers: %v", peers)
	}
	if peers, _ := followers[1].peerStore.Peers(); len(peers) != 2 {
		c.FailNowf("[ERR] too many peers: %v", peers)
	}

	// Get the leader
	time.Sleep(c.propagateTimeout)
	leader = c.Leader()

	// Rejoin. The follower will have a higher term than the leader,
	// this will cause the leader to step down, and a new round of elections
	// to take place. We should eventually re-stabilize.
	future = leader.AddPeer(follower.localAddr)
	if err := future.Error(); err != nil && err != ErrLeadershipLost {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Wait a while
	time.Sleep(c.propagateTimeout)

	// Other nodes should have fewer peers
	if peers, _ := leader.peerStore.Peers(); len(peers) != 3 {
		c.FailNowf("[ERR] missing peers: %v", peers)
	}
	if peers, _ := followers[1].peerStore.Peers(); len(peers) != 3 {
		c.FailNowf("[ERR] missing peers: %v", peers)
	}

	// Should be a follower now
	if follower.State() != Follower {
		c.FailNowf("[ERR] bad state: %v", follower.State())
	}
}

func TestRaft_LeaderLeaseExpire(t *testing.T) {
	// Make a cluster
	conf := inmemConfig(t)
	c := MakeCluster(2, t, conf)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Wait until we have a followers
	limit := time.Now().Add(c.longstopTimeout)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 1 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 1 {
		c.FailNowf("[ERR] expected a followers: %v", followers)
	}

	// Disconnect the follower now
	follower := followers[0]
	t.Logf("[INFO] Disconnecting %v", follower)
	c.Disconnect(follower.localAddr)

	// Watch the leaderCh
	select {
	case v := <-leader.LeaderCh():
		if v {
			c.FailNowf("[ERR] should step down as leader")
		}
	case <-time.After(conf.LeaderLeaseTimeout * 2):
		c.FailNowf("[ERR] timeout stepping down as leader")
	}

	// Ensure the last contact of the leader is non-zero
	if leader.LastContact().IsZero() {
		c.FailNowf("[ERR] expected non-zero contact time")
	}

	// Should be no leaders
	if len(c.GetInState(Leader)) != 0 {
		c.FailNowf("[ERR] expected step down")
	}

	// Verify no further contact
	last := follower.LastContact()
	time.Sleep(c.propagateTimeout)

	// Check that last contact has not changed
	if last != follower.LastContact() {
		c.FailNowf("[ERR] unexpected further contact")
	}

	// Ensure both have cleared their leader
	if l := leader.Leader(); l != "" {
		c.FailNowf("[ERR] bad: %v", l)
	}
	if l := follower.Leader(); l != "" {
		c.FailNowf("[ERR] bad: %v", l)
	}
}

func TestRaft_Barrier(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Commit a lot of things
	for i := 0; i < 100; i++ {
		leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for a barrier complete
	barrier := leader.Barrier(0)

	// Wait for the barrier future to apply
	if err := barrier.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Ensure all the logs are the same
	c.EnsureSame(t)
	if len(c.fsms[0].logs) != 100 {
		c.FailNowf("[ERR] Bad log length")
	}
}

func TestRaft_VerifyLeader(t *testing.T) {
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Verify we are leader
	verify := leader.VerifyLeader()

	// Wait for the verify to apply
	if err := verify.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}
}

func TestRaft_VerifyLeader_Single(t *testing.T) {
	// Make the cluster
	c := MakeCluster(1, t, nil)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Verify we are leader
	verify := leader.VerifyLeader()

	// Wait for the verify to apply
	if err := verify.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}
}

func TestRaft_VerifyLeader_Fail(t *testing.T) {
	// Make a cluster
	conf := inmemConfig(t)
	c := MakeCluster(2, t, conf)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Wait until we have a followers
	followers := c.Followers()

	// Force follower to different term
	follower := followers[0]
	follower.setCurrentTerm(follower.getCurrentTerm() + 1)

	// Verify we are leader
	verify := leader.VerifyLeader()

	// Wait for the leader to step down
	if err := verify.Error(); err != ErrNotLeader && err != ErrLeadershipLost {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Ensure the known leader is cleared
	if l := leader.Leader(); l != "" {
		c.FailNowf("[ERR] bad: %v", l)
	}
}

func TestRaft_VerifyLeader_ParitalConnect(t *testing.T) {
	// Make a cluster
	conf := inmemConfig(t)
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// Get the leader
	leader := c.Leader()

	// Wait until we have a followers
	limit := time.Now().Add(c.longstopTimeout)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 2 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		c.FailNowf("[ERR] expected two followers but got: %v", followers)
	}

	// Force partial disconnect
	follower := followers[0]
	t.Logf("[INFO] Disconnecting %v", follower)
	c.Disconnect(follower.localAddr)

	// Verify we are leader
	verify := leader.VerifyLeader()

	// Wait for the leader to step down
	if err := verify.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}
}

func TestRaft_SettingPeers(t *testing.T) {
	// Make the cluster
	c := MakeClusterNoPeers(3, t, nil)
	defer c.Close()

	peers := make([]string, 0, len(c.rafts))
	for _, v := range c.rafts {
		peers = append(peers, v.localAddr)
	}

	for _, v := range c.rafts {
		future := v.SetPeers(peers)
		if err := future.Error(); err != nil {
			c.FailNowf("[ERR] error setting peers: %v", err)
		}
	}

	// Wait a while
	time.Sleep(c.propagateTimeout)

	// Should have a new leader
	if leader := c.Leader(); leader == nil {
		c.FailNowf("[ERR] no leader?")
	}
}

func TestRaft_StartAsLeader(t *testing.T) {
	conf := inmemConfig(t)
	conf.StartAsLeader = true
	c := MakeCluster(1, t, conf)
	defer c.Close()
	raft := c.rafts[0]

	// Watch leaderCh for change
	select {
	case v := <-raft.LeaderCh():
		if !v {
			c.FailNowf("[ERR] should become leader")
		}
	case <-time.After(c.conf.HeartbeatTimeout * 4):
		// Longer than you think as possibility of multiple elections
		c.FailNowf("[ERR] timeout becoming leader")
	}

	// Should be leader
	if s := raft.State(); s != Leader {
		c.FailNowf("[ERR] expected leader: %v", s)
	}

	// Should be able to apply
	future := raft.Apply([]byte("test"), c.conf.CommitTimeout)
	if err := future.Error(); err != nil {
		c.FailNowf("[ERR] err: %v", err)
	}

	// Check the response
	if future.Response().(int) != 1 {
		c.FailNowf("[ERR] bad response: %v", future.Response())
	}

	// Check the index
	if idx := future.Index(); idx == 0 {
		c.FailNowf("[ERR] bad index: %d", idx)
	}

	// Check that it is applied to the FSM
	if len(c.fsms[0].logs) != 1 {
		c.FailNowf("[ERR] did not apply to FSM!")
	}
}

func TestRaft_NotifyCh(t *testing.T) {
	ch := make(chan bool, 1)
	conf := inmemConfig(t)
	conf.NotifyCh = ch
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// Watch leaderCh for change
	select {
	case v := <-ch:
		if !v {
			c.FailNowf("[ERR] should become leader")
		}
	case <-time.After(conf.HeartbeatTimeout * 8):
		c.FailNowf("[ERR] timeout becoming leader")
	}

	// Close the cluster
	c.Close()

	// Watch leaderCh for change
	select {
	case v := <-ch:
		if v {
			c.FailNowf("[ERR] should step down as leader")
		}
	case <-time.After(conf.HeartbeatTimeout * 6):
		c.FailNowf("[ERR] timeout on step down as leader")
	}
}

func TestRaft_Voting(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()
	followers := c.Followers()
	ldr := c.Leader()
	ldrT := c.trans[c.IndexOf(ldr)]

	reqVote := RequestVoteRequest{
		Term:         42,
		Candidate:    ldrT.EncodePeer(ldr.localAddr),
		LastLogIndex: ldr.LastIndex(),
		LastLogTerm:  1,
	}
	// a follower that thinks there's a leader should vote for that leader.
	var resp RequestVoteResponse
	if err := ldrT.RequestVote(followers[0].localAddr, &reqVote, &resp); err != nil {
		c.FailNowf("[ERR] RequestVote RPC failed %v", err)
	}
	if !resp.Granted {
		c.FailNowf("[ERR] expected vote to be granted, but wasn't %+v", resp)
	}
	// a follow that thinks there's a leader shouldn't vote for a different candidate
	reqVote.Candidate = ldrT.EncodePeer(followers[0].localAddr)
	if err := ldrT.RequestVote(followers[1].localAddr, &reqVote, &resp); err != nil {
		c.FailNowf("[ERR] RequestVote RPC failed %v", err)
	}
	if resp.Granted {
		c.FailNowf("[ERR] expected vote not to be granted, but was %+v", resp)
	}
}
