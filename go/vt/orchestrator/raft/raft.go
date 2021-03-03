/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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

package orcraft

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/util"

	"github.com/patrickmn/go-cache"

	"vitess.io/vitess/go/vt/orchestrator/external/raft"
)

const (
	YieldCommand     = "yield"
	YieldHintCommand = "yield-hint"
)

const (
	retainSnapshotCount    = 10
	snapshotInterval       = 30 * time.Minute
	asyncSnapshotTimeframe = 1 * time.Minute
	raftTimeout            = 10 * time.Second
)

var RaftNotRunning error = fmt.Errorf("raft is not configured/running")
var store *Store
var raftSetupComplete int64
var ThisHostname string
var healthRequestAuthenticationTokenCache = cache.New(config.RaftHealthPollSeconds*2*time.Second, time.Second)
var healthReportsCache = cache.New(config.RaftHealthPollSeconds*2*time.Second, time.Second)
var healthRequestReportCache = cache.New(time.Second, time.Second)

var fatalRaftErrorChan = make(chan error)

type leaderURI struct {
	uri string
	sync.Mutex
}

var LeaderURI leaderURI
var thisLeaderURI string // How this node identifies itself assuming it is the leader

func (luri *leaderURI) Get() string {
	luri.Lock()
	defer luri.Unlock()
	return luri.uri
}

func (luri *leaderURI) Set(uri string) {
	luri.Lock()
	defer luri.Unlock()
	luri.uri = uri
}

func (luri *leaderURI) IsThisLeaderURI() bool {
	luri.Lock()
	defer luri.Unlock()
	return luri.uri == thisLeaderURI
}

func IsRaftEnabled() bool {
	return store != nil
}

func FatalRaftError(err error) error {
	if err != nil {
		go func() { fatalRaftErrorChan <- err }()
	}
	return err
}

func computeLeaderURI() (uri string, err error) {
	if config.Config.HTTPAdvertise != "" {
		// Explicitly given
		return config.Config.HTTPAdvertise, nil
	}
	// Not explicitly given. Let's heuristically compute using RaftAdvertise
	scheme := "http"
	if config.Config.UseSSL {
		scheme = "https"
	}

	hostname := strings.Split(config.Config.RaftAdvertise, ":")[0]
	listenTokens := strings.Split(config.Config.ListenAddress, ":")
	if len(listenTokens) < 2 {
		return uri, fmt.Errorf("computeLeaderURI: cannot determine listen port out of config.Config.ListenAddress: %+v", config.Config.ListenAddress)
	}
	port := listenTokens[1]

	uri = fmt.Sprintf("%s://%s:%s", scheme, hostname, port)
	return uri, nil
}

// Setup creates the entire raft shananga. Creates the store, associates with the throttler,
// contacts peer nodes, and subscribes to leader changes to export them.
func Setup(applier CommandApplier, snapshotCreatorApplier SnapshotCreatorApplier, thisHostname string) error {
	log.Debugf("Setting up raft")
	ThisHostname = thisHostname
	raftBind, err := normalizeRaftNode(config.Config.RaftBind)
	if err != nil {
		return err
	}
	raftAdvertise, err := normalizeRaftNode(config.Config.RaftAdvertise)
	if err != nil {
		return err
	}
	store = NewStore(config.Config.RaftDataDir, raftBind, raftAdvertise, applier, snapshotCreatorApplier)
	peerNodes := []string{}
	for _, raftNode := range config.Config.RaftNodes {
		peerNode, err := normalizeRaftNode(raftNode)
		if err != nil {
			return err
		}
		peerNodes = append(peerNodes, peerNode)
	}
	if len(peerNodes) == 1 && peerNodes[0] == raftAdvertise {
		// To run in single node setup we will either specify an empty RaftNodes, or a single
		// raft node that is exactly RaftAdvertise
		peerNodes = []string{}
	}
	if err := store.Open(peerNodes); err != nil {
		return log.Errorf("failed to open raft store: %s", err.Error())
	}

	thisLeaderURI, err = computeLeaderURI()
	if err != nil {
		return FatalRaftError(err)
	}

	leaderCh := store.raft.LeaderCh()
	go func() {
		for isTurnedLeader := range leaderCh {
			if isTurnedLeader {
				PublishCommand("leader-uri", thisLeaderURI)
			}
		}
	}()

	setupHttpClient()

	atomic.StoreInt64(&raftSetupComplete, 1)
	return nil
}

func isRaftSetupComplete() bool {
	return atomic.LoadInt64(&raftSetupComplete) == 1
}

// getRaft is a convenience method
func getRaft() *raft.Raft {
	return store.raft
}

func normalizeRaftHostnameIP(host string) (string, error) {
	if ip := net.ParseIP(host); ip != nil {
		// this is a valid IP address.
		return host, nil
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		// resolve failed. But we don't want to fail the entire operation for that
		log.Errore(err)
		return host, nil
	}
	// resolve success!
	for _, ip := range ips {
		return ip.String(), nil
	}
	return host, fmt.Errorf("%+v resolved but no IP found", host)
}

// normalizeRaftNode attempts to make sure there's a port to the given node.
// It consults the DefaultRaftPort when there isn't
func normalizeRaftNode(node string) (string, error) {
	hostPort := strings.Split(node, ":")
	host, err := normalizeRaftHostnameIP(hostPort[0])
	if err != nil {
		return host, err
	}
	if len(hostPort) > 1 {
		return fmt.Sprintf("%s:%s", host, hostPort[1]), nil
	} else if config.Config.DefaultRaftPort != 0 {
		// No port specified, add one
		return fmt.Sprintf("%s:%d", host, config.Config.DefaultRaftPort), nil
	} else {
		return host, nil
	}
}

// IsPartOfQuorum returns `true` when this node is part of the raft quorum, meaning its
// data and opinion are trustworthy.
// Comapre that to a node which has left (or has not yet joined) the quorum: it has stale data.
func IsPartOfQuorum() bool {
	if GetLeader() == "" {
		return false
	}
	state := GetState()
	return state == raft.Leader || state == raft.Follower
}

// IsLeader tells if this node is the current raft leader
func IsLeader() bool {
	return GetState() == raft.Leader
}

// GetLeader returns identity of raft leader
func GetLeader() string {
	if !isRaftSetupComplete() {
		return ""
	}
	return getRaft().Leader()
}

func QuorumSize() (int, error) {
	peers, err := GetPeers()
	if err != nil {
		return 0, err
	}
	return len(peers)/2 + 1, nil
}

// GetState returns current raft state
func GetState() raft.RaftState {
	if !isRaftSetupComplete() {
		return raft.Candidate
	}
	return getRaft().State()
}

// IsHealthy checks whether this node is healthy in the raft group
func IsHealthy() bool {
	if !isRaftSetupComplete() {
		return false
	}
	state := GetState()
	return state == raft.Leader || state == raft.Follower
}

func Snapshot() error {
	future := getRaft().Snapshot()
	return future.Error()
}

func AsyncSnapshot() error {
	asyncDuration := (time.Duration(rand.Int63()) % asyncSnapshotTimeframe)
	go time.AfterFunc(asyncDuration, func() {
		Snapshot()
	})
	return nil
}

func StepDown() {
	getRaft().StepDown()
}

func Yield() error {
	if !IsRaftEnabled() {
		return RaftNotRunning
	}
	return getRaft().Yield()
}

func GetRaftBind() string {
	return store.raftBind
}

func GetRaftAdvertise() string {
	return store.raftAdvertise
}

func GetPeers() ([]string, error) {
	if !IsRaftEnabled() {
		return []string{}, RaftNotRunning
	}
	return store.peerStore.Peers()
}

func IsPeer(peer string) (bool, error) {
	if !IsRaftEnabled() {
		return false, RaftNotRunning
	}
	return (store.raftBind == peer), nil
}

// PublishCommand will distribute a command across the group
func PublishCommand(op string, value interface{}) (response interface{}, err error) {
	if !IsRaftEnabled() {
		return nil, RaftNotRunning
	}
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return store.genericCommand(op, b)
}

func AddPeer(addr string) (response interface{}, err error) {
	addr, err = normalizeRaftNode(addr)
	if err != nil {
		return "", err
	}
	err = store.AddPeer(addr)
	return addr, err
}

func RemovePeer(addr string) (response interface{}, err error) {
	addr, err = normalizeRaftNode(addr)
	if err != nil {
		return "", err
	}
	err = store.RemovePeer(addr)
	return addr, err
}

func PublishYield(toPeer string) (response interface{}, err error) {
	toPeer, err = normalizeRaftNode(toPeer)
	if err != nil {
		return "", err
	}
	return store.genericCommand(YieldCommand, []byte(toPeer))
}

func PublishYieldHostnameHint(hostnameHint string) (response interface{}, err error) {
	return store.genericCommand(YieldHintCommand, []byte(hostnameHint))
}

// ReportToRaftLeader tells the leader this raft node is raft-healthy
func ReportToRaftLeader(authenticationToken string) (err error) {
	if err := healthRequestReportCache.Add(config.Config.RaftBind, true, cache.DefaultExpiration); err != nil {
		// Recently reported
		return nil
	}
	path := fmt.Sprintf("raft-follower-health-report/%s/%s/%s", authenticationToken, config.Config.RaftBind, config.Config.RaftAdvertise)
	_, err = HttpGetLeader(path)
	return err
}

// OnHealthReport acts on a raft-member reporting its health
func OnHealthReport(authenticationToken, raftBind, raftAdvertise string) (err error) {
	if _, found := healthRequestAuthenticationTokenCache.Get(authenticationToken); !found {
		return log.Errorf("Raft health report: unknown token %s", authenticationToken)
	}
	healthReportsCache.Set(raftAdvertise, true, cache.DefaultExpiration)
	return nil
}

func HealthyMembers() (advertised []string) {
	items := healthReportsCache.Items()
	for raftAdvertised := range items {
		advertised = append(advertised, raftAdvertised)
	}
	return advertised
}

// Monitor is a utility function to routinely observe leadership state.
// It doesn't actually do much; merely takes notes.
//nolint SA1015: using time.Tick leaks the underlying ticker
func Monitor() {
	t := time.Tick(5 * time.Second)
	heartbeat := time.Tick(1 * time.Minute)
	followerHealthTick := time.Tick(config.RaftHealthPollSeconds * time.Second)
	for {
		select {
		case <-t:
			leaderHint := GetLeader()

			if IsLeader() {
				leaderHint = fmt.Sprintf("%s (this host)", leaderHint)
			}
			log.Debugf("raft leader is %s; state: %s", leaderHint, GetState().String())

		case <-heartbeat:
			if IsLeader() {
				go PublishCommand("heartbeat", "")
			}
		case <-followerHealthTick:
			if IsLeader() {
				athenticationToken := util.NewToken().Short()
				healthRequestAuthenticationTokenCache.Set(athenticationToken, true, cache.DefaultExpiration)
				go PublishCommand("request-health-report", athenticationToken)
			}
		case err := <-fatalRaftErrorChan:
			log.Fatale(err)
		}
	}
}
