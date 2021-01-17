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

package process

import (
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/util"

	"github.com/patrickmn/go-cache"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	orcraft "vitess.io/vitess/go/vt/orchestrator/raft"
)

var lastHealthCheckUnixNano int64
var lastGoodHealthCheckUnixNano int64
var LastContinousCheckHealthy int64

var lastHealthCheckCache = cache.New(config.HealthPollSeconds*time.Second, time.Second)

type NodeHealth struct {
	Hostname        string
	Token           string
	AppVersion      string
	FirstSeenActive string
	LastSeenActive  string
	ExtraInfo       string
	Command         string
	DBBackend       string

	LastReported time.Time
	onceHistory  sync.Once
	onceUpdate   sync.Once
}

func NewNodeHealth() *NodeHealth {
	return &NodeHealth{
		Hostname:   ThisHostname,
		Token:      util.ProcessToken.Hash,
		AppVersion: config.RuntimeCLIFlags.ConfiguredVersion,
	}
}

func (nodeHealth *NodeHealth) Update() *NodeHealth {
	nodeHealth.onceUpdate.Do(func() {
		nodeHealth.Hostname = ThisHostname
		nodeHealth.Token = util.ProcessToken.Hash
		nodeHealth.AppVersion = config.RuntimeCLIFlags.ConfiguredVersion
	})
	nodeHealth.LastReported = time.Now()
	return nodeHealth
}

var ThisNodeHealth = NewNodeHealth()

type HealthStatus struct {
	Healthy            bool
	Hostname           string
	Token              string
	IsActiveNode       bool
	ActiveNode         NodeHealth
	Error              error
	AvailableNodes     [](*NodeHealth)
	RaftLeader         string
	IsRaftLeader       bool
	RaftLeaderURI      string
	RaftAdvertise      string
	RaftHealthyMembers []string
}

type OrchestratorExecutionMode string

const (
	OrchestratorExecutionCliMode  OrchestratorExecutionMode = "CLIMode"
	OrchestratorExecutionHttpMode OrchestratorExecutionMode = "HttpMode"
)

var continuousRegistrationOnce sync.Once

func RegisterNode(nodeHealth *NodeHealth) (healthy bool, err error) {
	nodeHealth.Update()
	healthy, err = WriteRegisterNode(nodeHealth)
	atomic.StoreInt64(&lastHealthCheckUnixNano, time.Now().UnixNano())
	if healthy {
		atomic.StoreInt64(&lastGoodHealthCheckUnixNano, time.Now().UnixNano())
	}
	return healthy, err
}

// HealthTest attempts to write to the backend database and get a result
func HealthTest() (health *HealthStatus, err error) {
	cacheKey := util.ProcessToken.Hash
	if healthStatus, found := lastHealthCheckCache.Get(cacheKey); found {
		return healthStatus.(*HealthStatus), nil
	}

	health = &HealthStatus{Healthy: false, Hostname: ThisHostname, Token: util.ProcessToken.Hash}
	defer lastHealthCheckCache.Set(cacheKey, health, cache.DefaultExpiration)

	if healthy, err := RegisterNode(ThisNodeHealth); err != nil {
		health.Error = err
		return health, log.Errore(err)
	} else {
		health.Healthy = healthy
	}

	if orcraft.IsRaftEnabled() {
		health.ActiveNode.Hostname = orcraft.GetLeader()
		health.IsActiveNode = orcraft.IsLeader()
		health.RaftLeader = orcraft.GetLeader()
		health.RaftLeaderURI = orcraft.LeaderURI.Get()
		health.IsRaftLeader = orcraft.IsLeader()
		health.RaftAdvertise = config.Config.RaftAdvertise
		health.RaftHealthyMembers = orcraft.HealthyMembers()
	} else {
		if health.ActiveNode, health.IsActiveNode, err = ElectedNode(); err != nil {
			health.Error = err
			return health, log.Errore(err)
		}
	}
	health.AvailableNodes, _ = ReadAvailableNodes(true)

	return health, nil
}

func SinceLastHealthCheck() time.Duration {
	timeNano := atomic.LoadInt64(&lastHealthCheckUnixNano)
	if timeNano == 0 {
		return 0
	}
	return time.Since(time.Unix(0, timeNano))
}

func SinceLastGoodHealthCheck() time.Duration {
	timeNano := atomic.LoadInt64(&lastGoodHealthCheckUnixNano)
	if timeNano == 0 {
		return 0
	}
	return time.Since(time.Unix(0, timeNano))
}

// ContinuousRegistration will continuously update the node_health
// table showing that the current process is still running.
func ContinuousRegistration(extraInfo string, command string) {
	ThisNodeHealth.ExtraInfo = extraInfo
	ThisNodeHealth.Command = command
	continuousRegistrationOnce.Do(func() {
		tickOperation := func() {
			healthy, err := RegisterNode(ThisNodeHealth)
			if err != nil {
				log.Errorf("ContinuousRegistration: RegisterNode failed: %+v", err)
			}
			if healthy {
				atomic.StoreInt64(&LastContinousCheckHealthy, 1)
			} else {
				atomic.StoreInt64(&LastContinousCheckHealthy, 0)
			}
		}
		// First one is synchronous
		tickOperation()
		go func() {
			registrationTick := time.Tick(config.HealthPollSeconds * time.Second)
			for range registrationTick {
				// We already run inside a go-routine so
				// do not do this asynchronously.  If we
				// get stuck then we don't want to fill up
				// the backend pool with connections running
				// this maintenance operation.
				tickOperation()
			}
		}()
	})
}
