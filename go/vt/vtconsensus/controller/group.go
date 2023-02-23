/*
Copyright 2021 The Vitess Authors.

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

package controller

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtconsensus/db"
	"vitess.io/vitess/go/vt/vtconsensus/inst"
	"vitess.io/vitess/go/vt/vtconsensus/log"
)

var (
	heartbeatThreshold int
)

func init() {
	servenv.OnParseFor("vtconsensus", func(fs *pflag.FlagSet) {
		fs.IntVar(&heartbeatThreshold, "group_heartbeat_threshold", 0, "VTConsensus will trigger backoff on inconsistent state if the group heartbeat staleness exceeds this threshold (in seconds). Should be used along with --enable_heartbeat_check.")
	})
}

// SQLConsensusView contains views from all the nodes within the shard
type SQLConsensusView struct {
	view               *db.ConsensusGlobalView
	logger             *log.Logger
	heartbeatThreshold int
	statsTags          []string
	sync.Mutex
}

// NewSQLConsensusView creates a new SQLConsensusView
func NewSQLConsensusView(keyspace, shard string) *SQLConsensusView {
	return &SQLConsensusView{
		statsTags:          []string{keyspace, shard},
		logger:             log.NewVTConsensusLogger(keyspace, shard),
		heartbeatThreshold: heartbeatThreshold,
	}
}

// recordView adds a view to the group
func (consensusView *SQLConsensusView) recordView(view *db.ConsensusGlobalView) {
	consensusView.Lock()
	defer consensusView.Unlock()
	consensusView.view = view
}

// overrideView overrides a view to the group
func (consensusView *SQLConsensusView) overrideView(view *db.ConsensusGlobalView) {
	consensusView.Lock()
	defer consensusView.Unlock()
	consensusView.view = view
}

// clear reset the views
func (consensusView *SQLConsensusView) clear() {
	consensusView.Lock()
	defer consensusView.Unlock()
	consensusView.view = nil
}

// GetViews returns views from everyone in the group
func (consensusView *SQLConsensusView) GetViews() *db.ConsensusGlobalView {
	consensusView.Lock()
	defer consensusView.Unlock()
	return consensusView.view
}

// IsUnconnectedReplica checks if the node is connected to a group
func (consensusView *SQLConsensusView) IsUnconnectedReplica(instanceKey *inst.InstanceKey) bool {
	if instanceKey == nil {
		return false
	}
	consensusView.Lock()
	defer consensusView.Unlock()
	cg := consensusView.view
	if nil == cg {
		return true
	}
	rm := cg.ResolvedMember
	cm, ok := rm[*instanceKey]
	if !ok {
		return true
	}
	return !cm.Connected
}

// IsAllOfflineOrError returns true if all the nodes are in offline mode
func (consensusView *SQLConsensusView) IsAllOfflineOrError() bool {
	consensusView.Lock()
	defer consensusView.Unlock()
	cg := consensusView.view
	if nil == cg {
		return false
	}
	rm := cg.ResolvedMember
	for _, cm := range rm {
		if cm.Connected {
			return false
		}
	}
	return true
}

// GetConsensusMember returns ConsensusMember status for given a host
func (consensusView *SQLConsensusView) GetConsensusMember(instanceKey *inst.InstanceKey) *db.ConsensusMember {
	if instanceKey == nil {
		return nil
	}
	consensusView.Lock()
	defer consensusView.Unlock()
	cg := consensusView.view
	if nil == cg {
		return nil
	}
	views := cg.ResolvedMember
	status, ok := views[*instanceKey]
	if !ok {
		return nil
	}
	return &status
}

// GetPrimary returns the hostname, port of the primary that everyone agreed on
// isActive bool indicates if there is any node in the group whose primary is "ONLINE"
func (consensusView *SQLConsensusView) GetPrimary() (string, int, bool) {
	consensusView.Lock()
	defer consensusView.Unlock()
	return consensusView.getPrimaryLocked()
}

func (consensusView *SQLConsensusView) getPrimaryLocked() (string, int, bool) {
	cg := consensusView.view
	if nil == cg {
		return "", 0, false
	}
	rm := cg.ResolvedMember
	for instance, cm := range rm {
		if cm.Role == db.LEADER {
			return instance.Hostname, instance.Port, cm.Connected
		}
	}
	return "", 0, false
}

// ToString returns a string representation of the Consensus global view
func (consensusView *SQLConsensusView) ToString() string {
	consensusView.Lock()
	defer consensusView.Unlock()
	var sb strings.Builder
	cg := consensusView.view

	if nil == cg {
		return ""
	}
	lvs := cg.LocalView

	sb.WriteString(fmt.Sprintf("ConsensuGlobalView leaderserverid=%d leaderhost=%s leaderport=%d\n",
		cg.LeaderServerID,
		cg.LeaderMySQLHost,
		cg.LeaderMySQLPort))

	for _, clv := range lvs {
		sb.WriteString(fmt.Sprintf("TabletAlias:[%s] ConsensusLocalView", clv.TabletAlias))
		sb.WriteString(fmt.Sprintf(" | serverid=%d host=%s port=%d leaderhost=%s leaderport=%d role=%v",
			clv.ServerID,
			clv.MySQLHost,
			clv.MySQLPort,
			clv.LeaderHostName,
			clv.LeaderHostPort,
			clv.Role))
		sb.WriteString("\n")
	}
	rm := cg.ResolvedMember
	if rm != nil {
		sb.WriteString("ConsensusMember [resolved_view]\n")
		keys := make([]inst.InstanceKey, 0, len(rm))
		for k := range rm {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].Hostname < keys[j].Hostname
		})
		for _, instance := range keys {
			cm := rm[instance]
			sb.WriteString(fmt.Sprintf("serverid=%d host=%s port=%d role=%v connected=%v",
				cm.ServerID,
				cm.MySQLHost,
				cm.MySQLPort,
				cm.Role,
				cm.Connected))
			sb.WriteString("\n")
		}
	}
	return sb.String()
}
