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
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgr/db"
	"vitess.io/vitess/go/vt/vtgr/inst"
	"vitess.io/vitess/go/vt/vtgr/log"
)

var (
	groupOnlineSize = stats.NewGaugesWithMultiLabels("MysqlGroupOnlineSize", "Online MySQL server in the group", []string{"Keyspace", "Shard"})
	isLostQuorum    = stats.NewGaugesWithMultiLabels("MysqlGroupLostQuorum", "If MySQL group lost quorum", []string{"Keyspace", "Shard"})

	heartbeatThreshold int
)

func init() {
	servenv.OnParseFor("vtgr", func(fs *pflag.FlagSet) {
		fs.IntVar(&heartbeatThreshold, "group_heartbeat_threshold", 0, "VTGR will trigger backoff on inconsistent state if the group heartbeat staleness exceeds this threshold (in seconds). Should be used along with --enable_heartbeat_check.")
	})
}

// SQLGroup contains views from all the nodes within the shard
type SQLGroup struct {
	views                 []*db.GroupView
	resolvedView          *ResolvedView
	logger                *log.Logger
	expectedBootstrapSize int
	// rebootstrapSize is init to 0
	// when it is not 0, we allow some nodes to be unhealthy during a rebootstrap
	rebootstrapSize    int
	singlePrimary      bool
	heartbeatThreshold int
	statsTags          []string
	sync.Mutex
}

// NewSQLGroup creates a new SQLGroup
func NewSQLGroup(size int, singlePrimary bool, keyspace, shard string) *SQLGroup {
	return &SQLGroup{
		expectedBootstrapSize: size,
		rebootstrapSize:       0,
		singlePrimary:         singlePrimary,
		statsTags:             []string{keyspace, shard},
		logger:                log.NewVTGRLogger(keyspace, shard),
		heartbeatThreshold:    heartbeatThreshold,
	}
}

// ResolvedView is the resolved view
type ResolvedView struct {
	groupName string
	view      map[inst.InstanceKey]db.GroupMember
	logger    *log.Logger
}

// recordView adds a view to the group
func (group *SQLGroup) recordView(view *db.GroupView) {
	group.Lock()
	defer group.Unlock()
	group.views = append(group.views, view)
}

// overrideView overrides a view to the group
func (group *SQLGroup) overrideView(views []*db.GroupView) {
	group.Lock()
	defer group.Unlock()
	group.views = views
	group.resolveLocked()
}

// clear reset the views
func (group *SQLGroup) clear() {
	group.Lock()
	defer group.Unlock()
	group.views = nil
	group.resolvedView = nil
}

// GetViews returns views from everyone in the group
func (group *SQLGroup) GetViews() []*db.GroupView {
	group.Lock()
	defer group.Unlock()
	return group.views
}

// GetGroupName returns the group name
func (group *SQLGroup) GetGroupName() string {
	group.Lock()
	defer group.Unlock()
	rv := group.resolvedView
	return rv.groupName
}

// GetOnlineGroupInfo returns number of online members in the group and also if the primary is read only
func (group *SQLGroup) GetOnlineGroupInfo() (int, bool) {
	group.Lock()
	defer group.Unlock()
	rv := group.resolvedView
	view := rv.view
	onlineSize := 0
	isPrimaryReadOnly := false
	for _, status := range view {
		if status.State == db.ONLINE {
			onlineSize++
		}
		if status.Role == db.PRIMARY {
			isPrimaryReadOnly = isPrimaryReadOnly || status.ReadOnly
		}
	}
	return onlineSize, isPrimaryReadOnly
}

// IsUnconnectedReplica checks if the node is connected to a group
func (group *SQLGroup) IsUnconnectedReplica(instanceKey *inst.InstanceKey) bool {
	if instanceKey == nil {
		return false
	}
	group.Lock()
	defer group.Unlock()
	rv := group.resolvedView
	view := rv.view
	status, ok := view[*instanceKey]
	if !ok {
		return true
	}
	return status.State != db.ONLINE && status.State != db.RECOVERING
}

// IsAllOfflineOrError returns true if all the nodes are in offline mode
func (group *SQLGroup) IsAllOfflineOrError() bool {
	group.Lock()
	defer group.Unlock()
	rv := group.resolvedView
	view := rv.view
	for _, status := range view {
		if status.State != db.OFFLINE && status.State != db.ERROR {
			return false
		}
	}
	return true
}

// GetStatus returns GroupMember status for given a host
func (group *SQLGroup) GetStatus(instanceKey *inst.InstanceKey) *db.GroupMember {
	if instanceKey == nil {
		return nil
	}
	group.Lock()
	defer group.Unlock()
	rv := group.resolvedView
	view := rv.view
	status, ok := view[*instanceKey]
	if !ok {
		return nil
	}
	return &status
}

// IsSafeToBootstrap checks if it is safe to bootstrap a mysql group
func (group *SQLGroup) IsSafeToBootstrap() bool {
	group.Lock()
	defer group.Unlock()
	// for bootstrap we require group at least has quorum number of views
	// this is to make sure we don't bootstrap a group improperly
	if len(group.views) < group.expectedBootstrapSize {
		group.logger.Errorf("[sql_group] cannot bootstrap because we only have %v views | expected %v", len(group.views), group.expectedBootstrapSize)
		return false
	}
	return group.isSafeToRebootstrapLocked()
}

// IsSafeToRebootstrap checks if it is safe to rebootstrap a group
// It does not check group size as IsSafeToBootstrap, since when we
// reach here it means VTGR already checked there were group expectedBootstrapSize
// number of nodes in topo server, therefore we just rebootstrap
// as long as we can reach all the nodes in topo server
func (group *SQLGroup) IsSafeToRebootstrap() bool {
	group.Lock()
	defer group.Unlock()
	return group.isSafeToRebootstrapLocked()
}

func (group *SQLGroup) isSafeToRebootstrapLocked() bool {
	// we think it is safe to bootstrap a group if all the views don't have a primary host
	host, port, _ := group.getPrimaryLocked()
	if host != "" || port != 0 {
		group.logger.Warningf("not safe to bootstrap sql group because %v/%v might already be primary", host, port)
	}
	return host == "" && port == 0
}

// GetPrimary returns the hostname, port of the primary that everyone agreed on
// isActive bool indicates if there is any node in the group whose primary is "ONLINE"
func (group *SQLGroup) GetPrimary() (string, int, bool) {
	group.Lock()
	defer group.Unlock()
	return group.getPrimaryLocked()
}

func (group *SQLGroup) getPrimaryLocked() (string, int, bool) {
	rv := group.resolvedView
	view := rv.view
	for instance, status := range view {
		if status.Role == db.PRIMARY {
			return instance.Hostname, instance.Port, status.State == db.ONLINE
		}
	}
	return "", 0, false
}

// Resolve merges the views into a map
func (group *SQLGroup) Resolve() error {
	group.Lock()
	defer group.Unlock()
	return group.resolveLocked()
}
func (group *SQLGroup) resolveLocked() error {
	rv := &ResolvedView{logger: group.logger}
	group.resolvedView = rv
	// a node that is not in the group might be outlier with big lag
	// iterate over all views to get global minStalenessResult first
	minStalenessResult := math.MaxInt32
	for _, view := range group.views {
		if view.HeartbeatStaleness < minStalenessResult {
			minStalenessResult = view.HeartbeatStaleness
		}
	}
	m := make(map[inst.InstanceKey]db.GroupMember)
	for _, view := range group.views {
		if rv.groupName == "" && view.GroupName != "" {
			rv.groupName = view.GroupName
		}
		if view.GroupName != "" && rv.groupName != view.GroupName {
			group.logger.Errorf("previous group name %v found %v", rv.groupName, view.GroupName)
			return db.ErrGroupSplitBrain
		}
		for _, member := range view.UnresolvedMembers {
			instance := view.CreateInstanceKey(member)
			memberState := member.State
			memberRole := member.Role
			isReadOnly := member.ReadOnly
			st, ok := m[instance]
			if !ok {
				m[instance] = db.GroupMember{
					HostName: instance.Hostname,
					Port:     instance.Port,
					State:    memberState,
					Role:     memberRole,
					ReadOnly: isReadOnly,
				}
				continue
			}
			if st.State == memberState && st.Role == memberRole && st.ReadOnly == isReadOnly {
				continue
			}
			// Members in a group should eventually converge on a state
			// if there is a partition, then a node should be removed from
			// a group. If a node is reported as ONLINE together with
			// some other state, we back off if we see a node with diverged state
			if memberState != db.UNKNOWNSTATE &&
				st.State != db.UNKNOWNSTATE &&
				st.State != memberState &&
				(st.State == db.ONLINE || memberState == db.ONLINE) {
				group.logger.Warningf("found inconsistent member state for %v: %v vs %v", instance.Hostname, st.State, memberState)
				if group.heartbeatThreshold != 0 &&
					// Check minStalenessResult among the group is not math.MaxInt32
					// which means at least one node returns the lag from _vt.heartbeat table
					// otherwise we don't trigger backoff on inconsistent state
					minStalenessResult != math.MaxInt32 &&
					minStalenessResult >= group.heartbeatThreshold {
					group.logger.Warningf("ErrGroupBackoffError by staled heartbeat check %v", minStalenessResult)
					var sb strings.Builder
					for _, view := range group.views {
						sb.WriteString(fmt.Sprintf("%v staleness=%v\n", view.MySQLHost, view.HeartbeatStaleness))
					}
					group.logger.Warningf("%v", sb.String())
					return db.ErrGroupBackoffError
				}
			}
			m[instance] = db.GroupMember{
				HostName: instance.Hostname,
				Port:     instance.Port,
				State:    group.mergeState(st.State, memberState),
				Role:     group.mergeRole(st.Role, memberRole),
				ReadOnly: st.ReadOnly || isReadOnly,
			}
		}
	}
	rv.view = m
	return group.resolvedView.validate(group.singlePrimary, group.statsTags)
}

func (rv *ResolvedView) validate(singlePrimary bool, statsTags []string) error {
	if !rv.hasGroup() {
		rv.logger.Info("Resolved view does not have a group")
		return nil
	}
	hasPrimary := false
	primaryState := db.UNKNOWNSTATE
	var onlineCount, recoveringCount, unreachableCount, offlineCount, errorCount int
	for _, status := range rv.view {
		if status.Role == db.PRIMARY {
			if singlePrimary && hasPrimary {
				rv.logger.Errorf("Found more than one primary in the group")
				return db.ErrGroupSplitBrain
			}
			hasPrimary = true
			primaryState = status.State
			if status.State != db.ONLINE {
				rv.logger.Warningf("Found a PRIMARY not ONLINE (%v)", status.State)
			}
		}
		switch status.State {
		case db.ONLINE:
			onlineCount++
		case db.UNREACHABLE:
			unreachableCount++
		case db.OFFLINE:
			offlineCount++
		case db.ERROR:
			errorCount++
		case db.RECOVERING:
			recoveringCount++
		}
	}
	groupOnlineSize.Set(statsTags, int64(onlineCount))
	if unreachableCount > 0 || errorCount > 0 || offlineCount > 0 {
		rv.logger.Warningf("Some of nodes are unconnected in the group. hasPrimary=%v (%v), online_count=%v, recovering_count=%v, unreachable_count=%v, offline_count=%v, error_count=%v", hasPrimary, primaryState, onlineCount, recoveringCount, unreachableCount, offlineCount, errorCount)
	}
	if unreachableCount >= len(rv.view)/2+1 {
		rv.logger.Errorf("Backoff error by quorum unreachable: found %v number of UNREACHABLE nodes while quorum is %v", unreachableCount, len(rv.view)/2+1)
		isLostQuorum.Set(statsTags, 1)
	} else {
		isLostQuorum.Set(statsTags, 0)
	}
	// In theory there should be no UNREACHABLE nodes
	// raise ErrGroupBackoffError to backoff and wait
	// If we lost quorum, then the group is not writable
	// If we still have a functioning group, we can backoff and wait
	// the unreachable node should either be expelled or we have a frozen view
	// Note: this means we should set group_replication_unreachable_majority_timeout
	// greater than 0. Otherwise VTGR can see all nodes are ONLINE when a single node
	// is partitioned and end up doing nothing.
	if unreachableCount > 0 {
		return db.ErrGroupBackoffError
	}
	// Ongoing bootstrap, we should backoff and wait
	if recoveringCount == 1 && (offlineCount+recoveringCount == len(rv.view)) {
		rv.logger.Warningf("Group has one recovery node with all others in offline mode")
		return db.ErrGroupOngoingBootstrap
	}
	// We don't have quorum number of unreachable, but the primary is not online
	// This most likely means there is a failover in the group we should back off and wait
	if hasPrimary && primaryState != db.ONLINE {
		rv.logger.Warningf("Found a PRIMARY that is not ONLINE (%v)", primaryState)
		return db.ErrGroupBackoffError
	}
	// If all the node in view are OFFLINE or ERROR, it is an inactive group
	// It is expected to have no primary in this case
	if !hasPrimary && (offlineCount+errorCount != len(rv.view)) {
		rv.logger.Warningf("Group is NOT all offline or error without a primary node")
		return db.ErrGroupBackoffError
	}
	return nil
}

func (rv *ResolvedView) hasGroup() bool {
	return rv.groupName != ""
}

func (group *SQLGroup) mergeState(s1, s2 db.MemberState) db.MemberState {
	return db.MemberState(group.maxStatus(int(s1), int(s2)))
}

func (group *SQLGroup) mergeRole(r1, r2 db.MemberRole) db.MemberRole {
	return db.MemberRole(group.maxStatus(int(r1), int(r2)))
}

func (group *SQLGroup) maxStatus(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ToString returns a string representatino of the sql group
func (group *SQLGroup) ToString() string {
	group.Lock()
	defer group.Unlock()
	var sb strings.Builder
	views := group.views
	for _, view := range views {
		sb.WriteString(fmt.Sprintf("[%s] SQLGroup group=%s", view.TabletAlias, view.GroupName))
		for _, member := range view.UnresolvedMembers {
			sb.WriteString(fmt.Sprintf(" | %s %s %s readonly=%v", member.HostName, member.Role, member.State, member.ReadOnly))
		}
		sb.WriteString("\n")
	}
	rv := group.resolvedView
	if rv != nil {
		sb.WriteString("[resolved_view]\n")
		sb.WriteString(fmt.Sprintf("group_name=%v\n", rv.groupName))
		keys := make([]inst.InstanceKey, 0, len(rv.view))
		for k := range rv.view {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].Hostname < keys[j].Hostname
		})
		for _, instance := range keys {
			status := rv.view[instance]
			sb.WriteString(fmt.Sprintf("[%s] state=%v role=%v readonly=%v\n", instance.Hostname, status.State, status.Role, status.ReadOnly))

		}
	}
	return sb.String()
}
