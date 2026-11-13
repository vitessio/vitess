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

package mysql

import (
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
)

// OnlineGroupMembers returns the number of ONLINE members in the server's view of its group.
func OnlineGroupMembers(status *replicationdatapb.GroupReplicationStatus) int {
	if status == nil {
		return 0
	}
	online := 0
	for _, m := range status.Members {
		if m.State == GroupMemberStateOnline {
			online++
		}
	}
	return online
}

// GroupSupersedesSemiSync returns whether the server is an active member of a group with at
// least two ONLINE members. Such a group makes a transaction durable on a majority of its
// members before it commits, so semi-sync is not needed on its primary. Semi-sync with an
// infinite timeout even blocks every commit once its last acker has joined the group.
func GroupSupersedesSemiSync(status *replicationdatapb.GroupReplicationStatus) bool {
	return IsGroupMemberActive(status) && OnlineGroupMembers(status) >= 2
}

// ResetDefaultReplicationChannelCommand returns the statement that removes the configuration
// of the default (asynchronous) replication channel. Unlike RESET REPLICA ALL without a
// channel, which MySQL refuses on a running Group Replication member (ERROR 3139), it leaves
// the channels of the Group Replication plugin alone.
func ResetDefaultReplicationChannelCommand() string {
	return "RESET REPLICA ALL FOR CHANNEL ''"
}
