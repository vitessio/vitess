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

package tabletmanager

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// TestGroupReplicationSyncAppliesMemberWeight checks that the sync loop gives the tablet's MySQL the
// member weight of the shard's policy while it is an active member: MySQL applies
// group_replication_member_weight only when the member joins, so a voter that joined during the
// conversion, while the shard's policy was not a group replication policy yet, kept MySQL's default
// until its next join. The weight is dynamic, and the group's next election uses it.
func TestGroupReplicationSyncAppliesMemberWeight(t *testing.T) {
	for _, tt := range []struct {
		name        string
		shardPolicy string
		weight      int32
		want        int32
	}{{
		name:        "the policy's weight differs from MySQL's: the loop sets it",
		shardPolicy: weightedGroupReplicationPolicy,
		weight:      defaultGroupMemberWeight,
		want:        75,
	}, {
		name:        "the policy's weight is MySQL's: nothing changes",
		shardPolicy: weightedGroupReplicationPolicy,
		weight:      75,
		want:        75,
	}, {
		name:        "the shard's policy is not a group replication policy: the weight is left alone",
		shardPolicy: policy.DurabilitySemiSync,
		weight:      30,
		want:        30,
	}} {
		t.Run(tt.name, func(t *testing.T) {
			withGroupReplication(t)
			tm, fmd, _, _ := newConvertedShardTestTM(t, policy.DurabilitySemiSync, tt.shardPolicy)
			status := withViewID(groupStatus(testServerUUID(1),
				groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary),
				groupMember(testServerUUID(2), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary),
				groupMember(testServerUUID(3), mysql.GroupMemberStateOnline, mysql.GroupMemberRoleSecondary)), "1780000001:20")
			status.MemberWeight = tt.weight
			fmd.SetGroupReplicationStatus(status)

			newGroupReplicationSync(tm).reconcile(t.Context())

			assert.Equal(t, tt.want, tmStatus(t, fmd).MemberWeight)
		})
	}
}
