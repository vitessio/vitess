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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestDemotePrimaryRevertKeepsServingInvariant reproduces the TLA+ model's prs_demote_fail trace
// (doc/design-docs/group_replication_tla): the group's view shrank to the primary (the other two voters
// were expelled) while it still served, as in the window before the fence check fences it, and PRS's
// DemotePrimary then fails after it set super_read_only (here, the read of the primary status fails).
// The revert must not make MySQL writable again, nor the tablet serve, without a decision on the serving
// invariant: the primary's view holds one of three voters. Before, the revert redid the prepared
// transactions with super_read_only OFF, and served again, because no not-serving reason was set yet.
func TestDemotePrimaryRevertKeepsServingInvariant(t *testing.T) {
	withGroupReplication(t)
	tm, fmd, _, _ := newLegitimacyTestTM(t)
	setTabletType(t, tm, topodatapb.TabletType_PRIMARY)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	require.True(t, qsc.IsServing())
	fmd.SuperReadOnly.Store(false)
	fmd.ReadOnly = false

	// The primary of the recorded incarnation, alone in its view: a single voter of three.
	fmd.SetGroupReplicationStatus(withViewID(groupStatus(testServerUUID(1),
		groupMember(testServerUUID(1), mysql.GroupMemberStateOnline, mysql.GroupMemberRolePrimary)), "1780000001:9"))
	fmd.PrimaryStatusError = errors.New("lost connection to MySQL server during query")

	_, err := tm.DemotePrimary(t.Context(), false)
	require.Error(t, err)
	assert.True(t, fmd.SuperReadOnly.Load(), "the revert must not make MySQL writable for a view without the voter majority")
	assert.False(t, qsc.IsServing(), "the revert must not make the tablet serve for a view without the voter majority")
}
