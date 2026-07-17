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

package reparentutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/mysqlctl"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// TestReparentSorter tests that the sorting for tablets works correctly
func TestReparentSorter(t *testing.T) {
	sid1 := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	cell1 := "cell1"
	cell2 := "cell2"
	tabletReplica1_100 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell1,
			Uid:  100,
		},
		Type: topodatapb.TabletType_REPLICA,
	}
	tabletReplica2_100 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell2,
			Uid:  100,
		},
		Type: topodatapb.TabletType_REPLICA,
	}
	tabletReplica1_101 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell1,
			Uid:  101,
		},
		Type: topodatapb.TabletType_REPLICA,
	}
	tabletReplica3_103 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell1,
			Uid:  103,
		},
		Type: topodatapb.TabletType_REPLICA,
	}
	tabletRdonly1_102 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell1,
			Uid:  102,
		},
		Type: topodatapb.TabletType_RDONLY,
	}

	mysqlGTID1 := replication.Mysql56GTID{
		Server:   sid1,
		Sequence: 9,
	}
	mysqlGTID2 := replication.Mysql56GTID{
		Server:   sid2,
		Sequence: 10,
	}
	mysqlGTID3 := replication.Mysql56GTID{
		Server:   sid1,
		Sequence: 11,
	}

	positionMostAdvanced := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}
	positionMostAdvanced.Combined.GTIDSet = positionMostAdvanced.Combined.GTIDSet.AddGTID(mysqlGTID1)
	positionMostAdvanced.Combined.GTIDSet = positionMostAdvanced.Combined.GTIDSet.AddGTID(mysqlGTID2)
	positionMostAdvanced.Combined.GTIDSet = positionMostAdvanced.Combined.GTIDSet.AddGTID(mysqlGTID3)
	positionMostAdvanced.Executed.GTIDSet = positionMostAdvanced.Executed.GTIDSet.AddGTID(mysqlGTID1)
	positionMostAdvanced.Executed.GTIDSet = positionMostAdvanced.Executed.GTIDSet.AddGTID(mysqlGTID2)

	positionAlmostMostAdvanced := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}
	positionAlmostMostAdvanced.Combined.GTIDSet = positionAlmostMostAdvanced.Combined.GTIDSet.AddGTID(mysqlGTID1)
	positionAlmostMostAdvanced.Combined.GTIDSet = positionAlmostMostAdvanced.Combined.GTIDSet.AddGTID(mysqlGTID2)
	positionAlmostMostAdvanced.Combined.GTIDSet = positionAlmostMostAdvanced.Combined.GTIDSet.AddGTID(mysqlGTID3)
	positionAlmostMostAdvanced.Executed.GTIDSet = positionAlmostMostAdvanced.Executed.GTIDSet.AddGTID(mysqlGTID1)

	positionEmpty := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}

	positionIntermediate1 := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}
	positionIntermediate1.Combined.GTIDSet = positionIntermediate1.Combined.GTIDSet.AddGTID(mysqlGTID1)
	positionIntermediate1.Executed.GTIDSet = positionIntermediate1.Executed.GTIDSet.AddGTID(mysqlGTID1)

	positionIntermediate2 := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}
	positionIntermediate2.Combined.GTIDSet = positionIntermediate2.Combined.GTIDSet.AddGTID(mysqlGTID1)
	positionIntermediate2.Combined.GTIDSet = positionIntermediate2.Combined.GTIDSet.AddGTID(mysqlGTID2)
	positionIntermediate2.Executed.GTIDSet = positionIntermediate2.Executed.GTIDSet.AddGTID(mysqlGTID1)

	testcases := []struct {
		name             string
		tablets          []*topodatapb.Tablet
		innodbBufferPool []int
		mysqlVersions    []mysqlctl.ServerVersion
		positions        []*RelayLogPositions
		containsErr      string
		sortedTablets    []*topodatapb.Tablet
	}{
		{
			name:          "all advanced, sort via promotion rules",
			tablets:       []*topodatapb.Tablet{nil, tabletReplica1_100, tabletRdonly1_102},
			positions:     []*RelayLogPositions{positionMostAdvanced, positionMostAdvanced, positionMostAdvanced},
			sortedTablets: []*topodatapb.Tablet{tabletReplica1_100, tabletRdonly1_102, nil},
		}, {
			name:             "all advanced, sort via innodb buffer pool",
			tablets:          []*topodatapb.Tablet{tabletReplica1_101, tabletReplica2_100, tabletReplica1_100},
			positions:        []*RelayLogPositions{positionMostAdvanced, positionMostAdvanced, positionMostAdvanced},
			innodbBufferPool: []int{10, 40, 25},
			sortedTablets:    []*topodatapb.Tablet{tabletReplica2_100, tabletReplica1_100, tabletReplica1_101},
		}, {
			name:          "ordering by position",
			tablets:       []*topodatapb.Tablet{tabletReplica1_101, tabletReplica2_100, tabletReplica1_100, tabletRdonly1_102, tabletReplica3_103},
			positions:     []*RelayLogPositions{positionEmpty, positionIntermediate1, positionIntermediate2, positionMostAdvanced, positionAlmostMostAdvanced},
			sortedTablets: []*topodatapb.Tablet{tabletRdonly1_102, tabletReplica3_103, tabletReplica1_100, tabletReplica2_100, tabletReplica1_101},
		}, {
			name:        "tablets and positions count error",
			tablets:     []*topodatapb.Tablet{tabletReplica1_101, tabletReplica2_100},
			positions:   []*RelayLogPositions{positionEmpty, positionIntermediate1, positionMostAdvanced},
			containsErr: "unequal number of tablets and positions",
		}, {
			name:             "innodb buffer pool count error",
			tablets:          []*topodatapb.Tablet{tabletReplica1_101, tabletReplica2_100},
			positions:        []*RelayLogPositions{positionEmpty, positionIntermediate1},
			innodbBufferPool: []int{10},
			containsErr:      "unequal number of tablets and innodb buffer pool entries",
		}, {
			name:          "mysql versions count error",
			tablets:       []*topodatapb.Tablet{tabletReplica1_101, tabletReplica2_100},
			positions:     []*RelayLogPositions{positionEmpty, positionIntermediate1},
			mysqlVersions: []mysqlctl.ServerVersion{{Major: 8, Minor: 0, Patch: 35}},
			containsErr:   "unequal number of tablets and mysql versions",
		}, {
			name:          "promotion rule check",
			tablets:       []*topodatapb.Tablet{tabletReplica1_101, tabletRdonly1_102},
			positions:     []*RelayLogPositions{positionMostAdvanced, positionMostAdvanced},
			sortedTablets: []*topodatapb.Tablet{tabletReplica1_101, tabletRdonly1_102},
		}, {
			name:          "mixed",
			tablets:       []*topodatapb.Tablet{tabletReplica1_101, tabletReplica2_100, tabletReplica1_100, tabletRdonly1_102, tabletReplica3_103},
			positions:     []*RelayLogPositions{positionEmpty, positionIntermediate1, positionMostAdvanced, positionIntermediate1, positionAlmostMostAdvanced},
			sortedTablets: []*topodatapb.Tablet{tabletReplica1_100, tabletReplica3_103, tabletReplica2_100, tabletRdonly1_102, tabletReplica1_101},
		}, {
			name:             "mixed - another",
			tablets:          []*topodatapb.Tablet{tabletReplica1_101, tabletReplica2_100, tabletReplica1_100, tabletRdonly1_102, tabletReplica3_103},
			positions:        []*RelayLogPositions{positionIntermediate1, positionIntermediate1, positionMostAdvanced, positionIntermediate1, positionAlmostMostAdvanced},
			innodbBufferPool: []int{100, 200, 0, 200, 200},
			sortedTablets:    []*topodatapb.Tablet{tabletReplica1_100, tabletReplica3_103, tabletReplica2_100, tabletReplica1_101, tabletRdonly1_102},
		}, {
			name:          "equal candidates use full tablet alias as stable tiebreaker",
			tablets:       []*topodatapb.Tablet{tabletReplica3_103, tabletReplica2_100, tabletReplica1_101, tabletReplica1_100},
			positions:     []*RelayLogPositions{positionMostAdvanced, positionMostAdvanced, positionMostAdvanced, positionMostAdvanced},
			sortedTablets: []*topodatapb.Tablet{tabletReplica1_100, tabletReplica1_101, tabletReplica3_103, tabletReplica2_100},
		},
	}

	durability, err := policy.GetDurabilityPolicy(policy.DurabilityNone)
	require.NoError(t, err)
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			err := sortTabletsForReparent(testcase.tablets, testcase.positions, testcase.innodbBufferPool, testcase.mysqlVersions, durability, SortForERS)
			if testcase.containsErr != "" {
				require.EqualError(t, err, testcase.containsErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, testcase.sortedTablets, testcase.tablets)
			}
		})
	}
}

func TestReparentSorter_MySQLVersion(t *testing.T) {
	sid1 := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}

	tabletA := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Type:  topodatapb.TabletType_REPLICA,
	}
	tabletB := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
		Type:  topodatapb.TabletType_REPLICA,
	}
	tabletC := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 102},
		Type:  topodatapb.TabletType_REPLICA,
	}
	tabletRdonly := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 103},
		Type:  topodatapb.TabletType_RDONLY,
	}

	posAdvanced := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}
	posAdvanced.Combined.GTIDSet = posAdvanced.Combined.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid1, Sequence: 9})
	posAdvanced.Combined.GTIDSet = posAdvanced.Combined.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid2, Sequence: 10})
	posAdvanced.Executed.GTIDSet = posAdvanced.Executed.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid1, Sequence: 9})
	posAdvanced.Executed.GTIDSet = posAdvanced.Executed.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid2, Sequence: 10})

	posBehind := &RelayLogPositions{
		Combined: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
		Executed: replication.Position{GTIDSet: replication.Mysql56GTIDSet{}},
	}
	posBehind.Combined.GTIDSet = posBehind.Combined.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid1, Sequence: 9})
	posBehind.Executed.GTIDSet = posBehind.Executed.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid1, Sequence: 9})

	mysql80 := mysqlctl.ServerVersion{Major: 8, Minor: 0, Patch: 35}
	mysql84 := mysqlctl.ServerVersion{Major: 8, Minor: 4, Patch: 0}
	// A pre-8.0.34 patch and a later 8.0 patch: within the 8.0 series before the
	// bugfix-only cutoff, the patch is significant (see CompareForReplication).
	mysql8020 := mysqlctl.ServerVersion{Major: 8, Minor: 0, Patch: 20}

	durability, err := policy.GetDurabilityPolicy(policy.DurabilityNone)
	require.NoError(t, err)

	testcases := []struct {
		name          string
		tablets       []*topodatapb.Tablet
		positions     []*RelayLogPositions
		mysqlVersions []mysqlctl.ServerVersion
		mode          SortMode
		sortedTablets []*topodatapb.Tablet
	}{
		{
			name:          "ERS: lower version preferred when positions are equal",
			tablets:       []*topodatapb.Tablet{tabletA, tabletB},
			positions:     []*RelayLogPositions{posAdvanced, posAdvanced},
			mysqlVersions: []mysqlctl.ServerVersion{mysql84, mysql80},
			sortedTablets: []*topodatapb.Tablet{tabletB, tabletA},
		},
		{
			name:          "ERS: position wins over lower version",
			tablets:       []*topodatapb.Tablet{tabletA, tabletB},
			positions:     []*RelayLogPositions{posAdvanced, posBehind},
			mysqlVersions: []mysqlctl.ServerVersion{mysql84, mysql80},
			sortedTablets: []*topodatapb.Tablet{tabletA, tabletB},
		},
		{
			name:          "ERS: same version falls through to position",
			tablets:       []*topodatapb.Tablet{tabletA, tabletB},
			positions:     []*RelayLogPositions{posAdvanced, posBehind},
			mysqlVersions: []mysqlctl.ServerVersion{mysql80, mysql80},
			sortedTablets: []*topodatapb.Tablet{tabletA, tabletB},
		},
		{
			name:          "ERS: unknown version sorts last",
			tablets:       []*topodatapb.Tablet{tabletA, tabletB, tabletC},
			positions:     []*RelayLogPositions{posAdvanced, posAdvanced, posAdvanced},
			mysqlVersions: []mysqlctl.ServerVersion{unknownVersion, mysql84, mysql80},
			sortedTablets: []*topodatapb.Tablet{tabletC, tabletB, tabletA},
		},
		{
			name:          "ERS: nil version slice preserves position-based ordering",
			tablets:       []*topodatapb.Tablet{tabletA, tabletB},
			positions:     []*RelayLogPositions{posBehind, posAdvanced},
			mysqlVersions: nil,
			sortedTablets: []*topodatapb.Tablet{tabletB, tabletA},
		},
		{
			name:          "ERS: position still wins when lower version is behind",
			tablets:       []*topodatapb.Tablet{tabletA, tabletB},
			positions:     []*RelayLogPositions{posAdvanced, posBehind},
			mysqlVersions: []mysqlctl.ServerVersion{mysql84, mysql80},
			sortedTablets: []*topodatapb.Tablet{tabletA, tabletB},
		},
		{
			name:          "ERS: position gates before promotion rules - advanced MustNot beats behind Neutral",
			tablets:       []*topodatapb.Tablet{tabletA, tabletRdonly},
			positions:     []*RelayLogPositions{posBehind, posAdvanced},
			mysqlVersions: []mysqlctl.ServerVersion{mysql80, mysql80},
			sortedTablets: []*topodatapb.Tablet{tabletRdonly, tabletA},
		},
		{
			name:          "PRS: lower release wins even when slightly behind in position",
			tablets:       []*topodatapb.Tablet{tabletA, tabletB},
			positions:     []*RelayLogPositions{posAdvanced, posBehind},
			mysqlVersions: []mysqlctl.ServerVersion{mysql84, mysql80},
			mode:          SortForPRS,
			sortedTablets: []*topodatapb.Tablet{tabletB, tabletA},
		},
		{
			name:          "PRS: same release falls through to position",
			tablets:       []*topodatapb.Tablet{tabletA, tabletB},
			positions:     []*RelayLogPositions{posAdvanced, posBehind},
			mysqlVersions: []mysqlctl.ServerVersion{mysql80, mysql80},
			mode:          SortForPRS,
			sortedTablets: []*topodatapb.Tablet{tabletA, tabletB},
		},
		{
			name:          "PRS: promotion rule gates before version - MustNot on lower version loses",
			tablets:       []*topodatapb.Tablet{tabletRdonly, tabletA},
			positions:     []*RelayLogPositions{posAdvanced, posAdvanced},
			mysqlVersions: []mysqlctl.ServerVersion{mysql80, mysql84},
			mode:          SortForPRS,
			sortedTablets: []*topodatapb.Tablet{tabletA, tabletRdonly},
		},
		{
			name:          "ERS: lower 8.0 patch preferred below the bugfix-only cutoff when positions are equal",
			tablets:       []*topodatapb.Tablet{tabletA, tabletB},
			positions:     []*RelayLogPositions{posAdvanced, posAdvanced},
			mysqlVersions: []mysqlctl.ServerVersion{mysql80, mysql8020},
			sortedTablets: []*topodatapb.Tablet{tabletB, tabletA},
		},
		{
			name:          "PRS: lower 8.0 patch preferred below the bugfix-only cutoff even when slightly behind",
			tablets:       []*topodatapb.Tablet{tabletA, tabletB},
			positions:     []*RelayLogPositions{posAdvanced, posBehind},
			mysqlVersions: []mysqlctl.ServerVersion{mysql80, mysql8020},
			mode:          SortForPRS,
			sortedTablets: []*topodatapb.Tablet{tabletB, tabletA},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := sortTabletsForReparent(tc.tablets, tc.positions, nil, tc.mysqlVersions, durability, tc.mode)
			require.NoError(t, err)
			require.Equal(t, tc.sortedTablets, tc.tablets)
		})
	}
}

// TestReparentSorter_ExecutedTiebreak pins the sorter's behavior when two
// candidates have equal Combined (relay-log) positions but different Executed
// (applied) positions: the one that has applied more sorts first. This is the
// tie-break RelayLogPositions.AtLeast implements, exercised at the election
// level. It must hold independently of the version tiebreaker (no versions
// supplied here), so a change to AtLeast or the sorter is caught even if the
// version-aware tests change.
func TestReparentSorter_ExecutedTiebreak(t *testing.T) {
	sid := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	tabletBehind := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Type:  topodatapb.TabletType_REPLICA,
	}
	tabletCaughtUp := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
		Type:  topodatapb.TabletType_REPLICA,
	}

	// Both relay logs hold GTIDs 1-10 (equal Combined). tabletCaughtUp has applied
	// all of them; tabletBehind has applied only 1-5.
	combined := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	combined.GTIDSet = combined.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 5})
	combined.GTIDSet = combined.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 10})
	executedBehind := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	executedBehind.GTIDSet = executedBehind.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 5})

	posCaughtUp := &RelayLogPositions{Combined: combined, Executed: combined}
	posBehind := &RelayLogPositions{Combined: combined, Executed: executedBehind}

	durability, err := policy.GetDurabilityPolicy(policy.DurabilityNone)
	require.NoError(t, err)

	for _, mode := range []SortMode{SortForERS, SortForPRS} {
		t.Run(map[SortMode]string{SortForERS: "ERS", SortForPRS: "PRS"}[mode], func(t *testing.T) {
			// Input order deliberately puts the behind tablet first to prove the sort
			// reorders on the Executed tiebreak rather than preserving input order.
			tablets := []*topodatapb.Tablet{tabletBehind, tabletCaughtUp}
			positions := []*RelayLogPositions{posBehind, posCaughtUp}

			err := sortTabletsForReparent(tablets, positions, nil, nil, durability, mode)
			require.NoError(t, err)
			require.Equal(t, []*topodatapb.Tablet{tabletCaughtUp, tabletBehind}, tablets,
				"candidate that applied more relay log should sort first")
		})
	}
}

// TestUsableMySQLVersions verifies the guard that disables version-aware
// election when candidates span multiple flavor families (MariaDB vs
// MySQL/Percona), allows a MySQL+Percona mix (same family), and confirms that an
// unknown flavor among otherwise-uniform candidates does not disable it.
func TestUsableMySQLVersions(t *testing.T) {
	v80 := mysqlctl.ServerVersion{Major: 8, Minor: 0, Patch: 35}
	v84 := mysqlctl.ServerVersion{Major: 8, Minor: 4, Patch: 0}

	tests := []struct {
		name     string
		versions []mysqlctl.ServerVersion
		flavors  []mysqlctl.MySQLFlavor
		wantNil  bool
	}{
		{
			name:     "empty versions stays empty",
			versions: nil,
			flavors:  nil,
			wantNil:  true,
		},
		{
			name:     "single flavor is usable",
			versions: []mysqlctl.ServerVersion{v80, v84},
			flavors:  []mysqlctl.MySQLFlavor{mysqlctl.FlavorMySQL, mysqlctl.FlavorMySQL},
			wantNil:  false,
		},
		{
			name:     "MySQL and Percona mix is usable (same family)",
			versions: []mysqlctl.ServerVersion{v80, v84},
			flavors:  []mysqlctl.MySQLFlavor{mysqlctl.FlavorMySQL, mysqlctl.FlavorPercona},
			wantNil:  false,
		},
		{
			name:     "MariaDB alongside MySQL disables version ordering",
			versions: []mysqlctl.ServerVersion{v80, v84},
			flavors:  []mysqlctl.MySQLFlavor{mysqlctl.FlavorMySQL, mysqlctl.FlavorMariaDB},
			wantNil:  true,
		},
		{
			name:     "MariaDB alongside Percona disables version ordering",
			versions: []mysqlctl.ServerVersion{v80, v84},
			flavors:  []mysqlctl.MySQLFlavor{mysqlctl.FlavorPercona, mysqlctl.FlavorMariaDB},
			wantNil:  true,
		},
		{
			name:     "all MariaDB is usable (single family)",
			versions: []mysqlctl.ServerVersion{{Major: 10, Minor: 6}, {Major: 11, Minor: 4}},
			flavors:  []mysqlctl.MySQLFlavor{mysqlctl.FlavorMariaDB, mysqlctl.FlavorMariaDB},
			wantNil:  false,
		},
		{
			name:     "unknown flavor alongside a known flavor is still usable",
			versions: []mysqlctl.ServerVersion{unknownVersion, v80},
			flavors:  []mysqlctl.MySQLFlavor{mysqlctl.FlavorUnknown, mysqlctl.FlavorMySQL},
			wantNil:  false,
		},
		{
			name:     "all unknown flavors is usable (no conflicting known families)",
			versions: []mysqlctl.ServerVersion{unknownVersion, unknownVersion},
			flavors:  []mysqlctl.MySQLFlavor{mysqlctl.FlavorUnknown, mysqlctl.FlavorUnknown},
			wantNil:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := usableMySQLVersions(tt.versions, tt.flavors)
			if tt.wantNil {
				require.Nil(t, got)
			} else {
				require.NotNil(t, got)
			}
		})
	}
}

// TestScopedVersionMap verifies the map-based family guard used on the ERS
// election path (findMostAdvanced, identifyPrimaryCandidate). Returning nil for a
// mixed-family candidate set is what disables version-aware comparison in the
// final candidate selection, not just the intermediate-source sort. Crucially,
// the guard is scoped to the passed candidates: a non-candidate tablet elsewhere
// in the shard must not disable version ordering for the real candidates.
func TestScopedVersionMap(t *testing.T) {
	v80 := mysqlctl.ServerVersion{Major: 8, Minor: 0, Patch: 35}
	v106 := mysqlctl.ServerVersion{Major: 10, Minor: 6}

	tabletA := &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}}
	tabletB := &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}}
	tabletC := &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 102}}

	// A full-shard map where C is a MariaDB tablet, A and B are MySQL/Percona.
	versionMap := map[string]mysqlctl.ServerVersion{
		"zone1-0000000100": v80,
		"zone1-0000000101": v80,
		"zone1-0000000102": v106,
	}
	flavorMap := map[string]mysqlctl.MySQLFlavor{
		"zone1-0000000100": mysqlctl.FlavorMySQL,
		"zone1-0000000101": mysqlctl.FlavorPercona,
		"zone1-0000000102": mysqlctl.FlavorMariaDB,
	}

	tests := []struct {
		name       string
		candidates []*topodatapb.Tablet
		versionMap map[string]mysqlctl.ServerVersion
		flavorMap  map[string]mysqlctl.MySQLFlavor
		wantNil    bool
	}{
		{
			name:       "empty version map stays nil",
			candidates: []*topodatapb.Tablet{tabletA, tabletB},
			versionMap: nil,
			flavorMap:  nil,
			wantNil:    true,
		},
		{
			name:       "same family candidates are usable",
			candidates: []*topodatapb.Tablet{tabletA, tabletB},
			versionMap: versionMap,
			flavorMap:  flavorMap,
			wantNil:    false,
		},
		{
			name:       "mixed family candidates disable version ordering",
			candidates: []*topodatapb.Tablet{tabletA, tabletC},
			versionMap: versionMap,
			flavorMap:  flavorMap,
			wantNil:    true,
		},
		{
			// Point-1 regression: C (MariaDB) is in the shard-wide maps but NOT in
			// the candidate set. The guard must ignore it and keep version ordering
			// for the MySQL-only candidates A and B.
			name:       "non-candidate other-family tablet does not disable ordering",
			candidates: []*topodatapb.Tablet{tabletA, tabletB},
			versionMap: versionMap,
			flavorMap:  flavorMap,
			wantNil:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scopedVersionMap(tt.candidates, tt.versionMap, tt.flavorMap)
			if tt.wantNil {
				require.Nil(t, got)
			} else {
				require.NotNil(t, got)
			}
		})
	}
}
