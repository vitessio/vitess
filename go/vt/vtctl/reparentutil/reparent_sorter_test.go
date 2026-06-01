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
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := sortTabletsForReparent(tc.tablets, tc.positions, nil, tc.mysqlVersions, durability, tc.mode)
			require.NoError(t, err)
			require.Equal(t, tc.sortedTablets, tc.tablets)
		})
	}
}
