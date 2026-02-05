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
		},
	}

	durability, err := policy.GetDurabilityPolicy(policy.DurabilityNone)
	require.NoError(t, err)
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			err := sortTabletsForReparent(testcase.tablets, testcase.positions, testcase.innodbBufferPool, durability)
			if testcase.containsErr != "" {
				require.EqualError(t, err, testcase.containsErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, testcase.sortedTablets, testcase.tablets)
			}
		})
	}
}
