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

	"vitess.io/vitess/go/mysql"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestReparentSorter tests that the sorting for tablets works correctly
func TestReparentSorter(t *testing.T) {
	sid1 := mysql.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := mysql.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
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
	tabletRdonly1_102 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell1,
			Uid:  102,
		},
		Type: topodatapb.TabletType_RDONLY,
	}

	mysqlGTID1 := mysql.Mysql56GTID{
		Server:   sid1,
		Sequence: 9,
	}
	mysqlGTID2 := mysql.Mysql56GTID{
		Server:   sid2,
		Sequence: 10,
	}
	mysqlGTID3 := mysql.Mysql56GTID{
		Server:   sid1,
		Sequence: 11,
	}

	positionMostAdvanced := mysql.Position{GTIDSet: mysql.Mysql56GTIDSet{}}
	positionMostAdvanced.GTIDSet = positionMostAdvanced.GTIDSet.AddGTID(mysqlGTID1)
	positionMostAdvanced.GTIDSet = positionMostAdvanced.GTIDSet.AddGTID(mysqlGTID2)
	positionMostAdvanced.GTIDSet = positionMostAdvanced.GTIDSet.AddGTID(mysqlGTID3)

	positionEmpty := mysql.Position{GTIDSet: mysql.Mysql56GTIDSet{}}

	positionIntermediate1 := mysql.Position{GTIDSet: mysql.Mysql56GTIDSet{}}
	positionIntermediate1.GTIDSet = positionIntermediate1.GTIDSet.AddGTID(mysqlGTID1)

	positionIntermediate2 := mysql.Position{GTIDSet: mysql.Mysql56GTIDSet{}}
	positionIntermediate2.GTIDSet = positionIntermediate2.GTIDSet.AddGTID(mysqlGTID1)
	positionIntermediate2.GTIDSet = positionIntermediate2.GTIDSet.AddGTID(mysqlGTID2)

	testcases := []struct {
		name          string
		tablets       []*topodatapb.Tablet
		positions     []mysql.Position
		containsErr   string
		sortedTablets []*topodatapb.Tablet
	}{
		{
			name:          "all advanced, sort via promotion rules",
			tablets:       []*topodatapb.Tablet{nil, tabletReplica1_100, tabletRdonly1_102},
			positions:     []mysql.Position{positionMostAdvanced, positionMostAdvanced, positionMostAdvanced},
			sortedTablets: []*topodatapb.Tablet{tabletReplica1_100, tabletRdonly1_102, nil},
		}, {
			name:          "ordering by position",
			tablets:       []*topodatapb.Tablet{tabletReplica1_101, tabletReplica2_100, tabletReplica1_100, tabletRdonly1_102},
			positions:     []mysql.Position{positionEmpty, positionIntermediate1, positionIntermediate2, positionMostAdvanced},
			sortedTablets: []*topodatapb.Tablet{tabletRdonly1_102, tabletReplica1_100, tabletReplica2_100, tabletReplica1_101},
		}, {
			name:        "tablets and positions count error",
			tablets:     []*topodatapb.Tablet{tabletReplica1_101, tabletReplica2_100},
			positions:   []mysql.Position{positionEmpty, positionIntermediate1, positionMostAdvanced},
			containsErr: "unequal number of tablets and positions",
		}, {
			name:          "promotion rule check",
			tablets:       []*topodatapb.Tablet{tabletReplica1_101, tabletRdonly1_102},
			positions:     []mysql.Position{positionMostAdvanced, positionMostAdvanced},
			sortedTablets: []*topodatapb.Tablet{tabletReplica1_101, tabletRdonly1_102},
		}, {
			name:          "mixed",
			tablets:       []*topodatapb.Tablet{tabletReplica1_101, tabletReplica2_100, tabletReplica1_100, tabletRdonly1_102},
			positions:     []mysql.Position{positionEmpty, positionIntermediate1, positionMostAdvanced, positionIntermediate1},
			sortedTablets: []*topodatapb.Tablet{tabletReplica1_100, tabletReplica2_100, tabletRdonly1_102, tabletReplica1_101},
		},
	}

	durability, err := GetDurabilityPolicy("none")
	require.NoError(t, err)
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			err := sortTabletsForReparent(testcase.tablets, testcase.positions, durability)
			if testcase.containsErr != "" {
				require.EqualError(t, err, testcase.containsErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, testcase.sortedTablets, testcase.tablets)
			}
		})
	}
}
