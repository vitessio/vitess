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

package inst

import (
	"testing"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtorc/db"
)

func testRequireTabletAliasEqual(t *testing.T, expected, got *topodatapb.TabletAlias) {
	require.True(t, topoproto.TabletAliasEqual(expected, got), "expected %v, got %v", expected, got)
}

func TestSaveAndReadTablet(t *testing.T) {
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()

	tests := []struct {
		name         string
		tabletAlias  *topodatapb.TabletAlias
		tablet       *topodatapb.Tablet
		tabletWanted *topodatapb.Tablet
		err          string
	}{
		{
			name:        "Success with primary type",
			tabletAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Hostname:      "localhost",
				Keyspace:      "ks",
				Shard:         "0",
				Type:          topodatapb.TabletType_PRIMARY,
				MysqlHostname: "localhost",
				MysqlPort:     1030,
				PrimaryTermStartTime: &vttime.Time{
					Seconds:     1000,
					Nanoseconds: 387,
				},
			},
			tabletWanted: nil,
		}, {
			name:        "Success with replica type",
			tabletAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
				Hostname:      "localhost",
				Keyspace:      "ks",
				Shard:         "0",
				Type:          topodatapb.TabletType_REPLICA,
				MysqlHostname: "localhost",
				MysqlPort:     1030,
			},
			tabletWanted: nil,
		}, {
			name:         "No tablet found",
			tabletAlias:  &topodatapb.TabletAlias{Cell: "zone1", Uid: 190734},
			tablet:       nil,
			tabletWanted: nil,
			err:          ErrTabletAliasNil.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.tabletWanted == nil {
				tt.tabletWanted = tt.tablet
			}

			if tt.tablet != nil {
				err := SaveTablet(tt.tablet)
				require.NoError(t, err)
			}

			readTable, err := ReadTablet(tt.tabletAlias)
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.True(t, topotools.TabletEquality(tt.tabletWanted, readTable))
			testRequireTabletAliasEqual(t, tt.tabletAlias, readTable.Alias)
		})
	}
}

func TestReadTabletCountsByCell(t *testing.T) {
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()

	for i := range 100 {
		require.NoError(t, SaveTablet(&topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "cell1",
				Uid:  uint32(i),
			},
			Keyspace: "test",
			Shard:    "-",
		}))
	}
	tabletCounts, err := ReadTabletCountsByCell()
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"cell1": 100}, tabletCounts)
}
