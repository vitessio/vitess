/*
Copyright 2022 The Vitess Authors.

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

package logic

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

var (
	keyspace = "ks"
	shard    = "0"
	hostname = "localhost"
	cell1    = "zone-1"
	tab100   = &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell1,
			Uid:  100,
		},
		Hostname:      hostname,
		Keyspace:      keyspace,
		Shard:         shard,
		Type:          topodatapb.TabletType_PRIMARY,
		MysqlHostname: hostname,
		MysqlPort:     100,
		PrimaryTermStartTime: &vttime.Time{
			Seconds: 15,
		},
	}
	tab101 = &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell1,
			Uid:  101,
		},
		Hostname:      hostname,
		Keyspace:      keyspace,
		Shard:         shard,
		Type:          topodatapb.TabletType_REPLICA,
		MysqlHostname: hostname,
		MysqlPort:     101,
	}
	tab102 = &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell1,
			Uid:  102,
		},
		Hostname:      hostname,
		Keyspace:      keyspace,
		Shard:         shard,
		Type:          topodatapb.TabletType_RDONLY,
		MysqlHostname: hostname,
		MysqlPort:     102,
	}
	tab103 = &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell1,
			Uid:  103,
		},
		Hostname:      hostname,
		Keyspace:      keyspace,
		Shard:         shard,
		Type:          topodatapb.TabletType_PRIMARY,
		MysqlHostname: hostname,
		MysqlPort:     103,
		PrimaryTermStartTime: &vttime.Time{
			// Higher time than tab100
			Seconds: 3500,
		},
	}
)

func TestRefreshTabletsInKeyspaceShard(t *testing.T) {
	// Store the old flags and restore on test completion
	oldTs := ts
	defer func() {
		ts = oldTs
	}()

	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()

	// Create a memory topo-server and create the keyspace and shard records
	ts = memorytopo.NewServer(cell1)
	_, err := ts.GetOrCreateShard(context.Background(), keyspace, shard)
	require.NoError(t, err)

	// Add tablets to the topo-server
	tablets := []*topodatapb.Tablet{tab100, tab101, tab102}
	for _, tablet := range tablets {
		err := ts.CreateTablet(context.Background(), tablet)
		require.NoError(t, err)
	}

	t.Run("initial call to refreshTabletsInKeyspaceShard", func(t *testing.T) {
		// We expect all 3 tablets to be refreshed since they are being discovered for the first time
		verifyRefreshTabletsInKeyspaceShard(t, false, 3, tablets, nil)
	})

	t.Run("call refreshTabletsInKeyspaceShard again - no force refresh", func(t *testing.T) {
		// We expect no tablets to be refreshed since they are all already upto date
		verifyRefreshTabletsInKeyspaceShard(t, false, 0, tablets, nil)
	})

	t.Run("call refreshTabletsInKeyspaceShard again - force refresh", func(t *testing.T) {
		// We expect all 3 tablets to be refreshed since we requested force refresh
		verifyRefreshTabletsInKeyspaceShard(t, true, 3, tablets, nil)
	})

	t.Run("call refreshTabletsInKeyspaceShard again - force refresh with ignore", func(t *testing.T) {
		// We expect 2 tablets to be refreshed since we requested force refresh, but we are ignoring one of them.
		verifyRefreshTabletsInKeyspaceShard(t, true, 2, tablets, []string{topoproto.TabletAliasString(tab100.Alias)})
	})

	t.Run("tablet shutdown removes mysql hostname and port. We shouldn't forget the tablet", func(t *testing.T) {
		startPort := tab100.MysqlPort
		startHostname := tab100.MysqlHostname
		defer func() {
			tab100.MysqlPort = startPort
			tab100.MysqlHostname = startHostname
			_, err = ts.UpdateTabletFields(context.Background(), tab100.Alias, func(tablet *topodatapb.Tablet) error {
				tablet.MysqlHostname = startHostname
				tablet.MysqlPort = startPort
				return nil
			})
		}()
		// Let's assume tab100 shutdown. This would clear its tablet hostname and port.
		tab100.MysqlPort = 0
		tab100.MysqlHostname = ""
		_, err = ts.UpdateTabletFields(context.Background(), tab100.Alias, func(tablet *topodatapb.Tablet) error {
			tablet.MysqlHostname = ""
			tablet.MysqlPort = 0
			return nil
		})
		require.NoError(t, err)
		// tab100 shouldn't be forgotten
		verifyRefreshTabletsInKeyspaceShard(t, false, 1, tablets, nil)
	})

	t.Run("change a tablet and call refreshTabletsInKeyspaceShard again", func(t *testing.T) {
		startTimeInitially := tab100.PrimaryTermStartTime.Seconds
		defer func() {
			tab100.PrimaryTermStartTime.Seconds = startTimeInitially
			_, err = ts.UpdateTabletFields(context.Background(), tab100.Alias, func(tablet *topodatapb.Tablet) error {
				tablet.PrimaryTermStartTime.Seconds = startTimeInitially
				return nil
			})
		}()
		tab100.PrimaryTermStartTime.Seconds = 1000
		_, err = ts.UpdateTabletFields(context.Background(), tab100.Alias, func(tablet *topodatapb.Tablet) error {
			tablet.PrimaryTermStartTime.Seconds = 1000
			return nil
		})
		require.NoError(t, err)
		// We expect 1 tablet to be refreshed since that is the only one that has changed
		verifyRefreshTabletsInKeyspaceShard(t, false, 1, tablets, nil)
	})

	t.Run("change the port and call refreshTabletsInKeyspaceShard again", func(t *testing.T) {
		defer func() {
			_, err = ts.UpdateTabletFields(context.Background(), tab100.Alias, func(tablet *topodatapb.Tablet) error {
				tablet.MysqlPort = 100
				return nil
			})
			tab100.MysqlPort = 100
		}()
		// Let's assume tab100 restarted on a different pod. This would change its tablet hostname and port
		_, err = ts.UpdateTabletFields(context.Background(), tab100.Alias, func(tablet *topodatapb.Tablet) error {
			tablet.MysqlPort = 39293
			return nil
		})
		require.NoError(t, err)
		tab100.MysqlPort = 39293
		// We expect 1 tablet to be refreshed since that is the only one that has changed
		// Also the old tablet should be forgotten
		verifyRefreshTabletsInKeyspaceShard(t, false, 1, tablets, nil)
	})
}

func TestShardPrimary(t *testing.T) {
	testcases := []*struct {
		name            string
		tablets         []*topodatapb.Tablet
		expectedPrimary *topodatapb.Tablet
		expectedErr     string
	}{
		{
			name:            "One primary type tablet",
			tablets:         []*topodatapb.Tablet{tab100, tab101, tab102},
			expectedPrimary: tab100,
		}, {
			name:    "Two primary type tablets",
			tablets: []*topodatapb.Tablet{tab100, tab101, tab102, tab103},
			// In this case we expect the tablet with higher PrimaryTermStartTime to be the primary tablet
			expectedPrimary: tab103,
		}, {
			name:        "No primary type tablets",
			tablets:     []*topodatapb.Tablet{tab101, tab102},
			expectedErr: "no primary tablet found",
		},
	}

	oldTs := ts
	defer func() {
		ts = oldTs
	}()

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
			defer func() {
				db.ClearVTOrcDatabase()
			}()

			// Create a memory topo-server and create the keyspace and shard records
			ts = memorytopo.NewServer(cell1)
			_, err := ts.GetOrCreateShard(context.Background(), keyspace, shard)
			require.NoError(t, err)

			// Add tablets to the topo-server
			for _, tablet := range testcase.tablets {
				err := ts.CreateTablet(context.Background(), tablet)
				require.NoError(t, err)
			}

			// refresh the tablet info so that they are stored in the orch backend
			verifyRefreshTabletsInKeyspaceShard(t, false, len(testcase.tablets), testcase.tablets, nil)

			primary, err := shardPrimary(keyspace, shard)
			if testcase.expectedErr != "" {
				assert.Contains(t, err.Error(), testcase.expectedErr)
				assert.Nil(t, primary)
			} else {
				assert.NoError(t, err)
				diff := cmp.Diff(primary, testcase.expectedPrimary, cmp.Comparer(proto.Equal))
				assert.Empty(t, diff)
			}
		})
	}
}

// verifyRefreshTabletsInKeyspaceShard calls refreshTabletsInKeyspaceShard with the forceRefresh parameter provided and verifies that
// the number of instances refreshed matches the parameter and all the tablets match the ones provided
func verifyRefreshTabletsInKeyspaceShard(t *testing.T, forceRefresh bool, instanceRefreshRequired int, tablets []*topodatapb.Tablet, tabletsToIgnore []string) {
	var instancesRefreshed atomic.Int32
	instancesRefreshed.Store(0)
	// call refreshTabletsInKeyspaceShard while counting all the instances that are refreshed
	refreshTabletsInKeyspaceShard(context.Background(), keyspace, shard, func(string) {
		instancesRefreshed.Add(1)
	}, forceRefresh, tabletsToIgnore)
	// Verify that all the tablets are present in the database
	for _, tablet := range tablets {
		verifyTabletInfo(t, tablet, "")
	}
	verifyTabletCount(t, len(tablets))
	// Verify that refresh as many tablets as expected
	assert.EqualValues(t, instanceRefreshRequired, instancesRefreshed.Load())
}

// verifyTabletInfo verifies that the tablet information read from the vtorc database
// is the same as the one provided or reading it gives the same error as expected
func verifyTabletInfo(t *testing.T, tabletWanted *topodatapb.Tablet, errString string) {
	t.Helper()
	tabletAlias := topoproto.TabletAliasString(tabletWanted.Alias)
	tablet, err := inst.ReadTablet(tabletAlias)
	if errString != "" {
		assert.EqualError(t, err, errString)
	} else {
		assert.NoError(t, err)
		assert.EqualValues(t, tabletAlias, topoproto.TabletAliasString(tablet.Alias))
		diff := cmp.Diff(tablet, tabletWanted, cmp.Comparer(proto.Equal))
		assert.Empty(t, diff)
	}
}

// verifyTabletCount verifies that the number of tablets in the vitess_tablet table match the given count
func verifyTabletCount(t *testing.T, countWanted int) {
	t.Helper()
	totalTablets := 0
	err := db.QueryVTOrc("select count(*) as total_tablets from vitess_tablet", nil, func(rowMap sqlutils.RowMap) error {
		totalTablets = rowMap.GetInt("total_tablets")
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, countWanted, totalTablets)
}
