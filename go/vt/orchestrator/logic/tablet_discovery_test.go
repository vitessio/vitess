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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo/memorytopo"
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

	// Open the orchestrator
	// After the test completes delete everything from the vitess_tablet table
	orcDb, err := db.OpenOrchestrator()
	require.NoError(t, err)
	defer func() {
		_, err = orcDb.Exec("delete from vitess_tablet")
		require.NoError(t, err)
	}()

	// Create a memory topo-server and create the keyspace and shard records
	ts = memorytopo.NewServer(cell1)
	_, err = ts.GetOrCreateShard(context.Background(), keyspace, shard)
	require.NoError(t, err)

	// Add tablets to the topo-server
	tablets := []*topodatapb.Tablet{tab100, tab101, tab102}
	for _, tablet := range tablets {
		err := ts.CreateTablet(context.Background(), tablet)
		require.NoError(t, err)
	}

	t.Run("initial call to refreshTabletsInKeyspaceShard", func(t *testing.T) {
		// We expect all 3 tablets to be refreshed since they are being discovered for the first time
		verifyRefreshTabletsInKeyspaceShard(t, false, 3, tablets)
	})

	t.Run("call refreshTabletsInKeyspaceShard again - no force refresh", func(t *testing.T) {
		// We expect no tablets to be refreshed since they are all already upto date
		verifyRefreshTabletsInKeyspaceShard(t, false, 0, tablets)
	})

	t.Run("call refreshTabletsInKeyspaceShard again - force refresh", func(t *testing.T) {
		// We expect all 3 tablets to be refreshed since we requested force refresh
		verifyRefreshTabletsInKeyspaceShard(t, true, 3, tablets)
	})

	t.Run("change a tablet and call refreshTabletsInKeyspaceShard again", func(t *testing.T) {
		startTimeInitially := tab100.PrimaryTermStartTime.Seconds
		defer func() {
			tab100.PrimaryTermStartTime.Seconds = startTimeInitially
		}()
		tab100.PrimaryTermStartTime.Seconds = 1000
		ts.UpdateTabletFields(context.Background(), tab100.Alias, func(tablet *topodatapb.Tablet) error {
			tablet.PrimaryTermStartTime.Seconds = 1000
			return nil
		})
		// We expect 1 tablet to be refreshed since that is the only one that has changed
		verifyRefreshTabletsInKeyspaceShard(t, false, 1, tablets)
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

	// Open the orchestrator
	// After the test completes delete everything from the vitess_tablet table
	orcDb, err := db.OpenOrchestrator()
	require.NoError(t, err)
	defer func() {
		_, err = orcDb.Exec("delete from vitess_tablet")
		require.NoError(t, err)
	}()

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			_, err = orcDb.Exec("delete from vitess_tablet")

			// Create a memory topo-server and create the keyspace and shard records
			ts = memorytopo.NewServer(cell1)
			_, err = ts.GetOrCreateShard(context.Background(), keyspace, shard)
			require.NoError(t, err)

			// Add tablets to the topo-server
			for _, tablet := range testcase.tablets {
				err := ts.CreateTablet(context.Background(), tablet)
				require.NoError(t, err)
			}

			// refresh the tablet info so that they are stored in the orch backend
			verifyRefreshTabletsInKeyspaceShard(t, false, len(testcase.tablets), testcase.tablets)

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
func verifyRefreshTabletsInKeyspaceShard(t *testing.T, forceRefresh bool, instanceRefreshRequired int, tablets []*topodatapb.Tablet) {
	instancesRefreshed := 0
	// call refreshTabletsInKeyspaceShard while counting all the instances that are refreshed
	refreshTabletsInKeyspaceShard(context.Background(), keyspace, shard, func(instanceKey *inst.InstanceKey) {
		instancesRefreshed++
	}, forceRefresh)
	// Verify that all the tablets are present in the database
	for _, tablet := range tablets {
		verifyTabletInfo(t, tablet, "")
	}
	// Verify that refresh as many tablets as expected
	assert.EqualValues(t, instanceRefreshRequired, instancesRefreshed)
}

// verifyTabletInfo verifies that the tablet information read from the orchestrator database
// is the same as the one provided or reading it gives the same error as expected
func verifyTabletInfo(t *testing.T, tabletWanted *topodatapb.Tablet, errString string) {
	t.Helper()
	tabletKey := inst.InstanceKey{
		Hostname: hostname,
		Port:     int(tabletWanted.MysqlPort),
	}
	tablet, err := inst.ReadTablet(tabletKey)
	if errString != "" {
		assert.EqualError(t, err, errString)
	} else {
		assert.NoError(t, err)
		assert.EqualValues(t, tabletKey.Port, tablet.MysqlPort)
		diff := cmp.Diff(tablet, tabletWanted, cmp.Comparer(proto.Equal))
		assert.Empty(t, diff)
	}
}
