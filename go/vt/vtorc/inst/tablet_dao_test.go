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

// TestReadPrimaryAliasesByShard covers the batched lookup used by gossip quorum analysis.
func TestReadPrimaryAliasesByShard(t *testing.T) {
	defer db.ClearVTOrcDatabase()

	require.NoError(t, SaveTablet(&topodatapb.Tablet{
		Alias:                &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Keyspace:             "ks",
		Shard:                "0",
		Type:                 topodatapb.TabletType_PRIMARY,
		PrimaryTermStartTime: &vttime.Time{Seconds: 1000},
	}))
	require.NoError(t, SaveTablet(&topodatapb.Tablet{
		Alias:                &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
		Keyspace:             "ks",
		Shard:                "0",
		Type:                 topodatapb.TabletType_PRIMARY,
		PrimaryTermStartTime: &vttime.Time{Seconds: 2000},
	}))
	require.NoError(t, SaveTablet(&topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 102},
		Keyspace: "ks",
		Shard:    "0",
		Type:     topodatapb.TabletType_REPLICA,
	}))
	require.NoError(t, SaveTablet(&topodatapb.Tablet{
		Alias:                &topodatapb.TabletAlias{Cell: "zone1", Uid: 200},
		Keyspace:             "ks",
		Shard:                "-80",
		Type:                 topodatapb.TabletType_PRIMARY,
		PrimaryTermStartTime: &vttime.Time{Seconds: 3000},
	}))

	aliasesByShard, err := ReadPrimaryAliasesByShard()
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"ks/-80": "zone1-0000000200",
		"ks/0":   "zone1-0000000101",
	}, aliasesByShard)
}
