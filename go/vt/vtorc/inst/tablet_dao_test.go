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

func TestSaveAndReadTablet(t *testing.T) {
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()

	tests := []struct {
		name         string
		tabletAlias  string
		tablet       *topodatapb.Tablet
		tabletWanted *topodatapb.Tablet
		err          string
	}{
		{
			name:        "Success with primary type",
			tabletAlias: "zone1-0000000100",
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
			tabletAlias: "zone1-0000000100",
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
			tabletAlias:  "zone1-190734",
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
			require.Equal(t, tt.tabletAlias, topoproto.TabletAliasString(readTable.Alias))
		})
	}
}
