package topotools

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type fakeTMClient struct {
	tmclient.TabletManagerClient
	tabletReplicationPositions map[string]*replicationdatapb.Status
}

func (fake *fakeTMClient) ReplicationStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	if fake.tabletReplicationPositions == nil {
		return nil, assert.AnError
	}

	if tablet.Alias == nil {
		return nil, assert.AnError
	}

	if pos, ok := fake.tabletReplicationPositions[topoproto.TabletAliasString(tablet.Alias)]; ok {
		return pos, nil
	}

	return nil, assert.AnError
}

func TestMaxReplicationPositionSearcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		positions map[string]*replicationdatapb.Status
		tablets   []*topodatapb.Tablet
		expected  *topodatapb.Tablet
	}{
		{
			name: "success",
			positions: map[string]*replicationdatapb.Status{
				"zone1-0000000100": {
					Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
				},
				"zone1-0000000101": {
					Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:23-28",
				},
				"zone1-0000000102": {
					Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:23-30",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
				},
			},
			expected: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  102,
				},
			},
		},
		{
			name: "reverse order",
			positions: map[string]*replicationdatapb.Status{
				"zone1-0000000100": {
					Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
				},
				"zone1-0000000101": {
					Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:23-28",
				},
				"zone1-0000000102": {
					Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:23-30",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
			},
			expected: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  102,
				},
			},
		},
		{
			name: "no position for tablet is ignored",
			positions: map[string]*replicationdatapb.Status{
				"zone1-0000000100": {
					Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
				},
			},
			expected: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  100,
				},
			},
		},
		{
			name: "bad position is ignored",
			positions: map[string]*replicationdatapb.Status{
				"zone1-0000000100": {
					Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
				},
				"zone1-0000000101": {
					Position: "MySQL56/3E11FA47-71CA-11E1-9E33-C80AA9429562:23-28",
				},
				"zone1-0000000102": {
					Position: "junk position",
				},
			},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  100,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  101,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "zone1",
						Uid:  102,
					},
				},
			},
			expected: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  101,
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			tmc := &fakeTMClient{
				tabletReplicationPositions: tt.positions,
			}
			searcher := NewMaxReplicationPositionSearcher(tmc, logutil.NewMemoryLogger(), time.Millisecond*50)

			for _, tablet := range tt.tablets {
				searcher.ProcessTablet(ctx, tablet)
			}

			assert.Equal(t, tt.expected, searcher.MaxPositionTablet())
		})
	}
}
