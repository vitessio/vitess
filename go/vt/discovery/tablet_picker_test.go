/*
Copyright 2019 The Vitess Authors.
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

package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestPickPrimary(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell", "otherCell"})
	want := addTablet(te, 100, topodatapb.TabletType_PRIMARY, "cell", true, true)
	defer deleteTablet(t, te, want)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := te.topoServ.UpdateShardFields(ctx, te.keyspace, te.shard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = want.Alias
		return nil
	})
	require.NoError(t, err)

	tp, err := NewTabletPicker(context.Background(), te.topoServ, []string{"otherCell"}, "cell", te.keyspace, te.shard, "primary", TabletPickerOptions{})
	require.NoError(t, err)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	tablet, err := tp.PickForStreaming(ctx2)
	require.NoError(t, err)
	assert.True(t, proto.Equal(want, tablet), "Pick: %v, want %v", tablet, want)
}

func TestPickLocalPreferences(t *testing.T) {
	type tablet struct {
		id   uint32
		typ  topodatapb.TabletType
		cell string
	}

	type testCase struct {
		name string

		//inputs
		tablets       []tablet
		envCells      []string
		inCells       []string
		localCell     string
		inTabletTypes string
		options       TabletPickerOptions

		//expected
		tpCells     []string
		wantTablets []uint32
	}

	tcases := []testCase{
		{
			name: "pick simple",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "cell"},
			},
			envCells:      []string{"cell"},
			inCells:       []string{"cell"},
			localCell:     "cell",
			inTabletTypes: "replica",
			options:       TabletPickerOptions{},
			tpCells:       []string{"cell", "cella"},
			wantTablets:   []uint32{100},
		}, {
			name: "pick from two healthy",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "cell"},
				{101, topodatapb.TabletType_RDONLY, "cell"},
			},
			envCells:      []string{"cell"},
			inCells:       []string{"cell"},
			localCell:     "cell",
			inTabletTypes: "replica,rdonly",
			options:       TabletPickerOptions{},
			tpCells:       []string{"cell", "cella"},
			wantTablets:   []uint32{100, 101},
		}, {
			name: "pick in order replica",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "cell"},
				{101, topodatapb.TabletType_RDONLY, "cell"},
			},
			envCells:      []string{"cell"},
			inCells:       []string{"cell"},
			localCell:     "cell",
			inTabletTypes: "in_order:replica,rdonly",
			options:       TabletPickerOptions{},
			tpCells:       []string{"cell", "cella"},
			wantTablets:   []uint32{100},
		}, {
			name: "pick in order rdonly",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "cell"},
				{101, topodatapb.TabletType_RDONLY, "cell"},
			},
			envCells:      []string{"cell"},
			inCells:       []string{"cell"},
			localCell:     "cell",
			inTabletTypes: "in_order:rdonly,replica",
			options:       TabletPickerOptions{},
			tpCells:       []string{"cell", "cella"},
			wantTablets:   []uint32{101},
		}, {
			name: "pick in order multiple in group",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "cell"},
				{101, topodatapb.TabletType_RDONLY, "cell"},
				{102, topodatapb.TabletType_RDONLY, "cell"},
				{103, topodatapb.TabletType_RDONLY, "cell"},
			},
			envCells:      []string{"cell"},
			inCells:       []string{"cell"},
			localCell:     "cell",
			inTabletTypes: "in_order:rdonly,replica",
			options:       TabletPickerOptions{},
			tpCells:       []string{"cell", "cella"},
			wantTablets:   []uint32{101, 102, 103},
		}, {
			// Same test as above, except the in order preference is passed via the new TabletPickerOptions param.
			// This will replace the above test when we deprecate the "in_order" hint in the tabletTypeStr
			name: "pick in order multiple in group with new picker option",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "cell"},
				{101, topodatapb.TabletType_RDONLY, "cell"},
				{102, topodatapb.TabletType_RDONLY, "cell"},
				{103, topodatapb.TabletType_RDONLY, "cell"},
			},
			envCells:      []string{"cell"},
			inCells:       []string{"cell"},
			localCell:     "cell",
			inTabletTypes: "rdonly,replica",
			options:       TabletPickerOptions{TabletOrder: "InOrder"},
			tpCells:       []string{"cell", "cella"},
			wantTablets:   []uint32{101, 102, 103},
		}, {
			name: "picker respects tablet type",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "cell"},
				{101, topodatapb.TabletType_PRIMARY, "cell"},
			},
			envCells:      []string{"cell"},
			inCells:       []string{"cell"},
			localCell:     "cell",
			inTabletTypes: "replica,rdonly",
			options:       TabletPickerOptions{},
			tpCells:       []string{"cell", "cella"},
			wantTablets:   []uint32{100},
		}, {
			name: "pick multi cell",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "cell"},
			},
			envCells:      []string{"cell", "otherCell"},
			inCells:       []string{"cell", "otherCell"},
			localCell:     "cell",
			inTabletTypes: "replica",
			options:       TabletPickerOptions{},
			tpCells:       []string{"cell", "otherCell", "cella"},
			wantTablets:   []uint32{100},
		}, {
			name: "pick from other cell",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "otherCell"},
			},
			envCells:      []string{"cell", "otherCell"},
			inCells:       []string{"cell", "otherCell"},
			localCell:     "cell",
			inTabletTypes: "replica",
			options:       TabletPickerOptions{},
			tpCells:       []string{"cell", "otherCell", "cella"},
			wantTablets:   []uint32{100},
		}, {
			name: "don't pick from other cell",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "cell"},
				{101, topodatapb.TabletType_REPLICA, "otherCell"},
			},
			envCells:      []string{"cell", "otherCell"},
			inCells:       []string{"cell"},
			localCell:     "cell",
			inTabletTypes: "replica",
			options:       TabletPickerOptions{},
			tpCells:       []string{"cell", "cella"},
			wantTablets:   []uint32{100},
		}, {
			name: "multi cell two tablets, local preference default",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "cell"},
				{101, topodatapb.TabletType_REPLICA, "otherCell"},
			},
			envCells:      []string{"cell", "otherCell"},
			inCells:       []string{"cell", "otherCell"},
			localCell:     "cell",
			inTabletTypes: "replica",
			options:       TabletPickerOptions{},
			tpCells:       []string{"cell", "otherCell", "cella"},
			wantTablets:   []uint32{100},
		}, {
			name: "multi cell two tablets, only specified cells",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "cell"},
				{101, topodatapb.TabletType_REPLICA, "otherCell"},
			},
			envCells:      []string{"cell", "otherCell"},
			inCells:       []string{"cell", "otherCell"},
			localCell:     "cell",
			inTabletTypes: "replica",
			options:       TabletPickerOptions{CellPreference: "OnlySpecified"},
			tpCells:       []string{"cell", "otherCell"},
			wantTablets:   []uint32{100, 101},
		}, {
			name: "multi cell two tablet types, local preference default",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "cell"},
				{101, topodatapb.TabletType_RDONLY, "otherCell"},
			},
			envCells:      []string{"cell", "otherCell"},
			inCells:       []string{"cell", "otherCell"},
			localCell:     "cell",
			inTabletTypes: "replica,rdonly",
			options:       TabletPickerOptions{},
			tpCells:       []string{"cell", "otherCell", "cella"},
			wantTablets:   []uint32{100},
		}, {
			name: "multi cell two tablet types, only specified cells",
			tablets: []tablet{
				{100, topodatapb.TabletType_REPLICA, "cell"},
				{101, topodatapb.TabletType_RDONLY, "otherCell"},
			},
			envCells:      []string{"cell", "otherCell"},
			inCells:       []string{"cell", "otherCell"},
			localCell:     "cell",
			inTabletTypes: "replica,rdonly",
			options:       TabletPickerOptions{CellPreference: "OnlySpecified"},
			tpCells:       []string{"cell", "otherCell"},
			wantTablets:   []uint32{100, 101},
		},
	}

	ctx := context.Background()
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			te := newPickerTestEnv(t, tcase.envCells)
			var testTablets []*topodatapb.Tablet
			for _, tab := range tcase.tablets {
				testTablets = append(testTablets, addTablet(te, int(tab.id), tab.typ, tab.cell, true, true))
			}
			defer func() {
				for _, tab := range testTablets {
					deleteTablet(t, te, tab)
				}
			}()
			tp, err := NewTabletPicker(context.Background(), te.topoServ, tcase.inCells, tcase.localCell, te.keyspace, te.shard, tcase.inTabletTypes, tcase.options)
			require.NoError(t, err)
			require.Equal(t, tp.localCellInfo.localCell, tcase.localCell)
			require.ElementsMatch(t, tp.cells, tcase.tpCells)

			var selectedTablets []uint32
			selectedTabletMap := make(map[uint32]bool)
			for i := 0; i < 40; i++ {
				tab, err := tp.PickForStreaming(ctx)
				require.NoError(t, err)
				selectedTabletMap[tab.Alias.Uid] = true
			}
			for uid := range selectedTabletMap {
				selectedTablets = append(selectedTablets, uid)
			}
			require.ElementsMatch(t, selectedTablets, tcase.wantTablets)
		})
	}
}

func TestPickCellPreferenceLocalCell(t *testing.T) {
	// test env puts all cells into an alias called "cella"
	te := newPickerTestEnv(t, []string{"cell", "otherCell"})
	want1 := addTablet(te, 100, topodatapb.TabletType_REPLICA, "cell", true, true)
	defer deleteTablet(t, te, want1)

	// Local cell preference is default
	tp, err := NewTabletPicker(context.Background(), te.topoServ, []string{"cella"}, "cell", te.keyspace, te.shard, "replica", TabletPickerOptions{})
	require.NoError(t, err)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel1()
	tablet, err := tp.PickForStreaming(ctx1)
	require.NoError(t, err)
	assert.True(t, proto.Equal(want1, tablet), "Pick: %v, want %v", tablet, want1)

	// create a tablet in the other cell
	want2 := addTablet(te, 101, topodatapb.TabletType_REPLICA, "otherCell", true, true)
	defer deleteTablet(t, te, want2)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()

	// In 20 attempts, only tablet in "cell" will be picked because we give local cell priority by default
	var picked1, picked2 bool
	for i := 0; i < 20; i++ {
		tablet, err := tp.PickForStreaming(ctx2)
		require.NoError(t, err)
		if proto.Equal(tablet, want1) {
			picked1 = true
		}
		if proto.Equal(tablet, want2) {
			picked2 = true
		}
	}
	assert.True(t, picked1)
	assert.False(t, picked2)
}

func TestPickCellPreferenceLocalAlias(t *testing.T) {
	// test env puts all cells into an alias called "cella"
	te := newPickerTestEnv(t, []string{"cell", "otherCell"})
	tp, err := NewTabletPicker(context.Background(), te.topoServ, []string{"cella"}, "cell", te.keyspace, te.shard, "replica", TabletPickerOptions{})
	require.NoError(t, err)

	// create a tablet in the other cell, it should be picked
	want := addTablet(te, 101, topodatapb.TabletType_REPLICA, "otherCell", true, true)
	defer deleteTablet(t, te, want)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	tablet, err := tp.PickForStreaming(ctx)
	require.NoError(t, err)
	assert.True(t, proto.Equal(want, tablet), "Pick: %v, want %v", tablet, want)
}

func TestPickUsingCellAliasOnlySpecified(t *testing.T) {
	// test env puts all cells into an alias called "cella"
	te := newPickerTestEnv(t, []string{"cell", "otherCell"})
	want1 := addTablet(te, 100, topodatapb.TabletType_REPLICA, "cell", true, true)
	defer deleteTablet(t, te, want1)

	tp, err := NewTabletPicker(context.Background(), te.topoServ, []string{"cella"}, "cell", te.keyspace, te.shard, "replica", TabletPickerOptions{CellPreference: "OnlySpecified"})
	require.NoError(t, err)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel1()
	tablet, err := tp.PickForStreaming(ctx1)
	require.NoError(t, err)
	assert.True(t, proto.Equal(want1, tablet), "Pick: %v, want %v", tablet, want1)

	// create a tablet in the other cell, it should be picked
	deleteTablet(t, te, want1)
	want2 := addTablet(te, 101, topodatapb.TabletType_REPLICA, "otherCell", true, true)
	defer deleteTablet(t, te, want2)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	tablet, err = tp.PickForStreaming(ctx2)
	require.NoError(t, err)
	assert.True(t, proto.Equal(want2, tablet), "Pick: %v, want %v", tablet, want2)

	// addTablet again and test that both are picked at least once
	want1 = addTablet(te, 100, topodatapb.TabletType_REPLICA, "cell", true, true)
	ctx3, cancel3 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel3()

	// In 20 attempts each of the tablets should get picked at least once.
	// Local cell is not given preference
	var picked1, picked2 bool
	for i := 0; i < 20; i++ {
		tablet, err := tp.PickForStreaming(ctx3)
		require.NoError(t, err)
		if proto.Equal(tablet, want1) {
			picked1 = true
		}
		if proto.Equal(tablet, want2) {
			picked2 = true
		}
	}
	assert.True(t, picked1)
	assert.True(t, picked2)
}

func TestTabletAppearsDuringSleep(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell"})
	tp, err := NewTabletPicker(context.Background(), te.topoServ, te.cells, "cell", te.keyspace, te.shard, "replica", TabletPickerOptions{})
	require.NoError(t, err)

	delay := GetTabletPickerRetryDelay()
	defer func() {
		SetTabletPickerRetryDelay(delay)
	}()
	SetTabletPickerRetryDelay(11 * time.Millisecond)

	result := make(chan *topodatapb.Tablet)
	// start picker first, then add tablet
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		tablet, err := tp.PickForStreaming(ctx)
		assert.NoError(t, err)
		result <- tablet
	}()

	want := addTablet(te, 100, topodatapb.TabletType_REPLICA, "cell", true, true)
	defer deleteTablet(t, te, want)
	got := <-result
	require.NotNil(t, got, "Tablet should not be nil")
	assert.True(t, proto.Equal(want, got), "Pick: %v, want %v", got, want)
}

func TestPickErrorLocalPreferenceDefault(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell"})
	_, err := NewTabletPicker(context.Background(), te.topoServ, te.cells, "cell", te.keyspace, te.shard, "badtype", TabletPickerOptions{})
	assert.EqualError(t, err, "failed to parse list of tablet types: badtype")

	tp, err := NewTabletPicker(context.Background(), te.topoServ, te.cells, "cell", te.keyspace, te.shard, "replica", TabletPickerOptions{})
	require.NoError(t, err)
	delay := GetTabletPickerRetryDelay()
	defer func() {
		SetTabletPickerRetryDelay(delay)
	}()
	SetTabletPickerRetryDelay(11 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	// no tablets
	_, err = tp.PickForStreaming(ctx)
	require.EqualError(t, err, "context has expired")
	// no tablets of the correct type
	defer deleteTablet(t, te, addTablet(te, 200, topodatapb.TabletType_RDONLY, "cell", true, true))
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = tp.PickForStreaming(ctx)
	require.EqualError(t, err, "context has expired")
	// if local preference is selected, tp cells include's the local cell's alias
	require.Greater(t, globalTPStats.noTabletFoundError.Counts()["cell_cella.ks.0.replica"], int64(0))
}

func TestPickErrorOnlySpecified(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell"})

	tp, err := NewTabletPicker(context.Background(), te.topoServ, te.cells, "cell", te.keyspace, te.shard, "replica", TabletPickerOptions{CellPreference: "OnlySpecified"})
	require.NoError(t, err)
	delay := GetTabletPickerRetryDelay()
	defer func() {
		SetTabletPickerRetryDelay(delay)
	}()
	SetTabletPickerRetryDelay(11 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	// no tablets
	_, err = tp.PickForStreaming(ctx)
	require.EqualError(t, err, "context has expired")
	// no tablets of the correct type
	defer deleteTablet(t, te, addTablet(te, 200, topodatapb.TabletType_RDONLY, "cell", true, true))
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = tp.PickForStreaming(ctx)
	require.EqualError(t, err, "context has expired")

	require.Greater(t, globalTPStats.noTabletFoundError.Counts()["cell.ks.0.replica"], int64(0))
}

type pickerTestEnv struct {
	t        *testing.T
	keyspace string
	shard    string
	cells    []string

	topoServ *topo.Server
}

func newPickerTestEnv(t *testing.T, cells []string) *pickerTestEnv {
	ctx := context.Background()

	te := &pickerTestEnv{
		t:        t,
		keyspace: "ks",
		shard:    "0",
		cells:    cells,
		topoServ: memorytopo.NewServer(cells...),
	}
	// create cell alias
	err := te.topoServ.CreateCellsAlias(ctx, "cella", &topodatapb.CellsAlias{
		Cells: cells,
	})
	require.NoError(t, err)
	err = te.topoServ.CreateKeyspace(ctx, te.keyspace, &topodatapb.Keyspace{})
	require.NoError(t, err)
	err = te.topoServ.CreateShard(ctx, te.keyspace, te.shard)
	require.NoError(t, err)
	return te
}

func addTablet(te *pickerTestEnv, id int, tabletType topodatapb.TabletType, cell string, serving, healthy bool) *topodatapb.Tablet {
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  uint32(id),
		},
		Keyspace: te.keyspace,
		Shard:    te.shard,
		KeyRange: &topodatapb.KeyRange{},
		Type:     tabletType,
		PortMap: map[string]int32{
			"test": int32(id),
		},
	}
	err := te.topoServ.CreateTablet(context.Background(), tablet)
	require.NoError(te.t, err)

	if healthy {
		_ = createFixedHealthConn(tablet, &querypb.StreamHealthResponse{
			Serving: serving,
			Target: &querypb.Target{
				Keyspace:   te.keyspace,
				Shard:      te.shard,
				TabletType: tabletType,
			},
			RealtimeStats: &querypb.RealtimeStats{HealthError: ""},
		})
	}

	return tablet
}

func deleteTablet(t *testing.T, te *pickerTestEnv, tablet *topodatapb.Tablet) {
	if tablet == nil {
		return
	}
	{ //log error
		err := te.topoServ.DeleteTablet(context.Background(), tablet.Alias)
		require.NoError(t, err, "failed to DeleteTablet with alias: %v", err)
	}
	{ //This is not automatically removed from shard replication, which results in log spam and log error
		err := topo.DeleteTabletReplicationData(context.Background(), te.topoServ, tablet)
		require.NoError(t, err, "failed to automatically remove from shard replication: %v", err)
	}
}
