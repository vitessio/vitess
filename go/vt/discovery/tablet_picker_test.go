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
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"

	"context"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestPickSimple(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell"})
	want := addTablet(te, 100, topodatapb.TabletType_REPLICA, "cell", true, true)
	defer deleteTablet(te, want)

	tp, err := NewTabletPicker(te.topoServ, te.cells, te.keyspace, te.shard, "replica")
	require.NoError(t, err)

	tablet, err := tp.PickForStreaming(context.Background())
	require.NoError(t, err)
	assert.True(t, proto.Equal(want, tablet), "Pick: %v, want %v", tablet, want)
}

func TestPickFromTwoHealthy(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell"})
	want1 := addTablet(te, 100, topodatapb.TabletType_REPLICA, "cell", true, true)
	defer deleteTablet(te, want1)
	want2 := addTablet(te, 101, topodatapb.TabletType_RDONLY, "cell", true, true)
	defer deleteTablet(te, want2)

	tp, err := NewTabletPicker(te.topoServ, te.cells, te.keyspace, te.shard, "replica,rdonly")
	require.NoError(t, err)

	// In 20 attempts, both tablet types must be picked at least once.
	var picked1, picked2 bool
	for i := 0; i < 20; i++ {
		tablet, err := tp.PickForStreaming(context.Background())
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

func TestPickRespectsTabletType(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell"})
	want := addTablet(te, 100, topodatapb.TabletType_REPLICA, "cell", true, true)
	defer deleteTablet(te, want)
	dont := addTablet(te, 101, topodatapb.TabletType_MASTER, "cell", true, true)
	defer deleteTablet(te, dont)

	tp, err := NewTabletPicker(te.topoServ, te.cells, te.keyspace, te.shard, "replica,rdonly")
	require.NoError(t, err)

	// In 20 attempts, master tablet must be never picked
	for i := 0; i < 20; i++ {
		tablet, err := tp.PickForStreaming(context.Background())
		require.NoError(t, err)
		require.NotNil(t, tablet)
		require.True(t, proto.Equal(tablet, want), "picked wrong tablet type")
	}
}

func TestPickMultiCell(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell", "otherCell"})
	want := addTablet(te, 100, topodatapb.TabletType_REPLICA, "cell", true, true)
	defer deleteTablet(te, want)

	tp, err := NewTabletPicker(te.topoServ, te.cells, te.keyspace, te.shard, "replica")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	tablet, err := tp.PickForStreaming(ctx)
	require.NoError(t, err)
	assert.True(t, proto.Equal(want, tablet), "Pick: %v, want %v", tablet, want)
}

func TestPickMaster(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell", "otherCell"})
	want := addTablet(te, 100, topodatapb.TabletType_MASTER, "cell", true, true)
	defer deleteTablet(te, want)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := te.topoServ.UpdateShardFields(ctx, te.keyspace, te.shard, func(si *topo.ShardInfo) error {
		si.MasterAlias = want.Alias
		return nil
	})
	require.NoError(t, err)

	tp, err := NewTabletPicker(te.topoServ, []string{"otherCell"}, te.keyspace, te.shard, "master")
	require.NoError(t, err)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	tablet, err := tp.PickForStreaming(ctx2)
	require.NoError(t, err)
	assert.True(t, proto.Equal(want, tablet), "Pick: %v, want %v", tablet, want)
}

func TestPickFromOtherCell(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell", "otherCell"})
	want := addTablet(te, 100, topodatapb.TabletType_REPLICA, "otherCell", true, true)
	defer deleteTablet(te, want)

	tp, err := NewTabletPicker(te.topoServ, te.cells, te.keyspace, te.shard, "replica")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	tablet, err := tp.PickForStreaming(ctx)
	require.NoError(t, err)
	assert.True(t, proto.Equal(want, tablet), "Pick: %v, want %v", tablet, want)
}

func TestDontPickFromOtherCell(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell", "otherCell"})
	want1 := addTablet(te, 100, topodatapb.TabletType_REPLICA, "cell", true, true)
	defer deleteTablet(te, want1)
	want2 := addTablet(te, 101, topodatapb.TabletType_REPLICA, "otherCell", true, true)
	defer deleteTablet(te, want2)

	tp, err := NewTabletPicker(te.topoServ, []string{"cell"}, te.keyspace, te.shard, "replica")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// In 20 attempts, only want1 must be picked because TabletPicker.cells = "cell"
	var picked1, picked2 bool
	for i := 0; i < 20; i++ {
		tablet, err := tp.PickForStreaming(ctx)
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

func TestPickMultiCellTwoTablets(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell", "otherCell"})
	want1 := addTablet(te, 100, topodatapb.TabletType_REPLICA, "cell", true, true)
	defer deleteTablet(te, want1)
	want2 := addTablet(te, 101, topodatapb.TabletType_REPLICA, "otherCell", true, true)
	defer deleteTablet(te, want2)

	tp, err := NewTabletPicker(te.topoServ, te.cells, te.keyspace, te.shard, "replica")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// In 20 attempts, both tablet types must be picked at least once.
	var picked1, picked2 bool
	for i := 0; i < 20; i++ {
		tablet, err := tp.PickForStreaming(ctx)
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

func TestPickMultiCellTwoTabletTypes(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell", "otherCell"})
	want1 := addTablet(te, 100, topodatapb.TabletType_REPLICA, "cell", true, true)
	defer deleteTablet(te, want1)
	want2 := addTablet(te, 101, topodatapb.TabletType_RDONLY, "otherCell", true, true)
	defer deleteTablet(te, want2)

	tp, err := NewTabletPicker(te.topoServ, te.cells, te.keyspace, te.shard, "replica,rdonly")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// In 20 attempts, both tablet types must be picked at least once.
	var picked1, picked2 bool
	for i := 0; i < 20; i++ {
		tablet, err := tp.PickForStreaming(ctx)
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

func TestPickUsingCellAlias(t *testing.T) {
	// test env puts all cells into an alias called "cella"
	te := newPickerTestEnv(t, []string{"cell", "otherCell"})
	want1 := addTablet(te, 100, topodatapb.TabletType_REPLICA, "cell", true, true)
	defer deleteTablet(te, want1)

	tp, err := NewTabletPicker(te.topoServ, []string{"cella"}, te.keyspace, te.shard, "replica")
	require.NoError(t, err)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel1()
	tablet, err := tp.PickForStreaming(ctx1)
	require.NoError(t, err)
	assert.True(t, proto.Equal(want1, tablet), "Pick: %v, want %v", tablet, want1)

	// create a tablet in the other cell, it should be picked
	deleteTablet(te, want1)
	want2 := addTablet(te, 101, topodatapb.TabletType_REPLICA, "otherCell", true, true)
	defer deleteTablet(te, want2)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	tablet, err = tp.PickForStreaming(ctx2)
	require.NoError(t, err)
	assert.True(t, proto.Equal(want2, tablet), "Pick: %v, want %v", tablet, want2)

	// addTablet again and test that both are picked at least once
	want1 = addTablet(te, 100, topodatapb.TabletType_REPLICA, "cell", true, true)
	ctx3, cancel3 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel3()

	// In 20 attempts, both tablet types must be picked at least once.
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
	tp, err := NewTabletPicker(te.topoServ, te.cells, te.keyspace, te.shard, "replica")
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
	defer deleteTablet(te, want)
	got := <-result
	require.NotNil(t, got, "Tablet should not be nil")
	assert.True(t, proto.Equal(want, got), "Pick: %v, want %v", got, want)
}

func TestPickError(t *testing.T) {
	te := newPickerTestEnv(t, []string{"cell"})
	_, err := NewTabletPicker(te.topoServ, te.cells, te.keyspace, te.shard, "badtype")
	assert.EqualError(t, err, "failed to parse list of tablet types: badtype")

	tp, err := NewTabletPicker(te.topoServ, te.cells, te.keyspace, te.shard, "replica")
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
	defer deleteTablet(te, addTablet(te, 200, topodatapb.TabletType_RDONLY, "cell", true, true))
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = tp.PickForStreaming(ctx)
	require.EqualError(t, err, "context has expired")
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

func deleteTablet(te *pickerTestEnv, tablet *topodatapb.Tablet) {

	if tablet == nil {
		return
	}
	//log error
	if err := te.topoServ.DeleteTablet(context.Background(), tablet.Alias); err != nil {
		log.Errorf("failed to DeleteTablet with alias : %v", err)
	}

	//This is not automatically removed from shard replication, which results in log spam and log error
	if err := topo.DeleteTabletReplicationData(context.Background(), te.topoServ, tablet); err != nil {
		log.Errorf("failed to automatically remove from shard replication: %v", err)
	}
}
