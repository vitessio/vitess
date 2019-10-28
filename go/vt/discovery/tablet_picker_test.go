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

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestPickSimple(t *testing.T) {
	te := newPickerTestEnv(t)
	want := addTablet(te, 100, topodatapb.TabletType_REPLICA, true, true)
	defer deleteTablet(te, want)

	tp, err := NewTabletPicker(context.Background(), te.topoServ, te.cell, te.keyspace, te.shard, "replica", 1*time.Second, 1*time.Second, 1*time.Minute)
	require.NoError(t, err)
	defer tp.Close()

	tablet, err := tp.PickForStreaming(context.Background())
	require.NoError(t, err)
	if !proto.Equal(want, tablet) {
		t.Errorf("Pick: %v, want %v", tablet, want)
	}
}

func TestPickFromTwoHealthy(t *testing.T) {
	te := newPickerTestEnv(t)
	want1 := addTablet(te, 100, topodatapb.TabletType_REPLICA, true, true)
	defer deleteTablet(te, want1)
	want2 := addTablet(te, 101, topodatapb.TabletType_RDONLY, true, true)
	defer deleteTablet(te, want2)

	tp, err := NewTabletPicker(context.Background(), te.topoServ, te.cell, te.keyspace, te.shard, "replica,rdonly", 1*time.Second, 1*time.Second, 1*time.Minute)
	require.NoError(t, err)
	defer tp.Close()

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

func TestPickFromSomeUnhealthy(t *testing.T) {
	te := newPickerTestEnv(t)
	defer deleteTablet(te, addTablet(te, 100, topodatapb.TabletType_REPLICA, false, false))
	want := addTablet(te, 101, topodatapb.TabletType_RDONLY, false, true)
	defer deleteTablet(te, want)

	tp, err := NewTabletPicker(context.Background(), te.topoServ, te.cell, te.keyspace, te.shard, "replica,rdonly", 1*time.Second, 1*time.Second, 1*time.Minute)
	require.NoError(t, err)
	defer tp.Close()

	tablet, err := tp.PickForStreaming(context.Background())
	require.NoError(t, err)
	if !proto.Equal(tablet, want) {
		t.Errorf("Pick:\n%v, want\n%v", tablet, want)
	}
}

func TestPickError(t *testing.T) {
	te := newPickerTestEnv(t)
	defer deleteTablet(te, addTablet(te, 100, topodatapb.TabletType_REPLICA, false, false))

	_, err := NewTabletPicker(context.Background(), te.topoServ, te.cell, te.keyspace, te.shard, "badtype", 1*time.Second, 1*time.Second, 1*time.Minute)
	assert.EqualError(t, err, "failed to parse list of tablet types: badtype")

	tp, err := NewTabletPicker(context.Background(), te.topoServ, te.cell, te.keyspace, te.shard, "replica,rdonly", 1*time.Second, 1*time.Second, 1*time.Minute)
	require.NoError(t, err)
	defer tp.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = tp.PickForStreaming(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error waiting for tablets")
}

type pickerTestEnv struct {
	t        *testing.T
	keyspace string
	shard    string
	cell     string

	topoServ *topo.Server
}

func newPickerTestEnv(t *testing.T) *pickerTestEnv {
	ctx := context.Background()

	te := &pickerTestEnv{
		t:        t,
		keyspace: "ks",
		shard:    "0",
		cell:     "cell",
		topoServ: memorytopo.NewServer("cell"),
	}
	err := te.topoServ.CreateKeyspace(ctx, te.keyspace, &topodatapb.Keyspace{})
	require.NoError(t, err)
	err = te.topoServ.CreateShard(ctx, te.keyspace, te.shard)
	require.NoError(t, err)
	return te
}

func addTablet(te *pickerTestEnv, id int, tabletType topodatapb.TabletType, serving, healthy bool) *topodatapb.Tablet {
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: te.cell,
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

	var herr string
	if !healthy {
		herr = "err"
	}
	_ = createFixedHealthConn(tablet, &querypb.StreamHealthResponse{
		Serving: serving,
		Target: &querypb.Target{
			Keyspace:   te.keyspace,
			Shard:      te.shard,
			TabletType: tabletType,
		},
		RealtimeStats: &querypb.RealtimeStats{HealthError: herr},
	})

	return tablet
}

func deleteTablet(te *pickerTestEnv, tablet *topodatapb.Tablet) {
	te.topoServ.DeleteTablet(context.Background(), tablet.Alias)
	// This is not automatically removed from shard replication, which results in log spam.
	topo.DeleteTabletReplicationData(context.Background(), te.topoServ, tablet)
}
