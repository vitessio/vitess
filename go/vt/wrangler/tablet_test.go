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

package wrangler

import (
	"context"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// TestInitTabletShardConversion makes sure InitTablet converts the
// shard name to lower case when it's a keyrange, and populates
// KeyRange properly.
func TestInitTabletShardConversion(t *testing.T) {
	cell := "cell1"
	ts := memorytopo.NewServer(cell)
	wr := New(logutil.NewConsoleLogger(), ts, nil)

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  1,
		},
		Keyspace: "test",
		Shard:    "80-C0",
	}

	if err := wr.TopoServer().InitTablet(context.Background(), tablet, false /*allowPrimaryOverride*/, true /*createShardAndKeyspace*/, false /*allowUpdate*/); err != nil {
		t.Fatalf("InitTablet failed: %v", err)
	}

	ti, err := ts.GetTablet(context.Background(), tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Shard != "80-c0" {
		t.Errorf("Got wrong tablet.Shard, got %v expected 80-c0", ti.Shard)
	}
	if string(ti.KeyRange.Start) != "\x80" || string(ti.KeyRange.End) != "\xc0" {
		t.Errorf("Got wrong tablet.KeyRange, got %v expected 80-c0", ti.KeyRange)
	}
}

// TestDeleteTabletBasic tests delete of non-primary tablet
func TestDeleteTabletBasic(t *testing.T) {
	cell := "cell1"
	ts := memorytopo.NewServer(cell)
	wr := New(logutil.NewConsoleLogger(), ts, nil)

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  1,
		},
		Shard:    "0",
		Keyspace: "test",
	}

	if err := wr.TopoServer().InitTablet(context.Background(), tablet, false /*allowPrimaryOverride*/, true /*createShardAndKeyspace*/, false /*allowUpdate*/); err != nil {
		t.Fatalf("InitTablet failed: %v", err)
	}

	if _, err := ts.GetTablet(context.Background(), tablet.Alias); err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}

	if err := wr.DeleteTablet(context.Background(), tablet.Alias, false); err != nil {
		t.Fatalf("DeleteTablet failed: %v", err)
	}
}

// TestDeleteTabletTruePrimary tests that you can delete a true primary tablet
// only if allowPrimary is set to true
func TestDeleteTabletTruePrimary(t *testing.T) {
	cell := "cell1"
	ts := memorytopo.NewServer(cell)
	wr := New(logutil.NewConsoleLogger(), ts, nil)

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  1,
		},
		Keyspace: "test",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	}

	if err := wr.TopoServer().InitTablet(context.Background(), tablet, false /*allowPrimaryOverride*/, true /*createShardAndKeyspace*/, false /*allowUpdate*/); err != nil {
		t.Fatalf("InitTablet failed: %v", err)
	}
	if _, err := ts.GetTablet(context.Background(), tablet.Alias); err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}

	// set PrimaryAlias and PrimaryTermStartTime on shard to match chosen primary tablet
	if _, err := ts.UpdateShardFields(context.Background(), "test", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = tablet.Alias
		si.PrimaryTermStartTime = tablet.PrimaryTermStartTime
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	err := wr.DeleteTablet(context.Background(), tablet.Alias, false)
	wantError := "as it is a primary, use allow_primary flag"
	if err == nil || !strings.Contains(err.Error(), wantError) {
		t.Fatalf("DeleteTablet on primary: want error = %v, got error = %v", wantError, err)
	}

	if err := wr.DeleteTablet(context.Background(), tablet.Alias, true); err != nil {
		t.Fatalf("DeleteTablet failed: %v", err)
	}
}

// TestDeleteTabletFalsePrimary tests that you can delete a false primary tablet
// with allowPrimary set to false
func TestDeleteTabletFalsePrimary(t *testing.T) {
	cell := "cell1"
	ts := memorytopo.NewServer(cell)
	wr := New(logutil.NewConsoleLogger(), ts, nil)

	tablet1 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  1,
		},
		Keyspace: "test",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	}

	if err := wr.TopoServer().InitTablet(context.Background(), tablet1, false /*allowPrimaryOverride*/, true /*createShardAndKeyspace*/, false /*allowUpdate*/); err != nil {
		t.Fatalf("InitTablet failed: %v", err)
	}

	tablet2 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  2,
		},
		Keyspace: "test",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	}
	if err := wr.TopoServer().InitTablet(context.Background(), tablet2, true /*allowPrimaryOverride*/, false /*createShardAndKeyspace*/, false /*allowUpdate*/); err != nil {
		t.Fatalf("InitTablet failed: %v", err)
	}

	// set PrimaryAlias and PrimaryTermStartTime on shard to match chosen primary tablet
	if _, err := ts.UpdateShardFields(context.Background(), "test", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = tablet2.Alias
		si.PrimaryTermStartTime = tablet2.PrimaryTermStartTime
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// Should be able to delete old (false) primary with allowPrimary = false
	if err := wr.DeleteTablet(context.Background(), tablet1.Alias, false); err != nil {
		t.Fatalf("DeleteTablet failed: %v", err)
	}
}

// TestDeleteTabletShardNonExisting tests that you can delete a true primary
// tablet if a shard does not exists anymore.
func TestDeleteTabletShardNonExisting(t *testing.T) {
	cell := "cell1"
	ts := memorytopo.NewServer(cell)
	wr := New(logutil.NewConsoleLogger(), ts, nil)

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  1,
		},
		Keyspace: "test",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	}

	if err := wr.TopoServer().InitTablet(context.Background(), tablet, false /*allowPrimaryOverride*/, true /*createShardAndKeyspace*/, false /*allowUpdate*/); err != nil {
		t.Fatalf("InitTablet failed: %v", err)
	}
	if _, err := ts.GetTablet(context.Background(), tablet.Alias); err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}

	// set PrimaryAlias and PrimaryTermStartTime on shard to match chosen primary tablet
	if _, err := ts.UpdateShardFields(context.Background(), "test", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = tablet.Alias
		si.PrimaryTermStartTime = tablet.PrimaryTermStartTime
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// trigger a shard deletion
	if err := ts.DeleteShard(context.Background(), "test", "0"); err != nil {
		t.Fatalf("DeleteShard failed: %v", err)
	}

	// DeleteTablet should not fail if a shard no longer exist
	if err := wr.DeleteTablet(context.Background(), tablet.Alias, true); err != nil {
		t.Fatalf("DeleteTablet failed: %v", err)
	}
}
