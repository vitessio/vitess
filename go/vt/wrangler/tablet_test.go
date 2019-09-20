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
	"testing"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
		Shard: "80-C0",
	}

	if err := wr.InitTablet(context.Background(), tablet, false /*allowMasterOverride*/, true /*createShardAndKeyspace*/, false /*allowUpdate*/); err != nil {
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
