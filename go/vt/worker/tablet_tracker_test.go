// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/topo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var ts1 = discovery.TabletStats{
	Tablet: topo.NewTablet(10, "cell", "host1"),
	Target: &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
}
var ts2 = discovery.TabletStats{
	Tablet: topo.NewTablet(20, "cell", "host1"),
	Target: &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
}
var allTs = []discovery.TabletStats{ts1, ts2}

func TestTabletsInUse(t *testing.T) {
	tt := NewTabletTracker()

	tt.Track([]discovery.TabletStats{ts1})
	if got, want := tt.TabletsInUse(), "cell-0000000010"; got != want {
		t.Fatalf("TabletsInUse() = %v, want = %v", got, want)
	}

	tt.Track([]discovery.TabletStats{ts2})
	if got, want := tt.TabletsInUse(), "cell-0000000010 cell-0000000020"; got != want {
		t.Fatalf("TabletsInUse() = %v, want = %v", got, want)
	}
}

func TestTrackUntrack(t *testing.T) {
	tt := NewTabletTracker()
	// ts1 will be used because no tablet is in use yet and ts1 is the first.
	if got, want := tt.Track(allTs), ts1.Tablet; !proto.Equal(got, want) {
		t.Fatalf("Track(%v) = %v, want = %v", allTs, got, want)
	}

	// ts1 is already in use once, use ts2 now.
	if got, want := tt.Track(allTs), ts2.Tablet; !proto.Equal(got, want) {
		t.Fatalf("Track(%v) = %v, want = %v", allTs, got, want)
	}

	// ts2 is no longer in use after Untrack().
	tt.Untrack(ts2.Tablet.Alias)
	// ts2 instead of ts1 will be used because ts1 has a higher use count.
	if got, want := tt.Track(allTs), ts2.Tablet; !proto.Equal(got, want) {
		t.Fatalf("Track(%v) = %v, want = %v", allTs, got, want)
	}
}
