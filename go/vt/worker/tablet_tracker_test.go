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

package worker

import (
	"testing"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/topo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var ts1 = discovery.LegacyTabletStats{
	Tablet: topo.NewTablet(10, "cell", "host1"),
	Target: &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
}
var ts2 = discovery.LegacyTabletStats{
	Tablet: topo.NewTablet(20, "cell", "host1"),
	Target: &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
}
var allTs = []discovery.LegacyTabletStats{ts1, ts2}

func TestTabletsInUse(t *testing.T) {
	tt := NewTabletTracker()

	tt.Track([]discovery.LegacyTabletStats{ts1})
	if got, want := tt.TabletsInUse(), "cell-0000000010"; got != want {
		t.Fatalf("TabletsInUse() = %v, want = %v", got, want)
	}

	tt.Track([]discovery.LegacyTabletStats{ts2})
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
