package wrangler

import (
	"testing"

	"github.com/youtube/vitess/go/vt/logutil"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"golang.org/x/net/context"
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
