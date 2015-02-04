package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo"
)

func TestTabletData(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)

	tablet1 := testlib.NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	tablet1.StartActionLoop(t, wr)
	defer tablet1.StopActionLoop(t)

	thc := newTabletHealthCache(ts, tmclient.NewTabletManagerClient())

	// get the first result, it's not containing any data but the alias
	result, err := thc.get(tablet1.Tablet.Alias)
	if err != nil {
		t.Fatalf("thc.get failed: %v", err)
	}
	var unpacked TabletHealth
	if err := json.Unmarshal(result, &unpacked); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if unpacked.HealthStreamReply.Tablet.Alias != tablet1.Tablet.Alias {
		t.Fatalf("wrong alias: %v", &unpacked)
	}
	if unpacked.Version != 1 {
		t.Errorf("wrong version, got %v was expecting 1", unpacked.Version)
	}

	// wait for the streaming RPC to be established
	timeout := 5 * time.Second
	for {
		if tablet1.Agent.HealthStreamMapSize() > 0 {
			break
		}
		timeout -= 10 * time.Millisecond
		if timeout < 0 {
			t.Fatalf("timeout waiting for streaming RPC to be established")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// feed some data from the tablet, with just a data marker
	hsr := &actionnode.HealthStreamReply{
		BinlogPlayerMapSize: 42,
	}
	tablet1.Agent.BroadcastHealthStreamReply(hsr)

	// and wait for the cache to pick it up
	timeout = 5 * time.Second
	for {
		result, err = thc.get(tablet1.Tablet.Alias)
		if err != nil {
			t.Fatalf("thc.get failed: %v", err)
		}
		if err := json.Unmarshal(result, &unpacked); err != nil {
			t.Fatalf("bad json: %v", err)
		}
		if unpacked.HealthStreamReply.BinlogPlayerMapSize == 42 {
			if unpacked.Version != 2 {
				t.Errorf("wrong version, got %v was expecting 2", unpacked.Version)
			}
			break
		}
		timeout -= 10 * time.Millisecond
		if timeout < 0 {
			t.Fatalf("timeout waiting for streaming RPC to be established")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
