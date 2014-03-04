package drifter

import (
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test/faketopo"
)

const testCell = "oe"

func TestUpdate(t *testing.T) {

	fix := faketopo.New(t, []string{testCell})
	defer fix.TearDown()

	if err := fix.Topo.CreateKeyspace(faketopo.TestKeyspace, &topo.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace: %v", err)
	}
	if err := topo.CreateShard(fix.Topo, faketopo.TestKeyspace, faketopo.TestShard); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	master := fix.AddTablet(1, testCell, topo.TYPE_MASTER, nil)

	// Create a good replica.
	good := fix.AddTablet(2, testCell, topo.TYPE_REPLICA, master)

	// Create a drifter.
	drifter := fix.AddTablet(3, testCell, topo.TYPE_REPLICA, master)

	if err := fix.Topo.UpdateTabletFields(drifter.Alias, func(t *topo.Tablet) error {
		t.Parent = good.Alias
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := fix.Wrangler.RebuildShardGraph(faketopo.TestKeyspace, faketopo.TestShard, []string{testCell}); err != nil {
		t.Fatal(err)
	}

	janitor := New()

	if err := janitor.Configure(fix.Wrangler, faketopo.TestKeyspace, faketopo.TestShard); err != nil {
		t.Fatal(err)
	}

	if err := janitor.Run(false); err != nil {
		t.Fatal(err)
	}

	badTablets := janitor.BadTablets()
	if len(badTablets) != 1 {
		t.Fatalf("want exactly 1 drifter, got %v", len(badTablets))
	}
	bad := badTablets[0]
	if bad.Alias != drifter.Alias {
		t.Errorf("drifter: want %v, got %v", drifter, bad)
	}

	if err := janitor.Run(true); err != nil {
		t.Fatal(err)
	}

	if want, got := int64(1), varSlaveRestartCount.Get(); want != got {
		t.Errorf("varSlaveRestartCount: want %v, got %v", want, got)
	}

	if badTablets = janitor.BadTablets(); len(badTablets) > 0 {
		t.Fatalf("want no drifters, got %v", badTablets)
	}

}
