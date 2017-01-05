package vtctld

import (
	"net/http"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestHandleExplorerRedirect(t *testing.T) {
	ctx := context.Background()

	ts := memorytopo.NewServer("cell1")
	if err := ts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  123,
		},
		Keyspace: "test_keyspace",
		Shard:    "123-456",
	}); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}

	table := map[string]string{
		"/explorers/redirect?type=keyspace&keyspace=test_keyspace":                         "/app/#/keyspaces/",
		"/explorers/redirect?type=shard&keyspace=test_keyspace&shard=-80":                  "/app/#/shard/test_keyspace/-80",
		"/explorers/redirect?type=srv_keyspace&keyspace=test_keyspace&cell=cell1":          "/app/#/keyspaces/",
		"/explorers/redirect?type=tablet&alias=cell1-123":                                  "/app/#/shard/test_keyspace/123-456",
		"/explorers/redirect?type=replication&keyspace=test_keyspace&shard=-80&cell=cell1": "/app/#/shard/test_keyspace/-80",
	}

	for input, want := range table {
		request, err := http.NewRequest("GET", input, nil)
		if err != nil {
			t.Fatalf("NewRequest error: %v", err)
		}
		if err := request.ParseForm(); err != nil {
			t.Fatalf("ParseForm error: %v", err)
		}
		got, err := handleExplorerRedirect(ctx, ts, request)
		if err != nil {
			t.Fatalf("handleExplorerRedirect error: %v", err)
		}
		if got != want {
			t.Errorf("handlExplorerRedirect(%#v) = %#v, want %#v", input, got, want)
		}
	}
}
