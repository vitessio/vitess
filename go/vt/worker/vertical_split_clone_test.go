// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vttablet/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice/fakes"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	// verticalSplitCloneTestMin is the minimum value of the primary key.
	verticalSplitCloneTestMin int = 100
	// verticalSplitCloneTestMax is the maximum value of the primary key.
	verticalSplitCloneTestMax int = 200
)

func createVerticalSplitCloneDestinationFakeDb(t *testing.T, name string, insertCount int) *FakePoolConnection {
	f := NewFakePoolConnectionQuery(t, name)

	// Provoke a retry to test the error handling. (Let the first write fail.)
	f.addExpectedQuery("INSERT INTO `vt_destination_ks`.`moving1` (`id`, `msg`) VALUES (*", errReadOnly)

	for i := 1; i <= insertCount; i++ {
		f.addExpectedQuery("INSERT INTO `vt_destination_ks`.`moving1` (`id`, `msg`) VALUES (*", nil)
	}

	expectBlpCheckpointCreationQueries(f)

	return f
}

// TestVerticalSplitClone will run VerticalSplitClone in the combined
// online and offline mode. The online phase will copy 100 rows from the source
// to the destination and the offline phase won't copy any rows as the source
// has not changed in the meantime.
func TestVerticalSplitClone(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	ctx := context.Background()
	wi := NewInstance(ts, "cell1", time.Second)

	sourceMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 0,
		topodatapb.TabletType_MASTER, nil, testlib.TabletKeyspaceShard(t, "source_ks", "0"))
	sourceRdonly := testlib.NewFakeTablet(t, wi.wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(t, "source_ks", "0"))

	// Create the destination keyspace with the appropriate ServedFromMap
	ki := &topodatapb.Keyspace{
		ServedFroms: []*topodatapb.Keyspace_ServedFrom{
			{
				TabletType: topodatapb.TabletType_MASTER,
				Keyspace:   "source_ks",
			},
			{
				TabletType: topodatapb.TabletType_REPLICA,
				Keyspace:   "source_ks",
			},
			{
				TabletType: topodatapb.TabletType_RDONLY,
				Keyspace:   "source_ks",
			},
		},
	}
	wi.wr.TopoServer().CreateKeyspace(ctx, "destination_ks", ki)

	destMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 10,
		topodatapb.TabletType_MASTER, nil, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))
	destRdonly := testlib.NewFakeTablet(t, wi.wr, "cell1", 11,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))

	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly, destMaster, destRdonly} {
		ft.StartActionLoop(t, wi.wr)
		defer ft.StopActionLoop(t)
	}

	// add the topo and schema data we'll need
	if err := wi.wr.RebuildKeyspaceGraph(ctx, "source_ks", nil); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}
	if err := wi.wr.RebuildKeyspaceGraph(ctx, "destination_ks", nil); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}

	// Set up source rdonly which will be used as input for the diff during the clone.
	sourceRdonly.FakeMysqlDaemon.Schema = &tabletmanagerdatapb.SchemaDefinition{
		DatabaseSchema: "",
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:              "moving1",
				Columns:           []string{"id", "msg"},
				PrimaryKeyColumns: []string{"id"},
				Type:              tmutils.TableBaseTable,
				// Set the row count to avoid that --min_rows_per_chunk reduces the
				// number of chunks.
				RowCount: 100,
			},
			{
				Name: "view1",
				Type: tmutils.TableView,
			},
		},
	}
	sourceRdonly.FakeMysqlDaemon.DbAppConnectionFactory = sourceRdonlyFactory(
		t, "vt_source_ks", "moving1", verticalSplitCloneTestMin, verticalSplitCloneTestMax)
	sourceRdonly.FakeMysqlDaemon.CurrentMasterPosition = replication.Position{
		GTIDSet: replication.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678},
	}
	sourceRdonly.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP SLAVE",
		"START SLAVE",
	}
	sourceRdonlyShqs := fakes.NewStreamHealthQueryService(sourceRdonly.Target())
	sourceRdonlyShqs.AddDefaultHealthResponse()
	sourceRdonlyQs := newTestQueryService(t, sourceRdonly.Target(), sourceRdonlyShqs, 0, 1, topoproto.TabletAliasString(sourceRdonly.Tablet.Alias), true /* omitKeyspaceID */)
	sourceRdonlyQs.addGeneratedRows(verticalSplitCloneTestMin, verticalSplitCloneTestMax)
	grpcqueryservice.Register(sourceRdonly.RPCServer, sourceRdonlyQs)

	// Set up destination rdonly which will be used as input for the diff during the clone.
	destRdonlyShqs := fakes.NewStreamHealthQueryService(destRdonly.Target())
	destRdonlyShqs.AddDefaultHealthResponse()
	destRdonlyQs := newTestQueryService(t, destRdonly.Target(), destRdonlyShqs, 0, 1, topoproto.TabletAliasString(destRdonly.Tablet.Alias), true /* omitKeyspaceID */)
	// This tablet is empty and does not return any rows.
	grpcqueryservice.Register(destRdonly.RPCServer, destRdonlyQs)

	// We read 100 source rows. sourceReaderCount is set to 10, so
	// we'll have 100/10=10 rows per table chunk.
	// destinationPackCount is set to 4, so we take 4 source rows
	// at once. So we'll process 4 + 4 + 2 rows to get to 10.
	// That means 3 insert statements on the target. So 3 * 10
	// = 30 insert statements on the destination.
	destMasterFakeDb := createVerticalSplitCloneDestinationFakeDb(t, "destMaster", 30)
	defer destMasterFakeDb.verifyAllExecutedOrFail()
	destMaster.FakeMysqlDaemon.DbAppConnectionFactory = destMasterFakeDb.getFactory()

	// Fake stream health reponses because vtworker needs them to find the master.
	qs := fakes.NewStreamHealthQueryService(destMaster.Target())
	qs.AddDefaultHealthResponse()
	grpcqueryservice.Register(destMaster.RPCServer, qs)
	// Only wait 1 ms between retries, so that the test passes faster
	*executeFetchRetryTime = (1 * time.Millisecond)

	// When the online clone inserted the last rows, modify the destination test
	// query service such that it will return them as well.
	destMasterFakeDb.getEntry(29).AfterFunc = func() {
		destRdonlyQs.addGeneratedRows(verticalSplitCloneTestMin, verticalSplitCloneTestMax)
	}

	// Run the vtworker command.
	args := []string{
		"VerticalSplitClone",
		// --max_tps is only specified to enable the throttler and ensure that the
		// code is executed. But the intent here is not to throttle the test, hence
		// the rate limit is set very high.
		"-max_tps", "9999",
		"-tables", "/moving/,view1",
		"-source_reader_count", "10",
		// Each chunk pipeline will process 10 rows. To spread them out across 3
		// write queries, set the max row count per query to 4. (10 = 4+4+2)
		"-write_query_max_rows", "4",
		"-min_rows_per_chunk", "10",
		"-destination_writer_count", "10",
		// This test uses only one healthy RDONLY tablet.
		"-min_healthy_rdonly_tablets", "1",
		"destination_ks/0",
	}
	if err := runCommand(t, wi, wi.wr, args); err != nil {
		t.Fatal(err)
	}
	if inserts := statsOnlineInsertsCounters.Counts()["moving1"]; inserts != 100 {
		t.Errorf("wrong number of rows inserted: got = %v, want = %v", inserts, 100)
	}
	if updates := statsOnlineUpdatesCounters.Counts()["moving1"]; updates != 0 {
		t.Errorf("wrong number of rows updated: got = %v, want = %v", updates, 0)
	}
	if deletes := statsOnlineDeletesCounters.Counts()["moving1"]; deletes != 0 {
		t.Errorf("wrong number of rows deleted: got = %v, want = %v", deletes, 0)
	}
	if inserts := statsOfflineInsertsCounters.Counts()["moving1"]; inserts != 0 {
		t.Errorf("no stats for the offline clone phase should have been modified. got inserts = %v", inserts)
	}
	if updates := statsOfflineUpdatesCounters.Counts()["moving1"]; updates != 0 {
		t.Errorf("no stats for the offline clone phase should have been modified. got updates = %v", updates)
	}
	if deletes := statsOfflineDeletesCounters.Counts()["moving1"]; deletes != 0 {
		t.Errorf("no stats for the offline clone phase should have been modified. got deletes = %v", deletes)
	}

	wantRetryCount := int64(1)
	if got := statsRetryCount.Get(); got != wantRetryCount {
		t.Errorf("Wrong statsRetryCounter: got %v, wanted %v", got, wantRetryCount)
	}
	wantRetryReadOnlyCount := int64(1)
	if got := statsRetryCounters.Counts()[retryCategoryReadOnly]; got != wantRetryReadOnlyCount {
		t.Errorf("Wrong statsRetryCounters: got %v, wanted %v", got, wantRetryReadOnlyCount)
	}
}
