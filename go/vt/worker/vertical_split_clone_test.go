/*
Copyright 2017 Google Inc.

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
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
	"vitess.io/vitess/go/vt/wrangler/testlib"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	// verticalSplitCloneTestMin is the minimum value of the primary key.
	verticalSplitCloneTestMin int = 100
	// verticalSplitCloneTestMax is the maximum value of the primary key.
	verticalSplitCloneTestMax int = 200
)

func createVerticalSplitCloneDestinationFakeDb(t *testing.T, name string, insertCount int) *fakesqldb.DB {
	f := fakesqldb.New(t).SetName(name).OrderMatters()

	// Provoke a retry to test the error handling. (Let the first write fail.)
	f.AddExpectedQuery("INSERT INTO `vt_destination_ks`.`moving1` (`id`, `msg`) VALUES (*", errReadOnly)

	for i := 1; i <= insertCount; i++ {
		f.AddExpectedQuery("INSERT INTO `vt_destination_ks`.`moving1` (`id`, `msg`) VALUES (*", nil)
	}

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

	sourceRdonlyFakeDB := sourceRdonlyFakeDB(t, "vt_source_ks", "moving1", verticalSplitCloneTestMin, verticalSplitCloneTestMax)

	sourceMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 0,
		topodatapb.TabletType_MASTER, nil, testlib.TabletKeyspaceShard(t, "source_ks", "0"))
	sourceRdonly := testlib.NewFakeTablet(t, wi.wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, sourceRdonlyFakeDB, testlib.TabletKeyspaceShard(t, "source_ks", "0"))

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

	// We read 100 source rows. sourceReaderCount is set to 10, so
	// we'll have 100/10=10 rows per table chunk.
	// destinationPackCount is set to 4, so we take 4 source rows
	// at once. So we'll process 4 + 4 + 2 rows to get to 10.
	// That means 3 insert statements on the target. So 3 * 10
	// = 30 insert statements on the destination.
	destMasterFakeDb := createVerticalSplitCloneDestinationFakeDb(t, "destMaster", 30)
	defer destMasterFakeDb.VerifyAllExecutedOrFail()

	destMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 10,
		topodatapb.TabletType_MASTER, destMasterFakeDb, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))
	destRdonly := testlib.NewFakeTablet(t, wi.wr, "cell1", 11,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))

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
	sourceRdonly.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{mysql.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678}},
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

	// Set up destination master which will be used as input for the diff during the clone.
	destMasterShqs := fakes.NewStreamHealthQueryService(destMaster.Target())
	destMasterShqs.AddDefaultHealthResponse()
	destMasterQs := newTestQueryService(t, destMaster.Target(), destMasterShqs, 0, 1, topoproto.TabletAliasString(destMaster.Tablet.Alias), true /* omitKeyspaceID */)
	// This tablet is empty and does not return any rows.
	grpcqueryservice.Register(destMaster.RPCServer, destMasterQs)

	// Only wait 1 ms between retries, so that the test passes faster
	*executeFetchRetryTime = (1 * time.Millisecond)

	// When the online clone inserted the last rows, modify the destination test
	// query service such that it will return them as well.
	destMasterFakeDb.GetEntry(29).AfterFunc = func() {
		destMasterQs.addGeneratedRows(verticalSplitCloneTestMin, verticalSplitCloneTestMax)
	}

	// Start action loop after having registered all RPC services.
	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly, destMaster, destRdonly} {
		ft.StartActionLoop(t, wi.wr)
		defer ft.StopActionLoop(t)
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
