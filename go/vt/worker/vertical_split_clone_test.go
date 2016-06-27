// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice/fakes"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	// verticalSplitCloneTestMin is the minimum value of the primary key.
	verticalSplitCloneTestMin int = 100
	// verticalSplitCloneTestMax is the maximum value of the primary key.
	verticalSplitCloneTestMax int = 200
)

// verticalTabletServer is a local QueryService implementation to support the tests.
type verticalTabletServer struct {
	t *testing.T

	*fakes.StreamHealthQueryService
}

func (sq *verticalTabletServer) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, sendReply func(reply *sqltypes.Result) error) error {
	// Custom parsing of the query we expect
	min := verticalSplitCloneTestMin
	max := verticalSplitCloneTestMax
	var err error
	parts := strings.Split(sql, " ")
	for _, part := range parts {
		if strings.HasPrefix(part, "id>=") {
			min, err = strconv.Atoi(part[4:])
			if err != nil {
				return err
			}
		} else if strings.HasPrefix(part, "id<") {
			max, err = strconv.Atoi(part[3:])
		}
	}
	sq.t.Logf("verticalTabletServer: got query: %v with min %v max %v", sql, min, max)

	// Send the headers
	if err := sendReply(&sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: sqltypes.Int64,
			},
			{
				Name: "msg",
				Type: sqltypes.VarChar,
			},
		},
	}); err != nil {
		return err
	}

	// Send the values
	for i := min; i < max; i++ {
		if err := sendReply(&sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte(fmt.Sprintf("%v", i))),
					sqltypes.MakeString([]byte(fmt.Sprintf("Text for %v", i))),
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func createVerticalSplitCloneDestinationFakeDb(t *testing.T, name string, insertCount int) *FakePoolConnection {
	f := NewFakePoolConnectionQuery(t, name)

	// Provoke a retry to test the error handling. (Let the first write fail.)
	f.addExpectedQuery("INSERT INTO `vt_destination_ks`.moving1(id, msg) VALUES (*", errReadOnly)

	for i := 1; i <= insertCount; i++ {
		f.addExpectedQuery("INSERT INTO `vt_destination_ks`.moving1(id, msg) VALUES (*", nil)
	}

	expectBlpCheckpointCreationQueries(f)

	return f
}

func TestVerticalSplitClone(t *testing.T) {
	db := fakesqldb.Register()
	ts := zktestserver.New(t, []string{"cell1", "cell2"})
	ctx := context.Background()
	wi := NewInstance(ctx, ts, "cell1", time.Second)

	sourceMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 0,
		topodatapb.TabletType_MASTER, db, testlib.TabletKeyspaceShard(t, "source_ks", "0"))
	sourceRdonly1 := testlib.NewFakeTablet(t, wi.wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "source_ks", "0"))
	sourceRdonly2 := testlib.NewFakeTablet(t, wi.wr, "cell1", 2,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "source_ks", "0"))

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
		topodatapb.TabletType_MASTER, db, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))
	destRdonly := testlib.NewFakeTablet(t, wi.wr, "cell1", 11,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))

	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2, destMaster, destRdonly} {
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

	for _, sourceRdonly := range []*testlib.FakeTablet{sourceRdonly1, sourceRdonly2} {
		sourceRdonly.FakeMysqlDaemon.Schema = &tabletmanagerdatapb.SchemaDefinition{
			DatabaseSchema: "",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:              "moving1",
					Columns:           []string{"id", "msg"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
					// Set the table size to a value higher than --min_table_size_for_split.
					DataLength: 2048,
				},
				{
					Name: "view1",
					Type: tmutils.TableView,
				},
			},
		}
		sourceRdonly.FakeMysqlDaemon.DbAppConnectionFactory = sourceRdonlyFactory(
			t, "vt_source_ks.moving1", verticalSplitCloneTestMin, verticalSplitCloneTestMax)
		sourceRdonly.FakeMysqlDaemon.CurrentMasterPosition = replication.Position{
			GTIDSet: replication.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678},
		}
		sourceRdonly.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
			"STOP SLAVE",
			"START SLAVE",
		}
		qs := fakes.NewStreamHealthQueryService(sourceRdonly.Target())
		qs.AddDefaultHealthResponse()
		grpcqueryservice.RegisterForTest(sourceRdonly.RPCServer, &verticalTabletServer{
			t: t,
			StreamHealthQueryService: qs,
		})
	}

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
	grpcqueryservice.RegisterForTest(destMaster.RPCServer, qs)
	// Only wait 1 ms between retries, so that the test passes faster
	*executeFetchRetryTime = (1 * time.Millisecond)

	// Run the vtworker command.
	args := []string{
		"VerticalSplitClone",
		// --max_tps is only specified to enable the throttler and ensure that the
		// code is executed. But the intent here is not to throttle the test, hence
		// the rate limit is set very high.
		"-max_tps", "9999",
		"-tables", "moving.*,view1",
		"-source_reader_count", "10",
		"-destination_pack_count", "4",
		"-min_table_size_for_split", "1",
		"-destination_writer_count", "10",
		"destination_ks/0",
	}
	if err := runCommand(t, wi, wi.wr, args); err != nil {
		t.Fatal(err)
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
