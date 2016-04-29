// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"errors"
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
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	// splitCloneTestMin is the minimum value of the primary key.
	splitCloneTestMin int = 100
	// splitCloneTestMax is the maximum value of the primary key.
	splitCloneTestMax int = 200
)

var (
	errReadOnly = errors.New("The MariaDB server is running with the --read-only option so it cannot execute this statement (errno 1290) during query:")
)

type splitCloneTestCase struct {
	t *testing.T

	ts      topo.Server
	wi      *Instance
	tablets []*testlib.FakeTablet

	leftMasterFakeDb  *FakePoolConnection
	leftMasterQs      *fakes.StreamHealthQueryService
	rightMasterFakeDb *FakePoolConnection
	rightMasterQs     *fakes.StreamHealthQueryService

	// defaultWorkerArgs are the full default arguments to run SplitClone.
	defaultWorkerArgs []string
}

func (tc *splitCloneTestCase) setUp(v3 bool) {
	*useV3ReshardingMode = v3
	db := fakesqldb.Register()
	tc.ts = zktestserver.New(tc.t, []string{"cell1", "cell2"})
	ctx := context.Background()
	tc.wi = NewInstance(ctx, tc.ts, "cell1", time.Second)

	if v3 {
		// FIXME(alainjobart): ShardingColumnName and ShardingColumnType
		// are not used by v3 split_clone, but they are required
		// by tabletmanager to setup the rules. We have b/27901260
		// open internally to fix this.
		if err := tc.ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{
			ShardingColumnName: "keyspace_id",
			ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
		}); err != nil {
			tc.t.Fatalf("CreateKeyspace v3 failed: %v", err)
		}

		if err := tc.ts.SaveVSchema(ctx, "ks", `{
  "Sharded": true,
  "Vindexes": {
    "table1_index": {
      "Type": "numeric"
    }
  },
  "Tables": {
    "table1": {
      "ColVindexes": [
        {
          "Col": "keyspace_id",
          "Name": "table1_index"
        }
      ]
    }
  }
}`); err != nil {
			tc.t.Fatalf("SaveVSchema v3 failed: %v", err)
		}
	} else {
		if err := tc.ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{
			ShardingColumnName: "keyspace_id",
			ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
		}); err != nil {
			tc.t.Fatalf("CreateKeyspace v2 failed: %v", err)
		}
	}

	sourceMaster := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 0,
		topodatapb.TabletType_MASTER, db, testlib.TabletKeyspaceShard(tc.t, "ks", "-80"))
	sourceRdonly1 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(tc.t, "ks", "-80"))
	sourceRdonly2 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 2,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(tc.t, "ks", "-80"))

	leftMaster := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 10,
		topodatapb.TabletType_MASTER, db, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))
	leftRdonly := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 11,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))

	rightMaster := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 20,
		topodatapb.TabletType_MASTER, db, testlib.TabletKeyspaceShard(tc.t, "ks", "40-80"))
	rightRdonly := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 21,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(tc.t, "ks", "40-80"))

	tc.tablets = []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2, leftMaster, leftRdonly, rightMaster, rightRdonly}

	for _, ft := range tc.tablets {
		ft.StartActionLoop(tc.t, tc.wi.wr)
	}

	// add the topo and schema data we'll need
	if err := tc.ts.CreateShard(ctx, "ks", "80-"); err != nil {
		tc.t.Fatalf("CreateShard(\"-80\") failed: %v", err)
	}
	if err := tc.wi.wr.SetKeyspaceShardingInfo(ctx, "ks", "keyspace_id", topodatapb.KeyspaceIdType_UINT64, 4, false); err != nil {
		tc.t.Fatalf("SetKeyspaceShardingInfo failed: %v", err)
	}
	if err := tc.wi.wr.RebuildKeyspaceGraph(ctx, "ks", nil, true); err != nil {
		tc.t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}

	for _, sourceRdonly := range []*testlib.FakeTablet{sourceRdonly1, sourceRdonly2} {
		sourceRdonly.FakeMysqlDaemon.Schema = &tabletmanagerdatapb.SchemaDefinition{
			DatabaseSchema: "",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:              "table1",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
					// This informs how many rows we can pack into a single insert
					DataLength: 2048,
				},
			},
		}
		sourceRdonly.FakeMysqlDaemon.DbAppConnectionFactory = sourceRdonlyFactory(
			tc.t, "vt_ks.table1", splitCloneTestMin, splitCloneTestMax)
		sourceRdonly.FakeMysqlDaemon.CurrentMasterPosition = replication.Position{
			GTIDSet: replication.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678},
		}
		sourceRdonly.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
			"STOP SLAVE",
			"START SLAVE",
		}
		qs := fakes.NewStreamHealthQueryService(sourceRdonly.Target())
		qs.AddDefaultHealthResponse()
		grpcqueryservice.RegisterForTest(sourceRdonly.RPCServer, &testQueryService{
			t: tc.t,
			StreamHealthQueryService: qs,
		})
	}

	// We read 100 source rows. sourceReaderCount is set to 10, so
	// we'll have 100/10=10 rows per table chunk.
	// destinationPackCount is set to 4, so we take 4 source rows
	// at once. So we'll process 4 + 4 + 2 rows to get to 10.
	// That means 3 insert statements on each target (each
	// containing half of the rows, i.e. 2 + 2 + 1 rows). So 3 * 10
	// = 30 insert statements on each destination.
	tc.leftMasterFakeDb = NewFakePoolConnectionQuery(tc.t, "leftMaster")
	tc.rightMasterFakeDb = NewFakePoolConnectionQuery(tc.t, "rightMaster")

	for i := 1; i <= 30; i++ {
		tc.leftMasterFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.table1(id, msg, keyspace_id) VALUES (*", nil)
		// leftReplica is unused by default.
		tc.rightMasterFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.table1(id, msg, keyspace_id) VALUES (*", nil)
	}
	expectBlpCheckpointCreationQueries(tc.leftMasterFakeDb)
	expectBlpCheckpointCreationQueries(tc.rightMasterFakeDb)

	leftMaster.FakeMysqlDaemon.DbAppConnectionFactory = tc.leftMasterFakeDb.getFactory()
	rightMaster.FakeMysqlDaemon.DbAppConnectionFactory = tc.rightMasterFakeDb.getFactory()

	tc.defaultWorkerArgs = []string{
		"SplitClone",
		"-source_reader_count", "10",
		"-destination_pack_count", "4",
		"-min_table_size_for_split", "1",
		"-destination_writer_count", "10",
		"ks/-80"}
}

func (tc *splitCloneTestCase) tearDown() {
	for _, ft := range tc.tablets {
		ft.StopActionLoop(tc.t)
	}
	tc.leftMasterFakeDb.verifyAllExecutedOrFail()
	tc.rightMasterFakeDb.verifyAllExecutedOrFail()
}

// testQueryService is a local QueryService implementation to support the tests.
type testQueryService struct {
	t *testing.T

	*fakes.StreamHealthQueryService
}

func (sq *testQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, sessionID int64, sendReply func(reply *sqltypes.Result) error) error {
	// Custom parsing of the query we expect.
	min := splitCloneTestMin
	max := splitCloneTestMax
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
	sq.t.Logf("testQueryService: got query: %v with min %v max %v", sql, min, max)

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
			{
				Name: "keyspace_id",
				Type: sqltypes.Int64,
			},
		},
	}); err != nil {
		return err
	}

	// Send the values
	ksids := []uint64{0x2000000000000000, 0x6000000000000000}
	for i := min; i < max; i++ {
		if err := sendReply(&sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte(fmt.Sprintf("%v", i))),
					sqltypes.MakeString([]byte(fmt.Sprintf("Text for %v", i))),
					sqltypes.MakeString([]byte(fmt.Sprintf("%v", ksids[i%2]))),
				},
			},
		}); err != nil {
			return err
		}
	}
	// SELECT id, msg, keyspace_id FROM table1 WHERE id>=180 AND id<190 ORDER BY id
	return nil
}

func TestSplitCloneV2(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatal(err)
	}
}

// TestSplitCloneV2_RetryDueToReadonly is identical to the regular test
// TestSplitCloneV2 with the additional twist that the destination masters
// fail the first write because they are read-only and succeed after that.
func TestSplitCloneV2_RetryDueToReadonly(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// Provoke a retry to test the error handling.
	tc.leftMasterFakeDb.addExpectedQueryAtIndex(0, "INSERT INTO `vt_ks`.table1(id, msg, keyspace_id) VALUES (*", errReadOnly)
	tc.rightMasterFakeDb.addExpectedQueryAtIndex(0, "INSERT INTO `vt_ks`.table1(id, msg, keyspace_id) VALUES (*", errReadOnly)
	// Only wait 1 ms between retries, so that the test passes faster.
	*executeFetchRetryTime = 1 * time.Millisecond

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatal(err)
	}

  if statsDestinationAttemptedResolves.String() != "3" {
    t.Errorf("Wrong statsDestinationAttemptedResolves: wanted %v, got %v", "3", statsDestinationAttemptedResolves.String())
  }
  if statsDestinationActualResolves.String() != "1" {
    t.Errorf("Wrong statsDestinationActualResolves: wanted %v, got %v", "1", statsDestinationActualResolves.String())
  }
  if statsRetryCounters.String() != "{\"ReadOnly\": 2}" {
    t.Errorf("Wrong statsRetryCounters: wanted %v, got %v", "{\"ReadOnly\": 2}", statsRetryCounters.String())
  }
}


func TestSplitCloneV3(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(true /* v3 */)
	defer tc.tearDown()

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatal(err)
	}
}
