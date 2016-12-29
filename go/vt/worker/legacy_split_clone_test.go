// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

// TODO(mberlin): Remove this file when SplitClone supports merge-sorting
// primary key columns based on the MySQL collation.

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice/fakes"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

const (
	// legacySplitCloneTestMin is the minimum value of the primary key.
	legacySplitCloneTestMin int = 100
	// legacySplitCloneTestMax is the maximum value of the primary key.
	legacySplitCloneTestMax int = 200
)

type legacySplitCloneTestCase struct {
	t *testing.T

	ts      topo.Server
	wi      *Instance
	tablets []*testlib.FakeTablet

	leftMasterFakeDb  *FakePoolConnection
	leftMasterQs      *fakes.StreamHealthQueryService
	rightMasterFakeDb *FakePoolConnection
	rightMasterQs     *fakes.StreamHealthQueryService

	// leftReplica is used by the reparent test.
	leftReplica       *testlib.FakeTablet
	leftReplicaFakeDb *FakePoolConnection
	leftReplicaQs     *fakes.StreamHealthQueryService

	// defaultWorkerArgs are the full default arguments to run LegacySplitClone.
	defaultWorkerArgs []string
}

func (tc *legacySplitCloneTestCase) setUp(v3 bool) {
	*useV3ReshardingMode = v3
	db := fakesqldb.Register()
	tc.ts = memorytopo.NewServer("cell1", "cell2")
	ctx := context.Background()
	tc.wi = NewInstance(tc.ts, "cell1", time.Second)

	if v3 {
		if err := tc.ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}); err != nil {
			tc.t.Fatalf("CreateKeyspace v3 failed: %v", err)
		}

		vs := &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"table1_index": {
					Type: "numeric",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"table1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{
						{
							Column: "keyspace_id",
							Name:   "table1_index",
						},
					},
				},
			},
		}
		if err := tc.ts.SaveVSchema(ctx, "ks", vs); err != nil {
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
	// leftReplica is used by the reparent test.
	leftReplica := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 12,
		topodatapb.TabletType_REPLICA, db, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))
	tc.leftReplica = leftReplica

	rightMaster := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 20,
		topodatapb.TabletType_MASTER, db, testlib.TabletKeyspaceShard(tc.t, "ks", "40-80"))
	rightRdonly := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 21,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(tc.t, "ks", "40-80"))

	tc.tablets = []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2, leftMaster, leftRdonly, tc.leftReplica, rightMaster, rightRdonly}

	for _, ft := range tc.tablets {
		ft.StartActionLoop(tc.t, tc.wi.wr)
	}

	// add the topo and schema data we'll need
	if err := tc.ts.CreateShard(ctx, "ks", "80-"); err != nil {
		tc.t.Fatalf("CreateShard(\"-80\") failed: %v", err)
	}
	if err := tc.wi.wr.SetKeyspaceShardingInfo(ctx, "ks", "keyspace_id", topodatapb.KeyspaceIdType_UINT64, false); err != nil {
		tc.t.Fatalf("SetKeyspaceShardingInfo failed: %v", err)
	}
	if err := tc.wi.wr.RebuildKeyspaceGraph(ctx, "ks", nil); err != nil {
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
					// Note that LegacySplitClone does not support the flag --min_rows_per_chunk.
					// Therefore, we use the default value in our calculation.
					// * 10 because --source_reader_count is set to 10 i.e. there are 10 chunks.
					RowCount: defaultMinRowsPerChunk * 10,
				},
			},
		}
		sourceRdonly.FakeMysqlDaemon.DbAppConnectionFactory = sourceRdonlyFactory(
			tc.t, "vt_ks", "table1", legacySplitCloneTestMin, legacySplitCloneTestMax)
		sourceRdonly.FakeMysqlDaemon.CurrentMasterPosition = replication.Position{
			GTIDSet: replication.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678},
		}
		sourceRdonly.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
			"STOP SLAVE",
			"START SLAVE",
		}
		qs := fakes.NewStreamHealthQueryService(sourceRdonly.Target())
		qs.AddDefaultHealthResponse()
		grpcqueryservice.Register(sourceRdonly.RPCServer, &legacyTestQueryService{
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
	tc.leftReplicaFakeDb = NewFakePoolConnectionQuery(tc.t, "leftReplica")
	tc.rightMasterFakeDb = NewFakePoolConnectionQuery(tc.t, "rightMaster")

	for i := 1; i <= 30; i++ {
		tc.leftMasterFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", nil)
		// leftReplica is unused by default.
		tc.rightMasterFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", nil)
	}
	expectBlpCheckpointCreationQueries(tc.leftMasterFakeDb)
	expectBlpCheckpointCreationQueries(tc.rightMasterFakeDb)

	leftMaster.FakeMysqlDaemon.DbAppConnectionFactory = tc.leftMasterFakeDb.getFactory()
	leftReplica.FakeMysqlDaemon.DbAppConnectionFactory = tc.leftReplicaFakeDb.getFactory()
	rightMaster.FakeMysqlDaemon.DbAppConnectionFactory = tc.rightMasterFakeDb.getFactory()

	// Fake stream health reponses because vtworker needs them to find the master.
	tc.leftMasterQs = fakes.NewStreamHealthQueryService(leftMaster.Target())
	tc.leftMasterQs.AddDefaultHealthResponse()
	tc.leftReplicaQs = fakes.NewStreamHealthQueryService(leftReplica.Target())
	tc.leftReplicaQs.AddDefaultHealthResponse()
	tc.rightMasterQs = fakes.NewStreamHealthQueryService(rightMaster.Target())
	tc.rightMasterQs.AddDefaultHealthResponse()
	grpcqueryservice.Register(leftMaster.RPCServer, tc.leftMasterQs)
	grpcqueryservice.Register(leftReplica.RPCServer, tc.leftReplicaQs)
	grpcqueryservice.Register(rightMaster.RPCServer, tc.rightMasterQs)

	tc.defaultWorkerArgs = []string{
		"LegacySplitClone",
		"-source_reader_count", "10",
		"-destination_pack_count", "4",
		"-destination_writer_count", "10",
		"ks/-80"}
}

func (tc *legacySplitCloneTestCase) tearDown() {
	for _, ft := range tc.tablets {
		ft.StopActionLoop(tc.t)
	}
	tc.leftMasterFakeDb.verifyAllExecutedOrFail()
	tc.leftReplicaFakeDb.verifyAllExecutedOrFail()
	tc.rightMasterFakeDb.verifyAllExecutedOrFail()
}

// legacyTestQueryService is a local QueryService implementation to support the tests.
type legacyTestQueryService struct {
	t *testing.T

	*fakes.StreamHealthQueryService
}

func (sq *legacyTestQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, options *querypb.ExecuteOptions, sendReply func(reply *sqltypes.Result) error) error {
	// Custom parsing of the query we expect.
	min := legacySplitCloneTestMin
	max := legacySplitCloneTestMax
	var err error
	parts := strings.Split(sql, " ")
	for _, part := range parts {
		if strings.HasPrefix(part, "`id`>=") {
			min, err = strconv.Atoi(part[6:])
			if err != nil {
				return err
			}
		} else if strings.HasPrefix(part, "`id`<") {
			max, err = strconv.Atoi(part[5:])
		}
	}
	sq.t.Logf("legacyTestQueryService: got query: %v with min %v max %v", sql, min, max)

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

func TestLegacySplitCloneV2(t *testing.T) {
	tc := &legacySplitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatal(err)
	}
}

func TestLegacySplitCloneV2_Throttled(t *testing.T) {
	tc := &legacySplitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// Run LegacySplitClone throttled and verify that it took longer than usual (~25ms).

	// Modify args to set -max_tps to 300.
	args := []string{"LegacySplitClone", "-max_tps", "300"}
	args = append(args, tc.defaultWorkerArgs[1:]...)

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, args); err != nil {
		t.Fatal(err)
	}

	// 30 transactions (tx) at a rate of 300 TPS should take at least 33 ms since:
	// 300 TPS across 10 writer threads: 30 tx/second/thread
	// => minimum request interval between two tx: 1 s / 30 tx/s = 33 ms
	// 3 transactions are throttled for 33 ms at least because:
	// - 1st tx: goes through immediately
	// - 2nd tx: may not be throttled when 1st tx happened at the end of its
	//           throttle request interval (negligible backoff)
	// - 3rd tx: throttled for 33 ms at least since 2nd tx happened
	want := 33 * time.Millisecond
	copyDuration := time.Duration(statsStateDurationsNs.Counts()[string(WorkerStateCloneOffline)]) * time.Nanosecond
	if copyDuration < want {
		t.Errorf("throttled copy was too fast: %v < %v", copyDuration, want)
	}
	t.Logf("throttled copy took: %v", copyDuration)
	// At least one thread should have been throttled.
	if counts := statsThrottledCounters.Counts(); len(counts) == 0 {
		t.Error("worker should have had one throttled thread at least")
	}
}

// TestLegacySplitCloneV2_RetryDueToReadonly is identical to the regular test
// TestLegacySplitCloneV2 with the additional twist that the destination masters
// fail the first write because they are read-only and succeed after that.
func TestLegacySplitCloneV2_RetryDueToReadonly(t *testing.T) {
	tc := &legacySplitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// Provoke a retry to test the error handling.
	tc.leftMasterFakeDb.addExpectedQueryAtIndex(0, "INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", errReadOnly)
	tc.rightMasterFakeDb.addExpectedQueryAtIndex(0, "INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", errReadOnly)
	// Only wait 1 ms between retries, so that the test passes faster.
	*executeFetchRetryTime = 1 * time.Millisecond

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatal(err)
	}

	wantRetryCount := int64(2)
	if got := statsRetryCount.Get(); got != wantRetryCount {
		t.Errorf("Wrong statsRetryCounter: got %v, wanted %v", got, wantRetryCount)
	}
	wantRetryReadOnlyCount := int64(2)
	if got := statsRetryCounters.Counts()[retryCategoryReadOnly]; got != wantRetryReadOnlyCount {
		t.Errorf("Wrong statsRetryCounters: got %v, wanted %v", got, wantRetryReadOnlyCount)
	}
}

// TestLegacySplitCloneV2_RetryDueToReparent tests that vtworker correctly failovers
// during a reparent.
// NOTE: worker.py is an end-to-end test which tests this as well.
func TestLegacySplitCloneV2_RetryDueToReparent(t *testing.T) {
	tc := &legacySplitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// Provoke a reparent just before the copy finishes.
	// leftReplica will take over for the last, 30th, insert and the BLP checkpoint.
	tc.leftReplicaFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", nil)
	expectBlpCheckpointCreationQueries(tc.leftReplicaFakeDb)

	// Do not let leftMaster succeed the 30th write.
	tc.leftMasterFakeDb.deleteAllEntriesAfterIndex(28)
	tc.leftMasterFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", errReadOnly)
	tc.leftMasterFakeDb.enableInfinite()
	// When vtworker encounters the readonly error on leftMaster, do the reparent.
	tc.leftMasterFakeDb.getEntry(29).AfterFunc = func() {
		// Reparent from leftMaster to leftReplica.
		// NOTE: This step is actually not necessary due to our fakes which bypass
		//       a lot of logic. Let's keep it for correctness though.
		ti, err := tc.ts.GetTablet(context.Background(), tc.leftReplica.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet failed: %v", err)
		}
		tmc := tmclient.NewTabletManagerClient()
		if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, "wait id 1"); err != nil {
			t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
		}

		// Update targets in fake query service and send out a new health response.
		tc.leftMasterQs.UpdateType(topodatapb.TabletType_REPLICA)
		tc.leftMasterQs.AddDefaultHealthResponse()
		tc.leftReplicaQs.UpdateType(topodatapb.TabletType_MASTER)
		tc.leftReplicaQs.AddDefaultHealthResponse()

		// After this, vtworker will retry. The following situations can occur:
		// 1. HealthCheck picked up leftReplica as new MASTER
		//    => retry will succeed.
		// 2. HealthCheck picked up no changes (leftMaster remains MASTER)
		//    => retry will hit leftMaster which keeps responding with readonly err.
		// 3. HealthCheck picked up leftMaster as REPLICA, but leftReplica is still
		//    a REPLICA.
		//    => vtworker has no MASTER to go to and will keep retrying.
	}

	// Only wait 1 ms between retries, so that the test passes faster.
	*executeFetchRetryTime = 1 * time.Millisecond

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatal(err)
	}

	wantRetryCountMin := int64(1)
	if got := statsRetryCount.Get(); got < wantRetryCountMin {
		t.Errorf("Wrong statsRetryCounter: got %v, wanted >= %v", got, wantRetryCountMin)
	}
}

// TestLegacySplitCloneV2_NoMasterAvailable tests that vtworker correctly retries
// even in a period where no MASTER tablet is available according to the
// HealthCheck instance.
func TestLegacySplitCloneV2_NoMasterAvailable(t *testing.T) {
	tc := &legacySplitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// leftReplica will take over for the last, 30th, insert and the BLP checkpoint.
	tc.leftReplicaFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", nil)
	expectBlpCheckpointCreationQueries(tc.leftReplicaFakeDb)

	// During the 29th write, let the MASTER disappear.
	tc.leftMasterFakeDb.getEntry(28).AfterFunc = func() {
		tc.leftMasterQs.UpdateType(topodatapb.TabletType_REPLICA)
		tc.leftMasterQs.AddDefaultHealthResponse()
	}

	// If the HealthCheck didn't pick up the change yet, the 30th write would
	// succeed. To prevent this from happening, replace it with an error.
	tc.leftMasterFakeDb.deleteAllEntriesAfterIndex(28)
	tc.leftMasterFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", errReadOnly)
	tc.leftMasterFakeDb.enableInfinite()
	// vtworker may not retry on leftMaster again if HealthCheck picks up the
	// change very fast. In that case, the error was never encountered.
	// Delete it or verifyAllExecutedOrFail() will fail because it was not
	// processed.
	defer tc.leftMasterFakeDb.deleteAllEntriesAfterIndex(28)

	// Wait for a retry due to NoMasterAvailable to happen, expect the 30th write
	// on leftReplica and change leftReplica from REPLICA to MASTER.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		for {
			if statsRetryCounters.Counts()[retryCategoryNoMasterAvailable] >= 1 {
				break
			}

			select {
			case <-ctx.Done():
				t.Fatalf("timed out waiting for vtworker to retry due to NoMasterAvailable: %v", ctx.Err())
			case <-time.After(10 * time.Millisecond):
				// Poll constantly.
			}
		}

		// Make leftReplica the new MASTER.
		tc.leftReplicaQs.UpdateType(topodatapb.TabletType_MASTER)
		tc.leftReplicaQs.AddDefaultHealthResponse()
	}()

	// Only wait 1 ms between retries, so that the test passes faster.
	*executeFetchRetryTime = 1 * time.Millisecond

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatal(err)
	}
}

func TestLegacySplitCloneV3(t *testing.T) {
	tc := &legacySplitCloneTestCase{t: t}
	tc.setUp(true /* v3 */)
	defer tc.tearDown()

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatal(err)
	}
}
