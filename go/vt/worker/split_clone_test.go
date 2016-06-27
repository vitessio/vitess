// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
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
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
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

	// Source tablets.
	// This test uses two source and destination rdonly tablets because
	// --min_healthy_rdonly_tablets is set to 2 in the test.
	sourceRdonlyQs []*testQueryService

	// Destination tablets.
	leftMasterFakeDb  *FakePoolConnection
	leftMasterQs      *fakes.StreamHealthQueryService
	rightMasterFakeDb *FakePoolConnection
	rightMasterQs     *fakes.StreamHealthQueryService

	// leftReplica is used by the reparent test.
	leftReplica       *testlib.FakeTablet
	leftReplicaFakeDb *FakePoolConnection
	leftReplicaQs     *fakes.StreamHealthQueryService

	// leftRdonlyQs are used by the Clone to diff against the source.
	leftRdonlyQs []*testQueryService
	// rightRdonlyQs are used by the Clone to diff against the source.
	rightRdonlyQs []*testQueryService

	// defaultWorkerArgs are the full default arguments to run SplitClone.
	defaultWorkerArgs []string
}

func (tc *splitCloneTestCase) setUp(v3 bool) {
	tc.setUpWithConcurreny(v3, 10, 2)
}

func (tc *splitCloneTestCase) setUpWithConcurreny(v3 bool, concurrency, writeQueryMaxRows int) {
	// In the default test case there are 100 rows on the source.
	rowsTotal := 100

	*useV3ReshardingMode = v3
	db := fakesqldb.Register()
	tc.ts = zktestserver.New(tc.t, []string{"cell1", "cell2"})
	ctx := context.Background()
	tc.wi = NewInstance(ctx, tc.ts, "cell1", time.Second)

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
	// leftReplica is used by the reparent test.
	leftReplica := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 11,
		topodatapb.TabletType_REPLICA, db, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))
	tc.leftReplica = leftReplica
	leftRdonly1 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 12,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))
	leftRdonly2 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 13,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))

	rightMaster := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 20,
		topodatapb.TabletType_MASTER, db, testlib.TabletKeyspaceShard(tc.t, "ks", "40-80"))
	rightRdonly1 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 22,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(tc.t, "ks", "40-80"))
	rightRdonly2 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 23,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(tc.t, "ks", "40-80"))

	tc.tablets = []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2,
		leftMaster, tc.leftReplica, leftRdonly1, leftRdonly2, rightMaster, rightRdonly1, rightRdonly2}

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
					// Set the table size to a value higher than --min_table_size_for_split.
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
		shqs := fakes.NewStreamHealthQueryService(sourceRdonly.Target())
		shqs.AddDefaultHealthResponse()
		qs := newTestQueryService(tc.t, sourceRdonly.Target(), shqs, 0, 1, sourceRdonly.Tablet.Alias.Uid)
		qs.addGeneratedRows(100, 100+rowsTotal)
		grpcqueryservice.RegisterForTest(sourceRdonly.RPCServer, qs)
		tc.sourceRdonlyQs = append(tc.sourceRdonlyQs, qs)
	}
	// Set up destination rdonlys which will be used as input for the diff during the clone.
	for i, destRdonly := range []*testlib.FakeTablet{leftRdonly1, rightRdonly1, leftRdonly2, rightRdonly2} {
		shqs := fakes.NewStreamHealthQueryService(destRdonly.Target())
		shqs.AddDefaultHealthResponse()
		qs := newTestQueryService(tc.t, destRdonly.Target(), shqs, i%2, 2, destRdonly.Tablet.Alias.Uid)
		grpcqueryservice.RegisterForTest(destRdonly.RPCServer, qs)
		if i%2 == 0 {
			tc.leftRdonlyQs = append(tc.leftRdonlyQs, qs)
		} else {
			tc.rightRdonlyQs = append(tc.rightRdonlyQs, qs)
		}
	}

	tc.leftMasterFakeDb = NewFakePoolConnectionQuery(tc.t, "leftMaster")
	tc.leftReplicaFakeDb = NewFakePoolConnectionQuery(tc.t, "leftReplica")
	tc.rightMasterFakeDb = NewFakePoolConnectionQuery(tc.t, "rightMaster")

	// In the default test case there will be 30 inserts per destination shard
	// because 10 writer threads will insert 5 rows on each destination shard.
	// (100 source rows / 10 writers / 2 shards = 5 rows.)
	// Due to --write_query_max_rows=2 there will be 3 inserts for 5 rows.
	rowsPerDestinationShard := rowsTotal / 2
	rowsPerThread := rowsPerDestinationShard / concurrency
	insertsPerThread := math.Ceil(float64(rowsPerThread) / float64(writeQueryMaxRows))
	insertsTotal := int(insertsPerThread) * concurrency
	for i := 1; i <= insertsTotal; i++ {
		tc.leftMasterFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.table1 (id, msg, keyspace_id) VALUES (*", nil)
		// leftReplica is unused by default.
		tc.rightMasterFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.table1 (id, msg, keyspace_id) VALUES (*", nil)
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
	grpcqueryservice.RegisterForTest(leftMaster.RPCServer, tc.leftMasterQs)
	grpcqueryservice.RegisterForTest(leftReplica.RPCServer, tc.leftReplicaQs)
	grpcqueryservice.RegisterForTest(rightMaster.RPCServer, tc.rightMasterQs)

	tc.defaultWorkerArgs = []string{
		"SplitClone",
		"-online=false",
		// --max_tps is only specified to enable the throttler and ensure that the
		// code is executed. But the intent here is not to throttle the test, hence
		// the rate limit is set very high.
		"-max_tps", "9999",
		"-write_query_max_rows", strconv.Itoa(writeQueryMaxRows),
		"-source_reader_count", strconv.Itoa(concurrency),
		"-min_table_size_for_split", "1",
		"-destination_writer_count", strconv.Itoa(concurrency),
		"ks/-80"}
}

func (tc *splitCloneTestCase) tearDown() {
	for _, ft := range tc.tablets {
		ft.StopActionLoop(tc.t)
	}
	tc.leftMasterFakeDb.verifyAllExecutedOrFail()
	tc.leftReplicaFakeDb.verifyAllExecutedOrFail()
	tc.rightMasterFakeDb.verifyAllExecutedOrFail()
}

// testQueryService is a local QueryService implementation to support the tests.
type testQueryService struct {
	t *testing.T

	// target is used in the log output.
	target querypb.Target
	*fakes.StreamHealthQueryService
	shardIndex int
	shardCount int
	tabletUID  uint32
	fields     []*querypb.Field
	rows       [][]sqltypes.Value
}

func newTestQueryService(t *testing.T, target querypb.Target, shqs *fakes.StreamHealthQueryService, shardIndex, shardCount int, tabletUID uint32) *testQueryService {
	return &testQueryService{
		t:      t,
		target: target,
		StreamHealthQueryService: shqs,
		shardIndex:               shardIndex,
		shardCount:               shardCount,
		tabletUID:                tabletUID,
		fields:                   v2Fields,
	}
}

func (sq *testQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, sendReply func(reply *sqltypes.Result) error) error {
	// Custom parsing of the query we expect.
	// Example: SELECT id, msg, keyspace_id FROM table1 WHERE id>=180 AND id<190 ORDER BY id
	min := math.MinInt32
	max := math.MaxInt32
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
			if err != nil {
				return err
			}
		}
	}
	sq.t.Logf("testQueryService: %v,%v/%v/%v: got query: %v with min %v max (exclusive) %v", sq.tabletUID, sq.target.Keyspace, sq.target.Shard, sq.target.TabletType, sql, min, max)

	// Send the headers.
	if err := sendReply(&sqltypes.Result{Fields: sq.fields}); err != nil {
		return err
	}

	// Send the values.
	rowsAffected := 0
	for _, row := range sq.rows {
		primaryKey := row[0].ToNative().(int64)
		if primaryKey >= int64(min) && primaryKey < int64(max) {
			if err := sendReply(&sqltypes.Result{
				Rows: [][]sqltypes.Value{row},
			}); err != nil {
				return err
			}
			// Uncomment the next line during debugging when needed.
			// sq.t.Logf("testQueryService: %v,%v/%v/%v: sent row for id: %v row: %v", sq.tabletUID, sq.target.Keyspace, sq.target.Shard, sq.target.TabletType, primaryKey, row)
			rowsAffected++
		}
	}

	if rowsAffected == 0 {
		sq.t.Logf("testQueryService: %v,%v/%v/%v: no rows were sent (%v are available)", sq.tabletUID, sq.target.Keyspace, sq.target.Shard, sq.target.TabletType, len(sq.rows))
	}
	return nil
}

// addGeneratedRows will add from-to generated rows. The rows (their primary
// key) will be in the range [from, to).
func (sq *testQueryService) addGeneratedRows(from, to int) {
	var rows [][]sqltypes.Value
	// ksids has keyspace ids which are covered by the shard key ranges -40 and 40-80.
	ksids := []uint64{0x2000000000000000, 0x6000000000000000}

	for id := from; id < to; id++ {
		// Only return the rows which are covered by this shard.
		shardIndex := id % 2
		if sq.shardCount == 1 || shardIndex == sq.shardIndex {
			idValue, _ := sqltypes.BuildValue(int64(id))
			rows = append(rows, []sqltypes.Value{
				idValue,
				sqltypes.MakeString([]byte(fmt.Sprintf("Text for %v", id))),
				sqltypes.MakeString([]byte(fmt.Sprintf("%v", ksids[shardIndex]))),
			})
		}
	}

	if sq.rows == nil {
		sq.rows = rows
	} else {
		sq.rows = append(sq.rows, rows...)
	}
}

func (sq *testQueryService) modifyFirstRows(count int) {
	// Modify the text of the first "count" rows.
	for i := 0; i < count; i++ {
		row := sq.rows[i]
		row[1] = sqltypes.MakeString([]byte(fmt.Sprintf("OUTDATED ROW: %v", row[1].String())))
	}
}

func (sq *testQueryService) clearRows() {
	sq.rows = nil
}

var v2Fields = []*querypb.Field{
	{
		Name: "id",
		Type: sqltypes.Int64,
	},
	{
		Name: "msg",
		Type: sqltypes.VarChar,
	},
	// TODO(mberlin): Omit keyspace_id in the v3 test.
	{
		Name: "keyspace_id",
		Type: sqltypes.Int64,
	},
}

// TestSplitCloneV2_Offline tests the offline phase with an empty destination.
func TestSplitCloneV2_Offline(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatal(err)
	}
}

// TestSplitCloneV2_Online tests the online phase with an empty destination.
func TestSplitCloneV2_Online(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// In the online phase we won't enable filtered replication. Don't expect it.
	tc.leftMasterFakeDb.deleteAllEntriesAfterIndex(29)
	tc.rightMasterFakeDb.deleteAllEntriesAfterIndex(29)

	// Run the vtworker command.
	args := make([]string, len(tc.defaultWorkerArgs))
	copy(args, tc.defaultWorkerArgs)
	args[1] = "-offline=false"
	if err := runCommand(t, tc.wi, tc.wi.wr, args); err != nil {
		t.Fatal(err)
	}
	if inserts := statsOnlineInsertsCounters.Counts()["table1"]; inserts != 100 {
		t.Errorf("wrong number of rows inserted: got = %v, want = %v", inserts, 100)
	}
	if updates := statsOnlineUpdatesCounters.Counts()["table1"]; updates != 0 {
		t.Errorf("wrong number of rows updated: got = %v, want = %v", updates, 0)
	}
	if deletes := statsOnlineDeletesCounters.Counts()["table1"]; deletes != 0 {
		t.Errorf("wrong number of rows deleted: got = %v, want = %v", deletes, 0)
	}
	if inserts := statsOfflineInsertsCounters.Counts()["table1"]; inserts != 0 {
		t.Errorf("no stats for the offline clone phase should have been modified. got inserts = %v", inserts)
	}
	if updates := statsOfflineUpdatesCounters.Counts()["table1"]; updates != 0 {
		t.Errorf("no stats for the offline clone phase should have been modified. got updates = %v", updates)
	}
	if deletes := statsOfflineDeletesCounters.Counts()["table1"]; deletes != 0 {
		t.Errorf("no stats for the offline clone phase should have been modified. got deletes = %v", deletes)
	}
}

func TestSplitCloneV2_Online_Offline(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// When the online clone inserted the last rows, modify the destination test
	// query service such that it will return them as well.
	tc.leftMasterFakeDb.getEntry(29).AfterFunc = func() {
		for i := range []int{0, 1} {
			tc.leftRdonlyQs[i].addGeneratedRows(100, 200)
		}
	}
	tc.rightMasterFakeDb.getEntry(29).AfterFunc = func() {
		for i := range []int{0, 1} {
			tc.rightRdonlyQs[i].addGeneratedRows(100, 200)
		}
	}

	// Run the vtworker command.
	args := []string{"SplitClone"}
	args = append(args, tc.defaultWorkerArgs[2:]...)
	if err := runCommand(t, tc.wi, tc.wi.wr, args); err != nil {
		t.Fatal(err)
	}
	if inserts := statsOnlineInsertsCounters.Counts()["table1"]; inserts != 100 {
		t.Errorf("wrong number of rows inserted: got = %v, want = %v", inserts, 100)
	}
	if updates := statsOnlineUpdatesCounters.Counts()["table1"]; updates != 0 {
		t.Errorf("wrong number of rows updated: got = %v, want = %v", updates, 0)
	}
	if deletes := statsOnlineDeletesCounters.Counts()["table1"]; deletes != 0 {
		t.Errorf("wrong number of rows deleted: got = %v, want = %v", deletes, 0)
	}
	if inserts := statsOfflineInsertsCounters.Counts()["table1"]; inserts != 0 {
		t.Errorf("no stats for the offline clone phase should have been modified. got inserts = %v", inserts)
	}
	if updates := statsOfflineUpdatesCounters.Counts()["table1"]; updates != 0 {
		t.Errorf("no stats for the offline clone phase should have been modified. got updates = %v", updates)
	}
	if deletes := statsOfflineDeletesCounters.Counts()["table1"]; deletes != 0 {
		t.Errorf("no stats for the offline clone phase should have been modified. got deletes = %v", deletes)
	}
}

// TestSplitCloneV2_Reconciliation is identical to TestSplitCloneV2_Offline,
// but the destination has existing data which must be reconciled.
func TestSplitCloneV2_Reconciliation(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	// We reduce the parallelism to 1 to test the order of expected
	// insert/update/delete statements on the destination master.
	tc.setUpWithConcurreny(false /* v3 */, 1, 10)
	defer tc.tearDown()

	// We assume that an Online Clone ran before which copied the rows 100-199
	// to the destination.
	// In this Offline Clone the rows 96-99 are new on the destination, 100-103
	// must be updated and 190-199 have to be deleted from the destination shards.
	//
	// Add data on the source tablets.
	// In addition, the source has extra rows before the regular rows.
	for _, qs := range tc.sourceRdonlyQs {
		qs.clearRows()
		// Rows 96-100 are not on the destination.
		qs.addGeneratedRows(96, 100)
		// Rows 100-190 are on the destination as well.
		qs.addGeneratedRows(100, 190)
	}

	for i := range []int{0, 1} {
		// Both destination shards have the rows 100-200.
		tc.leftRdonlyQs[i].addGeneratedRows(100, 200)
		tc.rightRdonlyQs[i].addGeneratedRows(100, 200)
		// But some data is outdated data and must be updated.
		tc.leftRdonlyQs[i].modifyFirstRows(2)
		tc.rightRdonlyQs[i].modifyFirstRows(2)
	}

	// The destination tablets should see inserts, updates and deletes.
	// Clear the entries added by setUp() because the reconcilation will
	// produce different statements in this test case.
	tc.leftMasterFakeDb.deleteAllEntries()
	tc.rightMasterFakeDb.deleteAllEntries()
	// Update statements. (One query will update one row.)
	tc.leftMasterFakeDb.addExpectedQuery("UPDATE `vt_ks`.table1 SET msg='Text for 100',keyspace_id=2305843009213693952 WHERE id=100", nil)
	tc.leftMasterFakeDb.addExpectedQuery("UPDATE `vt_ks`.table1 SET msg='Text for 102',keyspace_id=2305843009213693952 WHERE id=102", nil)
	tc.rightMasterFakeDb.addExpectedQuery("UPDATE `vt_ks`.table1 SET msg='Text for 101',keyspace_id=6917529027641081856 WHERE id=101", nil)
	tc.rightMasterFakeDb.addExpectedQuery("UPDATE `vt_ks`.table1 SET msg='Text for 103',keyspace_id=6917529027641081856 WHERE id=103", nil)
	// Insert statements. (All are combined in one.)
	tc.leftMasterFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.table1 (id, msg, keyspace_id) VALUES (96,'Text for 96',2305843009213693952),(98,'Text for 98',2305843009213693952)", nil)
	tc.rightMasterFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.table1 (id, msg, keyspace_id) VALUES (97,'Text for 97',6917529027641081856),(99,'Text for 99',6917529027641081856)", nil)
	// Delete statements. (All are combined in one.)
	tc.leftMasterFakeDb.addExpectedQuery("DELETE FROM `vt_ks`.table1 WHERE (id) IN ((190),(192),(194),(196),(198))", nil)
	tc.rightMasterFakeDb.addExpectedQuery("DELETE FROM `vt_ks`.table1 WHERE (id) IN ((191),(193),(195),(197),(199))", nil)
	expectBlpCheckpointCreationQueries(tc.leftMasterFakeDb)
	expectBlpCheckpointCreationQueries(tc.rightMasterFakeDb)

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatal(err)
	}
	if inserts := statsOnlineInsertsCounters.Counts()["table1"]; inserts != 0 {
		t.Errorf("no stats for the online clone phase should have been modified. got inserts = %v", inserts)
	}
	if updates := statsOnlineUpdatesCounters.Counts()["table1"]; updates != 0 {
		t.Errorf("no stats for the online clone phase should have been modified. got updates = %v", updates)
	}
	if deletes := statsOnlineDeletesCounters.Counts()["table1"]; deletes != 0 {
		t.Errorf("no stats for the online clone phase should have been modified. got deletes = %v", deletes)
	}
	if inserts := statsOfflineInsertsCounters.Counts()["table1"]; inserts != 4 {
		t.Errorf("wrong number of rows inserted: got = %v, want = %v", inserts, 4)
	}
	if updates := statsOfflineUpdatesCounters.Counts()["table1"]; updates != 4 {
		t.Errorf("wrong number of rows updated: got = %v, want = %v", updates, 4)
	}
	if deletes := statsOfflineDeletesCounters.Counts()["table1"]; deletes != 10 {
		t.Errorf("wrong number of rows deleted: got = %v, want = %v", deletes, 10)
	}
}

func TestSplitCloneV2_Throttled(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// Run SplitClone throttled and verify that it took longer than usual (~25ms).

	// Modify args to set -max_tps to 300.
	args := make([]string, len(tc.defaultWorkerArgs))
	copy(args, tc.defaultWorkerArgs)
	args[3] = "300"

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, args); err != nil {
		t.Fatal(err)
	}

	// Each of the 10 writer threads will issue 3 transactions (30 in total).
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

// TestSplitCloneV2_RetryDueToReadonly is identical to the regular test
// TestSplitCloneV2 with the additional twist that the destination masters
// fail the first write because they are read-only and succeed after that.
func TestSplitCloneV2_RetryDueToReadonly(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// Provoke a retry to test the error handling.
	tc.leftMasterFakeDb.addExpectedQueryAtIndex(0, "INSERT INTO `vt_ks`.table1 (id, msg, keyspace_id) VALUES (*", errReadOnly)
	tc.rightMasterFakeDb.addExpectedQueryAtIndex(0, "INSERT INTO `vt_ks`.table1 (id, msg, keyspace_id) VALUES (*", errReadOnly)
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

// TestSplitCloneV2_RetryDueToReparent tests that vtworker correctly failovers
// during a reparent.
// NOTE: worker.py is an end-to-end test which tests this as well.
func TestSplitCloneV2_RetryDueToReparent(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// Provoke a reparent just before the copy finishes.
	// leftReplica will take over for the last, 30th, insert and the BLP checkpoint.
	tc.leftReplicaFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.table1 (id, msg, keyspace_id) VALUES (*", nil)
	expectBlpCheckpointCreationQueries(tc.leftReplicaFakeDb)

	// Do not let leftMaster succeed the 30th write.
	tc.leftMasterFakeDb.deleteAllEntriesAfterIndex(28)
	tc.leftMasterFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.table1 (id, msg, keyspace_id) VALUES (*", errReadOnly)
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

// TestSplitCloneV2_NoMasterAvailable tests that vtworker correctly retries
// even in a period where no MASTER tablet is available according to the
// HealthCheck instance.
func TestSplitCloneV2_NoMasterAvailable(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// leftReplica will take over for the last, 30th, insert and the BLP checkpoint.
	tc.leftReplicaFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.table1 (id, msg, keyspace_id) VALUES (*", nil)
	expectBlpCheckpointCreationQueries(tc.leftReplicaFakeDb)

	// During the 29th write, let the MASTER disappear.
	tc.leftMasterFakeDb.getEntry(28).AfterFunc = func() {
		tc.leftMasterQs.UpdateType(topodatapb.TabletType_REPLICA)
		tc.leftMasterQs.AddDefaultHealthResponse()
	}

	// If the HealthCheck didn't pick up the change yet, the 30th write would
	// succeed. To prevent this from happening, replace it with an error.
	tc.leftMasterFakeDb.deleteAllEntriesAfterIndex(28)
	tc.leftMasterFakeDb.addExpectedQuery("INSERT INTO `vt_ks`.table1 (id, msg, keyspace_id) VALUES (*", errReadOnly)
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

func TestSplitCloneV3(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(true /* v3 */)
	defer tc.tearDown()

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatal(err)
	}
}
