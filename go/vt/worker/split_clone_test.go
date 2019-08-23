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
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler/testlib"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

const (
	// splitCloneTestMin is the minimum value of the primary key.
	splitCloneTestMin int = 100
	// splitCloneTestMax is the maximum value of the primary key.
	splitCloneTestMax int = 200
	// In the default test case there are 100 rows on the source.
	splitCloneTestRowsCount = splitCloneTestMax - splitCloneTestMin
)

var (
	errReadOnly = errors.New("the MariaDB server is running with the --read-only option so it cannot execute this statement (errno 1290) during query: ")

	errStreamingQueryTimeout = errors.New("vttablet: generic::unknown: error: the query was killed either because it timed out or was canceled: (errno 2013) (sqlstate HY000) during query: ")
)

type splitCloneTestCase struct {
	t *testing.T

	ts      *topo.Server
	wi      *Instance
	tablets []*testlib.FakeTablet

	// Source tablets.
	// This test uses two source and destination rdonly tablets because
	// --min_healthy_rdonly_tablets is set to 2 in the test.
	sourceRdonlyQs []*testQueryService

	// Destination tablets.
	leftMasterFakeDb  *fakesqldb.DB
	leftMasterQs      *testQueryService
	rightMasterFakeDb *fakesqldb.DB
	rightMasterQs     *testQueryService

	// leftReplica is used by the reparent test.
	leftReplica       *testlib.FakeTablet
	leftReplicaFakeDb *fakesqldb.DB
	leftReplicaQs     *fakes.StreamHealthQueryService

	// leftRdonlyQs are used by the Clone to diff against the source.
	leftRdonlyQs []*testQueryService
	// rightRdonlyQs are used by the Clone to diff against the source.
	rightRdonlyQs []*testQueryService

	// defaultWorkerArgs are the full default arguments to run SplitClone.
	defaultWorkerArgs []string

	// Used to restore the default values after the test run
	defaultExecuteFetchRetryTime time.Duration
	defaultRetryDuration         time.Duration
}

func (tc *splitCloneTestCase) setUp(v3 bool) {
	tc.setUpWithConcurrency(v3, 10, 2, splitCloneTestRowsCount)
}

func (tc *splitCloneTestCase) setUpWithConcurrency(v3 bool, concurrency, writeQueryMaxRows, rowsCount int) {
	*useV3ReshardingMode = v3

	// Reset some retry flags for the tests that change that
	tc.defaultRetryDuration = *retryDuration
	tc.defaultExecuteFetchRetryTime = *executeFetchRetryTime

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

	// Create the fake databases.
	sourceRdonlyFakeDB := sourceRdonlyFakeDB(tc.t, "vt_ks", "table1", splitCloneTestMin, splitCloneTestMax)
	tc.leftMasterFakeDb = fakesqldb.New(tc.t).SetName("leftMaster").OrderMatters()
	tc.leftReplicaFakeDb = fakesqldb.New(tc.t).SetName("leftReplica").OrderMatters()
	tc.rightMasterFakeDb = fakesqldb.New(tc.t).SetName("rightMaster").OrderMatters()

	sourceMaster := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 0,
		topodatapb.TabletType_MASTER, nil, testlib.TabletKeyspaceShard(tc.t, "ks", "-80"))
	sourceRdonly1 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, sourceRdonlyFakeDB, testlib.TabletKeyspaceShard(tc.t, "ks", "-80"))
	sourceRdonly2 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 2,
		topodatapb.TabletType_RDONLY, sourceRdonlyFakeDB, testlib.TabletKeyspaceShard(tc.t, "ks", "-80"))

	leftMaster := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 10,
		topodatapb.TabletType_MASTER, tc.leftMasterFakeDb, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))
	// leftReplica is used by the reparent test.
	leftReplica := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 11,
		topodatapb.TabletType_REPLICA, tc.leftReplicaFakeDb, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))
	tc.leftReplica = leftReplica
	leftRdonly1 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 12,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))
	leftRdonly2 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 13,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))

	rightMaster := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 20,
		topodatapb.TabletType_MASTER, tc.rightMasterFakeDb, testlib.TabletKeyspaceShard(tc.t, "ks", "40-80"))
	rightRdonly1 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 22,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(tc.t, "ks", "40-80"))
	rightRdonly2 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 23,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(tc.t, "ks", "40-80"))

	tc.tablets = []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2,
		leftMaster, tc.leftReplica, leftRdonly1, leftRdonly2, rightMaster, rightRdonly1, rightRdonly2}

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
					Name: "table1",
					// "id" is the last column in the list on purpose to test for
					// regressions. The reconciliation code will SELECT with the primary
					// key columns first. The same ordering must be used throughout the
					// process e.g. by RowAggregator or the v2Resolver.
					Columns:           []string{"msg", "keyspace_id", "id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
					// Set the row count to avoid that --min_rows_per_chunk reduces the
					// number of chunks.
					RowCount: uint64(rowsCount),
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
		shqs := fakes.NewStreamHealthQueryService(sourceRdonly.Target())
		shqs.AddDefaultHealthResponse()
		qs := newTestQueryService(tc.t, sourceRdonly.Target(), shqs, 0, 1, topoproto.TabletAliasString(sourceRdonly.Tablet.Alias), false /* omitKeyspaceID */)
		qs.addGeneratedRows(100, 100+rowsCount)
		grpcqueryservice.Register(sourceRdonly.RPCServer, qs)
		tc.sourceRdonlyQs = append(tc.sourceRdonlyQs, qs)
	}
	// Set up destination rdonlys which will be used as input for the diff during the clone.
	for i, destRdonly := range []*testlib.FakeTablet{leftRdonly1, rightRdonly1, leftRdonly2, rightRdonly2} {
		shqs := fakes.NewStreamHealthQueryService(destRdonly.Target())
		shqs.AddDefaultHealthResponse()
		qs := newTestQueryService(tc.t, destRdonly.Target(), shqs, i%2, 2, topoproto.TabletAliasString(destRdonly.Tablet.Alias), false /* omitKeyspaceID */)
		grpcqueryservice.Register(destRdonly.RPCServer, qs)
		if i%2 == 0 {
			tc.leftRdonlyQs = append(tc.leftRdonlyQs, qs)
		} else {
			tc.rightRdonlyQs = append(tc.rightRdonlyQs, qs)
		}
	}

	// In the default test case there will be 30 inserts per destination shard
	// because 10 writer threads will insert 5 rows on each destination shard.
	// (100 rowsCount / 10 writers / 2 shards = 5 rows.)
	// Due to --write_query_max_rows=2 there will be 3 inserts for 5 rows.
	rowsPerDestinationShard := rowsCount / 2
	rowsPerThread := rowsPerDestinationShard / concurrency
	insertsPerThread := math.Ceil(float64(rowsPerThread) / float64(writeQueryMaxRows))
	insertsTotal := int(insertsPerThread) * concurrency
	for i := 1; i <= insertsTotal; i++ {
		tc.leftMasterFakeDb.AddExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", nil)
		// leftReplica is unused by default.
		tc.rightMasterFakeDb.AddExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", nil)
	}

	// Fake stream health reponses because vtworker needs them to find the master.
	shqs := fakes.NewStreamHealthQueryService(leftMaster.Target())
	shqs.AddDefaultHealthResponse()
	tc.leftMasterQs = newTestQueryService(tc.t, leftMaster.Target(), shqs, 0, 2, topoproto.TabletAliasString(leftMaster.Tablet.Alias), false /* omitKeyspaceID */)
	tc.leftReplicaQs = fakes.NewStreamHealthQueryService(leftReplica.Target())
	shqs = fakes.NewStreamHealthQueryService(rightMaster.Target())
	shqs.AddDefaultHealthResponse()
	tc.rightMasterQs = newTestQueryService(tc.t, rightMaster.Target(), shqs, 1, 2, topoproto.TabletAliasString(rightMaster.Tablet.Alias), false /* omitKeyspaceID */)
	grpcqueryservice.Register(leftMaster.RPCServer, tc.leftMasterQs)
	grpcqueryservice.Register(leftReplica.RPCServer, tc.leftReplicaQs)
	grpcqueryservice.Register(rightMaster.RPCServer, tc.rightMasterQs)

	tc.defaultWorkerArgs = []string{
		"SplitClone",
		"-online=false",
		// --max_tps is only specified to enable the throttler and ensure that the
		// code is executed. But the intent here is not to throttle the test, hence
		// the rate limit is set very high.
		"-max_tps", "9999",
		"-write_query_max_rows", strconv.Itoa(writeQueryMaxRows),
		"-chunk_count", strconv.Itoa(concurrency),
		"-min_rows_per_chunk", strconv.Itoa(rowsPerThread),
		"-source_reader_count", strconv.Itoa(concurrency),
		"-destination_writer_count", strconv.Itoa(concurrency),
		"ks/-80"}

	// Start action loop after having registered all RPC services.
	for _, ft := range tc.tablets {
		ft.StartActionLoop(tc.t, tc.wi.wr)
	}
}

func (tc *splitCloneTestCase) tearDown() {
	*retryDuration = tc.defaultRetryDuration
	*executeFetchRetryTime = tc.defaultExecuteFetchRetryTime

	for _, ft := range tc.tablets {
		ft.StopActionLoop(tc.t)
		ft.RPCServer.Stop()
		ft.FakeMysqlDaemon.Close()
		ft.Agent = nil
		ft.RPCServer = nil
		ft.FakeMysqlDaemon = nil
	}
	tc.leftMasterFakeDb.VerifyAllExecutedOrFail()
	tc.leftReplicaFakeDb.VerifyAllExecutedOrFail()
	tc.rightMasterFakeDb.VerifyAllExecutedOrFail()
}

// testQueryService is a local QueryService implementation to support the tests.
type testQueryService struct {
	t *testing.T

	// target is used in the log output.
	target querypb.Target
	*fakes.StreamHealthQueryService
	shardIndex int
	shardCount int
	alias      string
	// omitKeyspaceID is true when the returned rows should not contain the
	// "keyspace_id" column.
	omitKeyspaceID bool
	fields         []*querypb.Field
	rows           [][]sqltypes.Value

	// mu guards the fields in this group.
	// It is necessary because multiple Go routines will read from the same
	// tablet.
	mu sync.Mutex
	// forceError is set to true for a given int64 primary key value if
	// testQueryService should return an error instead of the actual row.
	forceError map[int64]int
	// errorCallback is run once after the first error is returned.
	errorCallback func()
}

func newTestQueryService(t *testing.T, target querypb.Target, shqs *fakes.StreamHealthQueryService, shardIndex, shardCount int, alias string, omitKeyspaceID bool) *testQueryService {
	fields := v2Fields
	if omitKeyspaceID {
		fields = v3Fields
	}
	return &testQueryService{
		t:                        t,
		target:                   target,
		StreamHealthQueryService: shqs,
		shardIndex:               shardIndex,
		shardCount:               shardCount,
		alias:                    alias,
		omitKeyspaceID:           omitKeyspaceID,
		fields:                   fields,
		forceError:               make(map[int64]int),
	}
}

func (sq *testQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(reply *sqltypes.Result) error) error {
	// Custom parsing of the query we expect.
	// Example: SELECT `id`, `msg`, `keyspace_id` FROM table1 WHERE id>=180 AND id<190 ORDER BY id
	min := math.MinInt32
	max := math.MaxInt32
	var err error
	parts := strings.Split(sql, " ")
	for _, part := range parts {
		if strings.HasPrefix(part, "`id`>=") {
			// Chunk start.
			min, err = strconv.Atoi(part[6:])
			if err != nil {
				return err
			}
		} else if strings.HasPrefix(part, "`id`>") {
			// Chunk start after restart.
			min, err = strconv.Atoi(part[5:])
			if err != nil {
				return err
			}
			// Increment by one to fulfill ">" instead of ">=".
			min++
		} else if strings.HasPrefix(part, "`id`<") {
			// Chunk end.
			max, err = strconv.Atoi(part[5:])
			if err != nil {
				return err
			}
		}
	}
	sq.t.Logf("testQueryService: %v,%v/%v/%v: got query: %v with min %v max (exclusive) %v", sq.alias, sq.target.Keyspace, sq.target.Shard, sq.target.TabletType, sql, min, max)

	if sq.forceErrorOnce(int64(min)) {
		sq.t.Logf("testQueryService: %v,%v/%v/%v: sending error for id: %v before sending the fields", sq.alias, sq.target.Keyspace, sq.target.Shard, sq.target.TabletType, min)
		return errStreamingQueryTimeout
	}

	// Send the headers.
	if err := callback(&sqltypes.Result{Fields: sq.fields}); err != nil {
		return err
	}

	// Send the values.
	rowsAffected := 0
	for _, row := range sq.rows {
		v, _ := sqltypes.ToNative(row[0])
		primaryKey := v.(int64)

		if primaryKey >= int64(min) && primaryKey < int64(max) {
			if sq.forceErrorOnce(primaryKey) {
				sq.t.Logf("testQueryService: %v,%v/%v/%v: sending error for id: %v row: %v", sq.alias, sq.target.Keyspace, sq.target.Shard, sq.target.TabletType, primaryKey, row)
				return errStreamingQueryTimeout
			}

			if err := callback(&sqltypes.Result{
				Rows: [][]sqltypes.Value{row},
			}); err != nil {
				return err
			}
			// Uncomment the next line during debugging when needed.
			//			sq.t.Logf("testQueryService: %v,%v/%v/%v: sent row for id: %v row: %v", sq.alias, sq.target.Keyspace, sq.target.Shard, sq.target.TabletType, primaryKey, row)
			rowsAffected++
		}
	}

	if rowsAffected == 0 {
		sq.t.Logf("testQueryService: %v,%v/%v/%v: no rows were sent (%v are available)", sq.alias, sq.target.Keyspace, sq.target.Shard, sq.target.TabletType, len(sq.rows))
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
			idValue := sqltypes.NewInt64(int64(id))

			row := []sqltypes.Value{
				idValue,
				sqltypes.NewVarBinary(fmt.Sprintf("Text for %v", id)),
			}
			if !sq.omitKeyspaceID {
				row = append(row, sqltypes.NewVarBinary(fmt.Sprintf("%v", ksids[shardIndex])))
			}
			rows = append(rows, row)
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
		row[1] = sqltypes.NewVarBinary(fmt.Sprintf("OUTDATED ROW: %v", row[1]))
	}
}

func (sq *testQueryService) clearRows() {
	sq.rows = nil
}

func (sq *testQueryService) errorStreamAtRow(primaryKey, times int) {
	sq.mu.Lock()
	defer sq.mu.Unlock()

	sq.forceError[int64(primaryKey)] = times
}

// setErrorCallback registers a function which will be called when the first
// error is injected. It will be run only once.
func (sq *testQueryService) setErrorCallback(cb func()) {
	sq.mu.Lock()
	defer sq.mu.Unlock()

	sq.errorCallback = cb
}

func (sq *testQueryService) forceErrorOnce(primaryKey int64) bool {
	sq.mu.Lock()
	defer sq.mu.Unlock()

	force := sq.forceError[primaryKey] > 0
	if force {
		sq.forceError[primaryKey]--
		if sq.errorCallback != nil {
			sq.errorCallback()
			sq.errorCallback = nil
		}
	}
	return force
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
	{
		Name: "keyspace_id",
		Type: sqltypes.Int64,
	},
}

// v3Fields is identical to v2Fields but lacks the "keyspace_id" column.
var v3Fields = []*querypb.Field{
	{
		Name: "id",
		Type: sqltypes.Int64,
	},
	{
		Name: "msg",
		Type: sqltypes.VarChar,
	},
}

// TestSplitCloneV2_Offline tests the offline phase with an empty destination.
func TestSplitCloneV2_Offline(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatalf("%+v", err)
	}
}

// TestSplitCloneV2_Offline_HighChunkCount is identical to
// TestSplitCloneV2_Offline but sets the --chunk_count to 1000. Given
// --source_reader_count=10, at most 10 out of the 1000 chunk pipeplines will
// get processed concurrently while the other pending ones are blocked.
func TestSplitCloneV2_Offline_HighChunkCount(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUpWithConcurrency(false /* v3 */, 10, 5 /* writeQueryMaxRows */, 1000 /* rowsCount */)
	defer tc.tearDown()

	args := make([]string, len(tc.defaultWorkerArgs))
	copy(args, tc.defaultWorkerArgs)
	// Set -write_query_max_rows to 5.
	args[5] = "5"
	// Set -chunk_count to 1000.
	args[7] = "1000"
	// Set -min_rows_per_chunk to 5.
	args[9] = "5"

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, args); err != nil {
		t.Fatal(err)
	}
}

// TestSplitCloneV2_Offline_RestartStreamingQuery is identical to
// TestSplitCloneV2_Offline but forces SplitClone to restart the streaming
// query on the source before reading the last row.
func TestSplitCloneV2_Offline_RestartStreamingQuery(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// Only wait 1 ms between retries, so that the test passes faster.
	*executeFetchRetryTime = 1 * time.Millisecond

	// Ensure that this test uses only the first tablet. This makes it easier
	// to verify that the restart actually happened for that tablet.
	// SplitClone will ignore the second tablet because we set its replication lag
	// to 1h.
	tc.sourceRdonlyQs[1].AddHealthResponseWithSecondsBehindMaster(3600)

	// TODO(mberlin): Change this test to use a multi-column primary key because
	// the restart generates a WHERE clause which includes all primary key
	// columns.
	// We fail when returning the last row to ensure that there is only one thread
	// left reading from the source tablet.
	tc.sourceRdonlyQs[0].errorStreamAtRow(199, 1)

	// Run the vtworker command.
	// We require only 1 instead of the default 2 replicas.
	args := []string{"SplitClone", "--min_healthy_rdonly_tablets", "1"}
	args = append(args, tc.defaultWorkerArgs[1:]...)
	if err := runCommand(t, tc.wi, tc.wi.wr, args); err != nil {
		t.Fatal(err)
	}

	alias := tc.sourceRdonlyQs[0].alias
	if got, want := statsStreamingQueryErrorsCounters.Counts()[alias], int64(1); got != want {
		t.Errorf("wrong number of errored streaming query for tablet: %v: got = %v, want = %v", alias, got, want)
	}
	if got, want := statsStreamingQueryCounters.Counts()[alias], int64(11); got != want {
		t.Errorf("wrong number of streaming query starts for tablet: %v: got = %v, want = %v", alias, got, want)
	}
}

// TestSplitCloneV2_Offline_FailOverStreamingQuery_NotAllowed is similar to
// TestSplitCloneV2_Offline_RestartStreamingQuery. However, the first restart
// of the streaming query does not succeed here and instead vtworker will fail.
func TestSplitCloneV2_Offline_FailOverStreamingQuery_NotAllowed(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUpWithConcurrency(false /* v3 */, 1, 10, splitCloneTestRowsCount)
	defer tc.tearDown()

	// Only wait 1 ms between retries, so that the test passes faster.
	*executeFetchRetryTime = 1 * time.Millisecond

	// Ensure that this test uses only the first tablet.
	tc.sourceRdonlyQs[1].AddHealthResponseWithSecondsBehindMaster(3600)

	// We fail when returning the last row to ensure that vtworker is forced to
	// give up after the one allowed restart.
	tc.sourceRdonlyQs[0].errorStreamAtRow(199, 1234567890 /* infinite */)

	// vtworker fails due to the read error and may write less than all but the
	// last errored error. We cannot reliably expect any number of written rows.
	defer tc.leftMasterFakeDb.DeleteAllEntries()
	defer tc.rightMasterFakeDb.DeleteAllEntries()

	// Run the vtworker command.
	args := []string{"SplitClone", "--min_healthy_rdonly_tablets", "1"}
	args = append(args, tc.defaultWorkerArgs[1:]...)
	if err := runCommand(t, tc.wi, tc.wi.wr, args); err == nil || !strings.Contains(err.Error(), "first retry to restart the streaming query on the same tablet failed. We're failing at this point") {
		t.Fatalf("worker should have failed because all tablets became unavailable and it gave up retrying. err: %v", err)
	}

	alias := tc.sourceRdonlyQs[0].alias
	if got, want := statsStreamingQueryErrorsCounters.Counts()[alias], int64(1); got != want {
		t.Errorf("wrong number of errored streaming query for tablet: %v: got = %v, want = %v", alias, got, want)
	}
	if got, want := statsStreamingQueryCounters.Counts()[alias], int64(1); got != want {
		t.Errorf("wrong number of streaming query starts for tablet: %v: got = %v, want = %v", alias, got, want)
	}
}

// TestSplitCloneV2_Online_FailOverStreamingQuery is identical to
// TestSplitCloneV2_Online but forces SplitClone to restart the streaming
// query on the source *and* failover to a different source tablet before
// reading the last row.
func TestSplitCloneV2_Online_FailOverStreamingQuery(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUpWithConcurrency(false /* v3 */, 1, 10, splitCloneTestRowsCount)
	defer tc.tearDown()

	// In the online phase we won't enable filtered replication. Don't expect it.
	tc.leftMasterFakeDb.DeleteAllEntriesAfterIndex(4)
	tc.rightMasterFakeDb.DeleteAllEntriesAfterIndex(4)

	// Ensure that this test uses only the first tablet initially.
	tc.sourceRdonlyQs[1].AddHealthResponseWithSecondsBehindMaster(3600)

	// Let the first tablet fail at the last row.
	tc.sourceRdonlyQs[0].errorStreamAtRow(199, 12345667890 /* infinite */)
	tc.sourceRdonlyQs[0].setErrorCallback(func() {
		// Make the first tablet unhealthy and the second one healthy again.
		// vtworker should failover from the first to the second tablet then.
		tc.sourceRdonlyQs[0].AddHealthResponseWithSecondsBehindMaster(3600)
		tc.sourceRdonlyQs[1].AddHealthResponseWithSecondsBehindMaster(1)
	})

	// Only wait 1 ns between retries, so that the test passes faster.
	*executeFetchRetryTime = 1 * time.Nanosecond

	// Run the vtworker command.
	args := []string{"SplitClone",
		"-offline=false",
		// We require only 1 instead of the default 2 replicas.
		"--min_healthy_rdonly_tablets", "1"}
	args = append(args, tc.defaultWorkerArgs[2:]...)
	if err := runCommand(t, tc.wi, tc.wi.wr, args); err != nil {
		t.Fatal(err)
	}

	first := tc.sourceRdonlyQs[0].alias
	second := tc.sourceRdonlyQs[1].alias
	if got, want := statsStreamingQueryErrorsCounters.Counts()[first], int64(2); got < want {
		t.Errorf("wrong number of errored streaming query for tablet: %v: got = %v, want >= %v", first, got, want)
	}
	if got, want := statsStreamingQueryCounters.Counts()[first], int64(1); got != want {
		t.Errorf("wrong number of streaming query starts for tablet: %v: got = %v, want = %v", first, got, want)
	}
	if got, want := statsStreamingQueryCounters.Counts()[second], int64(1); got != want {
		t.Errorf("wrong number of streaming query starts for tablet: %v: got = %v, want = %v", second, got, want)
	}
}

// TestSplitCloneV2_Online_TabletsUnavailableDuringRestart is similar to
// TestSplitCloneV2_Online_FailOverStreamingQuery because both succeed initially
// except for the last row. While the other test succeeds by failing over to a
// different tablet, this test fails eventually because the other tablet is
// unavailable as well.
// The purpose of this test is to cover code branches in
// restartable_result_reader.go where we keep retrying while no tablet may be
// available.
func TestSplitCloneV2_Online_TabletsUnavailableDuringRestart(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUpWithConcurrency(false /* v3 */, 1, 10, splitCloneTestRowsCount)
	defer tc.tearDown()

	// In the online phase we won't enable filtered replication. Don't expect it.
	tc.leftMasterFakeDb.DeleteAllEntriesAfterIndex(4)
	tc.rightMasterFakeDb.DeleteAllEntriesAfterIndex(4)
	// The last row will never make it. Don't expect it.
	tc.rightMasterFakeDb.DeleteAllEntriesAfterIndex(3)

	// Ensure that this test uses only the first tablet initially.
	tc.sourceRdonlyQs[1].AddHealthResponseWithNotServing()

	// Let the first tablet fail at the last row.
	tc.sourceRdonlyQs[0].errorStreamAtRow(199, 12345667890 /* infinite */)
	tc.sourceRdonlyQs[0].setErrorCallback(func() {
		// Make the second tablet unavailable as well. vtworker should keep retrying
		// and fail eventually because no tablet is there.
		tc.sourceRdonlyQs[0].AddHealthResponseWithNotServing()
	})

	// Let vtworker keep retrying and give up rather quickly because the test
	// will be blocked until it finally fails.
	*retryDuration = 500 * time.Millisecond

	// Run the vtworker command.
	args := []string{"SplitClone",
		"-offline=false",
		// We require only 1 instead of the default 2 replicas.
		"--min_healthy_rdonly_tablets", "1"}
	args = append(args, tc.defaultWorkerArgs[2:]...)
	if err := runCommand(t, tc.wi, tc.wi.wr, args); err == nil || !strings.Contains(err.Error(), "failed to restart the streaming connection") {
		t.Fatalf("worker should have failed because all tablets became unavailable and it gave up retrying. err: %v", err)
	}

	first := tc.sourceRdonlyQs[0].alias
	// Note that we can track only 2 errors for the first tablet because it
	// becomes unavailable after that.
	if got, want := statsStreamingQueryErrorsCounters.Counts()[first], int64(2); got < want {
		t.Errorf("wrong number of errored streaming query for tablet: %v: got = %v, want >= %v", first, got, want)
	}
	if got, want := statsStreamingQueryCounters.Counts()[first], int64(1); got != want {
		t.Errorf("wrong number of streaming query starts for tablet: %v: got = %v, want = %v", first, got, want)
	}
}

// TestSplitCloneV2_Online tests the online phase with an empty destination.
func TestSplitCloneV2_Online(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// In the online phase we won't enable filtered replication. Don't expect it.
	tc.leftMasterFakeDb.DeleteAllEntriesAfterIndex(29)
	tc.rightMasterFakeDb.DeleteAllEntriesAfterIndex(29)

	// Run the vtworker command.
	args := make([]string, len(tc.defaultWorkerArgs))
	copy(args, tc.defaultWorkerArgs)
	args[1] = "-offline=false"
	if err := runCommand(t, tc.wi, tc.wi.wr, args); err != nil {
		t.Fatal(err)
	}

	if err := verifyOnlineCounters(100, 0, 0, 0); err != nil {
		t.Fatalf("wrong Online counters: %v", err)
	}
	if err := verifyOfflineCounters(0, 0, 0, 0); err != nil {
		t.Fatalf("wrong Offline counters: %v", err)
	}
}

func TestSplitCloneV2_Online_Offline(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	tc.setUp(false /* v3 */)
	defer tc.tearDown()

	// When the online clone inserted the last rows, modify the destination test
	// query service such that it will return them as well.
	tc.leftMasterFakeDb.GetEntry(29).AfterFunc = func() {
		tc.leftMasterQs.addGeneratedRows(100, 200)
	}
	tc.rightMasterFakeDb.GetEntry(29).AfterFunc = func() {
		tc.rightMasterQs.addGeneratedRows(100, 200)
	}

	// Run the vtworker command.
	args := []string{"SplitClone"}
	args = append(args, tc.defaultWorkerArgs[2:]...)
	if err := runCommand(t, tc.wi, tc.wi.wr, args); err != nil {
		t.Fatal(err)
	}

	if err := verifyOnlineCounters(100, 0, 0, 0); err != nil {
		t.Fatalf("wrong Online counters: %v", err)
	}
	if err := verifyOfflineCounters(0, 0, 0, 100); err != nil {
		t.Fatalf("wrong Offline counters: %v", err)
	}
}

// TestSplitCloneV2_Offline_Reconciliation is identical to
// TestSplitCloneV2_Offline, but the destination has existing data which must be
// reconciled.
func TestSplitCloneV2_Offline_Reconciliation(t *testing.T) {
	tc := &splitCloneTestCase{t: t}
	// We reduce the parallelism to 1 to test the order of expected
	// insert/update/delete statements on the destination master.
	tc.setUpWithConcurrency(false /* v3 */, 1, 10, splitCloneTestRowsCount)
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

	// The destination has rows 100-190 with the source in common.
	// Rows 191-200 are extraneous on the destination.
	tc.leftMasterQs.addGeneratedRows(100, 200)
	tc.rightMasterQs.addGeneratedRows(100, 200)
	// But some data is outdated data and must be updated.
	tc.leftMasterQs.modifyFirstRows(2)
	tc.rightMasterQs.modifyFirstRows(2)

	// The destination tablets should see inserts, updates and deletes.
	// Clear the entries added by setUp() because the reconciliation will
	// produce different statements in this test case.
	tc.leftMasterFakeDb.DeleteAllEntries()
	tc.rightMasterFakeDb.DeleteAllEntries()
	// Update statements. (One query will update one row.)
	tc.leftMasterFakeDb.AddExpectedQuery("UPDATE `vt_ks`.`table1` SET `msg`='Text for 100',`keyspace_id`=2305843009213693952 WHERE `id`=100", nil)
	tc.leftMasterFakeDb.AddExpectedQuery("UPDATE `vt_ks`.`table1` SET `msg`='Text for 102',`keyspace_id`=2305843009213693952 WHERE `id`=102", nil)
	tc.rightMasterFakeDb.AddExpectedQuery("UPDATE `vt_ks`.`table1` SET `msg`='Text for 101',`keyspace_id`=6917529027641081856 WHERE `id`=101", nil)
	tc.rightMasterFakeDb.AddExpectedQuery("UPDATE `vt_ks`.`table1` SET `msg`='Text for 103',`keyspace_id`=6917529027641081856 WHERE `id`=103", nil)
	// Insert statements. (All are combined in one.)
	tc.leftMasterFakeDb.AddExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (96,'Text for 96',2305843009213693952),(98,'Text for 98',2305843009213693952)", nil)
	tc.rightMasterFakeDb.AddExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (97,'Text for 97',6917529027641081856),(99,'Text for 99',6917529027641081856)", nil)
	// Delete statements. (All are combined in one.)
	tc.leftMasterFakeDb.AddExpectedQuery("DELETE FROM `vt_ks`.`table1` WHERE (`id`=190) OR (`id`=192) OR (`id`=194) OR (`id`=196) OR (`id`=198)", nil)
	tc.rightMasterFakeDb.AddExpectedQuery("DELETE FROM `vt_ks`.`table1` WHERE (`id`=191) OR (`id`=193) OR (`id`=195) OR (`id`=197) OR (`id`=199)", nil)

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatal(err)
	}

	if err := verifyOnlineCounters(0, 0, 0, 0); err != nil {
		t.Fatalf("wrong Online counters: %v", err)
	}
	if err := verifyOfflineCounters(4, 4, 10, 86); err != nil {
		t.Fatalf("wrong Offline counters: %v", err)
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

	// Only wait 1 ms between retries, so that the test passes faster.
	*executeFetchRetryTime = 1 * time.Millisecond

	// Provoke a retry to test the error handling.
	tc.leftMasterFakeDb.AddExpectedQueryAtIndex(0, "INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", errReadOnly)
	tc.rightMasterFakeDb.AddExpectedQueryAtIndex(0, "INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", errReadOnly)

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

	// Only wait 1 ms between retries, so that the test passes faster.
	*executeFetchRetryTime = 1 * time.Millisecond

	// Provoke a reparent just before the copy finishes.
	// leftReplica will take over for the last, 30th, insert and the vreplication checkpoint.
	tc.leftReplicaFakeDb.AddExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", nil)

	// Do not let leftMaster succeed the 30th write.
	tc.leftMasterFakeDb.DeleteAllEntriesAfterIndex(28)
	tc.leftMasterFakeDb.AddExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", errReadOnly)
	tc.leftMasterFakeDb.EnableInfinite()
	// When vtworker encounters the readonly error on leftMaster, do the reparent.
	tc.leftMasterFakeDb.GetEntry(29).AfterFunc = func() {
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

	// Only wait 1 ms between retries, so that the test passes faster.
	*executeFetchRetryTime = 1 * time.Millisecond

	// leftReplica will take over for the last, 30th, insert and the vreplication checkpoint.
	tc.leftReplicaFakeDb.AddExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", nil)

	// During the 29th write, let the MASTER disappear.
	tc.leftMasterFakeDb.GetEntry(28).AfterFunc = func() {
		t.Logf("setting MASTER tablet to REPLICA")
		tc.leftMasterQs.UpdateType(topodatapb.TabletType_REPLICA)
		tc.leftMasterQs.AddDefaultHealthResponse()
	}

	// If the HealthCheck didn't pick up the change yet, the 30th write would
	// succeed. To prevent this from happening, replace it with an error.
	tc.leftMasterFakeDb.DeleteAllEntriesAfterIndex(28)
	tc.leftMasterFakeDb.AddExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", errReadOnly)
	tc.leftMasterFakeDb.EnableInfinite()
	// vtworker may not retry on leftMaster again if HealthCheck picks up the
	// change very fast. In that case, the error was never encountered.
	// Delete it or verifyAllExecutedOrFail() will fail because it was not
	// processed.
	defer tc.leftMasterFakeDb.DeleteAllEntriesAfterIndex(28)

	// Wait for a retry due to NoMasterAvailable to happen, expect the 30th write
	// on leftReplica and change leftReplica from REPLICA to MASTER.
	//
	// Reset the stats now. It also happens when the worker starts but that's too
	// late because this Go routine looks at it and can run before the worker.
	statsRetryCounters.ResetAll()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		for {
			retries := statsRetryCounters.Counts()[retryCategoryNoMasterAvailable]
			if retries >= 1 {
				t.Logf("retried on no MASTER %v times", retries)
				break
			}

			select {
			case <-ctx.Done():
				panic(fmt.Errorf("timed out waiting for vtworker to retry due to NoMasterAvailable: %v", ctx.Err()))
			case <-time.After(10 * time.Millisecond):
				// Poll constantly.
			}
		}

		// Make leftReplica the new MASTER.
		tc.leftReplica.Agent.TabletExternallyReparented(ctx, "1")
		t.Logf("resetting tablet back to MASTER")
		tc.leftReplicaQs.UpdateType(topodatapb.TabletType_MASTER)
		tc.leftReplicaQs.AddDefaultHealthResponse()
	}()

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

func verifyOnlineCounters(inserts, updates, deletes, equal int64) error {
	rec := concurrency.AllErrorRecorder{}
	if got, want := statsOnlineInsertsCounters.Counts()["table1"], inserts; got != want {
		rec.RecordError(fmt.Errorf("wrong online INSERTs count: got = %v, want = %v", got, want))
	}
	if got, want := statsOnlineUpdatesCounters.Counts()["table1"], updates; got != want {
		rec.RecordError(fmt.Errorf("wrong online UPDATEs count: got = %v, want = %v", got, want))
	}
	if got, want := statsOnlineDeletesCounters.Counts()["table1"], deletes; got != want {
		rec.RecordError(fmt.Errorf("wrong online DELETEs count: got = %v, want = %v", got, want))
	}
	if got, want := statsOnlineEqualRowsCounters.Counts()["table1"], equal; got != want {
		rec.RecordError(fmt.Errorf("wrong online equal rows count: got = %v, want = %v", got, want))
	}
	return rec.Error()
}

func verifyOfflineCounters(inserts, updates, deletes, equal int64) error {
	rec := concurrency.AllErrorRecorder{}
	if got, want := statsOfflineInsertsCounters.Counts()["table1"], inserts; got != want {
		rec.RecordError(fmt.Errorf("wrong offline INSERTs count: got = %v, want = %v", got, want))
	}
	if got, want := statsOfflineUpdatesCounters.Counts()["table1"], updates; got != want {
		rec.RecordError(fmt.Errorf("wrong offline UPDATEs count: got = %v, want = %v", got, want))
	}
	if got, want := statsOfflineDeletesCounters.Counts()["table1"], deletes; got != want {
		rec.RecordError(fmt.Errorf("wrong offline DELETEs count: got = %v, want = %v", got, want))
	}
	if got, want := statsOfflineEqualRowsCounters.Counts()["table1"], equal; got != want {
		rec.RecordError(fmt.Errorf("wrong offline equal rows count: got = %v, want = %v", got, want))
	}
	return rec.Error()
}
