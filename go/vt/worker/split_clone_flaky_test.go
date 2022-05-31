/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
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
	leftPrimaryFakeDb  *fakesqldb.DB
	leftPrimaryQs      *testQueryService
	rightPrimaryFakeDb *fakesqldb.DB
	rightPrimaryQs     *testQueryService

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
		if err := tc.ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}); err != nil {
			tc.t.Fatalf("CreateKeyspace v2 failed: %v", err)
		}
	}

	// Create the fake databases.
	sourceRdonlyFakeDB := sourceRdonlyFakeDB(tc.t, "vt_ks", "table1", splitCloneTestMin, splitCloneTestMax)
	tc.leftPrimaryFakeDb = fakesqldb.New(tc.t).SetName("leftPrimary").OrderMatters()
	tc.leftReplicaFakeDb = fakesqldb.New(tc.t).SetName("leftReplica").OrderMatters()
	tc.rightPrimaryFakeDb = fakesqldb.New(tc.t).SetName("rightPrimary").OrderMatters()

	sourcePrimary := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 0,
		topodatapb.TabletType_PRIMARY, nil, testlib.TabletKeyspaceShard(tc.t, "ks", "-80"))
	sourceRdonly1 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, sourceRdonlyFakeDB, testlib.TabletKeyspaceShard(tc.t, "ks", "-80"))
	sourceRdonly2 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 2,
		topodatapb.TabletType_RDONLY, sourceRdonlyFakeDB, testlib.TabletKeyspaceShard(tc.t, "ks", "-80"))

	leftPrimary := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 10,
		topodatapb.TabletType_PRIMARY, tc.leftPrimaryFakeDb, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))
	// leftReplica is used by the reparent test.
	leftReplica := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 11,
		topodatapb.TabletType_REPLICA, tc.leftReplicaFakeDb, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))
	tc.leftReplica = leftReplica
	leftRdonly1 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 12,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))
	leftRdonly2 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 13,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(tc.t, "ks", "-40"))

	rightPrimary := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 20,
		topodatapb.TabletType_PRIMARY, tc.rightPrimaryFakeDb, testlib.TabletKeyspaceShard(tc.t, "ks", "40-80"))
	rightRdonly1 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 22,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(tc.t, "ks", "40-80"))
	rightRdonly2 := testlib.NewFakeTablet(tc.t, tc.wi.wr, "cell1", 23,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(tc.t, "ks", "40-80"))

	tc.tablets = []*testlib.FakeTablet{sourcePrimary, sourceRdonly1, sourceRdonly2,
		leftPrimary, tc.leftReplica, leftRdonly1, leftRdonly2, rightPrimary, rightRdonly1, rightRdonly2}

	// add the topo and schema data we'll need
	if err := tc.ts.CreateShard(ctx, "ks", "80-"); err != nil {
		tc.t.Fatalf("CreateShard(\"-80\") failed: %v", err)
	}
	if err := topotools.RebuildKeyspace(ctx, tc.wi.wr.Logger(), tc.wi.wr.TopoServer(), "ks", nil, false); err != nil {
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
		sourceRdonly.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
			GTIDSet: mysql.MariadbGTIDSet{12: mysql.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678}},
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
		tc.leftPrimaryFakeDb.AddExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", nil)
		// leftReplica is unused by default.
		tc.rightPrimaryFakeDb.AddExpectedQuery("INSERT INTO `vt_ks`.`table1` (`id`, `msg`, `keyspace_id`) VALUES (*", nil)
	}

	// Fake stream health responses because vtworker needs them to find the primary.
	shqs := fakes.NewStreamHealthQueryService(leftPrimary.Target())
	shqs.AddDefaultHealthResponse()
	tc.leftPrimaryQs = newTestQueryService(tc.t, leftPrimary.Target(), shqs, 0, 2, topoproto.TabletAliasString(leftPrimary.Tablet.Alias), false /* omitKeyspaceID */)
	tc.leftReplicaQs = fakes.NewStreamHealthQueryService(leftReplica.Target())
	shqs = fakes.NewStreamHealthQueryService(rightPrimary.Target())
	shqs.AddDefaultHealthResponse()
	tc.rightPrimaryQs = newTestQueryService(tc.t, rightPrimary.Target(), shqs, 1, 2, topoproto.TabletAliasString(rightPrimary.Tablet.Alias), false /* omitKeyspaceID */)
	grpcqueryservice.Register(leftPrimary.RPCServer, tc.leftPrimaryQs)
	grpcqueryservice.Register(leftReplica.RPCServer, tc.leftReplicaQs)
	grpcqueryservice.Register(rightPrimary.RPCServer, tc.rightPrimaryQs)

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
		ft.TM = nil
		ft.RPCServer = nil
		ft.FakeMysqlDaemon = nil
	}
	tc.leftPrimaryFakeDb.VerifyAllExecutedOrFail()
	tc.leftReplicaFakeDb.VerifyAllExecutedOrFail()
	tc.rightPrimaryFakeDb.VerifyAllExecutedOrFail()
}

// testQueryService is a local QueryService implementation to support the tests.
type testQueryService struct {
	t *testing.T

	// target is used in the log output.
	target *querypb.Target
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

func newTestQueryService(t *testing.T, target *querypb.Target, shqs *fakes.StreamHealthQueryService, shardIndex, shardCount int, alias string, omitKeyspaceID bool) *testQueryService {
	fields := v2Fields
	if omitKeyspaceID {
		fields = v3Fields
	}
	return &testQueryService{
		t:              t,
		target:         target,
		shardIndex:     shardIndex,
		shardCount:     shardCount,
		alias:          alias,
		omitKeyspaceID: omitKeyspaceID,
		fields:         fields,
		forceError:     make(map[int64]int),

		StreamHealthQueryService: shqs,
	}
}

func (sq *testQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
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
		v, _ := evalengine.ToNative(row[0])
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

func TestSplitCloneV3(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	tc := &splitCloneTestCase{t: t}
	tc.setUp(true /* v3 */)
	defer tc.tearDown()

	// Run the vtworker command.
	if err := runCommand(t, tc.wi, tc.wi.wr, tc.defaultWorkerArgs); err != nil {
		t.Fatal(err)
	}
}
