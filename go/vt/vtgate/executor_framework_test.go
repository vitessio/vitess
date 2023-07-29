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

package vtgate

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
)

//go:embed testdata/executorVSchema.json
var executorVSchema string

//go:embed testdata/unshardedVschema.json
var unshardedVSchema string

var badVSchema = `
{
	"sharded": false,
	"tables": {
		"sharded_table": {}
	}
}
`

const (
	testBufferSize = 10
)

type DestinationAnyShardPickerFirstShard struct{}

func (dp DestinationAnyShardPickerFirstShard) PickShard(shardCount int) int {
	return 0
}

// keyRangeLookuper is for testing a lookup that returns a keyrange.
type keyRangeLookuper struct {
}

func (v *keyRangeLookuper) String() string   { return "keyrange_lookuper" }
func (*keyRangeLookuper) Cost() int          { return 0 }
func (*keyRangeLookuper) IsUnique() bool     { return false }
func (*keyRangeLookuper) NeedsVCursor() bool { return false }
func (*keyRangeLookuper) Verify(context.Context, vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*keyRangeLookuper) Map(ctx context.Context, vcursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return []key.Destination{
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{
				End: []byte{0x10},
			},
		},
	}, nil
}

func newKeyRangeLookuper(name string, params map[string]string) (vindexes.Vindex, error) {
	return &keyRangeLookuper{}, nil
}

// keyRangeLookuperUnique is for testing a unique lookup that returns a keyrange.
type keyRangeLookuperUnique struct {
}

func (v *keyRangeLookuperUnique) String() string   { return "keyrange_lookuper" }
func (*keyRangeLookuperUnique) Cost() int          { return 0 }
func (*keyRangeLookuperUnique) IsUnique() bool     { return true }
func (*keyRangeLookuperUnique) NeedsVCursor() bool { return false }
func (*keyRangeLookuperUnique) Verify(context.Context, vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*keyRangeLookuperUnique) Map(ctx context.Context, vcursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	return []key.Destination{
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{
				End: []byte{0x10},
			},
		},
	}, nil
}

func newKeyRangeLookuperUnique(name string, params map[string]string) (vindexes.Vindex, error) {
	return &keyRangeLookuperUnique{}, nil
}

func init() {
	vindexes.Register("keyrange_lookuper", newKeyRangeLookuper)
	vindexes.Register("keyrange_lookuper_unique", newKeyRangeLookuperUnique)
}

func createExecutorEnv() (executor *Executor, sbc1, sbc2, sbclookup *sandboxconn.SandboxConn) {
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(make(chan *discovery.TabletHealth))
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	serv := newSandboxForCells([]string{cell})
	serv.topoServer.CreateKeyspace(context.Background(), "TestExecutor", &topodatapb.Keyspace{SidecarDbName: sidecardb.DefaultName})
	// Force a new cache to use for lookups of the sidecar database identifier
	// in use by each keyspace -- as we want to use a different load function
	// than the one already created by the vtgate as it uses a different topo.
	if sdbc, _ := sidecardb.GetIdentifierCache(); sdbc != nil {
		sdbc.Destroy()
	}
	_, created := sidecardb.NewIdentifierCache(func(ctx context.Context, keyspace string) (string, error) {
		ki, err := serv.topoServer.GetKeyspace(ctx, keyspace)
		if err != nil {
			return "", err
		}
		return ki.SidecarDbName, nil
	})
	if !created {
		log.Fatal("Failed to [re]create a sidecar database identifier cache!")
	}
	resolver := newTestResolver(hc, serv, cell)
	sbc1 = hc.AddTestTablet(cell, "-20", 1, "TestExecutor", "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	sbc2 = hc.AddTestTablet(cell, "40-60", 1, "TestExecutor", "40-60", topodatapb.TabletType_PRIMARY, true, 1, nil)
	// Create these connections so scatter queries don't fail.
	_ = hc.AddTestTablet(cell, "20-40", 1, "TestExecutor", "20-40", topodatapb.TabletType_PRIMARY, true, 1, nil)
	_ = hc.AddTestTablet(cell, "60-60", 1, "TestExecutor", "60-80", topodatapb.TabletType_PRIMARY, true, 1, nil)
	_ = hc.AddTestTablet(cell, "80-a0", 1, "TestExecutor", "80-a0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	_ = hc.AddTestTablet(cell, "a0-c0", 1, "TestExecutor", "a0-c0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	_ = hc.AddTestTablet(cell, "c0-e0", 1, "TestExecutor", "c0-e0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	_ = hc.AddTestTablet(cell, "e0-", 1, "TestExecutor", "e0-", topodatapb.TabletType_PRIMARY, true, 1, nil)
	// Below is needed so that SendAnyWherePlan doesn't fail

	createSandbox(KsTestUnsharded)
	sbclookup = hc.AddTestTablet(cell, "0", 1, KsTestUnsharded, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	_ = hc.AddTestTablet(cell, "2", 3, KsTestUnsharded, "0", topodatapb.TabletType_REPLICA, true, 1, nil)

	// Ues the 'X' in the name to ensure it's not alphabetically first.
	// Otherwise, it would become the default keyspace for the dual table.
	bad := createSandbox("TestXBadSharding")
	bad.VSchema = badVSchema

	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	executor = NewExecutor(context.Background(), serv, cell, resolver, false, false, testBufferSize, cache.DefaultConfig, nil, false, querypb.ExecuteOptions_Gen4)

	key.AnyShardPicker = DestinationAnyShardPickerFirstShard{}
	// create a new session each time so that ShardSessions don't get re-used across tests
	primarySession = &vtgatepb.Session{
		TargetString: "@primary",
	}
	return executor, sbc1, sbc2, sbclookup
}

func createCustomExecutor(vschema string) (executor *Executor, sbc1, sbc2, sbclookup *sandboxconn.SandboxConn) {
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = vschema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	sbc1 = hc.AddTestTablet(cell, "-20", 1, "TestExecutor", "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	sbc2 = hc.AddTestTablet(cell, "40-60", 1, "TestExecutor", "40-60", topodatapb.TabletType_PRIMARY, true, 1, nil)

	createSandbox(KsTestUnsharded)
	sbclookup = hc.AddTestTablet(cell, "0", 1, KsTestUnsharded, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema

	executor = NewExecutor(context.Background(), serv, cell, resolver, false, false, testBufferSize, cache.DefaultConfig, nil, false, querypb.ExecuteOptions_Gen4)
	// create a new session each time so that ShardSessions don't get re-used across tests
	primarySession = &vtgatepb.Session{
		TargetString: "@primary",
	}
	return executor, sbc1, sbc2, sbclookup
}

func createCustomExecutorSetValues(vschema string, values []*sqltypes.Result) (executor *Executor, sbc1, sbc2, sbclookup *sandboxconn.SandboxConn) {
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = vschema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	sbcs := []*sandboxconn.SandboxConn{}
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		if values != nil {
			sbc.SetResults(values)
		}
		sbcs = append(sbcs, sbc)
	}

	createSandbox(KsTestUnsharded)
	sbclookup = hc.AddTestTablet(cell, "0", 1, KsTestUnsharded, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema

	executor = NewExecutor(context.Background(), serv, cell, resolver, false, false, testBufferSize, cache.DefaultConfig, nil, false, querypb.ExecuteOptions_Gen4)
	// create a new session each time so that ShardSessions don't get re-used across tests
	primarySession = &vtgatepb.Session{
		TargetString: "@primary",
	}
	return executor, sbcs[0], sbcs[1], sbclookup
}

func executorExecSession(executor *Executor, sql string, bv map[string]*querypb.BindVariable, session *vtgatepb.Session) (*sqltypes.Result, error) {
	return executor.Execute(
		context.Background(),
		nil,
		"TestExecute",
		NewSafeSession(session),
		sql,
		bv)
}

func executorExec(executor *Executor, sql string, bv map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return executorExecSession(executor, sql, bv, primarySession)
}

func executorPrepare(executor *Executor, sql string, bv map[string]*querypb.BindVariable) ([]*querypb.Field, error) {
	return executor.Prepare(
		context.Background(),
		"TestExecute",
		NewSafeSession(primarySession),
		sql,
		bv)
}

func executorStream(executor *Executor, sql string) (qr *sqltypes.Result, err error) {
	results := make(chan *sqltypes.Result, 100)
	err = executor.StreamExecute(
		context.Background(),
		nil,
		"TestExecuteStream",
		NewSafeSession(nil),
		sql,
		nil,
		func(qr *sqltypes.Result) error {
			results <- qr
			return nil
		},
	)
	close(results)
	if err != nil {
		return nil, err
	}
	first := true
	for r := range results {
		if first {
			qr = &sqltypes.Result{Fields: r.Fields}
			first = false
		}
		qr.Rows = append(qr.Rows, r.Rows...)
	}
	return qr, nil
}

func assertQueries(t *testing.T, sbc *sandboxconn.SandboxConn, wantQueries []*querypb.BoundQuery) {
	t.Helper()
	idx := 0
	for _, query := range sbc.Queries {
		if strings.HasPrefix(query.Sql, "savepoint") || strings.HasPrefix(query.Sql, "rollback to") {
			continue
		}
		if len(wantQueries) < idx {
			t.Errorf("got more queries than expected")
		}
		got := query.Sql
		expected := wantQueries[idx].Sql
		assert.Equal(t, expected, got)
		assert.Equal(t, wantQueries[idx].BindVariables, query.BindVariables)
		idx++
	}
}

func assertQueriesWithSavepoint(t *testing.T, sbc *sandboxconn.SandboxConn, wantQueries []*querypb.BoundQuery) {
	t.Helper()
	require.Equal(t, len(wantQueries), len(sbc.Queries), sbc.Queries)
	savepointStore := make(map[string]string)
	for idx, query := range sbc.Queries {
		require.Equal(t, wantQueries[idx].BindVariables, query.BindVariables)
		got := query.Sql
		expected := wantQueries[idx].Sql
		if strings.HasPrefix(got, "savepoint") {
			if !strings.HasPrefix(expected, "savepoint") {
				t.Fatal("savepoint expected")
			}
			if sp, exists := savepointStore[expected[10:]]; exists {
				assert.Equal(t, sp, got[10:])
			} else {
				savepointStore[expected[10:]] = got[10:]
			}
			continue
		}
		if strings.HasPrefix(got, "rollback to") {
			if !strings.HasPrefix(expected, "rollback to") {
				t.Fatal("rollback to expected")
			}
			assert.Equal(t, savepointStore[expected[12:]], got[12:])
			continue
		}
		assert.Equal(t, expected, got)
	}
}

func testCommitCount(t *testing.T, sbcName string, sbc *sandboxconn.SandboxConn, want int) {
	t.Helper()
	if got, want := sbc.CommitCount.Load(), int64(want); got != want {
		t.Errorf("%s.CommitCount: %d, want %d\n", sbcName, got, want)
	}
}

func testNonZeroDuration(t *testing.T, what, d string) {
	t.Helper()
	time, _ := strconv.ParseFloat(d, 64)
	if time == 0 {
		t.Errorf("querylog %s want non-zero duration got %s (%v)", what, d, time)
	}
}

func getQueryLog(logChan chan *logstats.LogStats) *logstats.LogStats {
	select {
	case log := <-logChan:
		return log
	default:
		return nil
	}
}

// Queries can hit the plan cache in less than a microsecond, which makes them
// appear to take 0.000000 time in the query log. To mitigate this in tests,
// keep an in-memory record of queries that we know have been planned during
// the current test execution and skip testing for non-zero plan time if this
// is a repeat query.
var testPlannedQueries = map[string]bool{}

func testQueryLog(t *testing.T, logChan chan *logstats.LogStats, method, stmtType, sql string, shardQueries int) *logstats.LogStats {
	t.Helper()

	logStats := getQueryLog(logChan)
	require.NotNil(t, logStats)

	var log bytes.Buffer
	streamlog.GetFormatter(QueryLogger)(&log, nil, logStats)
	fields := strings.Split(log.String(), "\t")

	// fields[0] is the method
	assert.Equal(t, method, fields[0], "logstats: method")

	// fields[1] - fields[6] are the caller id, start/end times, etc

	checkEqualQuery := true
	// The internal savepoints are created with uuids so the value of it not known to assert.
	// Therefore, the equal query check is ignored.
	switch stmtType {
	case "SAVEPOINT", "SAVEPOINT_ROLLBACK", "RELEASE":
		checkEqualQuery = false
	}
	// only test the durations if there is no error (fields[16])
	if fields[16] == "\"\"" {
		// fields[7] is the total execution time
		testNonZeroDuration(t, "TotalTime", fields[7])

		// fields[8] is the planner time. keep track of the planned queries to
		// avoid the case where we hit the plan in cache and it takes less than
		// a microsecond to plan it
		if testPlannedQueries[sql] == false {
			testNonZeroDuration(t, "PlanTime", fields[8])
		}
		testPlannedQueries[sql] = true

		// fields[9] is ExecuteTime which is not set for certain statements SET,
		// BEGIN, COMMIT, ROLLBACK, etc
		switch stmtType {
		case "BEGIN", "COMMIT", "SET", "ROLLBACK", "SAVEPOINT", "SAVEPOINT_ROLLBACK", "RELEASE":
		default:
			testNonZeroDuration(t, "ExecuteTime", fields[9])
		}

		// fields[10] is CommitTime which is set only in autocommit mode and
		// tested separately
	}

	// fields[11] is the statement type
	assert.Equal(t, stmtType, fields[11], "logstats: stmtType")

	if checkEqualQuery {
		// fields[12] is the original sql
		wantSQL := fmt.Sprintf("%q", sql)
		assert.Equal(t, wantSQL, fields[12], "logstats: SQL")
	}
	// fields[13] contains the formatted bind vars

	// fields[14] is the count of shard queries
	assert.Equal(t, fmt.Sprintf("%v", shardQueries), fields[14], "logstats: ShardQueries")

	return logStats
}

func newTestResolver(hc discovery.HealthCheck, serv srvtopo.Server, cell string) *Resolver {
	sc := newTestScatterConn(hc, serv, cell)
	srvResolver := srvtopo.NewResolver(serv, sc.gateway, cell)
	return NewResolver(srvResolver, serv, cell, sc)
}
