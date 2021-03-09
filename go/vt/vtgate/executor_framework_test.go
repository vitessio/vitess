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
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"context"

	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var executorVSchema = `
{
	"sharded": true,
	"vindexes": {
		"hash_index": {
			"type": "hash"
		},
		"music_user_map": {
			"type": "lookup_hash_unique",
			"owner": "music",
			"params": {
				"table": "music_user_map",
				"from": "music_id",
				"to": "user_id"
			}
		},
		"name_user_map": {
			"type": "lookup_hash",
			"owner": "user",
			"params": {
				"table": "name_user_map",
				"from": "name",
				"to": "user_id"
			}
		},
		"name_lastname_keyspace_id_map": {
			"type": "lookup",
			"owner": "user2",
			"params": {
				"table": "name_lastname_keyspace_id_map",
				"from": "name,lastname",
				"to": "keyspace_id"
			}
		},
		"insert_ignore_idx": {
			"type": "lookup_hash",
			"owner": "insert_ignore_test",
			"params": {
				"table": "ins_lookup",
				"from": "fromcol",
				"to": "tocol"
			}
		},
		"idx1": {
			"type": "hash"
		},
		"idx_noauto": {
			"type": "hash",
			"owner": "noauto_table"
		},
		"keyspace_id": {
			"type": "numeric"
		},
		"krcol_unique_vdx": {
			"type": "keyrange_lookuper_unique"
		},
		"krcol_vdx": {
			"type": "keyrange_lookuper"
		},
    	"t1_lkp_vdx": {
      		"type": "consistent_lookup_unique",
      		"params": {
        		"table": "t1_lkp_idx",
        		"from": "unq_col",
        		"to": "keyspace_id"
      		},
      	"owner": "t1"
    	}
	},
	"tables": {
		"user": {
			"column_vindexes": [
				{
					"column": "Id",
					"name": "hash_index"
				},
				{
					"column": "name",
					"name": "name_user_map"
				}
			],
			"auto_increment": {
				"column": "id",
				"sequence": "user_seq"
			},
			"columns": [
				{
					"name": "textcol",
					"type": "VARCHAR"
				}
			]
		},
		"user2": {
			"column_vindexes": [
				{
					"column": "id",
					"name": "hash_index"
				},
				{
					"columns": ["name", "lastname"],
					"name": "name_lastname_keyspace_id_map"
				}
			]
		},
		"user_extra": {
			"column_vindexes": [
				{
					"column": "user_id",
					"name": "hash_index"
				}
			]
		},
		"sharded_user_msgs": {
			"column_vindexes": [
				{
					"column": "user_id",
					"name": "hash_index"
				}
			]
		},
		"music": {
			"column_vindexes": [
				{
					"column": "user_id",
					"name": "hash_index"
				},
				{
					"column": "id",
					"name": "music_user_map"
				}
			],
			"auto_increment": {
				"column": "id",
				"sequence": "user_seq"
			}
		},
		"music_extra": {
			"column_vindexes": [
				{
					"column": "user_id",
					"name": "hash_index"
				},
				{
					"column": "music_id",
					"name": "music_user_map"
				}
			]
		},
		"music_extra_reversed": {
			"column_vindexes": [
				{
					"column": "music_id",
					"name": "music_user_map"
				},
				{
					"column": "user_id",
					"name": "hash_index"
				}
			]
		},
		"insert_ignore_test": {
			"column_vindexes": [
				{
					"column": "pv",
					"name": "music_user_map"
				},
				{
					"column": "owned",
					"name": "insert_ignore_idx"
				},
				{
					"column": "verify",
					"name": "hash_index"
				}
			]
		},
		"noauto_table": {
			"column_vindexes": [
				{
					"column": "id",
					"name": "idx_noauto"
				}
			]
		},
		"keyrange_table": {
			"column_vindexes": [
				{
					"column": "krcol_unique",
					"name": "krcol_unique_vdx"
				},
				{
					"column": "krcol",
					"name": "krcol_vdx"
				}
			]
		},
		"ksid_table": {
			"column_vindexes": [
				{
					"column": "keyspace_id",
					"name": "keyspace_id"
				}
			]
		},
		"t1": {
      		"column_vindexes": [
				{
				  	"column": "id",
				  	"name": "hash_index"
				},
				{
				  	"column": "unq_col",
				  	"name": "t1_lkp_vdx"
				}
            ]
    	},
		"t1_lkp_idx": {
			"column_vindexes": [
				{
					"column": "unq_col",
				  	"name": "hash_index"
				}
			]
		}
	}
}
`

var badVSchema = `
{
	"sharded": false,
	"tables": {
		"sharded_table": {}
	}
}
`

var unshardedVSchema = `
{
	"sharded": false,
	"tables": {
		"user_seq": {
			"type": "sequence"
		},
		"music_user_map": {},
		"name_user_map": {},
		"name_lastname_keyspace_id_map": {},
		"user_msgs": {},
		"ins_lookup": {},
		"main1": {
			"auto_increment": {
				"column": "id",
				"sequence": "user_seq"
			}
		},
		"simple": {}
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
func (*keyRangeLookuper) Verify(vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*keyRangeLookuper) Map(cursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
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
func (*keyRangeLookuperUnique) Verify(vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (*keyRangeLookuperUnique) Map(cursor vindexes.VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
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
	// Use legacy gateway until we can rewrite these tests to use new tabletgateway
	*GatewayImplementation = GatewayImplementationDiscovery
}

func createLegacyExecutorEnv() (executor *Executor, sbc1, sbc2, sbclookup *sandboxconn.SandboxConn) {
	// Use legacy gateway until we can rewrite these tests to use new tabletgateway
	*GatewayImplementation = GatewayImplementationDiscovery
	cell := "aa"
	hc := discovery.NewFakeLegacyHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestLegacyResolver(hc, serv, cell)
	sbc1 = hc.AddTestTablet(cell, "-20", 1, "TestExecutor", "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc2 = hc.AddTestTablet(cell, "40-60", 1, "TestExecutor", "40-60", topodatapb.TabletType_MASTER, true, 1, nil)
	// Create these connections so scatter queries don't fail.
	_ = hc.AddTestTablet(cell, "20-40", 1, "TestExecutor", "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "60-80", 1, "TestExecutor", "60-80", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "80-a0", 1, "TestExecutor", "80-a0", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "a0-c0", 1, "TestExecutor", "a0-c0", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "c0-e0", 1, "TestExecutor", "c0-e0", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "e0-", 1, "TestExecutor", "e0-", topodatapb.TabletType_MASTER, true, 1, nil)

	createSandbox(KsTestUnsharded)
	sbclookup = hc.AddTestTablet(cell, "0", 1, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	// Ues the 'X' in the name to ensure it's not alphabetically first.
	// Otherwise, it would become the default keyspace for the dual table.
	bad := createSandbox("TestXBadSharding")
	bad.VSchema = badVSchema

	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	executor = NewExecutor(context.Background(), serv, cell, resolver, false, false, testBufferSize, cache.DefaultConfig)

	key.AnyShardPicker = DestinationAnyShardPickerFirstShard{}
	return executor, sbc1, sbc2, sbclookup
}

func createExecutorEnv() (executor *Executor, sbc1, sbc2, sbclookup *sandboxconn.SandboxConn) {
	// Use legacy gateway until we can rewrite these tests to use new tabletgateway
	*GatewayImplementation = GatewayImplementationDiscovery
	cell := "aa"
	hc := discovery.NewFakeHealthCheck()
	vtgateHealthCheck = hc
	s := createSandbox("TestExecutor")
	s.VSchema = executorVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	sbc1 = hc.AddTestTablet(cell, "-20", 1, "TestExecutor", "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc2 = hc.AddTestTablet(cell, "40-60", 1, "TestExecutor", "40-60", topodatapb.TabletType_MASTER, true, 1, nil)
	// Create these connections so scatter queries don't fail.
	_ = hc.AddTestTablet(cell, "20-40", 1, "TestExecutor", "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "60-60", 1, "TestExecutor", "60-80", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "80-a0", 1, "TestExecutor", "80-a0", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "a0-c0", 1, "TestExecutor", "a0-c0", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "c0-e0", 1, "TestExecutor", "c0-e0", topodatapb.TabletType_MASTER, true, 1, nil)
	_ = hc.AddTestTablet(cell, "e0-", 1, "TestExecutor", "e0-", topodatapb.TabletType_MASTER, true, 1, nil)

	createSandbox(KsTestUnsharded)
	sbclookup = hc.AddTestTablet(cell, "0", 1, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	// Ues the 'X' in the name to ensure it's not alphabetically first.
	// Otherwise, it would become the default keyspace for the dual table.
	bad := createSandbox("TestXBadSharding")
	bad.VSchema = badVSchema

	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	executor = NewExecutor(context.Background(), serv, cell, resolver, false, false, testBufferSize, cache.DefaultConfig)

	key.AnyShardPicker = DestinationAnyShardPickerFirstShard{}
	return executor, sbc1, sbc2, sbclookup
}

func createCustomExecutor(vschema string) (executor *Executor, sbc1, sbc2, sbclookup *sandboxconn.SandboxConn) {
	cell := "aa"
	hc := discovery.NewFakeLegacyHealthCheck()
	s := createSandbox("TestExecutor")
	s.VSchema = vschema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestLegacyResolver(hc, serv, cell)
	sbc1 = hc.AddTestTablet(cell, "-20", 1, "TestExecutor", "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc2 = hc.AddTestTablet(cell, "40-60", 1, "TestExecutor", "40-60", topodatapb.TabletType_MASTER, true, 1, nil)

	createSandbox(KsTestUnsharded)
	sbclookup = hc.AddTestTablet(cell, "0", 1, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema

	executor = NewExecutor(context.Background(), serv, cell, resolver, false, false, testBufferSize, cache.DefaultConfig)
	return executor, sbc1, sbc2, sbclookup
}

func executorExec(executor *Executor, sql string, bv map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return executor.Execute(
		context.Background(),
		"TestExecute",
		NewSafeSession(masterSession),
		sql,
		bv)
}

func executorPrepare(executor *Executor, sql string, bv map[string]*querypb.BindVariable) ([]*querypb.Field, error) {
	return executor.Prepare(
		context.Background(),
		"TestExecute",
		NewSafeSession(masterSession),
		sql,
		bv)
}

func executorStream(executor *Executor, sql string) (qr *sqltypes.Result, err error) {
	results := make(chan *sqltypes.Result, 100)
	err = executor.StreamExecute(
		context.Background(),
		"TestExecuteStream",
		NewSafeSession(masterSession),
		sql,
		nil,
		querypb.Target{
			TabletType: topodatapb.TabletType_MASTER,
		},
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

func testQueries(t *testing.T, sbcName string, sbc *sandboxconn.SandboxConn, wantQueries []*querypb.BoundQuery) {
	t.Helper()
	if !reflect.DeepEqual(sbc.Queries, wantQueries) {
		t.Errorf("%s.Queries:\n%+v, want\n%+v\n", sbcName, sbc.Queries, wantQueries)
	}
}

func testCommitCount(t *testing.T, sbcName string, sbc *sandboxconn.SandboxConn, want int) {
	t.Helper()
	if got, want := sbc.CommitCount.Get(), int64(want); got != want {
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

func getQueryLog(logChan chan interface{}) *LogStats {
	var log interface{}

	select {
	case log = <-logChan:
		return log.(*LogStats)
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

func testQueryLog(t *testing.T, logChan chan interface{}, method, stmtType, sql string, shardQueries int) *LogStats {
	t.Helper()

	logStats := getQueryLog(logChan)
	require.NotNil(t, logStats)

	var log bytes.Buffer
	streamlog.GetFormatter(QueryLogger)(&log, nil, logStats)
	fields := strings.Split(log.String(), "\t")

	// fields[0] is the method
	assert.Equal(t, method, fields[0], "logstats: method")

	// fields[1] - fields[6] are the caller id, start/end times, etc

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
		case "BEGIN", "COMMIT", "ROLLBACK", "SET", "SAVEPOINT", "SAVEPOINT_ROLLBACK", "RELEASE":
		default:
			testNonZeroDuration(t, "ExecuteTime", fields[9])
		}

		// fields[10] is CommitTime which is set only in autocommit mode and
		// tested separately
	}

	// fields[11] is the statement type
	assert.Equal(t, stmtType, fields[11], "logstats: stmtType")

	// fields[12] is the original sql
	wantSQL := fmt.Sprintf("%q", sql)
	assert.Equal(t, wantSQL, fields[12], "logstats: SQL")

	// fields[13] contains the formatted bind vars

	// fields[14] is the count of shard queries
	assert.Equal(t, fmt.Sprintf("%v", shardQueries), fields[14], "logstats: ShardQueries")

	return logStats
}

func newTestLegacyResolver(hc discovery.LegacyHealthCheck, serv srvtopo.Server, cell string) *Resolver {
	sc := newTestLegacyScatterConn(hc, serv, cell)
	srvResolver := srvtopo.NewResolver(serv, sc.gateway, cell)
	return NewResolver(srvResolver, serv, cell, sc)
}
func newTestResolver(hc discovery.HealthCheck, serv srvtopo.Server, cell string) *Resolver {
	sc := newTestScatterConn(hc, serv, cell)
	srvResolver := srvtopo.NewResolver(serv, sc.gateway, cell)
	return NewResolver(srvResolver, serv, cell, sc)
}
