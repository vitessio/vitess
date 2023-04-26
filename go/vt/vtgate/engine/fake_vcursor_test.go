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

package engine

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

var testMaxMemoryRows = 100
var testIgnoreMaxMemoryRows = false

var _ VCursor = (*noopVCursor)(nil)
var _ SessionActions = (*noopVCursor)(nil)

// noopVCursor is used to build other vcursors.
type noopVCursor struct {
}

func (t *noopVCursor) InTransaction() bool {
	return false
}

func (t *noopVCursor) SetCommitOrder(co vtgatepb.CommitOrder) {
	//TODO implement me
	panic("implement me")
}

func (t *noopVCursor) StreamExecutePrimitiveStandalone(ctx context.Context, primitive Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(result *sqltypes.Result) error) error {
	return primitive.TryStreamExecute(ctx, t, bindVars, wantfields, callback)
}

func (t *noopVCursor) AnyAdvisoryLockTaken() bool {
	// TODO implement me
	panic("implement me")
}

func (t *noopVCursor) AddAdvisoryLock(name string) {
	// TODO implement me
	panic("implement me")
}

func (t *noopVCursor) RemoveAdvisoryLock(name string) {
	// TODO implement me
	panic("implement me")
}

func (t *noopVCursor) ReleaseLock(context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (t *noopVCursor) SetExec(ctx context.Context, name string, value string) error {
	panic("implement me")
}

func (t *noopVCursor) ShowExec(ctx context.Context, command sqlparser.ShowCommandType, filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	panic("implement me")
}

// SetContextWithValue implements VCursor interface.
func (t *noopVCursor) SetContextWithValue(key, value interface{}) func() {
	return func() {}
}

// ConnCollation implements VCursor
func (t *noopVCursor) ConnCollation() collations.ID {
	return collations.CollationUtf8mb4ID
}

func (t *noopVCursor) ExecutePrimitive(ctx context.Context, primitive Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return primitive.TryExecute(ctx, t, bindVars, wantfields)
}

func (t *noopVCursor) StreamExecutePrimitive(ctx context.Context, primitive Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return primitive.TryStreamExecute(ctx, t, bindVars, wantfields, callback)
}

func (t *noopVCursor) HasSystemVariables() bool {
	panic("implement me")
}

func (t *noopVCursor) GetSystemVariables(func(k string, v string)) {
	panic("implement me")
}

func (t *noopVCursor) GetWarnings() []*querypb.QueryWarning {
	panic("implement me")
}

func (t *noopVCursor) VStream(ctx context.Context, rss []*srvtopo.ResolvedShard, filter *binlogdatapb.Filter, gtid string, callback func(evs []*binlogdatapb.VEvent) error) error {
	panic("implement me")
}

func (t *noopVCursor) MessageStream(ctx context.Context, rss []*srvtopo.ResolvedShard, tableName string, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (t *noopVCursor) KeyspaceAvailable(ks string) bool {
	panic("implement me")
}

func (t *noopVCursor) SetDDLStrategy(strategy string) {
	panic("implement me")
}

func (t *noopVCursor) GetDDLStrategy() string {
	panic("implement me")
}

func (t *noopVCursor) GetSessionUUID() string {
	panic("implement me")
}

func (t *noopVCursor) SetReadAfterWriteGTID(s string) {
	panic("implement me")
}

func (t *noopVCursor) SetSessionEnableSystemSettings(ctx context.Context, allow bool) error {
	panic("implement me")
}

func (t *noopVCursor) GetSessionEnableSystemSettings() bool {
	panic("implement me")
}

func (t *noopVCursor) CanUseSetVar() bool {
	panic("implement me")
}

func (t *noopVCursor) SetReadAfterWriteTimeout(f float64) {
	panic("implement me")
}

func (t *noopVCursor) SetSessionTrackGTIDs(b bool) {
	panic("implement me")
}

func (t *noopVCursor) HasCreatedTempTable() {
	panic("implement me")
}

func (t *noopVCursor) LookupRowLockShardSession() vtgatepb.CommitOrder {
	panic("implement me")
}

func (t *noopVCursor) SetFoundRows(u uint64) {
	panic("implement me")
}

func (t *noopVCursor) InTransactionAndIsDML() bool {
	panic("implement me")
}

func (t *noopVCursor) FindRoutedTable(sqlparser.TableName) (*vindexes.Table, error) {
	panic("implement me")
}

func (t *noopVCursor) ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, lockFuncType sqlparser.LockingFuncType) (*sqltypes.Result, error) {
	panic("implement me")
}

func (t *noopVCursor) NeedsReservedConn() {
}

func (t *noopVCursor) SetUDV(key string, value any) error {
	panic("implement me")
}

func (t *noopVCursor) SetSysVar(name string, expr string) {
	// panic("implement me")
}

func (t *noopVCursor) InReservedConn() bool {
	panic("implement me")
}

func (t *noopVCursor) ShardSession() []*srvtopo.ResolvedShard {
	panic("implement me")
}

func (t *noopVCursor) ExecuteVSchema(ctx context.Context, keyspace string, vschemaDDL *sqlparser.AlterVschema) error {
	panic("implement me")
}

func (t *noopVCursor) Session() SessionActions {
	return t
}

func (t *noopVCursor) SetAutocommit(context.Context, bool) error {
	panic("implement me")
}

func (t *noopVCursor) SetClientFoundRows(context.Context, bool) error {
	panic("implement me")
}

func (t *noopVCursor) SetSkipQueryPlanCache(context.Context, bool) error {
	panic("implement me")
}

func (t *noopVCursor) SetSQLSelectLimit(int64) error {
	panic("implement me")
}

func (t *noopVCursor) SetTransactionMode(vtgatepb.TransactionMode) {
	panic("implement me")
}

func (t *noopVCursor) SetWorkload(querypb.ExecuteOptions_Workload) {
	panic("implement me")
}

func (t *noopVCursor) SetPlannerVersion(querypb.ExecuteOptions_PlannerVersion) {
	panic("implement me")
}

func (t *noopVCursor) SetTarget(string) error {
	panic("implement me")
}

func (t *noopVCursor) MaxMemoryRows() int {
	return testMaxMemoryRows
}

func (t *noopVCursor) ExceedsMaxMemoryRows(numRows int) bool {
	return !testIgnoreMaxMemoryRows && numRows > testMaxMemoryRows
}

func (t *noopVCursor) GetKeyspace() string {
	return ""
}

func (t *noopVCursor) RecordWarning(warning *querypb.QueryWarning) {
}

func (t *noopVCursor) Execute(ctx context.Context, method string, query string, bindvars map[string]*querypb.BindVariable, rollbackOnError bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t *noopVCursor) ExecuteMultiShard(ctx context.Context, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, canAutocommit bool) (*sqltypes.Result, []error) {
	panic("unimplemented")
}

func (t *noopVCursor) AutocommitApproval() bool {
	panic("unimplemented")
}

func (t *noopVCursor) ExecuteStandalone(ctx context.Context, query string, bindvars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t *noopVCursor) StreamExecuteMulti(ctx context.Context, query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, rollbackOnError bool, autocommit bool, callback func(reply *sqltypes.Result) error) []error {
	panic("unimplemented")
}

func (t *noopVCursor) ExecuteKeyspaceID(ctx context.Context, keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError, autocommit bool) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t *noopVCursor) ResolveDestinations(ctx context.Context, keyspace string, ids []*querypb.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	return nil, nil, nil
}

func (t *noopVCursor) ResolveDestinationsMultiCol(ctx context.Context, keyspace string, ids [][]sqltypes.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][][]sqltypes.Value, error) {
	panic("unimplemented")
}

func (t *noopVCursor) GetDBDDLPluginName() string {
	panic("unimplemented")
}

var _ VCursor = (*loggingVCursor)(nil)
var _ SessionActions = (*loggingVCursor)(nil)

// loggingVCursor logs requests and allows you to verify
// that the correct requests were made.
type loggingVCursor struct {
	noopVCursor

	shards          []string
	shardForKsid    []string
	curShardForKsid int
	shardErr        error

	results   []*sqltypes.Result
	curResult int
	resultErr error

	warnings []*querypb.QueryWarning

	// Optional errors that can be returned from nextResult() alongside the results for
	// multi-shard queries
	multiShardErrs []error

	log []string
	mu  sync.Mutex

	resolvedTargetTabletType topodatapb.TabletType

	tableRoutes     tableRoutes
	dbDDLPlugin     string
	ksAvailable     bool
	inReservedConn  bool
	systemVariables map[string]string
	disableSetVar   bool

	// map different shards to keyspaces in the test.
	ksShardMap map[string][]string
}

type tableRoutes struct {
	tbl *vindexes.Table
}

func (f *loggingVCursor) ExecutePrimitive(ctx context.Context, primitive Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return primitive.TryExecute(ctx, f, bindVars, wantfields)
}

func (f *loggingVCursor) StreamExecutePrimitive(ctx context.Context, primitive Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return primitive.TryStreamExecute(ctx, f, bindVars, wantfields, callback)
}

func (f *loggingVCursor) StreamExecutePrimitiveStandalone(ctx context.Context, primitive Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(result *sqltypes.Result) error) error {
	return primitive.TryStreamExecute(ctx, f, bindVars, wantfields, callback)
}

func (f *loggingVCursor) KeyspaceAvailable(ks string) bool {
	return f.ksAvailable
}

func (f *loggingVCursor) HasSystemVariables() bool {
	return len(f.systemVariables) > 0
}

func (f *loggingVCursor) GetSystemVariables(func(k string, v string)) {
	panic("implement me")
}

func (f *loggingVCursor) SetFoundRows(u uint64) {
	panic("implement me")
}

func (f *loggingVCursor) InTransactionAndIsDML() bool {
	return false
}

func (f *loggingVCursor) LookupRowLockShardSession() vtgatepb.CommitOrder {
	panic("implement me")
}

func (f *loggingVCursor) SetUDV(key string, value any) error {
	f.log = append(f.log, fmt.Sprintf("UDV set with (%s,%v)", key, value))
	return nil
}

func (f *loggingVCursor) SetSysVar(name string, expr string) {
	f.log = append(f.log, fmt.Sprintf("SysVar set with (%s,%v)", name, expr))
}

func (f *loggingVCursor) NeedsReservedConn() {
	f.log = append(f.log, "Needs Reserved Conn")
	f.inReservedConn = true
}

func (f *loggingVCursor) InReservedConn() bool {
	return f.inReservedConn
}

func (f *loggingVCursor) ShardSession() []*srvtopo.ResolvedShard {
	return nil
}

func (f *loggingVCursor) ExecuteVSchema(context.Context, string, *sqlparser.AlterVschema) error {
	panic("implement me")
}

func (f *loggingVCursor) Session() SessionActions {
	return f
}

func (f *loggingVCursor) SetTarget(target string) error {
	f.log = append(f.log, fmt.Sprintf("Target set to %s", target))
	return nil
}

func (f *loggingVCursor) GetKeyspace() string {
	return ""
}

func (f *loggingVCursor) RecordWarning(warning *querypb.QueryWarning) {
	f.warnings = append(f.warnings, warning)
}

func (f *loggingVCursor) Execute(ctx context.Context, method string, query string, bindvars map[string]*querypb.BindVariable, rollbackOnError bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error) {
	name := "Unknown"
	switch co {
	case vtgatepb.CommitOrder_NORMAL:
		name = "Execute"
	case vtgatepb.CommitOrder_PRE:
		name = "ExecutePre"
	case vtgatepb.CommitOrder_POST:
		name = "ExecutePost"
	case vtgatepb.CommitOrder_AUTOCOMMIT:
		name = "ExecuteAutocommit"
	}
	f.log = append(f.log, fmt.Sprintf("%s %s %v %v", name, query, printBindVars(bindvars), rollbackOnError))
	return f.nextResult()
}

func (f *loggingVCursor) ExecuteMultiShard(ctx context.Context, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, canAutocommit bool) (*sqltypes.Result, []error) {
	f.log = append(f.log, fmt.Sprintf("ExecuteMultiShard %v%v %v", printResolvedShardQueries(rss, queries), rollbackOnError, canAutocommit))
	res, err := f.nextResult()
	if err != nil {
		return nil, []error{err}
	}

	return res, f.multiShardErrs
}

func (f *loggingVCursor) AutocommitApproval() bool {
	return true
}

func (f *loggingVCursor) ExecuteStandalone(ctx context.Context, query string, bindvars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard) (*sqltypes.Result, error) {
	f.log = append(f.log, fmt.Sprintf("ExecuteStandalone %s %v %s %s", query, printBindVars(bindvars), rs.Target.Keyspace, rs.Target.Shard))
	return f.nextResult()
}

func (f *loggingVCursor) StreamExecuteMulti(ctx context.Context, query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, rollbackOnError bool, autocommit bool, callback func(reply *sqltypes.Result) error) []error {
	f.mu.Lock()
	f.log = append(f.log, fmt.Sprintf("StreamExecuteMulti %s %s", query, printResolvedShardsBindVars(rss, bindVars)))
	r, err := f.nextResult()
	f.mu.Unlock()
	if err != nil {
		return []error{err}
	}

	return []error{callback(r)}
}

func (f *loggingVCursor) ResolveDestinations(ctx context.Context, keyspace string, ids []*querypb.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	f.log = append(f.log, fmt.Sprintf("ResolveDestinations %v %v %v", keyspace, ids, key.DestinationsString(destinations)))
	if f.shardErr != nil {
		return nil, nil, f.shardErr
	}

	var rss []*srvtopo.ResolvedShard
	var values [][]*querypb.Value
	visited := make(map[string]int)
	for i, destination := range destinations {
		var shards []string

		switch d := destination.(type) {
		case key.DestinationAllShards:
			if f.ksShardMap != nil {
				if ksShards, exists := f.ksShardMap[keyspace]; exists {
					shards = ksShards
					break
				}
			}
			shards = f.shards
		case key.DestinationKeyRange:
			shards = f.shardForKsid
		case key.DestinationKeyspaceID:
			if f.shardForKsid == nil || f.curShardForKsid >= len(f.shardForKsid) {
				shards = []string{"-20"}
			} else {
				shards = []string{f.shardForKsid[f.curShardForKsid]}
				f.curShardForKsid++
			}
		case key.DestinationKeyspaceIDs:
			for _, ksid := range d {
				if string(ksid) < "\x20" {
					shards = append(shards, "-20")
				} else {
					shards = append(shards, "20-")
				}
			}
		case key.DestinationAnyShard:
			// Take the first shard.
			shards = f.shards[:1]
		case key.DestinationNone:
			// Nothing to do here.
		case key.DestinationShard:
			shards = []string{destination.String()}
		default:
			return nil, nil, fmt.Errorf("unsupported destination: %v", destination)
		}

		for _, shard := range shards {
			vi, ok := visited[shard]
			if !ok {
				vi = len(rss)
				visited[shard] = vi
				rss = append(rss, &srvtopo.ResolvedShard{
					Target: &querypb.Target{
						Keyspace:   keyspace,
						Shard:      shard,
						TabletType: f.resolvedTargetTabletType,
					},
				})
				if ids != nil {
					values = append(values, nil)
				}
			}
			if ids != nil {
				values[vi] = append(values[vi], ids[i])
			}
		}
	}
	return rss, values, nil
}

func (f *loggingVCursor) ResolveDestinationsMultiCol(ctx context.Context, keyspace string, ids [][]sqltypes.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][][]sqltypes.Value, error) {
	f.log = append(f.log, fmt.Sprintf("ResolveDestinationsMultiCol %v %v %v", keyspace, ids, key.DestinationsString(destinations)))
	if f.shardErr != nil {
		return nil, nil, f.shardErr
	}

	var rss []*srvtopo.ResolvedShard
	var values [][][]sqltypes.Value
	visited := make(map[string]int)
	for i, destination := range destinations {
		var shards []string

		switch d := destination.(type) {
		case key.DestinationAllShards:
			shards = f.shards
		case key.DestinationKeyRange:
			shards = f.shardForKsid
		case key.DestinationKeyspaceID:
			if f.shardForKsid == nil || f.curShardForKsid >= len(f.shardForKsid) {
				shards = []string{"-20"}
			} else {
				shards = []string{f.shardForKsid[f.curShardForKsid]}
				f.curShardForKsid++
			}
		case key.DestinationKeyspaceIDs:
			for _, ksid := range d {
				if string(ksid) < "\x20" {
					shards = append(shards, "-20")
				} else {
					shards = append(shards, "20-")
				}
			}
		case key.DestinationAnyShard:
			// Take the first shard.
			shards = f.shards[:1]
		case key.DestinationNone:
			// Nothing to do here.
		case key.DestinationShard:
			shards = []string{destination.String()}
		default:
			return nil, nil, fmt.Errorf("unsupported destination: %v", destination)
		}

		for _, shard := range shards {
			vi, ok := visited[shard]
			if !ok {
				vi = len(rss)
				visited[shard] = vi
				rss = append(rss, &srvtopo.ResolvedShard{
					Target: &querypb.Target{
						Keyspace:   keyspace,
						Shard:      shard,
						TabletType: f.resolvedTargetTabletType,
					},
				})
				if ids != nil {
					values = append(values, nil)
				}
			}
			if ids != nil {
				values[vi] = append(values[vi], ids[i])
			}
		}
	}
	return rss, values, nil
}

func (f *loggingVCursor) ExpectLog(t *testing.T, want []string) {
	t.Helper()
	if len(f.log) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(f.log, want) {
		t.Errorf("got:\n%s\nwant:\n%s", strings.Join(f.log, "\n"), strings.Join(want, "\n"))
	}
	utils.MustMatch(t, want, f.log, "")
}

func (f *loggingVCursor) ExpectWarnings(t *testing.T, want []*querypb.QueryWarning) {
	t.Helper()
	if !reflect.DeepEqual(f.warnings, want) {
		t.Errorf("vc.warnings:\n%+v\nwant:\n%+v", f.warnings, want)
	}
}

func (f *loggingVCursor) Rewind() {
	f.curShardForKsid = 0
	f.curResult = 0
	f.log = nil
	f.warnings = nil
}

func (f *loggingVCursor) SetAutocommit(context.Context, bool) error {
	panic("implement me")
}

func (f *loggingVCursor) SetClientFoundRows(context.Context, bool) error {
	panic("implement me")
}

func (f *loggingVCursor) SetSkipQueryPlanCache(context.Context, bool) error {
	panic("implement me")
}

func (f *loggingVCursor) SetSQLSelectLimit(int64) error {
	panic("implement me")
}

func (f *loggingVCursor) SetTransactionMode(vtgatepb.TransactionMode) {
	panic("implement me")
}

func (f *loggingVCursor) SetWorkload(querypb.ExecuteOptions_Workload) {
	panic("implement me")
}

func (f *loggingVCursor) SetPlannerVersion(querypb.ExecuteOptions_PlannerVersion) {
	panic("implement me")
}

func (f *loggingVCursor) FindRoutedTable(tbl sqlparser.TableName) (*vindexes.Table, error) {
	f.log = append(f.log, fmt.Sprintf("FindTable(%s)", sqlparser.String(tbl)))
	return f.tableRoutes.tbl, nil
}

func (f *loggingVCursor) GetDBDDLPluginName() string {
	return f.dbDDLPlugin
}

func (f *loggingVCursor) nextResult() (*sqltypes.Result, error) {
	if f.results == nil || f.curResult >= len(f.results) {
		return &sqltypes.Result{}, f.resultErr
	}

	r := f.results[f.curResult]
	f.curResult++
	if r == nil {
		return &sqltypes.Result{}, f.resultErr
	}
	return r, nil
}

func (f *loggingVCursor) CanUseSetVar() bool {
	useSetVar := sqlparser.IsMySQL80AndAbove() && !f.disableSetVar
	if useSetVar {
		f.log = append(f.log, "SET_VAR can be used")
	}
	return useSetVar
}

func (t *noopVCursor) VtExplainLogging() {}
func (t *noopVCursor) DisableLogging()   {}
func (t *noopVCursor) GetVTExplainLogs() []ExecuteEntry {
	return nil
}
func (t *noopVCursor) GetLogs() ([]ExecuteEntry, error) {
	return nil, nil
}

func expectResult(t *testing.T, msg string, result, want *sqltypes.Result) {
	t.Helper()
	if !reflect.DeepEqual(result, want) {
		t.Errorf("%s:\n%v\nwant:\n%v", msg, result, want)
	}
}

func printBindVars(bindvars map[string]*querypb.BindVariable) string {
	var keys []string
	for k := range bindvars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	buf := &bytes.Buffer{}
	for i, k := range keys {
		if i > 0 {
			fmt.Fprintf(buf, " ")
		}
		fmt.Fprintf(buf, "%s: %v", k, bindvars[k])
	}
	return buf.String()
}

func printResolvedShardQueries(rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery) string {
	buf := &bytes.Buffer{}
	for i, rs := range rss {
		fmt.Fprintf(buf, "%s.%s: %s {%s} ", rs.Target.Keyspace, rs.Target.Shard, queries[i].Sql, printBindVars(queries[i].BindVariables))
	}
	return buf.String()
}

func printResolvedShardsBindVars(rss []*srvtopo.ResolvedShard, bvs []map[string]*querypb.BindVariable) string {
	buf := &bytes.Buffer{}
	for i, rs := range rss {
		fmt.Fprintf(buf, "%s.%s: {%v} ", rs.Target.Keyspace, rs.Target.Shard, printBindVars(bvs[i]))
	}
	return buf.String()
}
