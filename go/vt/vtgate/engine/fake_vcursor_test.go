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
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/test/utils"

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"golang.org/x/sync/errgroup"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/vt/sqlparser"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/srvtopo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

var testMaxMemoryRows = 100
var testIgnoreMaxMemoryRows = false

var _ VCursor = (*noopVCursor)(nil)
var _ SessionActions = (*noopVCursor)(nil)

// noopVCursor is used to build other vcursors.
type noopVCursor struct {
	ctx context.Context
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

func (t *noopVCursor) SetSessionEnableSystemSettings(allow bool) error {
	panic("implement me")
}

func (t *noopVCursor) GetSessionEnableSystemSettings() bool {
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

func (t *noopVCursor) ExecuteLock(rs *srvtopo.ResolvedShard, query *querypb.BoundQuery) (*sqltypes.Result, error) {
	panic("implement me")
}

func (t *noopVCursor) NeedsReservedConn() {
}

func (t *noopVCursor) SetUDV(key string, value interface{}) error {
	panic("implement me")
}

func (t *noopVCursor) SetSysVar(name string, expr string) {
	//panic("implement me")
}

func (t *noopVCursor) InReservedConn() bool {
	panic("implement me")
}

func (t *noopVCursor) ShardSession() []*srvtopo.ResolvedShard {
	panic("implement me")
}

func (t *noopVCursor) ExecuteVSchema(keyspace string, vschemaDDL *sqlparser.AlterVschema) error {
	panic("implement me")
}

func (t *noopVCursor) Session() SessionActions {
	return t
}

func (t *noopVCursor) SetAutocommit(bool) error {
	panic("implement me")
}

func (t *noopVCursor) SetClientFoundRows(bool) error {
	panic("implement me")
}

func (t *noopVCursor) SetSkipQueryPlanCache(bool) error {
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

func (t *noopVCursor) Context() context.Context {
	if t.ctx == nil {
		return context.Background()
	}
	return t.ctx
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

func (t *noopVCursor) SetContextTimeout(timeout time.Duration) context.CancelFunc {
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	t.ctx = ctx
	return cancel
}

func (t *noopVCursor) ErrorGroupCancellableContext() (*errgroup.Group, func()) {
	g, ctx := errgroup.WithContext(t.ctx)
	t.ctx = ctx
	return g, func() {}
}

func (t *noopVCursor) RecordWarning(warning *querypb.QueryWarning) {
}

func (t *noopVCursor) Execute(method string, query string, bindvars map[string]*querypb.BindVariable, rollbackOnError bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t *noopVCursor) ExecuteMultiShard(rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, canAutocommit bool) (*sqltypes.Result, []error) {
	panic("unimplemented")
}

func (t *noopVCursor) AutocommitApproval() bool {
	panic("unimplemented")
}

func (t *noopVCursor) ExecuteStandalone(query string, bindvars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t *noopVCursor) StreamExecuteMulti(query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error {
	panic("unimplemented")
}

func (t *noopVCursor) ExecuteKeyspaceID(keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError, autocommit bool) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t *noopVCursor) ResolveDestinations(keyspace string, ids []*querypb.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	panic("unimplemented")
}

func (t *noopVCursor) SubmitOnlineDDL(onlineDDl *schema.OnlineDDL) error {
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

	resolvedTargetTabletType topodatapb.TabletType

	tableRoutes tableRoutes
	dbDDLPlugin string
	ksAvailable bool
}

type tableRoutes struct {
	tbl *vindexes.Table
}

func (f *loggingVCursor) KeyspaceAvailable(ks string) bool {
	return f.ksAvailable
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

func (f *loggingVCursor) SetUDV(key string, value interface{}) error {
	f.log = append(f.log, fmt.Sprintf("UDV set with (%s,%v)", key, value))
	return nil
}

func (f *loggingVCursor) SetSysVar(name string, expr string) {
	f.log = append(f.log, fmt.Sprintf("SysVar set with (%s,%v)", name, expr))
}

func (f *loggingVCursor) NeedsReservedConn() {
}

func (f *loggingVCursor) InReservedConn() bool {
	panic("implement me")
}

func (f *loggingVCursor) ShardSession() []*srvtopo.ResolvedShard {
	return nil
}

func (f *loggingVCursor) ExecuteVSchema(string, *sqlparser.AlterVschema) error {
	panic("implement me")
}

func (f *loggingVCursor) Session() SessionActions {
	return f
}

func (f *loggingVCursor) SetTarget(target string) error {
	f.log = append(f.log, fmt.Sprintf("Target set to %s", target))
	return nil
}

func (f *loggingVCursor) Context() context.Context {
	if f.ctx == nil {
		return context.Background()
	}
	return f.ctx
}

func (f *loggingVCursor) SetContextTimeout(timeout time.Duration) context.CancelFunc {
	ctx, cancel := context.WithTimeout(f.Context(), timeout)
	f.ctx = ctx
	return cancel
}

func (f *loggingVCursor) ErrorGroupCancellableContext() (*errgroup.Group, func()) {
	panic("implement me")
}

func (f *loggingVCursor) GetKeyspace() string {
	return ""
}

func (f *loggingVCursor) RecordWarning(warning *querypb.QueryWarning) {
	f.warnings = append(f.warnings, warning)
}

func (f *loggingVCursor) Execute(_ string, query string, bindvars map[string]*querypb.BindVariable, rollbackOnError bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error) {
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

func (f *loggingVCursor) ExecuteMultiShard(rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, canAutocommit bool) (*sqltypes.Result, []error) {
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

func (f *loggingVCursor) SubmitOnlineDDL(onlineDDL *schema.OnlineDDL) error {
	f.log = append(f.log, fmt.Sprintf("SubmitOnlineDDL: %s", onlineDDL.ToString()))
	return nil
}

func (f *loggingVCursor) ExecuteStandalone(query string, bindvars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard) (*sqltypes.Result, error) {
	f.log = append(f.log, fmt.Sprintf("ExecuteStandalone %s %v %s %s", query, printBindVars(bindvars), rs.Target.Keyspace, rs.Target.Shard))
	return f.nextResult()
}

func (f *loggingVCursor) StreamExecuteMulti(query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error {
	f.log = append(f.log, fmt.Sprintf("StreamExecuteMulti %s %s", query, printResolvedShardsBindVars(rss, bindVars)))
	r, err := f.nextResult()
	if err != nil {
		return err
	}
	return callback(r)
}

func (f *loggingVCursor) ResolveDestinations(keyspace string, ids []*querypb.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
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
			shards = f.shards
		case key.DestinationKeyRange:
			shards = []string{"-20", "20-"}
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

func (f *loggingVCursor) SetAutocommit(bool) error {
	panic("implement me")
}

func (f *loggingVCursor) SetClientFoundRows(bool) error {
	panic("implement me")
}

func (f *loggingVCursor) SetSkipQueryPlanCache(bool) error {
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
	for _, k := range keys {
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
