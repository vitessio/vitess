/*
Copyright 2018 The Vitess Authors.

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

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// noopVCursor is used to build other vcursors.
type noopVCursor struct {
}

func (t noopVCursor) Context() context.Context {
	return context.Background()
}

func (t noopVCursor) SetContextTimeout(timeout time.Duration) context.CancelFunc {
	return func() {}
}

func (t noopVCursor) RecordWarning(warning *querypb.QueryWarning) {
}

func (t noopVCursor) Execute(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t noopVCursor) ExecutePre(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t noopVCursor) ExecutePost(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t noopVCursor) ExecuteAutocommit(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t noopVCursor) ExecuteMultiShard(rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, isDML, autocommit bool) (*sqltypes.Result, []error) {
	panic("unimplemented")
}

func (t noopVCursor) AutocommitApproval() bool {
	panic("unimplemented")
}

func (t noopVCursor) ExecuteStandalone(query string, bindvars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t noopVCursor) StreamExecuteMulti(query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error {
	panic("unimplemented")
}

func (t noopVCursor) ResolveDestinations(keyspace string, ids []*querypb.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	panic("unimplemented")
}

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
}

func (f *loggingVCursor) Context() context.Context {
	return context.Background()
}

func (f *loggingVCursor) SetContextTimeout(timeout time.Duration) context.CancelFunc {
	return func() {}
}

func (f *loggingVCursor) RecordWarning(warning *querypb.QueryWarning) {
	f.warnings = append(f.warnings, warning)
}

func (f *loggingVCursor) Execute(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	f.log = append(f.log, fmt.Sprintf("Execute %s %v %v", query, printBindVars(bindvars), isDML))
	return f.nextResult()
}

func (f *loggingVCursor) ExecutePre(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	f.log = append(f.log, fmt.Sprintf("ExecutePre %s %v %v", query, printBindVars(bindvars), isDML))
	return f.nextResult()
}

func (f *loggingVCursor) ExecutePost(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	f.log = append(f.log, fmt.Sprintf("ExecutePost %s %v %v", query, printBindVars(bindvars), isDML))
	return f.nextResult()
}

func (f *loggingVCursor) ExecuteMultiShard(rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, isDML, canAutocommit bool) (*sqltypes.Result, []error) {
	f.log = append(f.log, fmt.Sprintf("ExecuteMultiShard %v%v %v", printResolvedShardQueries(rss, queries), isDML, canAutocommit))
	res, err := f.nextResult()
	if err != nil {
		return nil, []error{err}
	}

	return res, f.multiShardErrs
}

func (f *loggingVCursor) AutocommitApproval() bool {
	return true
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
						Keyspace: keyspace,
						Shard:    shard,
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
	if !reflect.DeepEqual(f.log, want) {
		t.Errorf("vc.log:\n%v\nwant:\n%v", strings.Join(f.log, "\n"), strings.Join(want, "\n"))
	}
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

func expectError(t *testing.T, msg string, err error, want string) {
	t.Helper()
	if err == nil || err.Error() != want {
		t.Errorf("%s: %v, want %s", msg, err, want)
	}
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
