/*
Copyright 2018 Google Inc.

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
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// noopVCursor is used to build other vcursors.
type noopVCursor struct {
}

func (t noopVCursor) Context() context.Context {
	return context.Background()
}

func (t noopVCursor) Execute(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t noopVCursor) ExecuteAutocommit(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t noopVCursor) ExecuteMultiShard(keyspace string, shardQueries map[string]*querypb.BoundQuery, isDML, canAutocommit bool) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t noopVCursor) ExecuteStandalone(query string, bindvars map[string]*querypb.BindVariable, keyspace, shard string) (*sqltypes.Result, error) {
	panic("unimplemented")
}

func (t noopVCursor) StreamExecuteMulti(query string, keyspace string, shardVars map[string]map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error {
	panic("unimplemented")
}

func (t noopVCursor) GetKeyspaceShards(vkeyspace *vindexes.Keyspace) (string, []*topodatapb.ShardReference, error) {
	panic("unimplemented")
}

func (t noopVCursor) GetShardsForKsids(allShards []*topodatapb.ShardReference, ksids vindexes.Ksids) ([]string, error) {
	panic("unimplemented")
}

func (t noopVCursor) GetShardForKeyspaceID(allShards []*topodatapb.ShardReference, keyspaceID []byte) (string, error) {
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

	log []string
}

func (f *loggingVCursor) Context() context.Context {
	return context.Background()
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

func (f *loggingVCursor) Execute(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	f.log = append(f.log, fmt.Sprintf("Execute %s %v %v", query, printBindVars(bindvars), isDML))
	return f.nextResult()
}

func (f *loggingVCursor) ExecuteMultiShard(keyspace string, shardQueries map[string]*querypb.BoundQuery, isDML, canAutocommit bool) (*sqltypes.Result, error) {
	f.log = append(f.log, fmt.Sprintf("ExecuteMultiShard %s %v %v %v", keyspace, printShardQueries(shardQueries), isDML, canAutocommit))
	return f.nextResult()
}

func (f *loggingVCursor) ExecuteStandalone(query string, bindvars map[string]*querypb.BindVariable, keyspace, shard string) (*sqltypes.Result, error) {
	f.log = append(f.log, fmt.Sprintf("ExecuteStandalone %s %v %s %s", query, printBindVars(bindvars), keyspace, shard))
	return f.nextResult()
}

func (f *loggingVCursor) GetKeyspaceShards(vkeyspace *vindexes.Keyspace) (string, []*topodatapb.ShardReference, error) {
	f.log = append(f.log, fmt.Sprintf("GetKeyspaceShards %v", vkeyspace))
	if f.shardErr != nil {
		return "", nil, f.shardErr
	}
	sr := make([]*topodatapb.ShardReference, len(f.shards))
	for i, shard := range f.shards {
		sr[i] = &topodatapb.ShardReference{Name: shard}
	}
	return vkeyspace.Name, sr, nil
}

func (f *loggingVCursor) GetShardForKeyspaceID(allShards []*topodatapb.ShardReference, keyspaceID []byte) (string, error) {
	f.log = append(f.log, fmt.Sprintf("GetShardForKeyspaceID %v %q", allShards, hex.EncodeToString(keyspaceID)))
	if f.shardForKsid == nil || f.curShardForKsid >= len(f.shardForKsid) {
		if f.shardErr != nil {
			return "", f.shardErr
		}
		return "-20", nil
	}
	r := f.shardForKsid[f.curShardForKsid]
	f.curShardForKsid++
	return r, nil
}

func (f *loggingVCursor) ExpectLog(t *testing.T, want []string) {
	t.Helper()
	if !reflect.DeepEqual(f.log, want) {
		t.Errorf("vc.log:\n%+v, want\n%+v", f.log, want)
	}
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
		t.Errorf("s: %v, want %v", result, want)
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

func printShardQueries(shardQueries map[string]*querypb.BoundQuery) string {
	var keys []string
	for k := range shardQueries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	buf := &bytes.Buffer{}
	for _, k := range keys {
		fmt.Fprintf(buf, "%s: %s %s", k, shardQueries[k].Sql, printBindVars(shardQueries[k].BindVariables))
	}
	return buf.String()
}
