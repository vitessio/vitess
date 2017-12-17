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

package engine

import (
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// SeqVarName is a reserved bind var name for sequence values.
const SeqVarName = "__seq"

// ListVarName is a reserved bind var name for list vars.
// This is used for sending different IN clause values
// to different shards.
const ListVarName = "__vals"

// VCursor defines the interface the engine will use
// to execute routes.
type VCursor interface {
	// Context returns the context of the current request.
	Context() context.Context
	Execute(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error)
	ExecuteMultiShard(keyspace string, shardQueries map[string]*querypb.BoundQuery, isDML bool) (*sqltypes.Result, error)
	ExecuteStandalone(query string, bindvars map[string]*querypb.BindVariable, keyspace, shard string) (*sqltypes.Result, error)
	StreamExecuteMulti(query string, keyspace string, shardVars map[string]map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error
	GetKeyspaceShards(vkeyspace *vindexes.Keyspace) (string, []*topodatapb.ShardReference, error)
	GetShardForKeyspaceID(allShards []*topodatapb.ShardReference, keyspaceID []byte) (string, error)
}

// Plan represents the execution strategy for a given query.
// For now it's a simple wrapper around the real instructions.
// An instruction (aka Primitive) is typically a tree where
// each node does its part by combining the results of the
// sub-nodes.
type Plan struct {
	// Original is the original query.
	Original string `json:",omitempty"`
	// Instructions contains the instructions needed to
	// fulfil the query.
	Instructions Primitive `json:",omitempty"`
	// Mutex to protect the stats
	mu sync.Mutex
	// Count of times this plan was executed
	ExecCount uint64
	// Total execution time
	ExecTime time.Duration
	// Total number of shard queries
	ShardQueries uint64
	// Total number of rows
	Rows uint64
	// Total number of errors
	Errors uint64
}

// AddStats updates the plan execution statistics
func (p *Plan) AddStats(execCount uint64, execTime time.Duration, shardQueries, rows, errors uint64) {
	p.mu.Lock()
	p.ExecCount += execCount
	p.ExecTime += execTime
	p.ShardQueries += shardQueries
	p.Rows += rows
	p.Errors += errors
	p.mu.Unlock()
}

// Stats returns a copy of the plan execution statistics
func (p *Plan) Stats() (execCount uint64, execTime time.Duration, shardQueries, rows, errors uint64) {
	p.mu.Lock()
	execCount = p.ExecCount
	execTime = p.ExecTime
	shardQueries = p.ShardQueries
	rows = p.Rows
	errors = p.Errors
	p.mu.Unlock()
	return
}

// Size is defined so that Plan can be given to a cache.LRUCache.
// VTGate needs to maintain a cache of plans. It uses LRUCache, which
// in turn requires its objects to define a Size function.
func (p *Plan) Size() int {
	return 1
}

// Primitive is the interface that needs to be satisfied by
// all primitives of a plan.
type Primitive interface {
	Execute(vcursor VCursor, bindVars, joinvars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error)
	StreamExecute(vcursor VCursor, bindVars, joinvars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error
	GetFields(vcursor VCursor, bindVars, joinvars map[string]*querypb.BindVariable) (*sqltypes.Result, error)
}
