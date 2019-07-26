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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

const (
	// SeqVarName is a reserved bind var name for sequence values.
	SeqVarName = "__seq"
	// ListVarName is a reserved bind var name for list vars.
	// This is used for sending different IN clause values
	// to different shards.
	ListVarName = "__vals"
)

// VCursor defines the interface the engine will use
// to execute routes.
type VCursor interface {
	// Context returns the context of the current request.
	Context() context.Context

	// MaxMemoryRows returns the maxMemoryRows flag value.
	MaxMemoryRows() int

	// SetContextTimeout updates the context and sets a timeout.
	SetContextTimeout(timeout time.Duration) context.CancelFunc

	// RecordWarning stores the given warning in the current session
	RecordWarning(warning *querypb.QueryWarning)

	// V3 functions.
	Execute(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error)
	AutocommitApproval() bool

	// Shard-level functions.
	ExecuteMultiShard(rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, isDML, canAutocommit bool) (*sqltypes.Result, []error)
	ExecuteStandalone(query string, bindvars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard) (*sqltypes.Result, error)
	StreamExecuteMulti(query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error

	// Keyspace ID level functions.
	ExecuteKeyspaceID(keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, isDML, autocommit bool) (*sqltypes.Result, error)

	// Resolver methods, from key.Destination to srvtopo.ResolvedShard.
	// Will replace all of the Topo functions.
	ResolveDestinations(keyspace string, ids []*querypb.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error)
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
	ExecCount uint64 `json:",omitempty"`
	// Total execution time
	ExecTime time.Duration `json:",omitempty"`
	// Total number of shard queries
	ShardQueries uint64 `json:",omitempty"`
	// Total number of rows
	Rows uint64 `json:",omitempty"`
	// Total number of errors
	Errors uint64 `json:",omitempty"`
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
	RouteType() string
	GetKeyspaceName() string
	GetTableName() string
	Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error)
	StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error
	GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error)
}
