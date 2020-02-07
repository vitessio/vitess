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

type (
	// VCursor defines the interface the engine will use
	// to execute routes.
	VCursor interface {
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

	// PlanStats keeps statistics of the usage of a Plan
	PlanStats struct {
		// Plan contains the engine Primitives and metadata about them
		Plan *Plan `json:",omitempty"`
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

	// Plan represents the execution strategy for a given query.
	// For now it's a simple wrapper around the real instructions.
	// An instruction (aka Primitive) is typically a tree where
	// each node does its part by combining the results of the
	// sub-nodes.
	Plan struct {
		// Original is the original query.
		Original string `json:",omitempty"`
		// Instructions contains the instructions needed to
		// fulfil the query.
		Instructions Primitive `json:",omitempty"`
		// NeedsLastInsertID signals whether this plan will need to be provided with last_insert_id
		NeedsLastInsertID bool `json:"-"` // don't include in the json representation
		// NeedsDatabaseName signals whether this plan will need to be provided with the database name
		NeedsDatabaseName bool `json:"-"` // don't include in the json representation
	}
)

// AddStats updates the plan execution statistics
func (p *PlanStats) AddStats(execCount uint64, execTime time.Duration, shardQueries, rows, errors uint64) {
	p.mu.Lock()
	p.ExecCount += execCount
	p.ExecTime += execTime
	p.ShardQueries += shardQueries
	p.Rows += rows
	p.Errors += errors
	p.mu.Unlock()
}

// Stats returns a copy of the plan execution statistics
func (p *PlanStats) Stats() (execCount uint64, execTime time.Duration, shardQueries, rows, errors uint64) {
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
func (p *PlanStats) Size() int {
	return 1
}

// Match is used to check if a Primitive matches
type Match func(node Primitive) bool

// Find will return the first Primitive that matches the evaluate function. If no match is found, nil will be returned
func Find(isMatch Match, start Primitive) Primitive {
	if isMatch(start) {
		return start
	}
	for _, input := range start.Inputs() {
		result := Find(isMatch, input)
		if result != nil {
			return result
		}
	}
	return nil
}

// Exists traverses recursively down the Primitive tree structure, and returns true when Match returns true
func Exists(m Match, p Primitive) bool {
	return Find(m, p) != nil
}

// Primitive is the building block of the engine execution plan. They form a tree structure, where the leaves typically
// issue queries to one or more vttablet.
// During execution, the Primitive's pass Result objects up the tree structure, until reaching the root,
// and its result is passed to the client.
type Primitive interface {
	RouteType() string
	GetKeyspaceName() string
	GetTableName() string
	Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error)
	StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error
	GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error)

	// The inputs to this Primitive
	Inputs() []Primitive
}

type noInputs struct{}

// Inputs implements no inputs
func (noInputs) Inputs() []Primitive {
	return nil
}
