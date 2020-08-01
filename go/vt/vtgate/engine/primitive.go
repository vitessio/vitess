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
	"encoding/json"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/vt/sqlparser"

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

		// ExceedsMaxMemoryRows returns a boolean indicating whether
		// the maxMemoryRows value has been exceeded. Returns false
		// if the max memory rows override directive is set to true
		ExceedsMaxMemoryRows(numRows int) bool

		// SetContextTimeout updates the context and sets a timeout.
		SetContextTimeout(timeout time.Duration) context.CancelFunc

		// ErrorGroupCancellableContext updates context that can be cancelled.
		ErrorGroupCancellableContext() *errgroup.Group

		// V3 functions.
		Execute(method string, query string, bindvars map[string]*querypb.BindVariable, rollbackOnError bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error)
		AutocommitApproval() bool

		// Shard-level functions.
		ExecuteMultiShard(rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, canAutocommit bool) (*sqltypes.Result, []error)
		ExecuteStandalone(query string, bindvars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard) (*sqltypes.Result, error)
		StreamExecuteMulti(query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error

		// Keyspace ID level functions.
		ExecuteKeyspaceID(keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError, autocommit bool) (*sqltypes.Result, error)

		// Resolver methods, from key.Destination to srvtopo.ResolvedShard.
		// Will replace all of the Topo functions.
		ResolveDestinations(keyspace string, ids []*querypb.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error)

		ExecuteVSchema(keyspace string, vschemaDDL *sqlparser.DDL) error

		Session() SessionActions
	}

	//SessionActions gives primitives ability to interact with the session state
	SessionActions interface {
		// RecordWarning stores the given warning in the current session
		RecordWarning(warning *querypb.QueryWarning)

		SetTarget(target string) error

		SetUDV(key string, value interface{}) error

		SetSysVar(name string, expr string)

		// NeedsReservedConn marks this session as needing a dedicated connection to underlying database
		NeedsReservedConn()

		// InReservedConn provides whether this session is using reserved connection
		InReservedConn() bool

		// ShardSession returns shard info about open connections
		ShardSession() []*srvtopo.ResolvedShard
	}

	// Plan represents the execution strategy for a given query.
	// For now it's a simple wrapper around the real instructions.
	// An instruction (aka Primitive) is typically a tree where
	// each node does its part by combining the results of the
	// sub-nodes.
	Plan struct {
		Type                   sqlparser.StatementType // The type of query we have
		Original               string                  // Original is the original query.
		Instructions           Primitive               // Instructions contains the instructions needed to fulfil the query.
		sqlparser.BindVarNeeds                         // Stores BindVars needed to be provided as part of expression rewriting

		mu           sync.Mutex    // Mutex to protect the fields below
		ExecCount    uint64        // Count of times this plan was executed
		ExecTime     time.Duration // Total execution time
		ShardQueries uint64        // Total number of shard queries
		Rows         uint64        // Total number of rows
		Errors       uint64        // Total number of errors
	}

	// Match is used to check if a Primitive matches
	Match func(node Primitive) bool

	// Primitive is the building block of the engine execution plan. They form a tree structure, where the leaves typically
	// issue queries to one or more vttablet.
	// During execution, the Primitive's pass Result objects up the tree structure, until reaching the root,
	// and its result is passed to the client.
	Primitive interface {
		RouteType() string
		GetKeyspaceName() string
		GetTableName() string
		Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error)
		StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error
		GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error)
		NeedsTransaction() bool

		// The inputs to this Primitive
		Inputs() []Primitive

		// description is the description, sans the inputs, of this Primitive.
		// to get the plan description with all children, use PrimitiveToPlanDescription()
		description() PrimitiveDescription
	}

	// noInputs default implementation for Primitives that are leaves
	noInputs struct{}

	// noTxNeeded is a default implementation for Primitives that don't need transaction handling
	noTxNeeded struct{}

	// txNeeded is a default implementation for Primitives that need transaction handling
	txNeeded struct{}
)

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

// Size is defined so that Plan can be given to a cache.LRUCache.
// VTGate needs to maintain a cache of plans. It uses LRUCache, which
// in turn requires its objects to define a Size function.
func (p *Plan) Size() int {
	return 1
}

//MarshalJSON serializes the plan into a JSON representation.
func (p *Plan) MarshalJSON() ([]byte, error) {
	var instructions *PrimitiveDescription
	if p.Instructions != nil {
		description := PrimitiveToPlanDescription(p.Instructions)
		instructions = &description
	}

	marshalPlan := struct {
		QueryType    string
		Original     string                `json:",omitempty"`
		Instructions *PrimitiveDescription `json:",omitempty"`
		ExecCount    uint64                `json:",omitempty"`
		ExecTime     time.Duration         `json:",omitempty"`
		ShardQueries uint64                `json:",omitempty"`
		Rows         uint64                `json:",omitempty"`
		Errors       uint64                `json:",omitempty"`
	}{
		QueryType:    p.Type.String(),
		Original:     p.Original,
		Instructions: instructions,
		ExecCount:    p.ExecCount,
		ExecTime:     p.ExecTime,
		ShardQueries: p.ShardQueries,
		Rows:         p.Rows,
		Errors:       p.Errors,
	}
	return json.Marshal(marshalPlan)
}

// Inputs implements no inputs
func (noInputs) Inputs() []Primitive {
	return nil
}

func (noTxNeeded) NeedsTransaction() bool {
	return false
}

func (txNeeded) NeedsTransaction() bool {
	return true
}
