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
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/vt/sqlparser"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/schema"
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

		GetKeyspace() string
		// MaxMemoryRows returns the maxMemoryRows flag value.
		MaxMemoryRows() int

		// ExceedsMaxMemoryRows returns a boolean indicating whether
		// the maxMemoryRows value has been exceeded. Returns false
		// if the max memory rows override directive is set to true
		ExceedsMaxMemoryRows(numRows int) bool

		// SetContextTimeout updates the context and sets a timeout.
		SetContextTimeout(timeout time.Duration) context.CancelFunc

		// ErrorGroupCancellableContext updates context that can be cancelled.
		ErrorGroupCancellableContext() (*errgroup.Group, func())

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

		ExecuteVSchema(keyspace string, vschemaDDL *sqlparser.AlterVschema) error

		SubmitOnlineDDL(onlineDDl *schema.OnlineDDL) error

		Session() SessionActions

		ExecuteLock(rs *srvtopo.ResolvedShard, query *querypb.BoundQuery) (*sqltypes.Result, error)

		InTransactionAndIsDML() bool

		LookupRowLockShardSession() vtgatepb.CommitOrder

		FindRoutedTable(tablename sqlparser.TableName) (*vindexes.Table, error)

		// GetDBDDLPlugin gets the configured plugin for DROP/CREATE DATABASE
		GetDBDDLPluginName() string

		// KeyspaceAvailable returns true when a keyspace is visible from vtgate
		KeyspaceAvailable(ks string) bool
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

		SetAutocommit(bool) error
		SetClientFoundRows(bool) error
		SetSkipQueryPlanCache(bool) error
		SetSQLSelectLimit(int64) error
		SetTransactionMode(vtgatepb.TransactionMode)
		SetWorkload(querypb.ExecuteOptions_Workload)
		SetPlannerVersion(querypb.ExecuteOptions_PlannerVersion)
		SetFoundRows(uint64)

		SetDDLStrategy(string)
		GetDDLStrategy() string

		GetSessionUUID() string

		SetSessionEnableSystemSettings(bool) error
		GetSessionEnableSystemSettings() bool

		// SetReadAfterWriteGTID sets the GTID that the user expects a replica to have caught up with before answering a query
		SetReadAfterWriteGTID(string)
		SetReadAfterWriteTimeout(float64)
		SetSessionTrackGTIDs(bool)

		// HasCreatedTempTable will mark the session as having created temp tables
		HasCreatedTempTable()
	}

	// Plan represents the execution strategy for a given query.
	// For now it's a simple wrapper around the real instructions.
	// An instruction (aka Primitive) is typically a tree where
	// each node does its part by combining the results of the
	// sub-nodes.
	Plan struct {
		Type         sqlparser.StatementType // The type of query we have
		Original     string                  // Original is the original query.
		Instructions Primitive               // Instructions contains the instructions needed to fulfil the query.
		BindVarNeeds *sqlparser.BindVarNeeds // Stores BindVars needed to be provided as part of expression rewriting
		Warnings     []*querypb.QueryWarning // Warnings that need to be yielded every time this query runs

		ExecCount    uint64 // Count of times this plan was executed
		ExecTime     uint64 // Total execution time
		ShardQueries uint64 // Total number of shard queries
		RowsReturned uint64 // Total number of rows
		RowsAffected uint64 // Total number of rows
		Errors       uint64 // Total number of errors
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
func (p *Plan) AddStats(execCount uint64, execTime time.Duration, shardQueries, rowsAffected, rowsReturned, errors uint64) {
	atomic.AddUint64(&p.ExecCount, execCount)
	atomic.AddUint64(&p.ExecTime, uint64(execTime))
	atomic.AddUint64(&p.ShardQueries, shardQueries)
	atomic.AddUint64(&p.RowsAffected, rowsAffected)
	atomic.AddUint64(&p.RowsReturned, rowsReturned)
	atomic.AddUint64(&p.Errors, errors)
}

// Stats returns a copy of the plan execution statistics
func (p *Plan) Stats() (execCount uint64, execTime time.Duration, shardQueries, rowsAffected, rowsReturned, errors uint64) {
	execCount = atomic.LoadUint64(&p.ExecCount)
	execTime = time.Duration(atomic.LoadUint64(&p.ExecTime))
	shardQueries = atomic.LoadUint64(&p.ShardQueries)
	rowsAffected = atomic.LoadUint64(&p.RowsAffected)
	rowsReturned = atomic.LoadUint64(&p.RowsReturned)
	errors = atomic.LoadUint64(&p.Errors)
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
		RowsAffected uint64                `json:",omitempty"`
		RowsReturned uint64                `json:",omitempty"`
		Errors       uint64                `json:",omitempty"`
	}{
		QueryType:    p.Type.String(),
		Original:     p.Original,
		Instructions: instructions,
		ExecCount:    atomic.LoadUint64(&p.ExecCount),
		ExecTime:     time.Duration(atomic.LoadUint64(&p.ExecTime)),
		ShardQueries: atomic.LoadUint64(&p.ShardQueries),
		RowsAffected: atomic.LoadUint64(&p.RowsAffected),
		RowsReturned: atomic.LoadUint64(&p.RowsReturned),
		Errors:       atomic.LoadUint64(&p.Errors),
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
