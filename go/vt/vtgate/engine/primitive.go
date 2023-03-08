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
	"context"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
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
		GetKeyspace() string
		// MaxMemoryRows returns the maxMemoryRows flag value.
		MaxMemoryRows() int

		// ExceedsMaxMemoryRows returns a boolean indicating whether
		// the maxMemoryRows value has been exceeded. Returns false
		// if the max memory rows override directive is set to true
		ExceedsMaxMemoryRows(numRows int) bool

		// V3 functions.
		Execute(ctx context.Context, method string, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error)
		AutocommitApproval() bool

		// Execute the given primitive
		ExecutePrimitive(ctx context.Context, primitive Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error)
		// Execute the given primitive in a new autocommit session
		ExecutePrimitiveStandalone(ctx context.Context, primitive Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error)

		// Execute the given primitive
		StreamExecutePrimitive(ctx context.Context, primitive Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error
		// Execute the given primitive in a new autocommit session
		StreamExecutePrimitiveStandalone(ctx context.Context, primitive Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(result *sqltypes.Result) error) error

		// Shard-level functions.
		ExecuteMultiShard(ctx context.Context, primitive Primitive, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, canAutocommit bool) (*sqltypes.Result, []error)
		ExecuteStandalone(ctx context.Context, primitive Primitive, query string, bindVars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard) (*sqltypes.Result, error)
		StreamExecuteMulti(ctx context.Context, primitive Primitive, query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, rollbackOnError bool, autocommit bool, callback func(reply *sqltypes.Result) error) []error

		// Keyspace ID level functions.
		ExecuteKeyspaceID(ctx context.Context, keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError, autocommit bool) (*sqltypes.Result, error)

		// Resolver methods, from key.Destination to srvtopo.ResolvedShard.
		// Will replace all of the Topo functions.
		ResolveDestinations(ctx context.Context, keyspace string, ids []*querypb.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error)

		ResolveDestinationsMultiCol(ctx context.Context, keyspace string, ids [][]sqltypes.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][][]sqltypes.Value, error)

		ExecuteVSchema(ctx context.Context, keyspace string, vschemaDDL *sqlparser.AlterVschema) error

		Session() SessionActions

		ConnCollation() collations.ID

		ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, lockFuncType sqlparser.LockingFuncType) (*sqltypes.Result, error)

		InTransactionAndIsDML() bool

		LookupRowLockShardSession() vtgatepb.CommitOrder

		FindRoutedTable(tablename sqlparser.TableName) (*vindexes.Table, error)

		// GetDBDDLPlugin gets the configured plugin for DROP/CREATE DATABASE
		GetDBDDLPluginName() string

		// KeyspaceAvailable returns true when a keyspace is visible from vtgate
		KeyspaceAvailable(ks string) bool

		MessageStream(ctx context.Context, rss []*srvtopo.ResolvedShard, tableName string, callback func(*sqltypes.Result) error) error

		VStream(ctx context.Context, rss []*srvtopo.ResolvedShard, filter *binlogdatapb.Filter, gtid string, callback func(evs []*binlogdatapb.VEvent) error) error

		// ShowExec takes in show command and use executor to execute the query, they are used when topo access is involved.
		ShowExec(ctx context.Context, command sqlparser.ShowCommandType, filter *sqlparser.ShowFilter) (*sqltypes.Result, error)
		// SetExec takes in k,v pair and use executor to set them in topo metadata.
		SetExec(ctx context.Context, name string, value string) error

		// CanUseSetVar returns true if system_settings can use SET_VAR hint.
		CanUseSetVar() bool

		// ReleaseLock releases all the held advisory locks.
		ReleaseLock(ctx context.Context) error
	}

	// SessionActions gives primitives ability to interact with the session state
	SessionActions interface {
		// RecordWarning stores the given warning in the current session
		RecordWarning(warning *querypb.QueryWarning)

		SetTarget(target string) error

		SetUDV(key string, value any) error

		SetSysVar(name string, expr string)

		// NeedsReservedConn marks this session as needing a dedicated connection to underlying database
		NeedsReservedConn()

		// InReservedConn provides whether this session is using reserved connection
		InReservedConn() bool

		// ShardSession returns shard info about open connections
		ShardSession() []*srvtopo.ResolvedShard

		SetAutocommit(ctx context.Context, autocommit bool) error
		SetClientFoundRows(context.Context, bool) error
		SetSkipQueryPlanCache(context.Context, bool) error
		SetSQLSelectLimit(int64) error
		SetTransactionMode(vtgatepb.TransactionMode)
		SetWorkload(querypb.ExecuteOptions_Workload)
		SetPlannerVersion(querypb.ExecuteOptions_PlannerVersion)
		SetConsolidator(querypb.ExecuteOptions_Consolidator)
		SetWorkloadName(string)
		SetFoundRows(uint64)

		SetDDLStrategy(string)
		GetDDLStrategy() string

		GetSessionUUID() string

		SetSessionEnableSystemSettings(context.Context, bool) error
		GetSessionEnableSystemSettings() bool

		GetSystemVariables(func(k string, v string))
		HasSystemVariables() bool

		// SetReadAfterWriteGTID sets the GTID that the user expects a replica to have caught up with before answering a query
		SetReadAfterWriteGTID(string)
		SetReadAfterWriteTimeout(float64)
		SetSessionTrackGTIDs(bool)

		// HasCreatedTempTable will mark the session as having created temp tables
		HasCreatedTempTable()
		GetWarnings() []*querypb.QueryWarning

		// AnyAdvisoryLockTaken returns true of any advisory lock is taken
		AnyAdvisoryLockTaken() bool
		// AddAdvisoryLock adds advisory lock to the session
		AddAdvisoryLock(name string)
		// RemoveAdvisoryLock removes advisory lock from the session
		RemoveAdvisoryLock(name string)

		// VExplainLogging enables logging of all interactions to the tablets so
		// VEXPLAIN QUERIES/ALL can report what's being done
		VExplainLogging()

		// GetVExplainLogs retrieves the vttablet interaction logs
		GetVExplainLogs() []ExecuteEntry

		// SetCommitOrder sets the commit order for the shard session in respect of the type of vindex lookup.
		// This is used to select the right shard session to perform the vindex lookup query.
		SetCommitOrder(co vtgatepb.CommitOrder)

		// GetQueryTimeout gets the query timeout and takes in the query timeout from comments
		GetQueryTimeout(queryTimeoutFromComment int) int

		// SetQueryTimeout sets the query timeout
		SetQueryTimeout(queryTimeout int64)

		// InTransaction returns true if the session has already opened transaction or
		// will start a transaction on the query execution.
		InTransaction() bool
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
		GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error)
		NeedsTransaction() bool

		TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error)
		TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error

		// Inputs is a slice containing the inputs to this Primitive
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

	// Gen4Comparer interfaces all Primitive used to compare Gen4 with other planners (V3, MySQL, ...).
	Gen4Comparer interface {
		Primitive
		GetGen4Primitive() Primitive
	}
)

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
