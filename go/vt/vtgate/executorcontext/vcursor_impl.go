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

package executorcontext

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/sysvars"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
)

var (
	_ engine.VCursor      = (*VCursorImpl)(nil)
	_ plancontext.VSchema = (*VCursorImpl)(nil)
	_ vindexes.VCursor    = (*VCursorImpl)(nil)
)

var ErrNoKeyspace = vterrors.VT09005()

type (
	ResultsObserver interface {
		Observe(*sqltypes.Result)
	}

	VCursorConfig struct {
		Collation collations.ID

		MaxMemoryRows      int
		EnableShardRouting bool
		DefaultTabletType  topodatapb.TabletType
		QueryTimeout       int
		DBDDLPlugin        string
		ForeignKeyMode     vschemapb.Keyspace_ForeignKeyMode
		SetVarEnabled      bool
		EnableViews        bool
		WarnShardedOnly    bool
		PlannerVersion     plancontext.PlannerVersion

		WarmingReadsPercent int
		WarmingReadsTimeout time.Duration
		WarmingReadsChannel chan bool
	}

	// vcursor_impl needs these facilities to be able to be able to execute queries for vindexes
	iExecute interface {
		Execute(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, method string, session *SafeSession, s string, vars map[string]*querypb.BindVariable, prepared bool) (*sqltypes.Result, error)
		ExecuteMultiShard(ctx context.Context, primitive engine.Primitive, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, session *SafeSession, autocommit bool, ignoreMaxMemoryRows bool, resultsObserver ResultsObserver, fetchLastInsertID bool) (qr *sqltypes.Result, errs []error)
		StreamExecuteMulti(ctx context.Context, primitive engine.Primitive, query string, rss []*srvtopo.ResolvedShard, vars []map[string]*querypb.BindVariable, session *SafeSession, autocommit bool, callback func(reply *sqltypes.Result) error, observer ResultsObserver, fetchLastInsertID bool) []error
		ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, session *SafeSession, lockFuncType sqlparser.LockingFuncType) (*sqltypes.Result, error)
		Commit(ctx context.Context, safeSession *SafeSession) error
		ExecuteMessageStream(ctx context.Context, rss []*srvtopo.ResolvedShard, name string, callback func(*sqltypes.Result) error) error
		ExecuteVStream(ctx context.Context, rss []*srvtopo.ResolvedShard, filter *binlogdatapb.Filter, gtid string, callback func(evs []*binlogdatapb.VEvent) error) error
		ReleaseLock(ctx context.Context, session *SafeSession) error

		ShowVitessReplicationStatus(ctx context.Context, filter *sqlparser.ShowFilter) (*sqltypes.Result, error)
		ShowShards(ctx context.Context, filter *sqlparser.ShowFilter, destTabletType topodatapb.TabletType) (*sqltypes.Result, error)
		ShowTablets(filter *sqlparser.ShowFilter) (*sqltypes.Result, error)
		ShowVitessMetadata(ctx context.Context, filter *sqlparser.ShowFilter) (*sqltypes.Result, error)
		SetVitessMetadata(ctx context.Context, name, value string) error

		// TODO: remove when resolver is gone
		VSchema() *vindexes.VSchema
		PlanPrepareStmt(ctx context.Context, safeSession *SafeSession, query string) (*engine.Plan, error)

		Environment() *vtenv.Environment
		ReadTransaction(ctx context.Context, transactionID string) (*querypb.TransactionMetadata, error)
		UnresolvedTransactions(ctx context.Context, targets []*querypb.Target) ([]*querypb.TransactionMetadata, error)
		AddWarningCount(name string, value int64)
	}

	// VSchemaOperator is an interface to Vschema Operations
	VSchemaOperator interface {
		GetCurrentSrvVschema() *vschemapb.SrvVSchema
		UpdateVSchema(ctx context.Context, ks *topo.KeyspaceVSchemaInfo, vschema *vschemapb.SrvVSchema) error
	}

	Resolver interface {
		GetGateway() srvtopo.Gateway
		ResolveDestinations(
			ctx context.Context,
			keyspace string,
			tabletType topodatapb.TabletType,
			ids []*querypb.Value,
			destinations []key.ShardDestination,
		) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error)
		ResolveDestinationsMultiCol(
			ctx context.Context,
			keyspace string,
			tabletType topodatapb.TabletType,
			ids [][]sqltypes.Value,
			destinations []key.ShardDestination,
		) ([]*srvtopo.ResolvedShard, [][][]sqltypes.Value, error)
	}

	Metrics interface {
		GetExecutionMetrics() *engine.Metrics
	}

	// VCursorImpl implements the VCursor functionality used by dependent
	// packages to call back into VTGate.
	VCursorImpl struct {
		config         VCursorConfig
		SafeSession    *SafeSession
		keyspace       string
		tabletType     topodatapb.TabletType
		destination    key.ShardDestination
		marginComments sqlparser.MarginComments
		executor       iExecute
		resolver       Resolver
		topoServer     *topo.Server
		logStats       *logstats.LogStats
		metrics        Metrics

		// fkChecksState stores the state of foreign key checks variable.
		// This state is meant to be the final fk checks state after consulting the
		// session state, and the given query's comments for `SET_VAR` optimizer hints.
		// A nil value represents that no foreign_key_checks value was provided.
		fkChecksState       *bool
		ignoreMaxMemoryRows bool
		vschema             *vindexes.VSchema
		vm                  VSchemaOperator
		semTable            *semantics.SemTable
		queryTimeout        time.Duration
		transactionTimeout  time.Duration

		warnings []*querypb.QueryWarning // any warnings that are accumulated during the planning phase are stored here

		observer ResultsObserver

		// this protects the interOpStats and shardsStats fields from concurrent writes
		mu sync.Mutex
		// this is a map of the number of rows that every primitive has returned
		// if this field is nil, it means that we are not logging operator traffic
		interOpStats map[engine.Primitive]engine.RowsReceived
		shardsStats  map[engine.Primitive]engine.ShardsQueried

		// For specializing plans for the current query
		bindVars map[string]*querypb.BindVariable
	}
)

// NewVCursorImpl creates a VCursorImpl. Before creating this object, you have to separate out any marginComments that came with
// the query and supply it here. Trailing comments are typically sent by the application for various reasons,
// including as identifying markers. So, they have to be added back to all queries that are executed
// on behalf of the original query.
func NewVCursorImpl(
	safeSession *SafeSession,
	marginComments sqlparser.MarginComments,
	executor iExecute,
	logStats *logstats.LogStats,
	vm VSchemaOperator,
	vschema *vindexes.VSchema,
	resolver Resolver,
	serv srvtopo.Server,
	observer ResultsObserver,
	cfg VCursorConfig,
	metrics Metrics,
) (*VCursorImpl, error) {
	keyspace, tabletType, destination, tabletAlias, err := ParseDestinationTarget(safeSession.TargetString, cfg.DefaultTabletType, vschema)
	if err != nil {
		return nil, err
	}

	// Store tablet alias from target string into session
	// This ensures tablet-specific routing persists across queries
	safeSession.SetTargetTabletAlias(tabletAlias)

	var ts *topo.Server
	// We don't have access to the underlying TopoServer if this vtgate is
	// filtering keyspaces because we don't have an accurate view of the topo.
	if serv != nil && !discovery.FilteringKeyspaces() {
		ts, err = serv.GetTopoServer()
		if err != nil {
			return nil, err
		}
	}

	return &VCursorImpl{
		config:         cfg,
		SafeSession:    safeSession,
		keyspace:       keyspace,
		tabletType:     tabletType,
		destination:    destination,
		marginComments: marginComments,
		executor:       executor,
		logStats:       logStats,
		metrics:        metrics,

		resolver:   resolver,
		vschema:    vschema,
		vm:         vm,
		topoServer: ts,
		observer:   observer,
	}, nil
}

func (vc *VCursorImpl) GetSafeSession() *SafeSession {
	return vc.SafeSession
}

func (vc *VCursorImpl) PrepareSetVarComment() string {
	var res []string
	vc.Session().GetSystemVariables(func(k, v string) {
		if sysvars.SupportsSetVar(k) {
			if k == "sql_mode" && v == "''" {
				// SET_VAR(sql_mode, '') is not accepted by MySQL, giving a warning:
				// | Warning | 1064 | Optimizer hint syntax error near ''') */
				v = "' '"
			}
			res = append(res, fmt.Sprintf("SET_VAR(%s = %s)", k, v))
		}
	})

	return strings.Join(res, " ")
}

// GetExecutionMetrics provides the execution metrics object.
func (vc *VCursorImpl) GetExecutionMetrics() *engine.Metrics {
	return vc.metrics.GetExecutionMetrics()
}

// ConnCollation returns the collation of this session
func (vc *VCursorImpl) ConnCollation() collations.ID {
	return vc.config.Collation
}

// Environment returns the vtenv associated with this session
func (vc *VCursorImpl) Environment() *vtenv.Environment {
	return vc.executor.Environment()
}

func (vc *VCursorImpl) TimeZone() *time.Location {
	return vc.SafeSession.TimeZone()
}

func (vc *VCursorImpl) SQLMode() string {
	// TODO: Implement return the current sql_mode.
	// This is currently hardcoded to the default in MySQL 8.0.
	return config.DefaultSQLMode
}

// MaxMemoryRows returns the maxMemoryRows flag value.
func (vc *VCursorImpl) MaxMemoryRows() int {
	return vc.config.MaxMemoryRows
}

// ExceedsMaxMemoryRows returns a boolean indicating whether the maxMemoryRows value has been exceeded.
// Returns false if the max memory rows override directive is set to true.
func (vc *VCursorImpl) ExceedsMaxMemoryRows(numRows int) bool {
	return !vc.ignoreMaxMemoryRows && numRows > vc.config.MaxMemoryRows
}

// SetIgnoreMaxMemoryRows sets the ignoreMaxMemoryRows value.
func (vc *VCursorImpl) SetIgnoreMaxMemoryRows(ignoreMaxMemoryRows bool) {
	vc.ignoreMaxMemoryRows = ignoreMaxMemoryRows
}

func (vc *VCursorImpl) IgnoreMaxMemoryRows() bool {
	return vc.ignoreMaxMemoryRows
}

func (vc *VCursorImpl) GetKeyspace() string {
	return vc.keyspace
}

func (vc *VCursorImpl) GetMarginComments() sqlparser.MarginComments {
	return vc.marginComments
}

// GetDBDDLPluginName implements the VCursor interface
func (vc *VCursorImpl) GetDBDDLPluginName() string {
	return vc.config.DBDDLPlugin
}

func (vc *VCursorImpl) CanUseSetVar() bool {
	return vc.Environment().Parser().IsMySQL80AndAbove() && vc.config.SetVarEnabled
}

func (vc *VCursorImpl) GetWarmingReadsPercent() int {
	return vc.config.WarmingReadsPercent
}

func (vc *VCursorImpl) GetWarmingReadsChannel() chan bool {
	return vc.config.WarmingReadsChannel
}

// RecordMirrorStats is used to record stats about a mirror query.
func (vc *VCursorImpl) RecordMirrorStats(sourceExecTime, targetExecTime time.Duration, targetErr error) {
	vc.logStats.MirrorSourceExecuteTime = sourceExecTime
	vc.logStats.MirrorTargetExecuteTime = targetExecTime
	vc.logStats.MirrorTargetError = targetErr
}
