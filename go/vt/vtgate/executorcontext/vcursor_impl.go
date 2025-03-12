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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/vt/sysvars"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	topoprotopb "vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/buffer"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/logstats"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"
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

		warnings []*querypb.QueryWarning // any warnings that are accumulated during the planning phase are stored here

		observer ResultsObserver

		// this protects the interOpStats and shardsStats fields from concurrent writes
		mu sync.Mutex
		// this is a map of the number of rows that every primitive has returned
		// if this field is nil, it means that we are not logging operator traffic
		interOpStats map[engine.Primitive]engine.RowsReceived
		shardsStats  map[engine.Primitive]engine.ShardsQueried
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
) (*VCursorImpl, error) {
	keyspace, tabletType, destination, err := ParseDestinationTarget(safeSession.TargetString, cfg.DefaultTabletType, vschema)
	if err != nil {
		return nil, err
	}

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
		resolver:       resolver,
		vschema:        vschema,
		vm:             vm,
		topoServer:     ts,

		observer: observer,
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

func (vc *VCursorImpl) CloneForMirroring(ctx context.Context) engine.VCursor {
	callerId := callerid.EffectiveCallerIDFromContext(ctx)
	immediateCallerId := callerid.ImmediateCallerIDFromContext(ctx)

	clonedCtx := callerid.NewContext(ctx, callerId, immediateCallerId)

	v := &VCursorImpl{
		config:              vc.config,
		SafeSession:         NewAutocommitSession(vc.SafeSession.Session),
		keyspace:            vc.keyspace,
		tabletType:          vc.tabletType,
		destination:         vc.destination,
		marginComments:      vc.marginComments,
		executor:            vc.executor,
		resolver:            vc.resolver,
		topoServer:          vc.topoServer,
		logStats:            &logstats.LogStats{Ctx: clonedCtx},
		ignoreMaxMemoryRows: vc.ignoreMaxMemoryRows,
		vschema:             vc.vschema,
		vm:                  vc.vm,
		semTable:            vc.semTable,
		warnings:            vc.warnings,
		observer:            vc.observer,
	}

	v.marginComments.Trailing += "/* mirror query */"

	return v
}

func (vc *VCursorImpl) CloneForReplicaWarming(ctx context.Context) engine.VCursor {
	callerId := callerid.EffectiveCallerIDFromContext(ctx)
	immediateCallerId := callerid.ImmediateCallerIDFromContext(ctx)

	timedCtx, _ := context.WithTimeout(context.Background(), vc.config.WarmingReadsTimeout) // nolint
	clonedCtx := callerid.NewContext(timedCtx, callerId, immediateCallerId)

	v := &VCursorImpl{
		config:         vc.config,
		SafeSession:    NewAutocommitSession(vc.SafeSession.Session),
		keyspace:       vc.keyspace,
		tabletType:     topodatapb.TabletType_REPLICA,
		destination:    vc.destination,
		marginComments: vc.marginComments,
		executor:       vc.executor,
		resolver:       vc.resolver,
		topoServer:     vc.topoServer,
		logStats:       &logstats.LogStats{Ctx: clonedCtx},

		ignoreMaxMemoryRows: vc.ignoreMaxMemoryRows,
		vschema:             vc.vschema,
		vm:                  vc.vm,
		semTable:            vc.semTable,
		warnings:            vc.warnings,
		observer:            vc.observer,
	}

	v.marginComments.Trailing += "/* warming read */"

	return v
}

func (vc *VCursorImpl) cloneWithAutocommitSession() *VCursorImpl {
	safeSession := vc.SafeSession.NewAutocommitSession()
	return &VCursorImpl{
		config:         vc.config,
		SafeSession:    safeSession,
		keyspace:       vc.keyspace,
		tabletType:     vc.tabletType,
		destination:    vc.destination,
		marginComments: vc.marginComments,
		executor:       vc.executor,
		logStats:       vc.logStats,
		resolver:       vc.resolver,
		vschema:        vc.vschema,
		vm:             vc.vm,
		topoServer:     vc.topoServer,
		observer:       vc.observer,
	}
}

// HasSystemVariables returns whether the session has set system variables or not
func (vc *VCursorImpl) HasSystemVariables() bool {
	return vc.SafeSession.HasSystemVariables()
}

// GetSystemVariables takes a visitor function that will save each system variables of the session
func (vc *VCursorImpl) GetSystemVariables(f func(k string, v string)) {
	vc.SafeSession.GetSystemVariables(f)
}

// GetSystemVariablesCopy returns a copy of the system variables of the session. Changes to the original map will not affect the session.
func (vc *VCursorImpl) GetSystemVariablesCopy() map[string]string {
	vc.SafeSession.mu.Lock()
	defer vc.SafeSession.mu.Unlock()
	return maps.Clone(vc.SafeSession.SystemVariables)
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

// RecordWarning stores the given warning in the current session
func (vc *VCursorImpl) RecordWarning(warning *querypb.QueryWarning) {
	vc.SafeSession.RecordWarning(warning)
}

// IsShardRoutingEnabled implements the VCursor interface.
func (vc *VCursorImpl) IsShardRoutingEnabled() bool {
	return vc.config.EnableShardRouting
}

func (vc *VCursorImpl) ReadTransaction(ctx context.Context, transactionID string) (*querypb.TransactionMetadata, error) {
	return vc.executor.ReadTransaction(ctx, transactionID)
}

// UnresolvedTransactions gets the unresolved transactions for the given keyspace. If the keyspace is not given,
// then we use the default keyspace.
func (vc *VCursorImpl) UnresolvedTransactions(ctx context.Context, keyspace string) ([]*querypb.TransactionMetadata, error) {
	if keyspace == "" {
		keyspace = vc.GetKeyspace()
	}
	rss, _, err := vc.ResolveDestinations(ctx, keyspace, nil, []key.ShardDestination{key.DestinationAllShards{}})
	if err != nil {
		return nil, err
	}
	var targets []*querypb.Target
	for _, rs := range rss {
		targets = append(targets, rs.Target)
	}
	return vc.executor.UnresolvedTransactions(ctx, targets)
}

func (vc *VCursorImpl) StartPrimitiveTrace() func() engine.Stats {
	vc.interOpStats = make(map[engine.Primitive]engine.RowsReceived)
	vc.shardsStats = make(map[engine.Primitive]engine.ShardsQueried)
	return func() engine.Stats {
		return engine.Stats{
			InterOpStats: vc.interOpStats,
			ShardsStats:  vc.shardsStats,
		}
	}
}

// FindTable finds the specified table. If the keyspace what specified in the input, it gets used as qualifier.
// Otherwise, the keyspace from the request is used, if one was provided.
func (vc *VCursorImpl) FindTable(name sqlparser.TableName) (*vindexes.BaseTable, string, topodatapb.TabletType, key.ShardDestination, error) {
	destKeyspace, destTabletType, dest, err := vc.parseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, "", destTabletType, nil, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.keyspace
	}
	table, err := vc.vschema.FindTable(destKeyspace, name.Name.String())
	if err != nil {
		return nil, "", destTabletType, nil, err
	}
	return table, destKeyspace, destTabletType, dest, err
}

func (vc *VCursorImpl) FindView(name sqlparser.TableName) sqlparser.TableStatement {
	ks, _, _, err := vc.parseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil
	}
	if ks == "" {
		ks = vc.keyspace
	}
	return vc.vschema.FindView(ks, name.Name.String())
}

func (vc *VCursorImpl) FindRoutedTable(name sqlparser.TableName) (*vindexes.BaseTable, error) {
	destKeyspace, destTabletType, _, err := vc.parseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.keyspace
	}

	table, err := vc.vschema.FindRoutedTable(destKeyspace, name.Name.String(), destTabletType)
	if err != nil {
		return nil, err
	}

	return table, nil
}

// FindTableOrVindex finds the specified table or vindex.
func (vc *VCursorImpl) FindTableOrVindex(name sqlparser.TableName) (*vindexes.BaseTable, vindexes.Vindex, string, topodatapb.TabletType, key.ShardDestination, error) {
	if name.Qualifier.IsEmpty() && name.Name.String() == "dual" {
		// The magical MySQL dual table should only be resolved
		// when it is not qualified by a database name.
		return vc.getDualTable()
	}

	destKeyspace, destTabletType, dest, err := vc.parseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, nil, "", destTabletType, nil, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.getActualKeyspace()
	}
	table, vindex, err := vc.vschema.FindTableOrVindex(destKeyspace, name.Name.String(), vc.tabletType)
	if err != nil {
		return nil, nil, "", destTabletType, nil, err
	}
	return table, vindex, destKeyspace, destTabletType, dest, nil
}

// FindViewTarget finds the specified view's target keyspace.
func (vc *VCursorImpl) FindViewTarget(name sqlparser.TableName) (*vindexes.Keyspace, error) {
	destKeyspace, _, _, err := vc.parseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, err
	}
	if destKeyspace != "" {
		return vc.FindKeyspace(destKeyspace)
	}

	tbl, err := vc.vschema.FindRoutedTable("", name.Name.String(), vc.tabletType)
	if err != nil || tbl == nil {
		return nil, err
	}
	return tbl.Keyspace, nil
}

func (vc *VCursorImpl) parseDestinationTarget(targetString string) (string, topodatapb.TabletType, key.ShardDestination, error) {
	return ParseDestinationTarget(targetString, vc.tabletType, vc.vschema)
}

// ParseDestinationTarget parses destination target string and provides a keyspace if possible.
func ParseDestinationTarget(targetString string, tablet topodatapb.TabletType, vschema *vindexes.VSchema) (string, topodatapb.TabletType, key.ShardDestination, error) {
	destKeyspace, destTabletType, dest, err := topoprotopb.ParseDestination(targetString, tablet)
	// If the keyspace is not specified, and there is only one keyspace in the VSchema, use that.
	if destKeyspace == "" && len(vschema.Keyspaces) == 1 {
		for k := range vschema.Keyspaces {
			destKeyspace = k
		}
	}
	return destKeyspace, destTabletType, dest, err
}

func (vc *VCursorImpl) getDualTable() (*vindexes.BaseTable, vindexes.Vindex, string, topodatapb.TabletType, key.ShardDestination, error) {
	ksName := vc.getActualKeyspace()
	var ks *vindexes.Keyspace
	if ksName == "" {
		ks = vc.vschema.FirstKeyspace()
		ksName = ks.Name
	} else {
		ks = vc.vschema.Keyspaces[ksName].Keyspace
	}
	tbl := &vindexes.BaseTable{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: ks,
		Type:     vindexes.TypeReference,
	}
	return tbl, nil, ksName, topodatapb.TabletType_PRIMARY, nil, nil
}

func (vc *VCursorImpl) getActualKeyspace() string {
	if !sqlparser.SystemSchema(vc.keyspace) {
		return vc.keyspace
	}
	ks, err := vc.AnyKeyspace()
	if err != nil {
		return ""
	}
	return ks.Name
}

// SelectedKeyspace returns the selected keyspace of the current request
// if there is one. If the keyspace specified in the target cannot be
// identified, it returns an error.
func (vc *VCursorImpl) SelectedKeyspace() (*vindexes.Keyspace, error) {
	if ignoreKeyspace(vc.keyspace) {
		return nil, ErrNoKeyspace
	}
	ks, ok := vc.vschema.Keyspaces[vc.keyspace]
	if !ok {
		return nil, vterrors.VT05003(vc.keyspace)
	}
	return ks.Keyspace, nil
}

var errNoDbAvailable = vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.NoDB, "no database available")

func (vc *VCursorImpl) AnyKeyspace() (*vindexes.Keyspace, error) {
	keyspace, err := vc.SelectedKeyspace()
	if err == nil {
		return keyspace, nil
	}
	if err != ErrNoKeyspace {
		return nil, err
	}

	if len(vc.vschema.Keyspaces) == 0 {
		return nil, errNoDbAvailable
	}

	keyspaces := vc.getSortedServingKeyspaces()

	// Look for any sharded keyspace if present, otherwise take the first keyspace,
	// sorted alphabetically
	for _, ks := range keyspaces {
		if ks.Sharded {
			return ks, nil
		}
	}
	return keyspaces[0], nil
}

// getSortedServingKeyspaces gets the sorted serving keyspaces
func (vc *VCursorImpl) getSortedServingKeyspaces() []*vindexes.Keyspace {
	var keyspaces []*vindexes.Keyspace

	if vc.resolver != nil && vc.resolver.GetGateway() != nil {
		keyspaceNames := vc.resolver.GetGateway().GetServingKeyspaces()
		for _, ksName := range keyspaceNames {
			ks, exists := vc.vschema.Keyspaces[ksName]
			if exists {
				keyspaces = append(keyspaces, ks.Keyspace)
			}
		}
	}

	if len(keyspaces) == 0 {
		for _, ks := range vc.vschema.Keyspaces {
			keyspaces = append(keyspaces, ks.Keyspace)
		}
	}
	sort.Slice(keyspaces, func(i, j int) bool {
		return keyspaces[i].Name < keyspaces[j].Name
	})
	return keyspaces
}

func (vc *VCursorImpl) FirstSortedKeyspace() (*vindexes.Keyspace, error) {
	if len(vc.vschema.Keyspaces) == 0 {
		return nil, errNoDbAvailable
	}
	keyspaces := vc.getSortedServingKeyspaces()

	return keyspaces[0], nil
}

// SysVarSetEnabled implements the ContextVSchema interface
func (vc *VCursorImpl) SysVarSetEnabled() bool {
	return vc.GetSessionEnableSystemSettings()
}

// KeyspaceExists provides whether the keyspace exists or not.
func (vc *VCursorImpl) KeyspaceExists(ks string) bool {
	return vc.vschema.Keyspaces[ks] != nil
}

// AllKeyspace implements the ContextVSchema interface
func (vc *VCursorImpl) AllKeyspace() ([]*vindexes.Keyspace, error) {
	if len(vc.vschema.Keyspaces) == 0 {
		return nil, errNoDbAvailable
	}
	var kss []*vindexes.Keyspace
	for _, ks := range vc.vschema.Keyspaces {
		kss = append(kss, ks.Keyspace)
	}
	return kss, nil
}

// FindKeyspace implements the VSchema interface
func (vc *VCursorImpl) FindKeyspace(keyspace string) (*vindexes.Keyspace, error) {
	if len(vc.vschema.Keyspaces) == 0 {
		return nil, errNoDbAvailable
	}
	for _, ks := range vc.vschema.Keyspaces {
		if ks.Keyspace.Name == keyspace {
			return ks.Keyspace, nil
		}
	}
	return nil, nil
}

// Planner implements the ContextVSchema interface
func (vc *VCursorImpl) Planner() plancontext.PlannerVersion {
	if vc.SafeSession.Options != nil &&
		vc.SafeSession.Options.PlannerVersion != querypb.ExecuteOptions_DEFAULT_PLANNER {
		return vc.SafeSession.Options.PlannerVersion
	}
	return vc.config.PlannerVersion
}

// GetSemTable implements the ContextVSchema interface
func (vc *VCursorImpl) GetSemTable() *semantics.SemTable {
	return vc.semTable
}

// TargetString returns the current TargetString of the session.
func (vc *VCursorImpl) TargetString() string {
	return vc.SafeSession.TargetString
}

// MaxBufferingRetries is to represent max retries on buffering.
const MaxBufferingRetries = 3

func (vc *VCursorImpl) ExecutePrimitive(ctx context.Context, primitive engine.Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	for try := 0; try < MaxBufferingRetries; try++ {
		res, err := primitive.TryExecute(ctx, vc, bindVars, wantfields)
		if err != nil && vterrors.RootCause(err) == buffer.ShardMissingError {
			continue
		}
		vc.logOpTraffic(primitive, res)
		return res, err
	}
	return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "upstream shards are not available")
}

func (vc *VCursorImpl) logOpTraffic(primitive engine.Primitive, res *sqltypes.Result) {
	if vc.interOpStats == nil {
		return
	}

	vc.mu.Lock()
	defer vc.mu.Unlock()

	rows := vc.interOpStats[primitive]
	if res == nil {
		rows = append(rows, 0)
	} else {
		rows = append(rows, len(res.Rows))
	}
	vc.interOpStats[primitive] = rows
}

func (vc *VCursorImpl) logShardsQueried(primitive engine.Primitive, shardsNb int) {
	if vc.shardsStats == nil {
		return
	}
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.shardsStats[primitive] += engine.ShardsQueried(shardsNb)
}

func (vc *VCursorImpl) ExecutePrimitiveStandalone(ctx context.Context, primitive engine.Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	// clone the VCursorImpl with a new session.
	newVC := vc.cloneWithAutocommitSession()
	for try := 0; try < MaxBufferingRetries; try++ {
		res, err := primitive.TryExecute(ctx, newVC, bindVars, wantfields)
		if err != nil && vterrors.RootCause(err) == buffer.ShardMissingError {
			continue
		}
		vc.logOpTraffic(primitive, res)
		return res, err
	}
	return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "upstream shards are not available")
}

func (vc *VCursorImpl) wrapCallback(callback func(*sqltypes.Result) error, primitive engine.Primitive) func(*sqltypes.Result) error {
	if vc.interOpStats == nil {
		return func(r *sqltypes.Result) error {
			if r.InsertIDUpdated() {
				vc.SafeSession.LastInsertId = r.InsertID
			}
			return callback(r)
		}
	}

	return func(r *sqltypes.Result) error {
		if r.InsertIDUpdated() {
			vc.SafeSession.LastInsertId = r.InsertID
		}
		vc.logOpTraffic(primitive, r)
		return callback(r)
	}
}

func (vc *VCursorImpl) StreamExecutePrimitive(ctx context.Context, primitive engine.Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	callback = vc.wrapCallback(callback, primitive)

	for try := 0; try < MaxBufferingRetries; try++ {
		err := primitive.TryStreamExecute(ctx, vc, bindVars, wantfields, callback)
		if err != nil && vterrors.RootCause(err) == buffer.ShardMissingError {
			continue
		}
		return err
	}
	return vterrors.New(vtrpcpb.Code_UNAVAILABLE, "upstream shards are not available")
}

func (vc *VCursorImpl) StreamExecutePrimitiveStandalone(ctx context.Context, primitive engine.Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(result *sqltypes.Result) error) error {
	callback = vc.wrapCallback(callback, primitive)

	// clone the VCursorImpl with a new session.
	newVC := vc.cloneWithAutocommitSession()
	for try := 0; try < MaxBufferingRetries; try++ {
		err := primitive.TryStreamExecute(ctx, newVC, bindVars, wantfields, callback)
		if err != nil && vterrors.RootCause(err) == buffer.ShardMissingError {
			continue
		}
		return err
	}
	return vterrors.New(vtrpcpb.Code_UNAVAILABLE, "upstream shards are not available")
}

// Execute is part of the engine.VCursor interface.
func (vc *VCursorImpl) Execute(ctx context.Context, method string, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error) {
	session := vc.SafeSession
	if co == vtgatepb.CommitOrder_AUTOCOMMIT {
		// For autocommit, we have to create an independent session.
		session = vc.SafeSession.NewAutocommitSession()
		rollbackOnError = false
	} else {
		session.SetCommitOrder(co)
		defer session.SetCommitOrder(vtgatepb.CommitOrder_NORMAL)
	}

	err := vc.markSavepoint(ctx, rollbackOnError, map[string]*querypb.BindVariable{})
	if err != nil {
		return nil, err
	}

	qr, err := vc.executor.Execute(ctx, nil, method, session, vc.marginComments.Leading+query+vc.marginComments.Trailing, bindVars, false)
	vc.setRollbackOnPartialExecIfRequired(err != nil, rollbackOnError)

	return qr, err
}

// markSavepoint opens an internal savepoint before executing the original query.
// This happens only when rollback is allowed and no other savepoint was executed
// and the query is executed in an explicit transaction (i.e. started by the client).
func (vc *VCursorImpl) markSavepoint(ctx context.Context, needsRollbackOnParialExec bool, bindVars map[string]*querypb.BindVariable) error {
	if !needsRollbackOnParialExec || !vc.SafeSession.CanAddSavepoint() {
		return nil
	}
	uID := fmt.Sprintf("_vt%s", strings.ReplaceAll(uuid.NewString(), "-", "_"))
	spQuery := fmt.Sprintf("%ssavepoint %s%s", vc.marginComments.Leading, uID, vc.marginComments.Trailing)
	_, err := vc.executor.Execute(ctx, nil, "MarkSavepoint", vc.SafeSession, spQuery, bindVars, false)
	if err != nil {
		return err
	}
	vc.SafeSession.SetSavepoint(uID)
	return nil
}

// ExecuteMultiShard is part of the engine.VCursor interface.
func (vc *VCursorImpl) ExecuteMultiShard(ctx context.Context, primitive engine.Primitive, rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, rollbackOnError, canAutocommit, fetchLastInsertID bool) (*sqltypes.Result, []error) {
	noOfShards := len(rss)
	atomic.AddUint64(&vc.logStats.ShardQueries, uint64(noOfShards))
	err := vc.markSavepoint(ctx, rollbackOnError && (noOfShards > 1), map[string]*querypb.BindVariable{})
	if err != nil {
		return nil, []error{err}
	}

	qr, errs := vc.executor.ExecuteMultiShard(ctx, primitive, rss, commentedShardQueries(queries, vc.marginComments), vc.SafeSession, canAutocommit, vc.ignoreMaxMemoryRows, vc.observer, fetchLastInsertID)
	vc.setRollbackOnPartialExecIfRequired(len(errs) != len(rss), rollbackOnError)
	vc.logShardsQueried(primitive, len(rss))
	if qr != nil && qr.InsertIDUpdated() {
		vc.SafeSession.LastInsertId = qr.InsertID
	}
	return qr, errs
}

// StreamExecuteMulti is the streaming version of ExecuteMultiShard.
func (vc *VCursorImpl) StreamExecuteMulti(ctx context.Context, primitive engine.Primitive, query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, rollbackOnError, autocommit, fetchLastInsertID bool, callback func(reply *sqltypes.Result) error) []error {
	callback = vc.wrapCallback(callback, primitive)

	noOfShards := len(rss)
	atomic.AddUint64(&vc.logStats.ShardQueries, uint64(noOfShards))
	err := vc.markSavepoint(ctx, rollbackOnError && (noOfShards > 1), map[string]*querypb.BindVariable{})
	if err != nil {
		return []error{err}
	}

	errs := vc.executor.StreamExecuteMulti(ctx, primitive, vc.marginComments.Leading+query+vc.marginComments.Trailing, rss, bindVars, vc.SafeSession, autocommit, callback, vc.observer, fetchLastInsertID)
	vc.setRollbackOnPartialExecIfRequired(len(errs) != len(rss), rollbackOnError)

	return errs
}

// ExecuteLock is for executing advisory lock statements.
func (vc *VCursorImpl) ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, lockFuncType sqlparser.LockingFuncType) (*sqltypes.Result, error) {
	query.Sql = vc.marginComments.Leading + query.Sql + vc.marginComments.Trailing
	return vc.executor.ExecuteLock(ctx, rs, query, vc.SafeSession, lockFuncType)
}

// ExecuteStandalone is part of the engine.VCursor interface.
func (vc *VCursorImpl) ExecuteStandalone(ctx context.Context, primitive engine.Primitive, query string, bindVars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard, fetchLastInsertID bool) (*sqltypes.Result, error) {
	rss := []*srvtopo.ResolvedShard{rs}
	bqs := []*querypb.BoundQuery{
		{
			Sql:           vc.marginComments.Leading + query + vc.marginComments.Trailing,
			BindVariables: bindVars,
		},
	}
	// The autocommit flag is always set to false because we currently don't
	// execute DMLs through ExecuteStandalone.
	qr, errs := vc.executor.ExecuteMultiShard(ctx, primitive, rss, bqs, NewAutocommitSession(vc.SafeSession.Session), false /* autocommit */, vc.ignoreMaxMemoryRows, vc.observer, fetchLastInsertID)
	vc.logShardsQueried(primitive, len(rss))
	if qr.InsertIDUpdated() {
		vc.SafeSession.LastInsertId = qr.InsertID
	}
	return qr, vterrors.Aggregate(errs)
}

// ExecuteKeyspaceID is part of the engine.VCursor interface.
func (vc *VCursorImpl) ExecuteKeyspaceID(ctx context.Context, keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError, autocommit bool) (*sqltypes.Result, error) {
	atomic.AddUint64(&vc.logStats.ShardQueries, 1)
	rss, _, err := vc.ResolveDestinations(ctx, keyspace, nil, []key.ShardDestination{key.DestinationKeyspaceID(ksid)})
	if err != nil {
		return nil, err
	}
	queries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: bindVars,
	}}

	// This applies only when VTGate works in SINGLE transaction_mode.
	// This function is only called from consistent_lookup vindex when the lookup row getting inserting finds a duplicate.
	// In such scenario, original row needs to be locked to check if it already exists or no other transaction is working on it or does not write to it.
	// This creates a transaction but that transaction is for locking purpose only and should not cause multi-db transaction error.
	// This fields helps in to ignore multi-db transaction error when it states `queryFromVindex`.
	if !rollbackOnError {
		vc.SafeSession.SetQueryFromVindex(true)
		defer func() {
			vc.SafeSession.SetQueryFromVindex(false)
		}()
	}
	qr, errs := vc.ExecuteMultiShard(ctx, nil, rss, queries, rollbackOnError, autocommit, false)
	return qr, vterrors.Aggregate(errs)
}

func (vc *VCursorImpl) InTransactionAndIsDML() bool {
	if !vc.SafeSession.InTransaction() {
		return false
	}
	switch vc.logStats.StmtType {
	case "INSERT", "REPLACE", "UPDATE", "DELETE":
		return true
	}
	return false
}

func (vc *VCursorImpl) LookupRowLockShardSession() vtgatepb.CommitOrder {
	switch vc.logStats.StmtType {
	case "DELETE", "UPDATE":
		return vtgatepb.CommitOrder_POST
	}
	return vtgatepb.CommitOrder_PRE
}

// AutocommitApproval is part of the engine.VCursor interface.
func (vc *VCursorImpl) AutocommitApproval() bool {
	return vc.SafeSession.AutocommitApproval()
}

// setRollbackOnPartialExecIfRequired sets the value on SafeSession.rollbackOnPartialExec
// when the query gets successfully executed on at least one shard,
// there does not exist any old savepoint for which rollback is already set
// and rollback on error is allowed.
func (vc *VCursorImpl) setRollbackOnPartialExecIfRequired(atleastOneSuccess bool, rollbackOnError bool) {
	if atleastOneSuccess && rollbackOnError && !vc.SafeSession.IsRollbackSet() {
		vc.SafeSession.SetRollbackCommand()
	}
}

// fixupPartiallyMovedShards checks if any of the shards in the route has a ShardRoutingRule (true when a keyspace
// is in the middle of being moved to another keyspace using MoveTables moving a subset of shards at a time
func (vc *VCursorImpl) fixupPartiallyMovedShards(rss []*srvtopo.ResolvedShard) ([]*srvtopo.ResolvedShard, error) {
	if vc.vschema.ShardRoutingRules == nil {
		return rss, nil
	}
	for ind, rs := range rss {
		targetKeyspace, err := vc.FindRoutedShard(rs.Target.Keyspace, rs.Target.Shard)
		if err != nil {
			return nil, err
		}
		if targetKeyspace == rs.Target.Keyspace {
			continue
		}
		rss[ind] = rs.WithKeyspace(targetKeyspace)
	}
	return rss, nil
}

func (vc *VCursorImpl) ResolveDestinations(ctx context.Context, keyspace string, ids []*querypb.Value, destinations []key.ShardDestination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	rss, values, err := vc.resolver.ResolveDestinations(ctx, keyspace, vc.tabletType, ids, destinations)
	if err != nil {
		return nil, nil, err
	}
	if vc.config.EnableShardRouting {
		rss, err = vc.fixupPartiallyMovedShards(rss)
		if err != nil {
			return nil, nil, err
		}
	}
	return rss, values, err
}

func (vc *VCursorImpl) ResolveDestinationsMultiCol(ctx context.Context, keyspace string, ids [][]sqltypes.Value, destinations []key.ShardDestination) ([]*srvtopo.ResolvedShard, [][][]sqltypes.Value, error) {
	rss, values, err := vc.resolver.ResolveDestinationsMultiCol(ctx, keyspace, vc.tabletType, ids, destinations)
	if err != nil {
		return nil, nil, err
	}
	if vc.config.EnableShardRouting {
		rss, err = vc.fixupPartiallyMovedShards(rss)
		if err != nil {
			return nil, nil, err
		}
	}
	return rss, values, err
}

func (vc *VCursorImpl) Session() engine.SessionActions {
	return vc
}

func (vc *VCursorImpl) SetTarget(target string) error {
	keyspace, tabletType, _, err := topoprotopb.ParseDestination(target, vc.config.DefaultTabletType)
	if err != nil {
		return err
	}
	if _, ok := vc.vschema.Keyspaces[keyspace]; !ignoreKeyspace(keyspace) && !ok {
		return vterrors.VT05003(keyspace)
	}

	if vc.SafeSession.InTransaction() && tabletType != topodatapb.TabletType_PRIMARY {
		return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.LockOrActiveTransaction, "can't execute the given command because you have an active transaction")
	}
	vc.SafeSession.SetTargetString(target)
	vc.keyspace = keyspace
	vc.tabletType = tabletType
	return nil
}

func ignoreKeyspace(keyspace string) bool {
	return keyspace == "" || sqlparser.SystemSchema(keyspace)
}

func (vc *VCursorImpl) SetUDV(key string, value any) error {
	bindValue, err := sqltypes.BuildBindVariable(value)
	if err != nil {
		return err
	}
	vc.SafeSession.SetUserDefinedVariable(key, bindValue)
	return nil
}

func (vc *VCursorImpl) SetSysVar(name string, expr string) {
	vc.SafeSession.SetSystemVariable(name, expr)
}

func (vc *VCursorImpl) CheckForReservedConnection(setVarComment string, stmt sqlparser.Statement) {
	if setVarComment == "" {
		return
	}
	switch stmt.(type) {
	// If the statement supports optimizer hints or a transaction statement or a SET statement
	// no reserved connection is needed
	case *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback, *sqlparser.Savepoint,
		*sqlparser.SRollback, *sqlparser.Release, *sqlparser.Set, *sqlparser.Show,
		sqlparser.SupportOptimizerHint:
	default:
		vc.NeedsReservedConn()
	}
}

// NeedsReservedConn implements the SessionActions interface
func (vc *VCursorImpl) NeedsReservedConn() {
	vc.SafeSession.SetReservedConn(true)
}

func (vc *VCursorImpl) InReservedConn() bool {
	return vc.SafeSession.InReservedConn()
}

func (vc *VCursorImpl) ShardSession() []*srvtopo.ResolvedShard {
	ss := vc.SafeSession.GetShardSessions()
	if len(ss) == 0 {
		return nil
	}
	rss := make([]*srvtopo.ResolvedShard, len(ss))
	for i, shardSession := range ss {
		rss[i] = &srvtopo.ResolvedShard{
			Target:  shardSession.Target,
			Gateway: vc.resolver.GetGateway(),
		}
	}
	return rss
}

// Destination implements the ContextVSchema interface
func (vc *VCursorImpl) ShardDestination() key.ShardDestination {
	return vc.destination
}

// TabletType implements the ContextVSchema interface
func (vc *VCursorImpl) TabletType() topodatapb.TabletType {
	return vc.tabletType
}

func commentedShardQueries(shardQueries []*querypb.BoundQuery, marginComments sqlparser.MarginComments) []*querypb.BoundQuery {
	if marginComments.Leading == "" && marginComments.Trailing == "" {
		return shardQueries
	}
	newQueries := make([]*querypb.BoundQuery, len(shardQueries))
	for i, v := range shardQueries {
		newQueries[i] = &querypb.BoundQuery{
			Sql:           marginComments.Leading + v.Sql + marginComments.Trailing,
			BindVariables: v.BindVariables,
		}
	}
	return newQueries
}

// TargetDestination implements the ContextVSchema interface
func (vc *VCursorImpl) TargetDestination(qualifier string) (key.ShardDestination, *vindexes.Keyspace, topodatapb.TabletType, error) {
	keyspaceName := vc.getActualKeyspace()
	if vc.destination == nil && qualifier != "" {
		keyspaceName = qualifier
	}
	if keyspaceName == "" {
		return nil, nil, 0, ErrNoKeyspace
	}
	keyspace := vc.vschema.Keyspaces[keyspaceName]
	if keyspace == nil {
		return nil, nil, 0, vterrors.VT05003(keyspaceName)
	}
	return vc.destination, keyspace.Keyspace, vc.tabletType, nil
}

// SetAutocommit implements the SessionActions interface
func (vc *VCursorImpl) SetAutocommit(ctx context.Context, autocommit bool) error {
	if autocommit && vc.SafeSession.InTransaction() {
		if err := vc.executor.Commit(ctx, vc.SafeSession); err != nil {
			return err
		}
	}
	vc.SafeSession.Autocommit = autocommit
	return nil
}

// SetQueryTimeout implements the SessionActions interface
func (vc *VCursorImpl) SetQueryTimeout(maxExecutionTime int64) {
	vc.SafeSession.QueryTimeout = maxExecutionTime
}

// SetClientFoundRows implements the SessionActions interface
func (vc *VCursorImpl) SetClientFoundRows(_ context.Context, clientFoundRows bool) error {
	vc.SafeSession.GetOrCreateOptions().ClientFoundRows = clientFoundRows
	return nil
}

// SetSkipQueryPlanCache implements the SessionActions interface
func (vc *VCursorImpl) SetSkipQueryPlanCache(_ context.Context, skipQueryPlanCache bool) error {
	vc.SafeSession.GetOrCreateOptions().SkipQueryPlanCache = skipQueryPlanCache
	return nil
}

// SetSQLSelectLimit implements the SessionActions interface
func (vc *VCursorImpl) SetSQLSelectLimit(limit int64) error {
	vc.SafeSession.GetOrCreateOptions().SqlSelectLimit = limit
	return nil
}

// SetTransactionMode implements the SessionActions interface
func (vc *VCursorImpl) SetTransactionMode(mode vtgatepb.TransactionMode) {
	vc.SafeSession.TransactionMode = mode
}

// SetWorkload implements the SessionActions interface
func (vc *VCursorImpl) SetWorkload(workload querypb.ExecuteOptions_Workload) {
	vc.SafeSession.GetOrCreateOptions().Workload = workload
}

// SetPlannerVersion implements the SessionActions interface
func (vc *VCursorImpl) SetPlannerVersion(v plancontext.PlannerVersion) {
	vc.SafeSession.GetOrCreateOptions().PlannerVersion = v
}

func (vc *VCursorImpl) SetPriority(priority string) {
	if priority != "" {
		vc.SafeSession.GetOrCreateOptions().Priority = priority
	} else if vc.SafeSession.Options != nil && vc.SafeSession.Options.Priority != "" {
		vc.SafeSession.Options.Priority = ""
	}
}

func (vc *VCursorImpl) SetExecQueryTimeout(timeout *int) {
	// Determine the effective timeout: use passed timeout if non-nil, otherwise use session's query timeout if available
	var execTimeout *int
	if timeout != nil {
		execTimeout = timeout
	} else if sessionTimeout := vc.getQueryTimeout(); sessionTimeout > 0 {
		execTimeout = &sessionTimeout
	}

	// If no effective timeout and no session options, return early
	if execTimeout == nil {
		if vc.SafeSession.GetOptions() == nil {
			return
		}
		vc.SafeSession.GetOrCreateOptions().Timeout = nil
		return
	}

	vc.queryTimeout = time.Duration(*execTimeout) * time.Millisecond
	// Set the authoritative timeout using the determined execTimeout
	vc.SafeSession.GetOrCreateOptions().Timeout = &querypb.ExecuteOptions_AuthoritativeTimeout{
		AuthoritativeTimeout: int64(*execTimeout),
	}
}

// getQueryTimeout returns timeout based on the priority
// session setting > global default specified by a flag.
func (vc *VCursorImpl) getQueryTimeout() int {
	sessionQueryTimeout := int(vc.SafeSession.GetQueryTimeout())
	if sessionQueryTimeout != 0 {
		return sessionQueryTimeout
	}
	return vc.config.QueryTimeout
}

// SetConsolidator implements the SessionActions interface
func (vc *VCursorImpl) SetConsolidator(consolidator querypb.ExecuteOptions_Consolidator) {
	// Avoid creating session Options when they do not yet exist and the
	// consolidator is unspecified.
	if consolidator == querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED && vc.SafeSession.GetOptions() == nil {
		return
	}
	vc.SafeSession.GetOrCreateOptions().Consolidator = consolidator
}

func (vc *VCursorImpl) SetWorkloadName(workloadName string) {
	if workloadName != "" {
		vc.SafeSession.GetOrCreateOptions().WorkloadName = workloadName
	}
}

// SetFoundRows implements the SessionActions interface
func (vc *VCursorImpl) SetFoundRows(foundRows uint64) {
	vc.SafeSession.SetFoundRows(foundRows)
}

// SetInDMLExecution implements the SessionActions interface
func (vc *VCursorImpl) SetInDMLExecution(inDMLExec bool) {
	vc.SafeSession.SetInDMLExecution(inDMLExec)
}

// SetDDLStrategy implements the SessionActions interface
func (vc *VCursorImpl) SetDDLStrategy(strategy string) {
	vc.SafeSession.SetDDLStrategy(strategy)
}

// GetDDLStrategy implements the SessionActions interface
func (vc *VCursorImpl) GetDDLStrategy() string {
	return vc.SafeSession.GetDDLStrategy()
}

// SetMigrationContext implements the SessionActions interface
func (vc *VCursorImpl) SetMigrationContext(migrationContext string) {
	vc.SafeSession.SetMigrationContext(migrationContext)
}

// GetMigrationContext implements the SessionActions interface
func (vc *VCursorImpl) GetMigrationContext() string {
	return vc.SafeSession.GetMigrationContext()
}

// GetSessionUUID implements the SessionActions interface
func (vc *VCursorImpl) GetSessionUUID() string {
	return vc.SafeSession.GetSessionUUID()
}

// SetSessionEnableSystemSettings implements the SessionActions interface
func (vc *VCursorImpl) SetSessionEnableSystemSettings(_ context.Context, allow bool) error {
	vc.SafeSession.SetSessionEnableSystemSettings(allow)
	return nil
}

// GetSessionEnableSystemSettings implements the SessionActions interface
func (vc *VCursorImpl) GetSessionEnableSystemSettings() bool {
	return vc.SafeSession.GetSessionEnableSystemSettings()
}

// SetReadAfterWriteGTID implements the SessionActions interface
func (vc *VCursorImpl) SetReadAfterWriteGTID(vtgtid string) {
	vc.SafeSession.SetReadAfterWriteGTID(vtgtid)
}

// SetReadAfterWriteTimeout implements the SessionActions interface
func (vc *VCursorImpl) SetReadAfterWriteTimeout(timeout float64) {
	vc.SafeSession.SetReadAfterWriteTimeout(timeout)
}

// SetSessionTrackGTIDs implements the SessionActions interface
func (vc *VCursorImpl) SetSessionTrackGTIDs(enable bool) {
	vc.SafeSession.SetSessionTrackGtids(enable)
}

// HasCreatedTempTable implements the SessionActions interface
func (vc *VCursorImpl) HasCreatedTempTable() {
	vc.SafeSession.GetOrCreateOptions().HasCreatedTempTables = true
}

// GetWarnings implements the SessionActions interface
func (vc *VCursorImpl) GetWarnings() []*querypb.QueryWarning {
	return vc.SafeSession.GetWarnings()
}

// AnyAdvisoryLockTaken implements the SessionActions interface
func (vc *VCursorImpl) AnyAdvisoryLockTaken() bool {
	return vc.SafeSession.HasAdvisoryLock()
}

// AddAdvisoryLock implements the SessionActions interface
func (vc *VCursorImpl) AddAdvisoryLock(name string) {
	vc.SafeSession.AddAdvisoryLock(name)
}

// RemoveAdvisoryLock implements the SessionActions interface
func (vc *VCursorImpl) RemoveAdvisoryLock(name string) {
	vc.SafeSession.RemoveAdvisoryLock(name)
}

func (vc *VCursorImpl) SetCommitOrder(co vtgatepb.CommitOrder) {
	vc.SafeSession.SetCommitOrder(co)
}

func (vc *VCursorImpl) InTransaction() bool {
	return vc.SafeSession.InTransaction()
}

func (vc *VCursorImpl) Commit(ctx context.Context) error {
	return vc.executor.Commit(ctx, vc.SafeSession)
}

// GetDBDDLPluginName implements the VCursor interface
func (vc *VCursorImpl) GetDBDDLPluginName() string {
	return vc.config.DBDDLPlugin
}

// KeyspaceAvailable implements the VCursor interface
func (vc *VCursorImpl) KeyspaceAvailable(ks string) bool {
	_, exists := vc.executor.VSchema().Keyspaces[ks]
	return exists
}

// ErrorIfShardedF implements the VCursor interface
func (vc *VCursorImpl) ErrorIfShardedF(ks *vindexes.Keyspace, warn, errFormat string, params ...any) error {
	if ks.Sharded {
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, errFormat, params...)
	}
	vc.WarnUnshardedOnly("'%s' not supported in sharded mode", warn)

	return nil
}

func (vc *VCursorImpl) GetAndEmptyWarnings() []*querypb.QueryWarning {
	w := vc.warnings
	vc.warnings = nil
	return w
}

// WarnUnshardedOnly implements the VCursor interface
func (vc *VCursorImpl) WarnUnshardedOnly(format string, params ...any) {
	if vc.config.WarnShardedOnly {
		vc.warnings = append(vc.warnings, &querypb.QueryWarning{
			Code:    uint32(sqlerror.ERNotSupportedYet),
			Message: fmt.Sprintf(format, params...),
		})
		vc.executor.AddWarningCount("WarnUnshardedOnly", 1)
	}
}

// PlannerWarning implements the VCursor interface
func (vc *VCursorImpl) PlannerWarning(message string) {
	if message == "" {
		return
	}
	vc.warnings = append(vc.warnings, &querypb.QueryWarning{
		Code:    uint32(sqlerror.ERNotSupportedYet),
		Message: message,
	})
}

// ForeignKeyMode implements the VCursor interface
func (vc *VCursorImpl) ForeignKeyMode(keyspace string) (vschemapb.Keyspace_ForeignKeyMode, error) {
	if vc.config.ForeignKeyMode == vschemapb.Keyspace_disallow {
		return vschemapb.Keyspace_disallow, nil
	}
	ks := vc.vschema.Keyspaces[keyspace]
	if ks == nil {
		return 0, vterrors.VT14004(keyspace)
	}
	return ks.ForeignKeyMode, nil
}

func (vc *VCursorImpl) KeyspaceError(keyspace string) error {
	ks := vc.vschema.Keyspaces[keyspace]
	if ks == nil {
		return vterrors.VT14004(keyspace)
	}
	return ks.Error
}

func (vc *VCursorImpl) GetAggregateUDFs() []string {
	return vc.vschema.GetAggregateUDFs()
}

// FindMirrorRule finds the mirror rule for the requested table name and
// VSchema tablet type.
func (vc *VCursorImpl) FindMirrorRule(name sqlparser.TableName) (*vindexes.MirrorRule, error) {
	destKeyspace, destTabletType, _, err := vc.parseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.keyspace
	}
	mirrorRule, err := vc.vschema.FindMirrorRule(destKeyspace, name.Name.String(), destTabletType)
	if err != nil {
		return nil, err
	}
	return mirrorRule, err
}

func (vc *VCursorImpl) GetKeyspace() string {
	return vc.keyspace
}

func (vc *VCursorImpl) ExecuteVSchema(ctx context.Context, keyspace string, vschemaDDL *sqlparser.AlterVschema) error {
	srvVschema := vc.vm.GetCurrentSrvVschema()
	if srvVschema == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema not loaded")
	}

	user := callerid.ImmediateCallerIDFromContext(ctx)
	allowed := vschemaacl.Authorized(user)
	if !allowed {
		return vterrors.NewErrorf(vtrpcpb.Code_PERMISSION_DENIED, vterrors.AccessDeniedError, "User '%s' is not authorized to perform vschema operations", user.GetUsername())
	}

	// Resolve the keyspace either from the table qualifier or the target keyspace
	var (
		ksName string
		err    error
	)
	if !vschemaDDL.Table.IsEmpty() {
		ksName = vschemaDDL.Table.Qualifier.String()
	}
	if ksName == "" {
		ksName = keyspace
	}
	if ksName == "" {
		return ErrNoKeyspace
	}

	ksvs, err := topotools.ApplyVSchemaDDL(ctx, ksName, vc.topoServer, vschemaDDL)
	if err != nil {
		return err
	}

	srvVschema.Keyspaces[ksName] = ksvs.Keyspace

	return vc.vm.UpdateVSchema(ctx, ksvs, srvVschema)
}

func (vc *VCursorImpl) MessageStream(ctx context.Context, rss []*srvtopo.ResolvedShard, tableName string, callback func(*sqltypes.Result) error) error {
	atomic.AddUint64(&vc.logStats.ShardQueries, uint64(len(rss)))
	return vc.executor.ExecuteMessageStream(ctx, rss, tableName, callback)
}

func (vc *VCursorImpl) VStream(ctx context.Context, rss []*srvtopo.ResolvedShard, filter *binlogdatapb.Filter, gtid string, callback func(evs []*binlogdatapb.VEvent) error) error {
	return vc.executor.ExecuteVStream(ctx, rss, filter, gtid, callback)
}

func (vc *VCursorImpl) ShowExec(ctx context.Context, command sqlparser.ShowCommandType, filter *sqlparser.ShowFilter) (*sqltypes.Result, error) {
	switch command {
	case sqlparser.VitessReplicationStatus:
		return vc.executor.ShowVitessReplicationStatus(ctx, filter)
	case sqlparser.VitessShards:
		return vc.executor.ShowShards(ctx, filter, vc.tabletType)
	case sqlparser.VitessTablets:
		return vc.executor.ShowTablets(filter)
	case sqlparser.VitessVariables:
		return vc.executor.ShowVitessMetadata(ctx, filter)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "bug: unexpected show command: %v", command)
	}
}

func (vc *VCursorImpl) GetVSchema() *vindexes.VSchema {
	return vc.vschema
}

func (vc *VCursorImpl) GetSrvVschema() *vschemapb.SrvVSchema {
	return vc.vm.GetCurrentSrvVschema()
}

func (vc *VCursorImpl) SetExec(ctx context.Context, name string, value string) error {
	return vc.executor.SetVitessMetadata(ctx, name, value)
}

func (vc *VCursorImpl) ThrottleApp(ctx context.Context, throttledAppRule *topodatapb.ThrottledAppRule) (err error) {
	if throttledAppRule == nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "ThrottleApp: nil rule")
	}
	if throttledAppRule.Name == "" {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "ThrottleApp: app name is empty")
	}
	// We don't strictly have to construct a UpdateThrottlerConfigRequest here, because we only populate it
	// with a couple variables; we could do without it. However, constructing the request makes the remaining code
	// consistent with vtctldclient/command/throttler.go and we prefer this consistency
	req := &vtctldatapb.UpdateThrottlerConfigRequest{
		Keyspace:     vc.keyspace,
		ThrottledApp: throttledAppRule,
	}

	update := func(throttlerConfig *topodatapb.ThrottlerConfig) *topodatapb.ThrottlerConfig {
		if throttlerConfig == nil {
			throttlerConfig = &topodatapb.ThrottlerConfig{}
		}
		if throttlerConfig.ThrottledApps == nil {
			throttlerConfig.ThrottledApps = make(map[string]*topodatapb.ThrottledAppRule)
		}
		if req.ThrottledApp != nil && req.ThrottledApp.Name != "" {
			timeNow := time.Now()
			if protoutil.TimeFromProto(req.ThrottledApp.ExpiresAt).After(timeNow) {
				throttlerConfig.ThrottledApps[req.ThrottledApp.Name] = req.ThrottledApp
			} else {
				delete(throttlerConfig.ThrottledApps, req.ThrottledApp.Name)
			}
		}
		return throttlerConfig
	}

	ctx, unlock, lockErr := vc.topoServer.LockKeyspace(ctx, req.Keyspace, "UpdateThrottlerConfig")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	ki, err := vc.topoServer.GetKeyspace(ctx, req.Keyspace)
	if err != nil {
		return err
	}

	ki.ThrottlerConfig = update(ki.ThrottlerConfig)

	err = vc.topoServer.UpdateKeyspace(ctx, ki)
	if err != nil {
		return err
	}

	_, err = vc.topoServer.UpdateSrvKeyspaceThrottlerConfig(ctx, req.Keyspace, []string{}, update)

	return err
}

func (vc *VCursorImpl) CanUseSetVar() bool {
	return vc.Environment().Parser().IsMySQL80AndAbove() && vc.config.SetVarEnabled
}

func (vc *VCursorImpl) ReleaseLock(ctx context.Context) error {
	return vc.executor.ReleaseLock(ctx, vc.SafeSession)
}

func (vc *VCursorImpl) VExplainLogging() {
	vc.SafeSession.EnableLogging(vc.Environment().Parser())
}

func (vc *VCursorImpl) GetVExplainLogs() []engine.ExecuteEntry {
	return vc.SafeSession.GetLogs()
}

func (vc *VCursorImpl) FindRoutedShard(keyspace, shard string) (keyspaceName string, err error) {
	return vc.vschema.FindRoutedShard(keyspace, shard)
}

func (vc *VCursorImpl) IsViewsEnabled() bool {
	return vc.config.EnableViews
}

func (vc *VCursorImpl) GetUDV(name string) *querypb.BindVariable {
	return vc.SafeSession.GetUDV(name)
}

func (vc *VCursorImpl) PlanPrepareStatement(ctx context.Context, query string) (*engine.Plan, error) {
	return vc.executor.PlanPrepareStmt(ctx, vc.SafeSession, query)
}

func (vc *VCursorImpl) ClearPrepareData(name string) {
	delete(vc.SafeSession.PrepareStatement, name)
}

func (vc *VCursorImpl) StorePrepareData(stmtName string, prepareData *vtgatepb.PrepareData) {
	vc.SafeSession.StorePrepareData(stmtName, prepareData)
}

func (vc *VCursorImpl) GetPrepareData(stmtName string) *vtgatepb.PrepareData {
	return vc.SafeSession.GetPrepareData(stmtName)
}

func (vc *VCursorImpl) GetWarmingReadsPercent() int {
	return vc.config.WarmingReadsPercent
}

func (vc *VCursorImpl) GetWarmingReadsChannel() chan bool {
	return vc.config.WarmingReadsChannel
}

// SetForeignKeyCheckState updates the foreign key checks state of the vcursor.
func (vc *VCursorImpl) SetForeignKeyCheckState(fkChecksState *bool) {
	vc.fkChecksState = fkChecksState
}

// GetForeignKeyChecksState gets the stored foreign key checks state in the vcursor.
func (vc *VCursorImpl) GetForeignKeyChecksState() *bool {
	return vc.fkChecksState
}

// RecordMirrorStats is used to record stats about a mirror query.
func (vc *VCursorImpl) RecordMirrorStats(sourceExecTime, targetExecTime time.Duration, targetErr error) {
	vc.logStats.MirrorSourceExecuteTime = sourceExecTime
	vc.logStats.MirrorTargetExecuteTime = targetExecTime
	vc.logStats.MirrorTargetError = targetErr
}

func (vc *VCursorImpl) GetMarginComments() sqlparser.MarginComments {
	return vc.marginComments
}

func (vc *VCursorImpl) CachePlan() bool {
	return vc.SafeSession.CachePlan()
}

func (vc *VCursorImpl) GetContextWithTimeOut(ctx context.Context) (context.Context, context.CancelFunc) {
	if vc.queryTimeout == 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, vc.queryTimeout)
}

func (vc *VCursorImpl) IgnoreMaxMemoryRows() bool {
	return vc.ignoreMaxMemoryRows
}

func (vc *VCursorImpl) SetLastInsertID(id uint64) {
	vc.SafeSession.mu.Lock()
	defer vc.SafeSession.mu.Unlock()
	vc.SafeSession.LastInsertId = id
}
