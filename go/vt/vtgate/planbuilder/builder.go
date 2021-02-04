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

package planbuilder

import (
	"errors"
	"sort"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// ContextVSchema defines the interface for this package to fetch
// info about tables.
type ContextVSchema interface {
	FindTable(tablename sqlparser.TableName) (*vindexes.Table, string, topodatapb.TabletType, key.Destination, error)
	FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error)
	DefaultKeyspace() (*vindexes.Keyspace, error)
	TargetString() string
	Destination() key.Destination
	TabletType() topodatapb.TabletType
	TargetDestination(qualifier string) (key.Destination, *vindexes.Keyspace, topodatapb.TabletType, error)
	AnyKeyspace() (*vindexes.Keyspace, error)
	FirstSortedKeyspace() (*vindexes.Keyspace, error)
	SysVarSetEnabled() bool
	KeyspaceExists(keyspace string) bool
	AllKeyspace() ([]*vindexes.Keyspace, error)
	GetSemTable() *semantics.SemTable
	Planner() PlannerVersion
}

// PlannerVersion is an alias here to make the code more readable
type PlannerVersion = querypb.ExecuteOptions_PlannerVersion

const (
	// V3 is also the default planner
	V3 = querypb.ExecuteOptions_V3
	// Gen4 uses the default Gen4 planner, which is the greedy planner
	Gen4 = querypb.ExecuteOptions_Gen4
	// Gen4GreedyOnly uses only the faster greedy planner
	Gen4GreedyOnly = querypb.ExecuteOptions_Gen4Greedy
	// Gen4Left2Right tries to emulate the V3 planner by only joining plans in the order they are listed in the FROM-clause
	Gen4Left2Right = querypb.ExecuteOptions_Gen4Left2Right
	// Gen4WithFallback first attempts to use the Gen4 planner, and if that fails, uses the V3 planner instead
	Gen4WithFallback = querypb.ExecuteOptions_Gen4WithFallback
)

type truncater interface {
	SetTruncateColumnCount(int)
}

// TestBuilder builds a plan for a query based on the specified vschema.
// This method is only used from tests
func TestBuilder(query string, vschema ContextVSchema) (*engine.Plan, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	result, err := sqlparser.RewriteAST(stmt, "")
	if err != nil {
		return nil, err
	}

	return BuildFromStmt(query, result.AST, vschema, result.BindVarNeeds)
}

// ErrPlanNotSupported is an error for plan building not supported
var ErrPlanNotSupported = errors.New("plan building not supported")

// BuildFromStmt builds a plan based on the AST provided.
func BuildFromStmt(query string, stmt sqlparser.Statement, vschema ContextVSchema, bindVarNeeds *sqlparser.BindVarNeeds) (*engine.Plan, error) {

	instruction, err := createInstructionFor(query, stmt, vschema)
	if err != nil {
		return nil, err
	}
	plan := &engine.Plan{
		Type:         sqlparser.ASTToStatementType(stmt),
		Original:     query,
		Instructions: instruction,
		BindVarNeeds: bindVarNeeds,
	}
	return plan, nil
}

func getConfiguredPlanner(vschema ContextVSchema) (selectPlanner, error) {
	switch vschema.Planner() {
	case V3:
		return buildSelectPlan, nil
	case Gen4, Gen4Left2Right, Gen4GreedyOnly:
		return gen4Planner, nil
	case Gen4WithFallback:
		fp := &fallbackPlanner{
			primary:  gen4Planner,
			fallback: buildSelectPlan,
		}
		return fp.plan, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown planner selected %d", vschema.Planner())
	}
}

func buildRoutePlan(stmt sqlparser.Statement, vschema ContextVSchema, f func(statement sqlparser.Statement, schema ContextVSchema) (engine.Primitive, error)) (engine.Primitive, error) {
	if vschema.Destination() != nil {
		return buildPlanForBypass(stmt, vschema)
	}
	return f(stmt, vschema)
}

type selectPlanner func(query string) func(sqlparser.Statement, ContextVSchema) (engine.Primitive, error)

func createInstructionFor(query string, stmt sqlparser.Statement, vschema ContextVSchema) (engine.Primitive, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		configuredPlanner, err := getConfiguredPlanner(vschema)
		if err != nil {
			return nil, err
		}
		return buildRoutePlan(stmt, vschema, configuredPlanner(query))
	case *sqlparser.Insert:
		return buildRoutePlan(stmt, vschema, buildInsertPlan)
	case *sqlparser.Update:
		return buildRoutePlan(stmt, vschema, buildUpdatePlan)
	case *sqlparser.Delete:
		return buildRoutePlan(stmt, vschema, buildDeletePlan)
	case *sqlparser.Union:
		return buildRoutePlan(stmt, vschema, buildUnionPlan)
	case sqlparser.DDLStatement:
		return buildGeneralDDLPlan(query, stmt, vschema)
	case *sqlparser.AlterVschema:
		return buildVSchemaDDLPlan(stmt, vschema)
	case *sqlparser.Use:
		return buildUsePlan(stmt, vschema)
	case sqlparser.Explain:
		return buildExplainPlan(stmt, vschema)
	case *sqlparser.OtherRead, *sqlparser.OtherAdmin:
		return buildOtherReadAndAdmin(query, vschema)
	case *sqlparser.Set:
		return buildSetPlan(stmt, vschema)
	case *sqlparser.Load:
		return buildLoadPlan(query, vschema)
	case sqlparser.DBDDLStatement:
		return buildRoutePlan(stmt, vschema, buildDBDDLPlan)
	case *sqlparser.SetTransaction:
		return nil, ErrPlanNotSupported
	case *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback, *sqlparser.Savepoint, *sqlparser.SRollback, *sqlparser.Release:
		// Empty by design. Not executed by a plan
		return nil, nil
	case *sqlparser.Show:
		return buildShowPlan(stmt, vschema)
	case *sqlparser.LockTables:
		return buildRoutePlan(stmt, vschema, buildLockPlan)
	case *sqlparser.UnlockTables:
		return buildRoutePlan(stmt, vschema, buildUnlockPlan)
	case *sqlparser.Flush:
		return buildFlushPlan(stmt, vschema)
	case *sqlparser.CallProc:
		return buildCallProcPlan(stmt, vschema)
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unexpected statement type: %T", stmt)
}

func buildDBDDLPlan(stmt sqlparser.Statement, vschema ContextVSchema) (engine.Primitive, error) {
	dbDDLstmt := stmt.(sqlparser.DBDDLStatement)
	ksName := dbDDLstmt.GetDatabaseName()
	if ksName == "" {
		ks, err := vschema.DefaultKeyspace()
		if err != nil {
			return nil, err
		}
		ksName = ks.Name
	}
	ksExists := vschema.KeyspaceExists(ksName)

	switch dbDDL := dbDDLstmt.(type) {
	case *sqlparser.DropDatabase:
		if dbDDL.IfExists && !ksExists {
			return engine.NewRowsPrimitive(make([][]sqltypes.Value, 0), make([]*querypb.Field, 0)), nil
		}
		if !ksExists {
			return nil, mysql.NewSQLError(mysql.ERDbDropExists, mysql.SSUnknownSQLState, "Can't drop database '%s'; database doesn't exists", ksName)
		}
		return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "drop database not allowed")
	case *sqlparser.AlterDatabase:
		if !ksExists {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Can't alter database '%s'; database doesn't exists", ksName)
		}
		return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "alter database not allowed")
	case *sqlparser.CreateDatabase:
		if dbDDL.IfNotExists && ksExists {
			return engine.NewRowsPrimitive(make([][]sqltypes.Value, 0), make([]*querypb.Field, 0)), nil
		}
		if !dbDDL.IfNotExists && ksExists {
			return nil, mysql.NewSQLError(mysql.ERDbCreateExists, mysql.SSUnknownSQLState, "Can't create database '%s'; database exists", ksName)
		}
		return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "create database not allowed")
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unreachable code path: %s", sqlparser.String(dbDDLstmt))
}

func buildLoadPlan(query string, vschema ContextVSchema) (engine.Primitive, error) {
	keyspace, err := vschema.DefaultKeyspace()
	if err != nil {
		return nil, err
	}

	destination := vschema.Destination()
	if destination == nil {
		if keyspace.Sharded {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: this construct is not supported on sharded keyspace")
		}
		destination = key.DestinationAnyShard{}
	}

	return &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             query,
		IsDML:             true,
		SingleShardOnly:   true,
	}, nil
}

func buildVSchemaDDLPlan(stmt *sqlparser.AlterVschema, vschema ContextVSchema) (engine.Primitive, error) {
	_, keyspace, _, err := vschema.TargetDestination(stmt.Table.Qualifier.String())
	if err != nil {
		return nil, err
	}
	return &engine.AlterVSchema{
		Keyspace:        keyspace,
		AlterVschemaDDL: stmt,
	}, nil
}

func buildFlushPlan(stmt *sqlparser.Flush, vschema ContextVSchema) (engine.Primitive, error) {
	if len(stmt.TableNames) == 0 {
		return buildFlushOptions(stmt, vschema)
	}
	return buildFlushTables(stmt, vschema)
}

func buildFlushOptions(stmt *sqlparser.Flush, vschema ContextVSchema) (engine.Primitive, error) {
	destination, keyspace, _, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	return &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sqlparser.String(stmt),
		IsDML:             false,
		SingleShardOnly:   false,
	}, nil
}

func buildFlushTables(stmt *sqlparser.Flush, vschema ContextVSchema) (engine.Primitive, error) {
	type sendDest struct {
		ks   *vindexes.Keyspace
		dest key.Destination
	}

	flushStatements := map[sendDest]*sqlparser.Flush{}
	for i, tab := range stmt.TableNames {
		var destinationTab key.Destination
		var keyspaceTab *vindexes.Keyspace
		var table *vindexes.Table
		var err error
		table, _, _, _, destinationTab, err = vschema.FindTableOrVindex(tab)

		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, vindexes.NotFoundError{TableName: tab.Name.String()}
		}
		keyspaceTab = table.Keyspace
		stmt.TableNames[i] = sqlparser.TableName{
			Name: table.Name,
		}
		if destinationTab == nil {
			destinationTab = key.DestinationAllShards{}
		}

		flush, isAvail := flushStatements[sendDest{keyspaceTab, destinationTab}]
		if isAvail {
			flush.TableNames = append(flush.TableNames, stmt.TableNames[i])
		} else {
			flush = &sqlparser.Flush{
				IsLocal:      stmt.IsLocal,
				FlushOptions: nil,
				TableNames:   sqlparser.TableNames{stmt.TableNames[i]},
				WithLock:     stmt.WithLock,
				ForExport:    stmt.ForExport,
			}
		}
		flushStatements[sendDest{keyspaceTab, destinationTab}] = flush
	}

	if len(flushStatements) == 1 {
		for sendDest, flush := range flushStatements {
			return &engine.Send{
				Keyspace:          sendDest.ks,
				TargetDestination: sendDest.dest,
				Query:             sqlparser.String(flush),
				IsDML:             false,
				SingleShardOnly:   false,
			}, nil
		}
	}

	keys := make([]sendDest, len(flushStatements))

	// Collect keys of the map
	i := 0
	for k := range flushStatements {
		keys[i] = k
		i++
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].ks.Name < keys[j].ks.Name
	})

	finalPlan := &engine.Concatenate{
		Sources: nil,
	}
	for _, sendDest := range keys {
		plan := &engine.Send{
			Keyspace:          sendDest.ks,
			TargetDestination: sendDest.dest,
			Query:             sqlparser.String(flushStatements[sendDest]),
			IsDML:             false,
			SingleShardOnly:   false,
		}
		finalPlan.Sources = append(finalPlan.Sources, plan)
	}

	return finalPlan, nil
}
