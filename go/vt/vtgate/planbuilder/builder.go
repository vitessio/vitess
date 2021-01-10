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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"

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
}

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

func buildRoutePlan(stmt sqlparser.Statement, vschema ContextVSchema, f func(statement sqlparser.Statement, schema ContextVSchema) (engine.Primitive, error)) (engine.Primitive, error) {
	if vschema.Destination() != nil {
		return buildPlanForBypass(stmt, vschema)
	}
	return f(stmt, vschema)
}

func createInstructionFor(query string, stmt sqlparser.Statement, vschema ContextVSchema) (engine.Primitive, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return buildRoutePlan(stmt, vschema, buildSelectPlan(query))
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
	case *sqlparser.Explain:
		if stmt.Type == sqlparser.VitessType {
			innerInstruction, err := createInstructionFor(query, stmt.Statement, vschema)
			if err != nil {
				return nil, err
			}
			return buildExplainPlan(innerInstruction)
		}
		return buildOtherReadAndAdmin(query, vschema)
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
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot drop database '%s'; database does not exists", ksName)
		}
		return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "drop database not allowed")
	case *sqlparser.AlterDatabase:
		if !ksExists {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot alter database '%s'; database does not exists", ksName)
		}
		return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "alter database not allowed")
	case *sqlparser.CreateDatabase:
		if dbDDL.IfNotExists && ksExists {
			return engine.NewRowsPrimitive(make([][]sqltypes.Value, 0), make([]*querypb.Field, 0)), nil
		}
		if !dbDDL.IfNotExists && ksExists {
			return nil, vterrors.Errorf(vtrpcpb.Code_ALREADY_EXISTS, "cannot create database '%s'; database exists", ksName)
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
