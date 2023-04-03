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
	"encoding/json"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	execLimit = &sqlparser.Limit{Rowcount: sqlparser.NewArgument("#maxLimit")}

	// PassthroughDMLs will return plans that pass-through the DMLs without changing them.
	PassthroughDMLs = false
)

// _______________________________________________

// PlanType indicates a query plan type.
type PlanType int

// The following are PlanType values.
const (
	PlanSelect PlanType = iota
	PlanNextval
	PlanSelectImpossible
	PlanSelectLockFunc
	PlanInsert
	PlanInsertMessage
	PlanUpdate
	PlanUpdateLimit
	PlanDelete
	PlanDeleteLimit
	PlanDDL
	PlanSet
	// PlanOtherRead is for statements like show, etc.
	PlanOtherRead
	// PlanOtherAdmin is for statements like repair, lock table, etc.
	PlanOtherAdmin
	PlanSelectStream
	// PlanMessageStream is for "stream" statements.
	PlanMessageStream
	PlanSavepoint
	PlanRelease
	PlanSRollback
	PlanShow
	// PlanLoad is for Load data statements
	PlanLoad
	// PlanFlush is for FLUSH statements
	PlanFlush
	PlanLockTables
	PlanUnlockTables
	PlanCallProc
	PlanAlterMigration
	PlanRevertMigration
	PlanShowMigrations
	PlanShowMigrationLogs
	PlanShowThrottledApps
	PlanShowThrottlerStatus
	PlanViewDDL
	NumPlans
)

// Must exactly match order of plan constants.
var planName = []string{
	"Select",
	"Nextval",
	"SelectImpossible",
	"SelectLockFunc",
	"Insert",
	"InsertMessage",
	"Update",
	"UpdateLimit",
	"Delete",
	"DeleteLimit",
	"DDL",
	"Set",
	"OtherRead",
	"OtherAdmin",
	"SelectStream",
	"MessageStream",
	"Savepoint",
	"Release",
	"RollbackSavepoint",
	"Show",
	"Load",
	"Flush",
	"LockTables",
	"UnlockTables",
	"CallProcedure",
	"AlterMigration",
	"RevertMigration",
	"ShowMigrations",
	"ShowMigrationLogs",
	"ShowThrottledApps",
	"ShowThrottlerStatus",
	"ViewDDL",
}

func (pt PlanType) String() string {
	if pt < 0 || pt >= NumPlans {
		return ""
	}
	return planName[pt]
}

// PlanByName find a PlanType by its string name.
func PlanByName(s string) (pt PlanType, ok bool) {
	for i, v := range planName {
		if v == s {
			return PlanType(i), true
		}
	}
	return NumPlans, false
}

// PlanByNameIC finds a plan type by its string name without case sensitivity
func PlanByNameIC(s string) (pt PlanType, ok bool) {
	for i, v := range planName {
		if strings.EqualFold(v, s) {
			return PlanType(i), true
		}
	}
	return NumPlans, false
}

// MarshalJSON returns a json string for PlanType.
func (pt PlanType) MarshalJSON() ([]byte, error) {
	return json.Marshal(pt.String())
}

// _______________________________________________

// Plan contains the parameters for executing a request.
type Plan struct {
	PlanID PlanType
	// When the query indicates a single table
	Table *schema.Table
	// SELECT, UPDATE, DELETE statements may list multiple tables
	AllTables []*schema.Table

	// Permissions stores the permissions for the tables accessed in the query.
	Permissions []Permission

	// FullQuery will be set for all plans.
	FullQuery *sqlparser.ParsedQuery

	// NextCount stores the count for "select next".
	NextCount evalengine.Expr

	// WhereClause is set for DMLs. It is used by the hot row protection
	// to serialize e.g. UPDATEs going to the same row.
	WhereClause *sqlparser.ParsedQuery

	// FullStmt can be used when the query does not operate on tables
	FullStmt sqlparser.Statement

	// NeedsReservedConn indicates at a reserved connection is needed to execute this plan
	NeedsReservedConn bool
}

// TableName returns the table name for the plan.
func (plan *Plan) TableName() sqlparser.IdentifierCS {
	var tableName sqlparser.IdentifierCS
	if plan.Table != nil {
		tableName = plan.Table.Name
	}
	return tableName
}

// TableNames returns the table names for all tables in the plan.
func (plan *Plan) TableNames() (names []string) {
	if len(plan.AllTables) == 0 {
		tableName := plan.TableName()
		return []string{tableName.String()}
	}
	for _, table := range plan.AllTables {
		names = append(names, table.Name.String())
	}
	return names
}

// Build builds a plan based on the schema.
func Build(statement sqlparser.Statement, tables map[string]*schema.Table, dbName string, viewsEnabled bool) (plan *Plan, err error) {
	switch stmt := statement.(type) {
	case *sqlparser.Union:
		plan, err = &Plan{
			PlanID:    PlanSelect,
			FullQuery: GenerateLimitQuery(stmt),
		}, nil
	case *sqlparser.Select:
		plan, err = analyzeSelect(stmt, tables)
	case *sqlparser.Insert:
		plan, err = analyzeInsert(stmt, tables)
	case *sqlparser.Update:
		plan, err = analyzeUpdate(stmt, tables)
	case *sqlparser.Delete:
		plan, err = analyzeDelete(stmt, tables)
	case *sqlparser.Set:
		plan, err = analyzeSet(stmt), nil
	case sqlparser.DDLStatement:
		plan, err = analyzeDDL(stmt, viewsEnabled)
	case *sqlparser.AlterMigration:
		plan, err = &Plan{PlanID: PlanAlterMigration, FullStmt: stmt}, nil
	case *sqlparser.RevertMigration:
		plan, err = &Plan{PlanID: PlanRevertMigration, FullStmt: stmt}, nil
	case *sqlparser.ShowMigrationLogs:
		plan, err = &Plan{PlanID: PlanShowMigrationLogs, FullStmt: stmt}, nil
	case *sqlparser.ShowThrottledApps:
		plan, err = &Plan{PlanID: PlanShowThrottledApps, FullStmt: stmt}, nil
	case *sqlparser.ShowThrottlerStatus:
		plan, err = &Plan{PlanID: PlanShowThrottlerStatus, FullStmt: stmt}, nil
	case *sqlparser.Show:
		plan, err = analyzeShow(stmt, dbName)
	case *sqlparser.OtherRead, sqlparser.Explain:
		plan, err = &Plan{PlanID: PlanOtherRead}, nil
	case *sqlparser.OtherAdmin:
		plan, err = &Plan{PlanID: PlanOtherAdmin}, nil
	case *sqlparser.Savepoint:
		plan, err = &Plan{PlanID: PlanSavepoint}, nil
	case *sqlparser.Release:
		plan, err = &Plan{PlanID: PlanRelease}, nil
	case *sqlparser.SRollback:
		plan, err = &Plan{PlanID: PlanSRollback}, nil
	case *sqlparser.Load:
		plan, err = &Plan{PlanID: PlanLoad}, nil
	case *sqlparser.Flush:
		plan, err = &Plan{PlanID: PlanFlush, FullQuery: GenerateFullQuery(stmt)}, nil
	case *sqlparser.CallProc:
		plan, err = &Plan{PlanID: PlanCallProc, FullQuery: GenerateFullQuery(stmt)}, nil
	default:
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "invalid SQL")
	}
	if err != nil {
		return nil, err
	}
	plan.Permissions = BuildPermissions(statement)
	return plan, nil
}

// BuildStreaming builds a streaming plan based on the schema.
func BuildStreaming(sql string, tables map[string]*schema.Table) (*Plan, error) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}

	plan := &Plan{
		PlanID:      PlanSelectStream,
		FullQuery:   GenerateFullQuery(statement),
		Permissions: BuildPermissions(statement),
	}

	switch stmt := statement.(type) {
	case *sqlparser.Select:
		if hasLockFunc(stmt) {
			plan.NeedsReservedConn = true
		}
		plan.Table, plan.AllTables = lookupTables(stmt.From, tables)
	case *sqlparser.OtherRead, *sqlparser.Show, *sqlparser.Union, *sqlparser.CallProc, sqlparser.Explain:
		// pass
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s not allowed for streaming", sqlparser.ASTToStatementType(statement))
	}

	return plan, nil
}

// BuildMessageStreaming builds a plan for message streaming.
func BuildMessageStreaming(name string, tables map[string]*schema.Table) (*Plan, error) {
	plan := &Plan{
		PlanID: PlanMessageStream,
		Table:  tables[name],
	}
	if plan.Table == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "table %s not found in schema", name)
	}
	if plan.Table.Type != schema.Message {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "'%s' is not a message table", name)
	}
	plan.Permissions = []Permission{{
		TableName: plan.Table.Name.String(),
		Role:      tableacl.WRITER,
	}}
	return plan, nil
}

// hasLockFunc looks for get_lock function in the select query.
// If it is present then it returns true otherwise false
func hasLockFunc(sel *sqlparser.Select) bool {
	var found bool
	_ = sqlparser.Walk(func(in sqlparser.SQLNode) (bool, error) {
		lFunc, isLFunc := in.(*sqlparser.LockingFunc)
		if !isLFunc {
			return true, nil
		}
		if lFunc.Type == sqlparser.GetLock {
			found = true
			return false, nil
		}
		return true, nil
	}, sel.SelectExprs)
	return found
}

// BuildSettingQuery builds a query for system settings.
func BuildSettingQuery(settings []string) (query string, resetQuery string, err error) {
	if len(settings) == 0 {
		return "", "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG]: plan called for empty system settings")
	}
	var setExprs sqlparser.SetExprs
	var resetSetExprs sqlparser.SetExprs
	lDefault := sqlparser.NewStrLiteral("default")
	for _, setting := range settings {
		stmt, err := sqlparser.Parse(setting)
		if err != nil {
			return "", "", vterrors.Wrapf(err, "[BUG]: failed to parse system setting: %s", setting)
		}
		set, ok := stmt.(*sqlparser.Set)
		if !ok {
			return "", "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG]: invalid set statement: %s", setting)
		}
		setExprs = append(setExprs, set.Exprs...)
		for _, sExpr := range set.Exprs {
			sysVar := sExpr.Var
			if sysVar.Scope != sqlparser.SessionScope {
				return "", "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG]: session scope expected, got: %s", sysVar.Scope.ToString())
			}
			resetSetExprs = append(resetSetExprs, &sqlparser.SetExpr{Var: sysVar, Expr: lDefault})
		}
	}
	return sqlparser.String(&sqlparser.Set{Exprs: setExprs}), sqlparser.String(&sqlparser.Set{Exprs: resetSetExprs}), nil
}
