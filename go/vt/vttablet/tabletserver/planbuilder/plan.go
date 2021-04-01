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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	execLimit = &sqlparser.Limit{Rowcount: sqlparser.NewArgument(":#maxLimit")}

	// PassthroughDMLs will return plans that pass-through the DMLs without changing them.
	PassthroughDMLs = false
)

//_______________________________________________

// PlanType indicates a query plan type.
type PlanType int

// The following are PlanType values.
const (
	PlanSelect PlanType = iota
	PlanNextval
	PlanSelectImpossible
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
	NumPlans
)

// Must exactly match order of plan constants.
var planName = []string{
	"Select",
	"Nextval",
	"SelectImpossible",
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

// IsSelect returns true if PlanType is about a select query.
func (pt PlanType) IsSelect() bool {
	return pt == PlanSelect || pt == PlanSelectImpossible
}

// MarshalJSON returns a json string for PlanType.
func (pt PlanType) MarshalJSON() ([]byte, error) {
	return json.Marshal(pt.String())
}

//_______________________________________________

// Plan contains the parameters for executing a request.
type Plan struct {
	PlanID PlanType
	Table  *schema.Table

	// Permissions stores the permissions for the tables accessed in the query.
	Permissions []Permission

	// FieldQuery is used to fetch field info
	FieldQuery *sqlparser.ParsedQuery

	// FullQuery will be set for all plans.
	FullQuery *sqlparser.ParsedQuery

	// NextCount stores the count for "select next".
	NextCount sqltypes.PlanValue

	// WhereClause is set for DMLs. It is used by the hot row protection
	// to serialize e.g. UPDATEs going to the same row.
	WhereClause *sqlparser.ParsedQuery

	// FullStmt can be used when the query does not operate on tables
	FullStmt sqlparser.Statement
}

// TableName returns the table name for the plan.
func (plan *Plan) TableName() sqlparser.TableIdent {
	var tableName sqlparser.TableIdent
	if plan.Table != nil {
		tableName = plan.Table.Name
	}
	return tableName
}

// Build builds a plan based on the schema.
func Build(statement sqlparser.Statement, tables map[string]*schema.Table, isReservedConn bool, dbName string) (plan *Plan, err error) {
	if !isReservedConn {
		err = checkForPoolingUnsafeConstructs(statement)
		if err != nil {
			return nil, err
		}
	}

	switch stmt := statement.(type) {
	case *sqlparser.Union:
		plan, err = &Plan{
			PlanID:     PlanSelect,
			FieldQuery: GenerateFieldQuery(stmt),
			FullQuery:  GenerateLimitQuery(stmt),
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
		// DDLs and some other statements below don't get fully parsed.
		// We have to use the original query at the time of execution.
		// We are in the process of changing this
		var fullQuery *sqlparser.ParsedQuery
		// If the query is fully parsed, then use the ast and store the fullQuery
		if stmt.IsFullyParsed() {
			fullQuery = GenerateFullQuery(stmt)
		}
		plan = &Plan{PlanID: PlanDDL, FullQuery: fullQuery}
	case *sqlparser.AlterMigration:
		plan, err = &Plan{PlanID: PlanAlterMigration, FullStmt: stmt}, nil
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
func BuildStreaming(sql string, tables map[string]*schema.Table, isReservedConn bool) (*Plan, error) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}

	if !isReservedConn {
		err = checkForPoolingUnsafeConstructs(statement)
		if err != nil {
			return nil, err
		}
	}

	plan := &Plan{
		PlanID:      PlanSelectStream,
		FullQuery:   GenerateFullQuery(statement),
		Permissions: BuildPermissions(statement),
	}

	switch stmt := statement.(type) {
	case *sqlparser.Select:
		if stmt.Lock != sqlparser.NoLock {
			return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "select with lock not allowed for streaming")
		}
		plan.Table = lookupTable(stmt.From, tables)
	case *sqlparser.OtherRead, *sqlparser.Show, *sqlparser.Union, *sqlparser.CallProc, sqlparser.Explain:
		// pass
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "'%v' not allowed for streaming", sqlparser.String(stmt))
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

// checkForPoolingUnsafeConstructs returns an error if the SQL expression contains
// a call to GET_LOCK(), which is unsafe with server-side connection pooling.
// For more background, see https://github.com/vitessio/vitess/issues/3631.
func checkForPoolingUnsafeConstructs(expr sqlparser.SQLNode) error {

	genError := func(node sqlparser.SQLNode) error {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s not allowed without a reserved connections", sqlparser.String(node))
	}

	return sqlparser.Walk(func(in sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := in.(type) {
		case *sqlparser.Set:
			return false, genError(node)
		case *sqlparser.FuncExpr:
			if sqlparser.IsLockingFunc(node) {
				return false, genError(node)
			}
		}

		// TODO: This could be smarter about not walking down parts of the AST that can't contain
		// function calls.
		return true, nil
	}, expr)
}
