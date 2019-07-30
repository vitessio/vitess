/*
Copyright 2017 Google Inc.

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
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	// ErrTooComplex indicates given sql query is too complex.
	ErrTooComplex = vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "Complex")
	execLimit     = &sqlparser.Limit{Rowcount: sqlparser.NewValArg([]byte(":#maxLimit"))}

	// PassthroughDMLs will return PlanPassDML for all update or delete statements
	PassthroughDMLs = false
)

//_______________________________________________

// PlanType indicates a query plan type.
type PlanType int

const (
	// PlanPassSelect is pass through select statements. This is the
	// default plan for select statements.
	PlanPassSelect PlanType = iota
	// PlanSelectLock is for a select that locks.
	PlanSelectLock
	// PlanNextval is for NEXTVAL.
	PlanNextval
	// PlanPassDML is pass through update & delete statements. This is
	// the default plan for update and delete statements.
	// If PassthroughDMLs is true, then it is used for all DML statements
	// and is valid in all replication modes.
	// Otherwise is only allowed in row based replication mode
	PlanPassDML
	// PlanDMLPK is an update or delete with an equality where clause(s)
	// on primary key(s).
	PlanDMLPK
	// PlanDMLSubquery is an update or delete with a subselect statement
	PlanDMLSubquery
	// PlanInsertPK is insert statement where the PK value is
	// supplied with the query.
	PlanInsertPK
	// PlanInsertSubquery is same as PlanDMLSubquery but for inserts.
	PlanInsertSubquery
	// PlanUpsertPK is for insert ... on duplicate key constructs.
	PlanUpsertPK
	// PlanInsertMessage is for inserting into message tables.
	PlanInsertMessage
	// PlanSet is for SET statements.
	PlanSet
	// PlanDDL is for DDL statements.
	PlanDDL
	// PlanSelectStream is used for streaming queries.
	PlanSelectStream
	// PlanOtherRead is for SHOW, DESCRIBE & EXPLAIN statements.
	PlanOtherRead
	// PlanOtherAdmin is for REPAIR, OPTIMIZE and TRUNCATE statements.
	PlanOtherAdmin
	// PlanMessageStream is used for streaming messages.
	PlanMessageStream
	// PlanSelectImpossible is used for where or having clauses that can never be true.
	PlanSelectImpossible
	// NumPlans stores the total number of plans
	NumPlans
)

// Must exactly match order of plan constants.
var planName = [NumPlans]string{
	"PASS_SELECT",
	"SELECT_LOCK",
	"NEXTVAL",
	"PASS_DML",
	"DML_PK",
	"DML_SUBQUERY",
	"INSERT_PK",
	"INSERT_SUBQUERY",
	"UPSERT_PK",
	"INSERT_MESSAGE",
	"SET",
	"DDL",
	"SELECT_STREAM",
	"OTHER_READ",
	"OTHER_ADMIN",
	"MESSAGE_STREAM",
	"SELECT_IMPOSSIBLE",
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

// IsSelect returns true if PlanType is about a select query.
func (pt PlanType) IsSelect() bool {
	return pt == PlanPassSelect || pt == PlanSelectLock || pt == PlanSelectImpossible
}

// MarshalJSON returns a json string for PlanType.
func (pt PlanType) MarshalJSON() ([]byte, error) {
	return json.Marshal(pt.String())
}

//_______________________________________________

// ReasonType indicates why a query plan fails to build
type ReasonType int

// Reason codes give a hint about why a certain plan was chosen.
const (
	ReasonDefault ReasonType = iota
	ReasonTable
	ReasonTableNoIndex
	ReasonPKChange
	ReasonComplexExpr
	ReasonUpsertSubquery
	ReasonUpsertMultiRow
	ReasonReplace
	ReasonMultiTable
	NumReasons
)

// Must exactly match order of reason constants.
var reasonName = [NumReasons]string{
	"DEFAULT",
	"TABLE",
	"TABLE_NOINDEX",
	"PK_CHANGE",
	"COMPLEX_EXPR",
	"UPSERT_SUBQUERY",
	"UPSERT_MULTI_ROW",
	"REPLACE",
	"MULTI_TABLE",
}

// String returns a string representation of a ReasonType.
func (rt ReasonType) String() string {
	return reasonName[rt]
}

// MarshalJSON returns a json string for ReasonType.
func (rt ReasonType) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", rt.String())), nil
}

//_______________________________________________

// Plan is built for selects and DMLs.
type Plan struct {
	PlanID PlanType
	Reason ReasonType
	Table  *schema.Table
	// NewName is the new name of the table. Set for DDLs which create or change the table.
	NewName sqlparser.TableIdent

	// Permissions stores the permissions for the tables accessed in the query.
	Permissions []Permission

	// FieldQuery is used to fetch field info
	FieldQuery *sqlparser.ParsedQuery

	// FullQuery will be set for all plans.
	FullQuery *sqlparser.ParsedQuery

	// For PK plans, only OuterQuery is set.
	// For SUBQUERY plans, Subquery is also set.
	OuterQuery  *sqlparser.ParsedQuery
	Subquery    *sqlparser.ParsedQuery
	UpsertQuery *sqlparser.ParsedQuery

	// PlanInsertSubquery: columns to be inserted.
	ColumnNumbers []int

	// PKValues is an sqltypes.Value if it's sourced
	// from the query. If it's a bind var then it's
	// a string including the ':' prefix(es).
	// PlanDMLPK: where clause values.
	// PlanInsertPK: values clause.
	// PlanNextVal: increment.
	PKValues []sqltypes.PlanValue

	// For update: set clause if pk is changing.
	SecondaryPKValues []sqltypes.PlanValue

	// WhereClause is set for DMLs. It is used by the hot row protection
	// to serialize e.g. UPDATEs going to the same row.
	WhereClause *sqlparser.ParsedQuery

	// For PlanInsertSubquery: pk columns in the subquery result.
	SubqueryPKColumns []int
}

// TableName returns the table name for the plan.
func (plan *Plan) TableName() sqlparser.TableIdent {
	var tableName sqlparser.TableIdent
	if plan.Table != nil {
		tableName = plan.Table.Name
	}
	return tableName
}

func (plan *Plan) setTable(tableName sqlparser.TableIdent, tables map[string]*schema.Table) (*schema.Table, error) {
	if plan.Table = tables[tableName.String()]; plan.Table == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "table %s not found in schema", tableName)
	}
	return plan.Table, nil
}

// Build builds a plan based on the schema.
func Build(statement sqlparser.Statement, tables map[string]*schema.Table) (*Plan, error) {
	var plan *Plan

	err := checkForPoolingUnsafeConstructs(statement)
	if err != nil {
		return nil, err
	}

	switch stmt := statement.(type) {
	case *sqlparser.Union:
		plan, err = &Plan{
			PlanID:     PlanPassSelect,
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
	case *sqlparser.DDL:
		plan, err = analyzeDDL(stmt, tables), nil
	case *sqlparser.Show:
		plan, err = &Plan{PlanID: PlanOtherRead}, nil
	case *sqlparser.OtherRead:
		plan, err = &Plan{PlanID: PlanOtherRead}, nil
	case *sqlparser.OtherAdmin:
		plan, err = &Plan{PlanID: PlanOtherAdmin}, nil
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

	err = checkForPoolingUnsafeConstructs(statement)
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
		if stmt.Lock != "" {
			return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "select with lock not allowed for streaming")
		}
		if tableName := analyzeFrom(stmt.From); !tableName.IsEmpty() {
			plan.setTable(tableName, tables)
		}
	case *sqlparser.OtherRead, *sqlparser.Show, *sqlparser.Union:
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
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if f, ok := node.(*sqlparser.FuncExpr); ok {
			if f.Name.Lowered() == "get_lock" {
				return false, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "get_lock() not allowed")
			}
		}

		// TODO: This could be smarter about not walking down parts of the AST that can't contain
		// function calls.
		return true, nil
	}, expr)
}
