// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
)

var (
	// ErrTooComplex indicates given sql query is too complex.
	ErrTooComplex = errors.New("Complex")
	execLimit     = &sqlparser.Limit{Rowcount: sqlparser.NewValArg([]byte(":#maxLimit"))}
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
	// PlanNextval is for NEXTVAL
	PlanNextval
	// PlanPassDML is pass through update & delete statements. This is
	// the default plan for update and delete statements.
	PlanPassDML
	// PlanDMLPK is an update or delete with an equality where clause(s)
	// on primary key(s)
	PlanDMLPK
	// PlanDMLSubquery is an update or delete with a subselect statement
	PlanDMLSubquery
	// PlanInsertPK is insert statement where the PK value is
	// supplied with the query
	PlanInsertPK
	// PlanInsertSubquery is same as PlanDMLSubquery but for inserts
	PlanInsertSubquery
	// PlanUpsertPK is for insert ... on duplicate key constructs
	PlanUpsertPK
	// PlanInsertMessage is for inserting into message tables
	PlanInsertMessage
	// PlanSet is for SET statements
	PlanSet
	// PlanDDL is for DDL statements
	PlanDDL
	// PlanSelectStream is used for streaming queries
	PlanSelectStream
	// PlanOther is for SHOW, DESCRIBE & EXPLAIN statements
	PlanOther
	// NumPlans stores the total number of plans
	NumPlans
)

// Must exactly match order of plan constants.
var planName = []string{
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
	"OTHER",
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
	return pt == PlanPassSelect || pt == PlanSelectLock
}

// MarshalJSON returns a json string for PlanType.
func (pt PlanType) MarshalJSON() ([]byte, error) {
	return json.Marshal(pt.String())
}

// MinRole is the minimum Role required to execute this PlanType.
func (pt PlanType) MinRole() tableacl.Role {
	return tableACLRoles[pt]
}

//_______________________________________________

var tableACLRoles = map[PlanType]tableacl.Role{
	PlanPassSelect:     tableacl.READER,
	PlanSelectLock:     tableacl.READER,
	PlanSet:            tableacl.READER,
	PlanPassDML:        tableacl.WRITER,
	PlanDMLPK:          tableacl.WRITER,
	PlanDMLSubquery:    tableacl.WRITER,
	PlanInsertPK:       tableacl.WRITER,
	PlanInsertSubquery: tableacl.WRITER,
	PlanDDL:            tableacl.ADMIN,
	PlanSelectStream:   tableacl.READER,
	PlanOther:          tableacl.ADMIN,
	PlanUpsertPK:       tableacl.WRITER,
	PlanNextval:        tableacl.WRITER,
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
	ReasonUpsert
	ReasonUpsertColMismatch
)

// Must exactly match order of reason constants.
var reasonName = []string{
	"DEFAULT",
	"TABLE",
	"TABLE_NOINDEX",
	"PK_CHANGE",
	"COMPLEX_EXPR",
	"UPSERT",
	"UPSERT_COL_MISMATCH",
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

// MessageRowValues is used to store the values
// of a message row in a plan.
type MessageRowValues struct {
	TimeNext interface{}
	ID       interface{}
	Message  interface{}
}

//_______________________________________________

// Plan is built for selects and DMLs.
type Plan struct {
	PlanID PlanType
	Reason ReasonType
	Table  *schema.Table

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
	PKValues []interface{}

	// For update: set clause if pk is changing.
	SecondaryPKValues []interface{}

	// WhereClause is set for DMLs. It is used by the hot row protection
	// to serialize e.g. UPDATEs going to the same row.
	WhereClause *sqlparser.ParsedQuery

	// For PlanInsertSubquery: pk columns in the subquery result.
	SubqueryPKColumns []int

	// For PlanInsertMessage. Query used to reload inserted messages.
	MessageReloaderQuery *sqlparser.ParsedQuery
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
		return nil, fmt.Errorf("table %s not found in schema", tableName)
	}
	return plan.Table, nil
}

// Build builds a plan based on the schema.
func Build(sql string, tables map[string]*schema.Table) (plan *Plan, err error) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	switch stmt := statement.(type) {
	case *sqlparser.Union:
		return &Plan{
			PlanID:     PlanPassSelect,
			FieldQuery: GenerateFieldQuery(stmt),
			FullQuery:  GenerateLimitQuery(stmt),
		}, nil
	case *sqlparser.Select:
		return analyzeSelect(stmt, tables)
	case *sqlparser.Insert:
		return analyzeInsert(stmt, tables)
	case *sqlparser.Update:
		return analyzeUpdate(stmt, tables)
	case *sqlparser.Delete:
		return analyzeDelete(stmt, tables)
	case *sqlparser.Set:
		return analyzeSet(stmt), nil
	case *sqlparser.DDL:
		return analyzeDDL(stmt, tables), nil
	case *sqlparser.Show:
		return &Plan{PlanID: PlanOther}, nil
	case *sqlparser.Other:
		return &Plan{PlanID: PlanOther}, nil
	}
	return nil, errors.New("invalid SQL")
}

// BuildStreaming builds a streaming plan based on the schema.
func BuildStreaming(sql string, tables map[string]*schema.Table) (plan *Plan, err error) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}

	plan = &Plan{
		PlanID:    PlanSelectStream,
		FullQuery: GenerateFullQuery(statement),
	}

	switch stmt := statement.(type) {
	case *sqlparser.Select:
		if stmt.Lock != "" {
			return nil, errors.New("select with lock not allowed for streaming")
		}
		if tableName := analyzeFrom(stmt.From); !tableName.IsEmpty() {
			plan.setTable(tableName, tables)
		}
	case *sqlparser.Union:
		// pass
	default:
		return nil, fmt.Errorf("'%v' not allowed for streaming", sqlparser.String(stmt))
	}

	return plan, nil
}
