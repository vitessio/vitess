// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tableacl"
)

var (
	// ErrTooComplex indicates given sql query is too complex.
	ErrTooComplex = errors.New("Complex")
	execLimit     = &sqlparser.Limit{Rowcount: sqlparser.ValArg(":#maxLimit")}
)

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
	return tableAclRoles[pt]
}

var tableAclRoles = map[PlanType]tableacl.Role{
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
)

// Must exactly match order of reason constants.
var reasonName = []string{
	"DEFAULT",
	"TABLE",
	"TABLE_NOINDEX",
	"PK_CHANGE",
	"COMPLEX_EXPR",
	"UPSERT",
}

// String returns a string representation of a ReasonType.
func (rt ReasonType) String() string {
	return reasonName[rt]
}

// MarshalJSON returns a json string for ReasonType.
func (rt ReasonType) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", rt.String())), nil
}

// ExecPlan is built for selects and DMLs.
// PK Values values within ExecPlan can be:
// sqltypes.Value: sourced form the query, or
// string: bind variable name starting with ':', or
// nil if no value was specified
type ExecPlan struct {
	PlanID    PlanType
	Reason    ReasonType `json:",omitempty"`
	TableName string     `json:",omitempty"`

	// FieldQuery is used to fetch field info
	FieldQuery *sqlparser.ParsedQuery `json:",omitempty"`

	// FullQuery will be set for all plans.
	FullQuery *sqlparser.ParsedQuery `json:",omitempty"`

	// For PK plans, only OuterQuery is set.
	// For SUBQUERY plans, Subquery is also set.
	OuterQuery  *sqlparser.ParsedQuery `json:",omitempty"`
	Subquery    *sqlparser.ParsedQuery `json:",omitempty"`
	UpsertQuery *sqlparser.ParsedQuery `json:",omitempty"`

	// PlanInsertSubquery: columns to be inserted.
	ColumnNumbers []int `json:",omitempty"`

	// PlanDMLPK: where clause values.
	// PlanInsertPK: values clause.
	// PlanNextVal: increment.
	PKValues []interface{} `json:",omitempty"`

	// For update: set clause if pk is changing.
	SecondaryPKValues []interface{} `json:",omitempty"`

	// For PlanInsertSubquery: pk columns in the subquery result.
	SubqueryPKColumns []int `json:",omitempty"`
}

func (plan *ExecPlan) setTableInfo(tableName string, getTable TableGetter) (*schema.Table, error) {
	tableInfo, ok := getTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found in schema", tableName)
	}
	plan.TableName = tableInfo.Name
	return tableInfo, nil
}

// TableGetter returns a schema.Table given the table name.
type TableGetter func(tableName string) (*schema.Table, bool)

// GetExecPlan generates a ExecPlan given a sql query and a TableGetter.
func GetExecPlan(sql string, getTable TableGetter) (plan *ExecPlan, err error) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	switch stmt := statement.(type) {
	case *sqlparser.Union:
		return &ExecPlan{
			PlanID:     PlanPassSelect,
			FieldQuery: GenerateFieldQuery(stmt),
			FullQuery:  GenerateFullQuery(stmt),
		}, nil
	case *sqlparser.Select:
		return analyzeSelect(stmt, getTable)
	case *sqlparser.Insert:
		return analyzeInsert(stmt, getTable)
	case *sqlparser.Update:
		return analyzeUpdate(stmt, getTable)
	case *sqlparser.Delete:
		return analyzeDelete(stmt, getTable)
	case *sqlparser.Set:
		return analyzeSet(stmt), nil
	case *sqlparser.DDL:
		return analyzeDDL(stmt, getTable), nil
	case *sqlparser.Other:
		return &ExecPlan{PlanID: PlanOther}, nil
	}
	return nil, errors.New("invalid SQL")
}

// GetStreamExecPlan generates a ExecPlan given a sql query and a TableGetter.
func GetStreamExecPlan(sql string, getTable TableGetter) (plan *ExecPlan, err error) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}

	plan = &ExecPlan{
		PlanID:    PlanSelectStream,
		FullQuery: GenerateFullQuery(statement),
	}

	switch stmt := statement.(type) {
	case *sqlparser.Select:
		if stmt.Lock != "" {
			return nil, errors.New("select with lock not allowed for streaming")
		}
		if tableName := analyzeFrom(stmt.From); tableName != "" {
			plan.setTableInfo(tableName, getTable)
		}
	case *sqlparser.Union:
		// pass
	default:
		return nil, fmt.Errorf("'%v' not allowed for streaming", sqlparser.String(stmt))
	}

	return plan, nil
}
