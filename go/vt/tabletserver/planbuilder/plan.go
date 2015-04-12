// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"

	log "github.com/golang/glog"
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
	// PLAN_PASS_SELECT is pass through select statements. This is the
	// default plan for select statements.
	PLAN_PASS_SELECT PlanType = iota
	// PLAN_PASS_DML is pass through update & delete statements. This is
	// the default plan for update and delete statements.
	PLAN_PASS_DML
	// PLAN_PK_EQUAL is deprecated. Use PLAN_PK_IN instead.
	PLAN_PK_EQUAL
	// PLAN_PK_IN is select statement with a single IN clause on primary key
	PLAN_PK_IN
	// PLAN_SELECT_SUBQUERY is select statement with a subselect statement
	PLAN_SELECT_SUBQUERY
	// PLAN_DML_PK is an update or delete with an equality where clause(s)
	// on primary key(s)
	PLAN_DML_PK
	// PLAN_DML_SUBQUERY is an update or delete with a subselect statement
	PLAN_DML_SUBQUERY
	// PLAN_INSERT_PK is insert statement where the PK value is
	// supplied with the query
	PLAN_INSERT_PK
	// PLAN_INSERT_SUBQUERY is same as PLAN_DML_SUBQUERY but for inserts
	PLAN_INSERT_SUBQUERY
	// PLAN_SET is for SET statements
	PLAN_SET
	// PLAN_DDL is for DDL statements
	PLAN_DDL
	// PLAN_SELECT_STREAM is used for streaming queries
	PLAN_SELECT_STREAM
	// PLAN_OTHER is for SHOW, DESCRIBE & EXPLAIN statements
	PLAN_OTHER
	// NumPlans stores the total number of plans
	NumPlans
)

// Must exactly match order of plan constants.
var planName = []string{
	"PASS_SELECT",
	"PASS_DML",
	"PK_EQUAL",
	"PK_IN",
	"SELECT_SUBQUERY",
	"DML_PK",
	"DML_SUBQUERY",
	"INSERT_PK",
	"INSERT_SUBQUERY",
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
	return pt == PLAN_PASS_SELECT || pt == PLAN_PK_IN || pt == PLAN_SELECT_SUBQUERY || pt == PLAN_SELECT_STREAM
}

// MarshalJSON returns a json string for PlanType.
func (pt PlanType) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", pt.String())), nil
}

// MinRole is the minimum Role required to execute this PlanType.
func (pt PlanType) MinRole() tableacl.Role {
	return tableAclRoles[pt]
}

var tableAclRoles = map[PlanType]tableacl.Role{
	PLAN_PASS_SELECT:     tableacl.READER,
	PLAN_PK_IN:           tableacl.READER,
	PLAN_SELECT_SUBQUERY: tableacl.READER,
	PLAN_SET:             tableacl.READER,
	PLAN_PASS_DML:        tableacl.WRITER,
	PLAN_DML_PK:          tableacl.WRITER,
	PLAN_DML_SUBQUERY:    tableacl.WRITER,
	PLAN_INSERT_PK:       tableacl.WRITER,
	PLAN_INSERT_SUBQUERY: tableacl.WRITER,
	PLAN_DDL:             tableacl.ADMIN,
	PLAN_SELECT_STREAM:   tableacl.READER,
	PLAN_OTHER:           tableacl.ADMIN,
}

// ReasonType indicates why a query plan fails to build
type ReasonType int

const (
	REASON_DEFAULT ReasonType = iota
	REASON_SELECT
	REASON_TABLE
	REASON_NOCACHE
	REASON_SELECT_LIST
	REASON_LOCK
	REASON_WHERE
	REASON_ORDER
	REASON_LIMIT
	REASON_PKINDEX
	REASON_COVERING
	REASON_NOINDEX_MATCH
	REASON_TABLE_NOINDEX
	REASON_PK_CHANGE
	REASON_HAS_HINTS
	REASON_UPSERT
)

// Must exactly match order of reason constants.
var reasonName = []string{
	"DEFAULT",
	"SELECT",
	"TABLE",
	"NOCACHE",
	"SELECT_LIST",
	"LOCK",
	"WHERE",
	"ORDER",
	"LIMIT",
	"PKINDEX",
	"COVERING",
	"NOINDEX_MATCH",
	"TABLE_NOINDEX",
	"PK_CHANGE",
	"HAS_HINTS",
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
	PlanId    PlanType
	Reason    ReasonType
	TableName string

	// FieldQuery is used to fetch field info
	FieldQuery *sqlparser.ParsedQuery

	// FullQuery will be set for all plans.
	FullQuery *sqlparser.ParsedQuery

	// For PK plans, only OuterQuery is set.
	// For SUBQUERY plans, Subquery is also set.
	// IndexUsed is set only for PLAN_SELECT_SUBQUERY
	OuterQuery *sqlparser.ParsedQuery
	Subquery   *sqlparser.ParsedQuery
	IndexUsed  string

	// For selects, columns to be returned
	// For PLAN_INSERT_SUBQUERY, columns to be inserted
	ColumnNumbers []int

	// PLAN_PK_IN, PLAN_DML_PK: where clause values
	// PLAN_INSERT_PK: values clause
	PKValues []interface{}

	// PK_IN. Limit clause value.
	Limit interface{}

	// For update: set clause if pk is changing
	SecondaryPKValues []interface{}

	// For PLAN_INSERT_SUBQUERY: pk columns in the subquery result
	SubqueryPKColumns []int

	// PLAN_SET
	SetKey   string
	SetValue interface{}
}

func (node *ExecPlan) setTableInfo(tableName string, getTable TableGetter) (*schema.Table, error) {
	tableInfo, ok := getTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found in schema", tableName)
	}
	node.TableName = tableInfo.Name
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
	plan, err = analyzeSQL(statement, getTable)
	if err != nil {
		return nil, err
	}
	if plan.PlanId == PLAN_PASS_DML {
		log.Warningf("PASS_DML: %s", sql)
	}
	return plan, nil
}

// GetStreamExecPlan generates a ExecPlan given a sql query and a TableGetter.
func GetStreamExecPlan(sql string, getTable TableGetter) (plan *ExecPlan, err error) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}

	plan = &ExecPlan{
		PlanId:    PLAN_SELECT_STREAM,
		FullQuery: GenerateFullQuery(statement),
	}

	switch stmt := statement.(type) {
	case *sqlparser.Select:
		if stmt.Lock != "" {
			return nil, errors.New("select with lock disallowed with streaming")
		}
		tableName, _ := analyzeFrom(stmt.From)
		if tableName != "" {
			plan.setTableInfo(tableName, getTable)
		}

	case *sqlparser.Union:
		// pass
	default:
		return nil, fmt.Errorf("'%v' not allowed for streaming", sqlparser.String(stmt))
	}

	return plan, nil
}

func analyzeSQL(statement sqlparser.Statement, getTable TableGetter) (plan *ExecPlan, err error) {
	switch stmt := statement.(type) {
	case *sqlparser.Union:
		return &ExecPlan{
			PlanId:     PLAN_PASS_SELECT,
			FieldQuery: GenerateFieldQuery(stmt),
			FullQuery:  GenerateFullQuery(stmt),
			Reason:     REASON_SELECT,
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
		return &ExecPlan{PlanId: PLAN_OTHER}, nil
	}
	return nil, errors.New("invalid SQL")
}
