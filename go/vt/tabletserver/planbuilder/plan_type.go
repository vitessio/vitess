// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/tableacl"
)

type PlanType int

const (
	// PLAN_PASS_SELECT is pass through select statements. This is the
	// default plan for select statements.
	PLAN_PASS_SELECT PlanType = iota
	// PLAN_PASS_DML is pass through update & delete statements. This is
	// the default plan for update and delete statements.
	PLAN_PASS_DML
	// PLAN_PK_EQUAL is select statement which has an equality where clause
	// on primary key
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

func PlanByName(s string) (pt PlanType, ok bool) {
	for i, v := range planName {
		if v == s {
			return PlanType(i), true
		}
	}
	return NumPlans, false
}

func (pt PlanType) IsSelect() bool {
	return pt == PLAN_PASS_SELECT || pt == PLAN_PK_EQUAL || pt == PLAN_PK_IN || pt == PLAN_SELECT_SUBQUERY || pt == PLAN_SELECT_STREAM
}

func (pt PlanType) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", pt.String())), nil
}

// MinRole is the minimum Role required to execute this PlanType
func (pt PlanType) MinRole() tableacl.Role {
	return tableAclRoles[pt]
}

var tableAclRoles = map[PlanType]tableacl.Role{
	PLAN_PASS_SELECT:     tableacl.READER,
	PLAN_PK_EQUAL:        tableacl.READER,
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
	REASON_PKINDEX
	REASON_NOINDEX_MATCH
	REASON_TABLE_NOINDEX
	REASON_PK_CHANGE
	REASON_COMPOSITE_PK
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
	"PKINDEX",
	"NOINDEX_MATCH",
	"TABLE_NOINDEX",
	"PK_CHANGE",
	"COMPOSITE_PK",
	"HAS_HINTS",
	"UPSERT",
}

func (rt ReasonType) String() string {
	return reasonName[rt]
}

func (rt ReasonType) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", rt.String())), nil
}
