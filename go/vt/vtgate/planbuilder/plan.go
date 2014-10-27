// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

type PlanID int

const (
	NoPlan = PlanID(iota)
	SelectUnsharded
	SelectSinglePrimary
	SelectMultiPrimary
	SelectSingleLookup
	SelectMultiLookup
	SelectScatter
	NumPlans
)

type Plan struct {
	ID        PlanID
	Reason    string
	TableName string
	Query     string
	Index     *VTGateIndex
	Values    []interface{}
}

func (pln *Plan) Size() int {
	return 1
}

// Must exactly match order of plan constants.
var planName = []string{
	"NoPlan",
	"SelectUnsharded",
	"SelectSinglePrimary",
	"SelectMultiPrimary",
	"SelectSingleLookup",
	"SelectMultiLookup",
	"SelectScatter",
}

func (id PlanID) String() string {
	if id < 0 || id >= NumPlans {
		return ""
	}
	return planName[id]
}

func PlanByName(s string) (id PlanID, ok bool) {
	for i, v := range planName {
		if v == s {
			return PlanID(i), true
		}
	}
	return NumPlans, false
}

func (id PlanID) IsMulti() bool {
	return id == SelectMultiPrimary || id == SelectMultiLookup
}

func (id PlanID) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", id.String())), nil
}

func BuildPlan(query string, schema *VTGateSchema) *Plan {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return &Plan{
			ID:     NoPlan,
			Reason: "syntax error",
			Query:  query,
		}
	}
	switch statement := statement.(type) {
	case *sqlparser.Union:
	case *sqlparser.Select:
		return buildSelectPlan(statement, schema)
	case *sqlparser.Insert:
	case *sqlparser.Update:
	case *sqlparser.Delete:
	case *sqlparser.Set:
	case *sqlparser.DDL:
	case *sqlparser.Other:
	default:
		panic("unexpected")
	}
	return &Plan{
		ID:     NoPlan,
		Reason: "too complex",
		Query:  query,
	}
}

func generateQuery(statement sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	statement.Format(buf)
	return buf.String()
}
