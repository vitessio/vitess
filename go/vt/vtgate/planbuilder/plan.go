// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// PlanID is number representing the plan id.
type PlanID int

// The following constants define all the PlanID values.
const (
	NoPlan = PlanID(iota)
	SelectUnsharded
	SelectEqualUnique
	SelectEqual
	SelectIN
	SelectScatter
	UpdateUnsharded
	UpdateEqual
	DeleteUnsharded
	DeleteEqual
	InsertUnsharded
	InsertSharded
	NewSelect
	NumPlans
)

// Must exactly match order of plan constants.
var planName = [NumPlans]string{
	"NoPlan",
	"SelectUnsharded",
	"SelectEqualUnique",
	"SelectEqual",
	"SelectIN",
	"SelectScatter",
	"UpdateUnsharded",
	"UpdateEqual",
	"DeleteUnsharded",
	"DeleteEqual",
	"InsertUnsharded",
	"InsertSharded",
	"NewSelect",
}

func (id PlanID) String() string {
	if id < 0 || id >= NumPlans {
		return ""
	}
	return planName[id]
}

// MarshalJSON serializes the plan id as a JSON string.
func (id PlanID) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", id.String())), nil
}

// Plan represents the routing strategy for a given query.
type Plan struct {
	// Original is the original query.
	Original string `json:",omitempty"`
	// Instructions contains the instructions needed to
	// fulfil the query. It's a tree of primitives.
	Instructions interface{} `json:",omitempty"`
}

// Size is defined so that Plan can be given to an LRUCache.
func (pln *Plan) Size() int {
	return 1
}

// DMLRoute represents the instructions to execute a DML.
type DMLRoute struct {
	PlanID PlanID
	// Table points to the VSchema table for the DML. A DML
	// is only allowed to change one table.
	Table *Table
	// Query is the query to be executed.
	Query string
	// Subquery is used for DeleteUnsharded to fetch the column values
	// for owned vindexes so they can be deleted.
	Subquery string
	// Vindex and Values determine the route for the query.
	Vindex Vindex
	Values interface{}
}

// MarshalJSON serializes the Plan into a JSON representation.
func (rt *DMLRoute) MarshalJSON() ([]byte, error) {
	var tname, vindexName string
	if rt.Table != nil {
		tname = rt.Table.Name
	}
	if rt.Vindex != nil {
		vindexName = rt.Vindex.String()
	}
	marshalPlan := struct {
		PlanID   PlanID      `json:",omitempty"`
		Table    string      `json:",omitempty"`
		Query    string      `json:",omitempty"`
		Subquery string      `json:",omitempty"`
		Vindex   string      `json:",omitempty"`
		Values   interface{} `json:",omitempty"`
	}{
		PlanID:   rt.PlanID,
		Table:    tname,
		Query:    rt.Query,
		Subquery: rt.Subquery,
		Vindex:   vindexName,
		Values:   prettyValue(rt.Values),
	}
	return json.Marshal(marshalPlan)
}

// BuildPlan builds a plan for a query based on the specified vschema.
func BuildPlan(query string, vschema *VSchema) (*Plan, error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	plan := &Plan{
		Original: query,
	}
	switch statement := statement.(type) {
	case *sqlparser.Select:
		//plan = buildSelectPlan(statement, vschema)
		plan.Instructions, err = buildSelectPlan2(statement, vschema)
	case *sqlparser.Insert:
		plan.Instructions, err = buildInsertPlan(statement, vschema)
	case *sqlparser.Update:
		plan.Instructions, err = buildUpdatePlan(statement, vschema)
	case *sqlparser.Delete:
		plan.Instructions, err = buildDeletePlan(statement, vschema)
	case *sqlparser.Union, *sqlparser.Set, *sqlparser.DDL, *sqlparser.Other:
		return nil, errors.New("cannot build a plan for this construct")
	default:
		panic("unexpected")
	}
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func generateQuery(statement sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	statement.Format(buf)
	return buf.String()
}
