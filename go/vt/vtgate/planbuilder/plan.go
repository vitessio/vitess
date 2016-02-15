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

// Route represents the instructions to execute a statement.
// It can be a select or dml.
type Route struct {
	PlanID     PlanID
	Keyspace   *Keyspace
	Query      string
	FieldQuery string
	Vindex     Vindex
	Values     interface{}
	JoinVars   map[string]struct{}
	Table      *Table
	Subquery   string
}

// MarshalJSON serializes the Plan into a JSON representation.
func (rt *Route) MarshalJSON() ([]byte, error) {
	var tname, vindexName string
	if rt.Table != nil {
		tname = rt.Table.Name
	}
	if rt.Vindex != nil {
		vindexName = rt.Vindex.String()
	}
	marshalPlan := struct {
		PlanID     PlanID              `json:",omitempty"`
		Keyspace   *Keyspace           `json:",omitempty"`
		Query      string              `json:",omitempty"`
		FieldQuery string              `json:",omitempty"`
		Vindex     string              `json:",omitempty"`
		Values     interface{}         `json:",omitempty"`
		JoinVars   map[string]struct{} `json:",omitempty"`
		Table      string              `json:",omitempty"`
		Subquery   string              `json:",omitempty"`
	}{
		PlanID:     rt.PlanID,
		Keyspace:   rt.Keyspace,
		Query:      rt.Query,
		FieldQuery: rt.FieldQuery,
		Vindex:     vindexName,
		Values:     prettyValue(rt.Values),
		JoinVars:   rt.JoinVars,
		Table:      tname,
		Subquery:   rt.Subquery,
	}
	return json.Marshal(marshalPlan)
}

// SetPlan updates the plan info for the route.
func (rt *Route) SetPlan(planID PlanID, vindex Vindex, values interface{}) {
	rt.PlanID = planID
	rt.Vindex = vindex
	rt.Values = values
}

// prettyValue converts the Values to a form that will
// be human-readable when converted to JSON. This is
// for testing and diagnostics.
func prettyValue(value interface{}) interface{} {
	switch value := value.(type) {
	case []byte:
		return string(value)
	case []interface{}:
		newvals := make([]interface{}, len(value))
		for i, old := range value {
			newvals[i] = prettyValue(old)
		}
		return newvals
	case sqlparser.SQLNode:
		return sqlparser.String(value)
	}
	return value
}

// Join is the join plan.
type Join struct {
	IsLeft      bool           `json:",omitempty"`
	Left, Right interface{}    `json:",omitempty"`
	Cols        []int          `json:",omitempty"`
	Vars        map[string]int `json:",omitempty"`
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
		plan.Instructions, err = buildSelectPlan(statement, vschema)
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
