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

// Plan represents the routing strategy for a given query.
// For now it's a simple wrapper around the real instructions.
// An instruction (aka primitive), tells VTGate how to execute
// a single command. Primitives can be cascaded as long as
// their inputs and outpus can be combined meaningfully.
// For example, a Join can depend on another Join or a Route.
// However, a Route cannot depend on another primitive.
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

// PlanID is a number representing the plan id.
// These are opcodes for the Route primitve.
type PlanID int

// This is the list of PlanID values. The PlanID
// dictates which fields must be set in the Route.
// All routes require the Query and a Keyspace
// to be correctly set.
// For any Select PlanID, the FieldQuery is set
// to a statement with an impossible where clause.
// This gets used to build the field info in situations
// where joins end up returning no rows.
// In the case of a join, Joinvars will also be set.
// These are variables that will be supplied by the
// Join primitive when it invokes a Route.
// All DMLs must have the Table field set. The
// ColVindexes in the field will be used to perform
// various computations and sanity checks.
// The rest of the fields depend on the opcode.
const (
	NoPlan = PlanID(iota)
	// SelectUnsharded is the opcode for routing a
	// select statement to an unsharded database.
	SelectUnsharded
	// SelectEqualUnique is for routing a query to
	// a single shard. Requires: A Unique Vindex, and
	// a single Value.
	SelectEqualUnique
	// SelectEqual is for routing a query using a
	// non-unique vindex. Requires: A Vindex, and
	// a single Value.
	SelectEqual
	// SelectIN is for routing a query that has an IN
	// clause using a Vindex. Requires: A Vindex,
	// and a Values list.
	SelectIN
	// SelectScatter is for running a scatter query
	// to all shards of a keyspace.
	SelectScatter
	// UpdateUnsharded is for routing an update statement
	// to an unsharded keyspace.
	UpdateUnsharded
	// UpdateEqual is for routing an update statement
	// to a single shard: Requires: A Vindex, and
	// a single Value.
	UpdateEqual
	// DeleteUnsharded is for routing a delete statement
	// to an unsharded keyspace.
	DeleteUnsharded
	// DeleteEqual is for routing a delete statement
	// to a single shard. Requires: A Vindex, a single
	// Value, and a Subquery, which will be used to
	// determine if lookup rows need to be deleted.
	DeleteEqual
	// InsertUnsharded is for routing an insert statement
	// to an unsharded keyspace.
	InsertUnsharded
	// InsertUnsharded is for routing an insert statement
	// to a single shard. Requires: A list of Values, one
	// for each ColVindex.
	InsertSharded
	// NumPlans is the total number of opcodes for routes.
	NumPlans
)

// planName must exactly match order of plan constants.
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
}

func (id PlanID) String() string {
	if id < 0 || id >= NumPlans {
		return ""
	}
	return planName[id]
}

// MarshalJSON serializes the plan id as a JSON string.
// It's used for testing and diagnostics.
func (id PlanID) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", id.String())), nil
}

// Route represents the instructions to route a query to
// one or many vttablets. The meaning and values for the
// the fields are described in the PlanID values comments.
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
// It's used for testing and diagnostics.
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

// prettyValue converts the Values field of a Route
// to a form that will be human-readable when
// converted to JSON. This is for testing and diagnostics.
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
	}
	return value
}

// Join specifies the parameters for a join primitive.
type Join struct {
	// IsLeft is true if it's a LEFT JOIN.
	IsLeft bool `json:",omitempty"`
	// Left and Right are the LHS and RHS primitives
	// of the Join. They can be any primitive.
	Left, Right interface{} `json:",omitempty"`
	// Cols defines which columns from the left
	// or right results should be used to build the
	// return result. For results coming from the
	// left query, the index values go as -1, -2, etc.
	// For the right query, they're 1, 2, etc.
	// If Cols is {-1, -2, 1, 2}, it means that
	// the returned result will be {Left0, Left1, Right0, Right1}.
	Cols []int `json:",omitempty"`
	// Vars defines the list of JoinVars that need to
	// be built from the LHS result before invoking
	// the RHS subqquery.
	Vars map[string]int `json:",omitempty"`
}

// BuildPlan builds a plan for a query based on the specified vschema.
// It's the main entry point for this package.
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
		return nil, errors.New("unsupported construct")
	default:
		panic("unexpected")
	}
	if err != nil {
		return nil, err
	}
	return plan, nil
}
