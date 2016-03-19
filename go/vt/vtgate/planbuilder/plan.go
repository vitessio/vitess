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

// Plan represents the execution strategy for a given query.
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
	Instructions Primitive `json:",omitempty"`
}

// Size is defined so that Plan can be given to a cache.LRUCache.
// VTGate needs to maintain a cache of plans. It uses LRUCache, which
// in turn requires its objects to define a Size function.
func (pln *Plan) Size() int {
	return 1
}

// Primitive is the interface that needs to be satisfied by
// all primitives of a plan. For now, the primitives just
// have to define an isPrimitive function that does nothing.
type Primitive interface {
	isPrimitive()
}

// Route represents the instructions to route a query to
// one or many vttablets. The meaning and values for the
// the fields are described in the RouteOpcode values comments.
type Route struct {
	Opcode     RouteOpcode
	Keyspace   *Keyspace
	Query      string
	FieldQuery string
	Vindex     Vindex
	Values     interface{}
	JoinVars   map[string]struct{}
	Table      *Table
	Subquery   string
	Generate   *Generate
}

func (rt *Route) isPrimitive() {}

// MarshalJSON serializes the Route into a JSON representation.
// It's used for testing and diagnostics.
func (rt *Route) MarshalJSON() ([]byte, error) {
	var tname, vindexName string
	if rt.Table != nil {
		tname = rt.Table.Name
	}
	if rt.Vindex != nil {
		vindexName = rt.Vindex.String()
	}
	marshalRoute := struct {
		Opcode     RouteOpcode         `json:",omitempty"`
		Keyspace   *Keyspace           `json:",omitempty"`
		Query      string              `json:",omitempty"`
		FieldQuery string              `json:",omitempty"`
		Vindex     string              `json:",omitempty"`
		Values     interface{}         `json:",omitempty"`
		JoinVars   map[string]struct{} `json:",omitempty"`
		Table      string              `json:",omitempty"`
		Subquery   string              `json:",omitempty"`
		Generate   *Generate           `json:",omitempty"`
	}{
		Opcode:     rt.Opcode,
		Keyspace:   rt.Keyspace,
		Query:      rt.Query,
		FieldQuery: rt.FieldQuery,
		Vindex:     vindexName,
		Values:     prettyValue(rt.Values),
		JoinVars:   rt.JoinVars,
		Table:      tname,
		Subquery:   rt.Subquery,
		Generate:   rt.Generate,
	}
	return json.Marshal(marshalRoute)
}

// Generate represents the instruction to generate
// a value from a sequence. We cannot reuse a Route
// for this because this needs to be always executed
// outside a transaction.
type Generate struct {
	// Opcode can only be SelectUnsharded for now.
	Opcode   RouteOpcode
	Keyspace *Keyspace
	Query    string
	// Value is the supplied value. A new value will be generated
	// only if Value was NULL. Otherwise, the supplied value will
	// be used.
	Value interface{}
}

// MarshalJSON serializes Generate into a JSON representation.
// It's used for testing and diagnostics.
func (gen *Generate) MarshalJSON() ([]byte, error) {
	jsongen := struct {
		Opcode   RouteOpcode `json:",omitempty"`
		Keyspace *Keyspace   `json:",omitempty"`
		Query    string      `json:",omitempty"`
		Value    interface{} `json:",omitempty"`
	}{
		Opcode:   gen.Opcode,
		Keyspace: gen.Keyspace,
		Query:    gen.Query,
		Value:    prettyValue(gen.Value),
	}
	return json.Marshal(jsongen)
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

// RouteOpcode is a number representing the opcode
// for the Route primitve.
type RouteOpcode int

// This is the list of RouteOpcode values. The opcode
// dictates which fields must be set in the Route.
// All routes require the Query and a Keyspace
// to be correctly set.
// For any Select opcode, the FieldQuery is set
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
	NoCode = RouteOpcode(iota)
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
	// SelectScatter is for routing a scatter query
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
	// for each ColVindex. If the table has an Autoinc column,
	// A Generate subplan must be created.
	InsertSharded
	// NumCodes is the total number of opcodes for routes.
	NumCodes
)

// opcodeName must exactly match order of opcode constants.
var opcodeName = [NumCodes]string{
	"NoCode",
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

func (code RouteOpcode) String() string {
	if code < 0 || code >= NumCodes {
		return ""
	}
	return opcodeName[code]
}

// MarshalJSON serializes the RouteOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code RouteOpcode) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", code.String())), nil
}

// Join specifies the parameters for a join primitive.
type Join struct {
	// IsLeft is true if it's a LEFT JOIN.
	IsLeft bool `json:",omitempty"`
	// Left and Right are the LHS and RHS primitives
	// of the Join. They can be any primitive.
	Left, Right Primitive `json:",omitempty"`
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

func (jn *Join) isPrimitive() {}

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
		panic("unexpected statement type")
	}
	if err != nil {
		return nil, err
	}
	return plan, nil
}
