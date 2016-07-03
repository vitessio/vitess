// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package engine

import (
	"encoding/json"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// Route represents the instructions to route a query to
// one or many vttablets. The meaning and values for the
// the fields are described in the RouteOpcode values comments.
type Route struct {
	Opcode     RouteOpcode
	Keyspace   *vindexes.Keyspace
	Query      string
	FieldQuery string
	Vindex     vindexes.Vindex
	Values     interface{}
	JoinVars   map[string]struct{}
	Table      *vindexes.Table
	Subquery   string
	Generate   *Generate
}

// Execute performs a non-streaming exec.
func (rt *Route) Execute(vcursor VCursor, joinvars map[string]interface{}, wantields bool) (*sqltypes.Result, error) {
	return vcursor.ExecuteRoute(rt, joinvars)
}

// StreamExecute performs a streaming exec.
func (rt *Route) StreamExecute(vcursor VCursor, joinvars map[string]interface{}, wantfields bool, sendReply func(*sqltypes.Result) error) error {
	return vcursor.StreamExecuteRoute(rt, joinvars, sendReply)
}

// GetFields fetches the field info.
func (rt *Route) GetFields(vcursor VCursor, joinvars map[string]interface{}) (*sqltypes.Result, error) {
	return vcursor.GetRouteFields(rt, joinvars)
}

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
		Keyspace   *vindexes.Keyspace  `json:",omitempty"`
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
	Keyspace *vindexes.Keyspace
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
		Opcode   RouteOpcode        `json:",omitempty"`
		Keyspace *vindexes.Keyspace `json:",omitempty"`
		Query    string             `json:",omitempty"`
		Value    interface{}        `json:",omitempty"`
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
	// MultiInsertUnsharded is for routing a multi-row insert statement
	// to a multiple shard. Requires: A list of Values, one
	// for each ColVindex. If the table has an Autoinc column,
	// A Generate subplan must be created.
	MultiInsertSharded
	// NumCodes is the total number of opcodes for routes.
	NumCodes
)

// routeName must exactly match order of opcode constants.
var routeName = [NumCodes]string{
	"Error",
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
	return routeName[code]
}

// MarshalJSON serializes the RouteOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code RouteOpcode) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", code.String())), nil
}
