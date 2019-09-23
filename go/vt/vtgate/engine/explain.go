/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package engine

import (
	"encoding/json"
	"strings"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Explain)(nil)

// Explain will return the query plan for a query instead of actually running it
type Explain struct {
	Input Primitive
}

// RouteType returns a description of the query routing type used by the primitive
func (e *Explain) RouteType() string {
	return e.Input.RouteType()
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (e *Explain) GetKeyspaceName() string {
	return e.Input.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (e *Explain) GetTableName() string {
	return e.Input.GetTableName()
}

// MarshalJSON allows us to add the opcode in
func (e *Explain) MarshalJSON() ([]byte, error) {
	type Alias Explain
	return json.Marshal(&struct {
		OpCode string `json:"Opcode"`
		*Alias
	}{
		OpCode: "Explain",
		Alias:  (*Alias)(e),
	})
}

// Execute satisfies the Primtive interface.
func (e *Explain) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	var fields []*querypb.Field
	fields = append(fields, &querypb.Field{
		Name: "column",
		Type: querypb.Type_VARCHAR,
	})

	var rows [][]sqltypes.Value

	buf, err := json.MarshalIndent(e.Input, "", " ")
	if err != nil {
		return nil, err
	}

	outputText := string(buf)
	noOfRows := 0
	for _, line := range strings.Split(outputText, "\n") {
		rows = append(rows, []sqltypes.Value{sqltypes.NewVarChar(line)})
		noOfRows++
	}

	return &sqltypes.Result{Fields: fields, Rows: rows, RowsAffected: uint64(noOfRows), InsertID: 0}, nil
}

// StreamExecute satisfies the Primtive interface.
func (e *Explain) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("oh noes")
}

// GetFields satisfies the Primtive interface.
func (e *Explain) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return e.Input.GetFields(vcursor, bindVars)
}
