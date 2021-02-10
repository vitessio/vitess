/*
Copyright 2020 The Vitess Authors.

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
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Rows)(nil)

//Rows simply returns a number or rows
type Rows struct {
	rows   [][]sqltypes.Value
	fields []*querypb.Field

	noInputs
	noTxNeeded
}

//NewRowsPrimitive returns a new Rows primitie
func NewRowsPrimitive(rows [][]sqltypes.Value, fields []*querypb.Field) *Rows {
	return &Rows{rows: rows, fields: fields}
}

//RouteType implements the Primitive interface
func (r *Rows) RouteType() string {
	return "Rows"
}

//GetKeyspaceName implements the Primitive interface
func (r *Rows) GetKeyspaceName() string {
	return ""
}

//GetTableName implements the Primitive interface
func (r *Rows) GetTableName() string {
	return ""
}

//Execute implements the Primitive interface
func (r *Rows) Execute(VCursor, map[string]*querypb.BindVariable, bool) (*sqltypes.Result, error) {
	return &sqltypes.Result{
		Fields:   r.fields,
		InsertID: 0,
		Rows:     r.rows,
	}, nil
}

//StreamExecute implements the Primitive interface
func (r *Rows) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	result, err := r.Execute(vcursor, bindVars, wantields)
	if err != nil {
		return err
	}
	return callback(result)
}

//GetFields implements the Primitive interface
func (r *Rows) GetFields(VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{
		Fields:   r.fields,
		InsertID: 0,
		Rows:     nil,
	}, nil
}

func (r *Rows) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: "Rows"}
}
