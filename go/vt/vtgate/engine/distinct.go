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
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// Distinct Primitive is used to uniqueify results
var _ Primitive = (*Distinct)(nil)

// Distinct Primitive is used to uniqueify results
type Distinct struct {
	Source Primitive
}

type row = []sqltypes.Value

type probeTable struct {
	m map[int64][]row
}

func (pt *probeTable) exists(inputRow row) (bool, error) {
	// calculate hashcode from all column values in the input row
	code := int64(17)
	for _, value := range inputRow {
		hashcode, err := evalengine.NullsafeHashcode(value)
		if err != nil {
			return false, err
		}
		code = code*31 + hashcode
	}

	existingRows, found := pt.m[code]
	if !found {
		// nothing with this hash code found, we can be sure it's a not seen row
		pt.m[code] = []row{inputRow}
		return false, nil
	}

	// we found something in the map - still need to check all individual values
	// so we don't just fall for a hash collision
	for _, existingRow := range existingRows {
		exists, err := equal(existingRow, inputRow)
		if err != nil {
			return false, err
		}
		if exists {
			return true, nil
		}
	}

	pt.m[code] = append(existingRows, inputRow)

	return false, nil
}

func equal(a, b []sqltypes.Value) (bool, error) {
	for i, aVal := range a {
		cmp, err := evalengine.NullsafeCompare(aVal, b[i])
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func newProbeTable() *probeTable {
	return &probeTable{m: map[int64][]row{}}
}

// Execute implements the Primitive interface
func (d *Distinct) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	input, err := d.Source.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	result := &sqltypes.Result{
		Fields:   input.Fields,
		InsertID: input.InsertID,
	}

	pt := newProbeTable()

	for _, row := range input.Rows {
		exists, err := pt.exists(row)
		if err != nil {
			return nil, err
		}
		if !exists {
			result.Rows = append(result.Rows, row)
		}
	}

	return result, err
}

// StreamExecute implements the Primitive interface
func (d *Distinct) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	pt := newProbeTable()

	err := d.Source.StreamExecute(vcursor, bindVars, wantfields, func(input *sqltypes.Result) error {
		result := &sqltypes.Result{
			Fields:   input.Fields,
			InsertID: input.InsertID,
		}
		for _, row := range input.Rows {
			exists, err := pt.exists(row)
			if err != nil {
				return err
			}
			if !exists {
				result.Rows = append(result.Rows, row)
			}
		}
		return callback(result)
	})

	return err
}

// RouteType implements the Primitive interface
func (d *Distinct) RouteType() string {
	return d.Source.RouteType()
}

// GetKeyspaceName implements the Primitive interface
func (d *Distinct) GetKeyspaceName() string {
	return d.Source.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (d *Distinct) GetTableName() string {
	return d.Source.GetTableName()
}

// GetFields implements the Primitive interface
func (d *Distinct) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return d.Source.GetFields(vcursor, bindVars)
}

// NeedsTransaction implements the Primitive interface
func (d *Distinct) NeedsTransaction() bool {
	return d.Source.NeedsTransaction()
}

// Inputs implements the Primitive interface
func (d *Distinct) Inputs() []Primitive {
	return []Primitive{d.Source}
}

func (d *Distinct) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Distinct",
	}
}
