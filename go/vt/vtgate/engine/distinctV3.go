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
	"context"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// DistinctV3 Primitive is used to uniqueify results
// It does not always work, and should be removed once the V3 planner has been removed
var _ Primitive = (*DistinctV3)(nil)

// Distinct Primitive is used to uniqueify results
type DistinctV3 struct {
	Source Primitive
}

type row = []sqltypes.Value

type probeTableV3 struct {
	m map[evalengine.HashCode][]row
}

func (pt *probeTableV3) exists(inputRow row) (bool, error) {
	// calculate hashcode from all column values in the input row
	code := evalengine.HashCode(17)
	for _, value := range inputRow {
		hashcode, err := evalengine.NullsafeHashcode(value, collations.Unknown, value.Type())
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
		exists, err := equalV3(existingRow, inputRow)
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

func equalV3(a, b []sqltypes.Value) (bool, error) {
	for i, aVal := range a {
		cmp, err := evalengine.NullsafeCompare(aVal, b[i], collations.Unknown)
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func newProbeTableV3() *probeTableV3 {
	return &probeTableV3{m: map[evalengine.HashCode][]row{}}
}

// TryExecute implements the Primitive interface
func (d *DistinctV3) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	input, err := vcursor.ExecutePrimitive(ctx, d.Source, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	result := &sqltypes.Result{
		Fields:   input.Fields,
		InsertID: input.InsertID,
	}

	pt := newProbeTableV3()

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

// TryStreamExecute implements the Primitive interface
func (d *DistinctV3) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	pt := newProbeTableV3()

	err := vcursor.StreamExecutePrimitive(ctx, d.Source, bindVars, wantfields, func(input *sqltypes.Result) error {
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
func (d *DistinctV3) RouteType() string {
	return d.Source.RouteType()
}

// GetKeyspaceName implements the Primitive interface
func (d *DistinctV3) GetKeyspaceName() string {
	return d.Source.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (d *DistinctV3) GetTableName() string {
	return d.Source.GetTableName()
}

// GetFields implements the Primitive interface
func (d *DistinctV3) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return d.Source.GetFields(ctx, vcursor, bindVars)
}

// NeedsTransaction implements the Primitive interface
func (d *DistinctV3) NeedsTransaction() bool {
	return d.Source.NeedsTransaction()
}

// Inputs implements the Primitive interface
func (d *DistinctV3) Inputs() []Primitive {
	return []Primitive{d.Source}
}

func (d *DistinctV3) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Distinct",
	}
}
