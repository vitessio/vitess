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
	"fmt"
	"sync"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vthash"
)

// Distinct Primitive is used to uniqueify results
var _ Primitive = (*Distinct)(nil)

type (
	// Distinct Primitive is used to uniqueify results
	Distinct struct {
		Source    Primitive
		CheckCols []CheckCol
		Truncate  int
	}
	CheckCol struct {
		Col          int
		WsCol        *int
		Type         evalengine.Type
		CollationEnv *collations.Environment
	}
	probeTable struct {
		seenRows     map[vthash.Hash]struct{}
		checkCols    []CheckCol
		sqlmode      evalengine.SQLMode
		collationEnv *collations.Environment
	}
)

func (pt *probeTable) exists(inputRow sqltypes.Row) (sqltypes.Row, error) {
	code, err := pt.hashCodeForRow(inputRow)
	if err != nil {
		return nil, err
	}

	if _, found := pt.seenRows[code]; found {
		return nil, nil
	}

	pt.seenRows[code] = struct{}{}
	return inputRow, nil
}

func (pt *probeTable) hashCodeForRow(inputRow sqltypes.Row) (vthash.Hash, error) {
	hasher := vthash.New()
	for i, checkCol := range pt.checkCols {
		if i >= len(inputRow) {
			return vthash.Hash{}, vterrors.VT13001("index out of range in row when creating the DISTINCT hash code")
		}
		col := inputRow[checkCol.Col]
		err := evalengine.NullsafeHashcode128(&hasher, col, checkCol.Type.Collation(), checkCol.Type.Type(), pt.sqlmode)
		if err != nil {
			if err != evalengine.UnsupportedCollationHashError || checkCol.WsCol == nil {
				return vthash.Hash{}, err
			}
			checkCol = checkCol.SwitchToWeightString()
			pt.checkCols[i] = checkCol
			err = evalengine.NullsafeHashcode128(&hasher, inputRow[checkCol.Col], checkCol.Type.Collation(), checkCol.Type.Type(), pt.sqlmode)
			if err != nil {
				return vthash.Hash{}, err
			}
		}
	}
	return hasher.Sum128(), nil
}

func newProbeTable(checkCols []CheckCol, collationEnv *collations.Environment) *probeTable {
	cols := make([]CheckCol, len(checkCols))
	copy(cols, checkCols)
	return &probeTable{
		seenRows:     make(map[vthash.Hash]struct{}),
		checkCols:    cols,
		collationEnv: collationEnv,
	}
}

// TryExecute implements the Primitive interface
func (d *Distinct) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	input, err := vcursor.ExecutePrimitive(ctx, d.Source, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	result := &sqltypes.Result{
		Fields:   input.Fields,
		InsertID: input.InsertID,
	}

	pt := newProbeTable(d.CheckCols, vcursor.Environment().CollationEnv())

	for _, row := range input.Rows {
		appendRow, err := pt.exists(row)
		if err != nil {
			return nil, err
		}
		if appendRow != nil {
			result.Rows = append(result.Rows, appendRow)
		}
	}
	if d.Truncate > 0 {
		return result.Truncate(d.Truncate), nil
	}
	return result, err
}

// TryStreamExecute implements the Primitive interface
func (d *Distinct) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var mu sync.Mutex

	pt := newProbeTable(d.CheckCols, vcursor.Environment().CollationEnv())
	err := vcursor.StreamExecutePrimitive(ctx, d.Source, bindVars, wantfields, func(input *sqltypes.Result) error {
		result := &sqltypes.Result{
			Fields:   input.Fields,
			InsertID: input.InsertID,
		}
		mu.Lock()
		defer mu.Unlock()
		for _, row := range input.Rows {
			appendRow, err := pt.exists(row)
			if err != nil {
				return err
			}
			if appendRow != nil {
				result.Rows = append(result.Rows, appendRow)
			}
		}
		return callback(result.Truncate(len(d.CheckCols)))
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
func (d *Distinct) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return d.Source.GetFields(ctx, vcursor, bindVars)
}

// NeedsTransaction implements the Primitive interface
func (d *Distinct) NeedsTransaction() bool {
	return d.Source.NeedsTransaction()
}

// Inputs implements the Primitive interface
func (d *Distinct) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{d.Source}, nil
}

func (d *Distinct) description() PrimitiveDescription {
	other := map[string]any{}

	var colls []string
	for _, checkCol := range d.CheckCols {
		colls = append(colls, checkCol.String())
	}
	if colls != nil {
		other["Collations"] = colls
	}

	if d.Truncate > 0 {
		other["ResultColumns"] = d.Truncate
	}
	return PrimitiveDescription{
		Other:        other,
		OperatorType: "Distinct",
	}
}

// SwitchToWeightString returns a new CheckCol that works on the weight string column instead
func (cc CheckCol) SwitchToWeightString() CheckCol {
	return CheckCol{
		Col:          *cc.WsCol,
		WsCol:        nil,
		Type:         evalengine.NewType(sqltypes.VarBinary, collations.CollationBinaryID),
		CollationEnv: cc.CollationEnv,
	}
}

func (cc CheckCol) String() string {
	var collation string
	if cc.Type.Valid() && sqltypes.IsText(cc.Type.Type()) && cc.Type.Collation() != collations.Unknown {
		collation = ": " + cc.CollationEnv.LookupName(cc.Type.Collation())
	}

	var column string
	if cc.WsCol == nil {
		column = fmt.Sprintf("%d", cc.Col)
	} else {
		column = fmt.Sprintf("(%d:%d)", cc.Col, *cc.WsCol)
	}
	return column + collation
}
