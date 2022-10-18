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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// Distinct Primitive is used to uniqueify results
var _ Primitive = (*Distinct)(nil)

type (
	// Distinct Primitive is used to uniqueify results
	Distinct struct {
		Source    Primitive
		CheckCols []CheckCol
		Truncate  bool
	}
	CheckCol struct {
		Col       int
		WsCol     *int
		Collation collations.ID
	}
	probeTable struct {
		seenRows  map[evalengine.HashCode][]sqltypes.Row
		checkCols []CheckCol
	}
)

func (pt *probeTable) exists(inputRow sqltypes.Row) (bool, error) {
	// the two prime numbers used here (17 and 31) are used to
	// calculate hashcode from all column values in the input sqltypes.Row
	code, err := pt.hashCodeForRow(inputRow)
	if err != nil {
		return false, err
	}

	existingRows, found := pt.seenRows[code]
	if !found {
		// nothing with this hash code found, we can be sure it's a not seen sqltypes.Row
		pt.seenRows[code] = []sqltypes.Row{inputRow}
		return false, nil
	}

	// we found something in the map - still need to check all individual values
	// so we don't just fall for a hash collision
	for _, existingRow := range existingRows {
		exists, err := pt.equal(existingRow, inputRow)
		if err != nil {
			return false, err
		}
		if exists {
			return true, nil
		}
	}

	pt.seenRows[code] = append(existingRows, inputRow)

	return false, nil
}

func (pt *probeTable) hashCodeForRow(inputRow sqltypes.Row) (evalengine.HashCode, error) {
	// Why use 17 and 31 in this method?
	// Copied from an old usenet discussion on the topic:
	// https://groups.google.com/g/comp.programming/c/HSurZEyrZ1E?pli=1#d887b5bdb2dac99d
	// > It's a mixture of superstition and good sense.
	// > Suppose the multiplier were 26, and consider
	// > hashing a hundred-character string. How much influence does
	// > the string's first character have on the final value of `h',
	// > just before the mod operation? The first character's value
	// > will have been multiplied by MULT 99 times, so if the arithmetic
	// > were done in infinite precision the value would consist of some
	// > jumble of bits followed by 99 low-order zero bits -- each time
	// > you multiply by MULT you introduce another low-order zero, right?
	// > The computer's finite arithmetic just chops away all the excess
	// > high-order bits, so the first character's actual contribution to
	// > `h' is ... precisely zero! The `h' value depends only on the
	// > rightmost 32 string characters (assuming a 32-bit int), and even
	// > then things are not wonderful: the first of those final 32 bytes
	// > influences only the leftmost bit of `h' and has no effect on
	// > the remaining 31. Clearly, an even-valued MULT is a poor idea.
	// >
	// > Need MULT be prime? Not as far as I know (I don't know
	// > everything); any odd value ought to suffice. 31 may be attractive
	// > because it is close to a power of two, and it may be easier for
	// > the compiler to replace a possibly slow multiply instruction with
	// > a shift and subtract (31*x == (x << 5) - x) on machines where it
	// > makes a difference. Setting MULT one greater than a power of two
	// > (e.g., 33) would also be easy to optimize, but might produce too
	// > "simple" an arrangement: mostly a juxtaposition of two copies
	// > of the original set of bits, with a little mixing in the middle.
	// > So you want an odd MULT that has plenty of one-bits.

	code := evalengine.HashCode(17)
	for i, checkCol := range pt.checkCols {
		if i >= len(inputRow) {
			return 0, vterrors.VT13001("index out of range in row when creating the DISTINCT hash code")
		}
		col := inputRow[checkCol.Col]
		hashcode, err := evalengine.NullsafeHashcode(col, checkCol.Collation, col.Type())
		if err != nil {
			if err != evalengine.UnsupportedCollationHashError || checkCol.WsCol == nil {
				return 0, err
			}
			checkCol = checkCol.SwitchToWeightString()
			pt.checkCols[i] = checkCol
			hashcode, err = evalengine.NullsafeHashcode(inputRow[checkCol.Col], checkCol.Collation, col.Type())
			if err != nil {
				return 0, err
			}
		}
		code = code*31 + hashcode
	}
	return code, nil
}

func (pt *probeTable) equal(a, b sqltypes.Row) (bool, error) {
	for i, checkCol := range pt.checkCols {
		cmp, err := evalengine.NullsafeCompare(a[i], b[i], checkCol.Collation)
		if err != nil {
			_, isComparisonErr := err.(evalengine.UnsupportedComparisonError)
			if !isComparisonErr || checkCol.WsCol == nil {
				return false, err
			}
			checkCol = checkCol.SwitchToWeightString()
			pt.checkCols[i] = checkCol
			cmp, err = evalengine.NullsafeCompare(a[i], b[i], checkCol.Collation)
			if err != nil {
				return false, err
			}
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func newProbeTable(checkCols []CheckCol) *probeTable {
	cols := make([]CheckCol, len(checkCols))
	copy(cols, checkCols)
	return &probeTable{
		seenRows:  map[evalengine.HashCode][]sqltypes.Row{},
		checkCols: cols,
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

	pt := newProbeTable(d.CheckCols)

	for _, row := range input.Rows {
		exists, err := pt.exists(row)
		if err != nil {
			return nil, err
		}
		if !exists {
			result.Rows = append(result.Rows, row)
		}
	}
	if d.Truncate {
		return result.Truncate(len(d.CheckCols)), nil
	}
	return result, err
}

// TryStreamExecute implements the Primitive interface
func (d *Distinct) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	pt := newProbeTable(d.CheckCols)

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
func (d *Distinct) Inputs() []Primitive {
	return []Primitive{d.Source}
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

	if d.Truncate {
		other["ResultColumns"] = len(d.CheckCols)
	}
	return PrimitiveDescription{
		Other:        other,
		OperatorType: "Distinct",
	}
}

// SwitchToWeightString returns a new CheckCol that works on the weight string column instead
func (cc CheckCol) SwitchToWeightString() CheckCol {
	return CheckCol{
		Col:       *cc.WsCol,
		WsCol:     nil,
		Collation: collations.CollationBinaryID,
	}
}

func (cc CheckCol) String() string {
	coll := collations.Local().LookupByID(cc.Collation)
	var collation string
	if coll != nil {
		collation = ": " + coll.Name()
	}

	var column string
	if cc.WsCol == nil {
		column = fmt.Sprintf("%d", cc.Col)
	} else {
		column = fmt.Sprintf("(%d:%d)", cc.Col, *cc.WsCol)
	}
	return column + collation
}
