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
	"fmt"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// Distinct Primitive is used to uniqueify results
var _ Primitive = (*Distinct)(nil)

// Distinct Primitive is used to uniqueify results
type Distinct struct {
	Source    Primitive
	CheckCols []CheckCol
	Truncate  bool
}

type CheckCol struct {
	Col       int
	WsCol     *int
	Collation collations.ID
}

func (cc CheckCol) String() string {
	coll := collations.Local().LookupByID(cc.Collation)
	var collation string
	if coll == nil {
		collation = ""
	} else {
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

type probeTable struct {
	seenRows  map[evalengine.HashCode][]sqltypes.Row
	checkCols []CheckCol
}

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
		exists, err := equal(existingRow, inputRow, pt.checkCols)
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
			return 0, vterrors.New(vtrpcpb.Code_INTERNAL, "distinct check colls is larger than its input row")
		}
		col := inputRow[i]
		hashcode, err := evalengine.NullsafeHashcode(col, checkCol.Collation, col.Type())
		if err != nil {
			return 0, err
		}
		code = code*31 + hashcode
	}
	return code, nil
}

func equal(a, b []sqltypes.Value, checkCols []CheckCol) (bool, error) {
	for i, col := range checkCols {
		cmp, err := evalengine.NullsafeCompare(a[i], b[i], col.Collation)
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func newProbeTable(checkCols []CheckCol) *probeTable {
	return &probeTable{
		seenRows:  map[uintptr][]sqltypes.Row{},
		checkCols: checkCols,
	}
}

// TryExecute implements the Primitive interface
func (d *Distinct) TryExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	input, err := vcursor.ExecutePrimitive(d.Source, bindVars, wantfields)
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
func (d *Distinct) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	pt := newProbeTable(d.CheckCols)

	err := vcursor.StreamExecutePrimitive(d.Source, bindVars, wantfields, func(input *sqltypes.Result) error {
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
