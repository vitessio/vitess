/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"fmt"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// DMLWithInput is used to represent a DML Operator taking input from a Source Operator
type DMLWithInput struct {
	Source Operator
	DML    Operator

	cols    []*sqlparser.ColName
	Offsets []int

	noColumns
	noPredicates
}

func (d *DMLWithInput) Clone(inputs []Operator) Operator {
	newD := *d
	newD.SetInputs(inputs)
	return &newD
}

func (d *DMLWithInput) Inputs() []Operator {
	return []Operator{d.Source, d.DML}
}

func (d *DMLWithInput) SetInputs(inputs []Operator) {
	if len(inputs) != 2 {
		panic("unexpected number of inputs for DMLWithInput operator")
	}
	d.Source = inputs[0]
	d.DML = inputs[1]
}

func (d *DMLWithInput) ShortDescription() string {
	colStrings := slice.Map(d.cols, func(from *sqlparser.ColName) string {
		return sqlparser.String(from)
	})
	out := ""
	for idx, colString := range colStrings {
		out += colString
		if len(d.Offsets) > idx {
			out += fmt.Sprintf(":%d", d.Offsets[idx])
		}
		out += " "
	}
	return out
}

func (d *DMLWithInput) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return nil
}

func (d *DMLWithInput) planOffsets(ctx *plancontext.PlanningContext) Operator {
	for _, col := range d.cols {
		offset := d.Source.AddColumn(ctx, true, false, aeWrap(col))
		d.Offsets = append(d.Offsets, offset)
	}
	return d
}

var _ Operator = (*DMLWithInput)(nil)
