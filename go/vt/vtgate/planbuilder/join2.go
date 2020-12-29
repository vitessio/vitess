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

package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*join2)(nil)

// join is used to build a Join primitive.
// It's used to build a normal join or a left join
// operation.
type join2 struct {
	// Left and Right are the nodes for the join.
	Left, Right logicalPlan
	Cols        []int
	Vars        map[string]int
}

// Order implements the logicalPlan interface
func (j *join2) Order() int {
	panic("implement me")
}

// ResultColumns implements the logicalPlan interface
func (j *join2) ResultColumns() []*resultColumn {
	panic("implement me")
}

// Reorder implements the logicalPlan interface
func (j *join2) Reorder(i int) {
	panic("implement me")
}

// Wireup implements the logicalPlan interface
func (j *join2) Wireup(lp logicalPlan, jt *jointab) error {
	panic("implement me")
}

// Wireup2 implements the logicalPlan interface
func (j *join2) Wireup2(semTable *semantics.SemTable) error {
	err := j.Left.Wireup2(semTable)
	if err != nil {
		return err
	}
	return j.Right.Wireup2(semTable)
}

// SupplyVar implements the logicalPlan interface
func (j *join2) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("implement me")
}

// SupplyCol implements the logicalPlan interface
func (j *join2) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	panic("implement me")
}

// SupplyWeightString implements the logicalPlan interface
func (j *join2) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	panic("implement me")
}

// Primitive implements the logicalPlan interface
func (j *join2) Primitive() engine.Primitive {
	return &engine.Join{
		Left:  j.Left.Primitive(),
		Right: j.Right.Primitive(),
		Cols:  j.Cols,
		Vars:  j.Vars,
	}
}

// Inputs implements the logicalPlan interface
func (j *join2) Inputs() []logicalPlan {
	panic("implement me")
}

// Rewrite implements the logicalPlan interface
func (j *join2) Rewrite(inputs ...logicalPlan) error {
	panic("implement me")
}

// Solves implements the logicalPlan interface
func (j *join2) Solves() semantics.TableSet {
	return j.Left.Solves().Merge(j.Right.Solves())
}
