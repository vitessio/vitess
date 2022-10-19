/*
Copyright 2022 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func VisitTopDown(root Operator, visitor func(Operator) error) error {
	queue := []Operator{root}
	for len(queue) > 0 {
		this := queue[0]
		queue = append(queue[1:], this.Inputs()...)
		err := visitor(this)
		if err != nil {
			return err
		}
	}
	return nil
}

func TableID(op Operator) (result semantics.TableSet) {
	_ = VisitTopDown(op, func(this Operator) error {
		if tbl, ok := this.(tableIDIntroducer); ok {
			result.MergeInPlace(tbl.Introduces())
		}
		return nil
	})
	return
}

func unresolvedPredicates(op Operator, st *semantics.SemTable) (result []sqlparser.Expr) {
	_ = VisitTopDown(op, func(this Operator) error {
		if tbl, ok := this.(unresolved); ok {
			result = append(result, tbl.UnsolvedPredicates(st)...)
		}

		return nil
	})
	return
}

func CheckValid(op Operator) error {
	return VisitTopDown(op, func(this Operator) error {
		if chk, ok := this.(checked); ok {
			return chk.CheckValid()
		}
		return nil
	})
}

func CostOf(op Operator) (cost int) {
	_ = VisitTopDown(op, func(op Operator) error {
		if costlyOp, ok := op.(costly); ok {
			cost += costlyOp.Cost()
		}
		return nil
	})
	return
}

func Clone(op Operator) Operator {
	inputs := op.Inputs()
	clones := make([]Operator, len(inputs))
	for i, input := range inputs {
		clones[i] = Clone(input)
	}
	return op.Clone(clones)
}

func checkSize(inputs []Operator, shouldBe int) {
	if len(inputs) != shouldBe {
		panic(fmt.Sprintf("BUG: got the wrong number of inputs: got %d, expected %d", len(inputs), shouldBe))
	}
}
