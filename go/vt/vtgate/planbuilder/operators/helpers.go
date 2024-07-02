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
	"slices"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type compactable interface {
	// Compact implement this interface for operators that have easy to see optimisations
	Compact(ctx *plancontext.PlanningContext) (Operator, *ApplyResult)
}

// compact will optimise the operator tree into a smaller but equivalent version
func compact(ctx *plancontext.PlanningContext, op Operator) Operator {
	newOp := BottomUp(op, TableID, func(op Operator, _ semantics.TableSet, _ bool) (Operator, *ApplyResult) {
		newOp, ok := op.(compactable)
		if !ok {
			return op, NoRewrite
		}
		return newOp.Compact(ctx)
	}, stopAtRoute)
	return newOp
}

func checkValid(op Operator) {
	type checkable interface {
		CheckValid()
	}

	_ = Visit(op, func(this Operator) error {
		if chk, ok := this.(checkable); ok {
			chk.CheckValid()
		}
		return nil
	})
}

func Clone(op Operator) Operator {
	inputs := op.Inputs()
	clones := make([]Operator, len(inputs))
	for i, input := range inputs {
		clones[i] = Clone(input)
	}
	return op.Clone(clones)
}

// tableIDIntroducer is used to signal that this operator introduces data from a new source
type tableIDIntroducer interface {
	introducesTableID() semantics.TableSet
}

func TableID(op Operator) (result semantics.TableSet) {
	_ = Visit(op, func(this Operator) error {
		if tbl, ok := this.(tableIDIntroducer); ok {
			result = result.Merge(tbl.introducesTableID())
		}
		return nil
	})
	return
}

// TableUser is used to signal that this operator directly interacts with one or more tables
type TableUser interface {
	TablesUsed() []sqlparser.TableName
}

func TablesUsed(op Operator) []sqlparser.TableName {
	addTableName, collect := collectSortedUniqueTableNames()
	_ = Visit(op, func(this Operator) error {
		if tbl, ok := this.(TableUser); ok {
			for _, u := range tbl.TablesUsed() {
				addTableName(u)
			}
		}
		return nil
	})
	return collect()
}

func CostOf(op Operator) (cost int) {
	type costly interface {
		// Cost returns the cost for this operator. All the costly operators in the tree are summed together to get the
		// total cost of the operator tree.
		// TODO: We should really calculate this using cardinality estimation,
		//       but until then this is better than nothing
		Cost() int
	}

	_ = Visit(op, func(op Operator) error {
		if costlyOp, ok := op.(costly); ok {
			cost += costlyOp.Cost()
		}
		return nil
	})
	return
}

func QualifiedTableNames(ks *vindexes.Keyspace, ts []sqlparser.TableName) []sqlparser.TableName {
	add, collect := collectSortedUniqueTableNames()
	for _, t := range ts {
		add(sqlparser.NewTableNameWithQualifier(t.Name.String(), ks.Name))
	}
	return collect()
}

func QualifiedTables(ks *vindexes.Keyspace, vts []*vindexes.Table) []sqlparser.TableName {
	add, collect := collectSortedUniqueTableNames()
	for _, vt := range vts {
		add(sqlparser.NewTableNameWithQualifier(vt.Name.String(), ks.Name))
	}
	return collect()
}

func SingleTableName(ks *vindexes.Keyspace, i sqlparser.IdentifierCS) []sqlparser.TableName {
	return []sqlparser.TableName{sqlparser.NewTableNameWithQualifier(i.String(), ks.Name)}
}

func SortTableNames(ts []sqlparser.TableName) {
	slices.SortFunc[[]sqlparser.TableName, sqlparser.TableName](ts, func(a, b sqlparser.TableName) int {
		if a.Qualifier.NotEmpty() && b.Qualifier.NotEmpty() {
			if cq := strings.Compare(a.Qualifier.String(), b.Qualifier.String()); cq != 0 {
				return cq
			}
			return strings.Compare(a.Name.String(), b.Name.String())
		}
		if a.Qualifier.NotEmpty() {
			return strings.Compare(a.Qualifier.String(), b.Name.String())
		}
		if b.Qualifier.NotEmpty() {
			return strings.Compare(a.Name.String(), b.Qualifier.String())
		}
		return strings.Compare(a.Name.String(), b.Name.String())
	})
}

func collectSortedUniqueTableNames() (add func(sqlparser.TableName), collect func() []sqlparser.TableName) {
	uniq := make(map[sqlparser.TableName]any)

	add = func(v sqlparser.TableName) {
		uniq[v] = nil
	}

	collect = func() []sqlparser.TableName {
		sorted := make([]sqlparser.TableName, 0, len(uniq))
		for v := range uniq {
			sorted = append(sorted, v)
		}

		SortTableNames(sorted)

		return sorted
	}

	return add, collect
}
