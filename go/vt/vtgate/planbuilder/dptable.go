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

package planbuilder

import "vitess.io/vitess/go/vt/vtgate/semantics"

// dpTable, is the hashmap we store results in during
// the dynamic programming part of query planning
type dpTable struct {
	// hash map of the best solution for each seen table
	m map[semantics.TableSet]joinTree

	highest semantics.TableSet
}

func makeDPTable() *dpTable {
	return &dpTable{
		m: map[semantics.TableSet]joinTree{},
	}
}

func (dpt *dpTable) add(tree joinTree) {
	solved := tree.solves()
	if dpt.highest < solved {
		dpt.highest = solved
	}
	dpt.m[solved] = tree
}

func (dpt *dpTable) planFor(id semantics.TableSet) joinTree {
	return dpt.m[id]
}

func (dpt *dpTable) bitSetsOfSize(wanted int) []joinTree {
	var result []joinTree
	for x := semantics.TableSet(1); x <= dpt.highest; x++ {
		if x.NumberOfTables() == wanted {
			t, ok := dpt.m[x]
			if ok {
				result = append(result, t)
			}
		}
	}
	return result
}
