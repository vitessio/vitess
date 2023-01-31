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

package schemadiff

import (
	"fmt"

	"vitess.io/vitess/go/mathutil"
	"vitess.io/vitess/go/vt/sqlparser"
)

type DiffDepType int

// diff dependencies in increasing restriction severity
const (
	DiffDepNone DiffDepType = iota
	DiffDepOrderUnknown
	DiffDepInOrderCompletion
	DiffDepSequentialExecution
)

type DiffDep struct {
	diff    EntityDiff
	depDiff EntityDiff
	depType DiffDepType
}

func NewDiffDep(diff EntityDiff, depDiff EntityDiff, depType DiffDepType) *DiffDep {
	return &DiffDep{
		diff:    diff,
		depDiff: depDiff,
		depType: depType,
	}
}

func (d *DiffDep) hashKey() string {
	return d.diff.CanonicalStatementString() + "/" + d.depDiff.CanonicalStatementString()
}

/*
Adapted from https://yourbasic.org/golang/generate-permutation-slice-string/
Licensed under https://creativecommons.org/licenses/by/3.0/
Modified to have an early break
*/

// Perm calls f with each permutation of a.
func permutateDiffs(a []EntityDiff, callback func([]EntityDiff) (earlyBreak bool)) (earlyBreak bool) {
	return permDiff(a, callback, 0)
}

// Permute the values at index i to len(a)-1.
func permDiff(a []EntityDiff, callback func([]EntityDiff) (earlyBreak bool), i int) (earlyBreak bool) {
	if i > len(a) {
		return callback(a)
	}
	if permDiff(a, callback, i+1) {
		return true
	}
	for j := i + 1; j < len(a); j++ {
		a[i], a[j] = a[j], a[i]
		if permDiff(a, callback, i+1) {
			return true
		}
		a[i], a[j] = a[j], a[i]
	}
	return false
}

type SchemaDiff struct {
	schema *Schema
	diffs  []EntityDiff

	diffMap map[string]EntityDiff // key is diff's CanonicalStatementString()
	deps    map[string]*DiffDep

	r *mathutil.EquivalenceRelation
}

func NewSchemaDiff(schema *Schema) *SchemaDiff {
	return &SchemaDiff{
		schema:  schema,
		deps:    make(map[string]*DiffDep),
		diffMap: make(map[string]EntityDiff),
		r:       mathutil.NewEquivalenceRelation(),
	}
}

func (d *SchemaDiff) loadDiffs(diffs []EntityDiff) {
	for _, diff := range diffs {
		allSubsequent := AllSubsequent(diff)
		for i, sdiff := range allSubsequent {
			d.diffs = append(d.diffs, sdiff)
			d.diffMap[sdiff.CanonicalStatementString()] = sdiff
			if i > 0 {
				// So this is a 2nd, 3rd etc. diff operating on same table
				// Two migrations on same entity (table in our case) must run sequentially.
				d.AddDep(sdiff, allSubsequent[0], DiffDepSequentialExecution)
			}
			d.r.Add(sdiff.CanonicalStatementString())
			// since we've exploded the subsequent diffs, we now clear any subsequent diffs
			// so that they do not auto-Apply() when we compute a valid path.
			sdiff.SetSubsequentDiff(nil)
		}
	}
}

func (d *SchemaDiff) AddDep(diff EntityDiff, depDiff EntityDiff, depType DiffDepType) *DiffDep {
	_, _ = d.r.Relate(diff.CanonicalStatementString(), depDiff.CanonicalStatementString())
	diffDep := NewDiffDep(diff, depDiff, depType)
	if existingDep, ok := d.deps[diffDep.hashKey()]; ok {
		if existingDep.depType >= diffDep.depType {
			// nothing new here.
			return existingDep
		}
	}
	// Either the dep wasn't found, or we've just introduced a dep with a more severe type
	d.deps[diffDep.hashKey()] = diffDep
	return diffDep
}

func (d *SchemaDiff) DiffByStatementString(s string) (EntityDiff, bool) {
	diff, ok := d.diffMap[s]
	return diff, ok
}

func (d *SchemaDiff) DiffByStatement(stmt sqlparser.Statement) (EntityDiff, bool) {
	s := sqlparser.CanonicalString(stmt)
	return d.DiffByStatementString(s)
}

func (d *SchemaDiff) DiffsByEntityName(name string) (diffs []EntityDiff) {
	for _, diff := range d.diffs {
		if diff.EntityName() == name {
			diffs = append(diffs, diff)
		}
	}
	return diffs
}

func (d *SchemaDiff) AllDiffs() []EntityDiff {
	return d.diffs
}

func (d *SchemaDiff) AllDeps() (deps []*DiffDep) {
	for _, dep := range d.deps {
		deps = append(deps, dep)
	}
	return deps
}

func (d *SchemaDiff) HasDeps() bool {
	return len(d.deps) > 0
}

func (d *SchemaDiff) AllSequentialExecutionDeps() (deps []*DiffDep) {
	for _, dep := range d.deps {
		if dep.depType >= DiffDepSequentialExecution {
			deps = append(deps, dep)
		}
	}
	return deps
}

func (d *SchemaDiff) HasSequentialExecutionDeps() bool {
	for _, dep := range d.deps {
		if dep.depType >= DiffDepSequentialExecution {
			return true
		}
	}
	return false
}

func (d *SchemaDiff) OrderedDiffs() ([]EntityDiff, error) {
	lastGoodSchema := d.schema
	var orderedDiffs []EntityDiff
	m := d.r.Map()
	for _, class := range d.r.OrderedClasses() {
		classDiffs := []EntityDiff{}
		for _, statementString := range m[class] {
			diff, ok := d.DiffByStatementString(statementString)
			if !ok {
				return nil, fmt.Errorf("unexpected error: cannot find diff: %v", statementString)
			}
			classDiffs = append(classDiffs, diff)
		}
		foundValidPathForClass := permutateDiffs(classDiffs, func(permutatedDiffs []EntityDiff) bool {
			// We want to apply the changes one by one, and validate the schema after each change
			for i := range permutatedDiffs {
				permutationSchema, err := lastGoodSchema.Apply(permutatedDiffs[i : i+1])
				if err != nil {
					// permutation is invalid
					return false // continue searching
				}
				lastGoodSchema = permutationSchema
			}
			// Good news, we managed to apply all of the permutations!
			orderedDiffs = append(orderedDiffs, permutatedDiffs...)
			return true // early break! No need to keep searching
		})
		if !foundValidPathForClass {
			return nil, ErrImpossibleDiffOrder
		}
	}
	return orderedDiffs, nil
}
