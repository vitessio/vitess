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

type SchemaDiff struct {
	diffs []EntityDiff

	deps    map[string]*DiffDep
	diffMap map[string]EntityDiff // key is diff's CanonicalStatementString()
}

func NewSchemaDiff() *SchemaDiff {
	return &SchemaDiff{
		deps:    make(map[string]*DiffDep),
		diffMap: make(map[string]EntityDiff),
	}
}

func (d *SchemaDiff) LoadDiffs(diffs []EntityDiff) {
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
		}
	}
}

func (d *SchemaDiff) AddDep(diff EntityDiff, depDiff EntityDiff, depType DiffDepType) *DiffDep {
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
