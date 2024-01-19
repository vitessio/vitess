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
	"context"
	"fmt"
	"sort"

	"vitess.io/vitess/go/mathutil"
)

type DiffDependencyType int

// diff dependencies in increasing restriction severity
const (
	DiffDependencyNone DiffDependencyType = iota // not a dependency
	DiffDependencyOrderUnknown
	DiffDependencyInOrderCompletion
	DiffDependencySequentialExecution
)

// DiffDependency indicates a dependency between two diffs, and the type of that dependency
type DiffDependency struct {
	diff          EntityDiff
	dependentDiff EntityDiff // depends on the above diff
	typ           DiffDependencyType
}

// NewDiffDependency returns a new diff dependency pairing.
func NewDiffDependency(diff EntityDiff, dependentDiff EntityDiff, typ DiffDependencyType) *DiffDependency {
	return &DiffDependency{
		diff:          diff,
		dependentDiff: dependentDiff,
		typ:           typ,
	}
}

func (d *DiffDependency) hashKey() string {
	return d.diff.CanonicalStatementString() + "/" + d.dependentDiff.CanonicalStatementString()
}

// Diff returns the "benefactor" diff, on which DependentDiff() depends on, ie, should run 1st.
func (d *DiffDependency) Diff() EntityDiff {
	return d.diff
}

// DependentDiff returns the diff that depends on the "benefactor" diff, ie must run 2nd
func (d *DiffDependency) DependentDiff() EntityDiff {
	return d.dependentDiff
}

// Type returns the dependency type. Types are numeric and comparable: the higher the value, the
// stricter, or more constrained, the dependency is.
func (d *DiffDependency) Type() DiffDependencyType {
	return d.typ
}

// IsInOrder returns true if this dependency indicates a known order
func (d *DiffDependency) IsInOrder() bool {
	return d.typ >= DiffDependencyInOrderCompletion
}

// IsSequential returns true if this is a sequential dependency
func (d *DiffDependency) IsSequential() bool {
	return d.typ >= DiffDependencySequentialExecution
}

/*
The below is adapted from https://yourbasic.org/golang/generate-permutation-slice-string/
Licensed under https://creativecommons.org/licenses/by/3.0/
Modified to have an early break
*/

// permutateDiffs calls `callback` with each permutation of a. If the function returns `true`, that means
// the callback has returned `true` for an early break, thus possibly not all permutations have been evaluated.
func permutateDiffs(ctx context.Context, diffs []EntityDiff, callback func([]EntityDiff) (earlyBreak bool)) (earlyBreak bool, err error) {
	if len(diffs) == 0 {
		return false, nil
	}
	// Sort by a heuristic (DROPs first, ALTERs next, CREATEs last). This ordering is then used first in the permutation
	// search and serves as seed for the rest of permutations.

	return permDiff(ctx, diffs, callback, 0)
}

// permDiff is a recursive function to permutate given `a` and call `callback` for each permutation.
// If `callback` returns `true`, then so does this function, and this indicates a request for an early
// break, in which case this function will not be called again.
func permDiff(ctx context.Context, a []EntityDiff, callback func([]EntityDiff) (earlyBreak bool), i int) (earlyBreak bool, err error) {
	if err := ctx.Err(); err != nil {
		return true, err // early break
	}
	if i > len(a) {
		return callback(a), nil
	}
	if brk, err := permDiff(ctx, a, callback, i+1); brk {
		return true, err
	}
	for j := i + 1; j < len(a); j++ {
		// An optimization: we don't really need all possible permutations. We can skip some of the recursive search.
		// We know we begin with a heuristic order where DROP VIEW comes first, then DROP TABLE, then ALTER TABLE & VIEW,
		// then CREATE TABLE, then CREATE VIEW. And the entities in that initial order are sorted by dependency. That's
		// thank's to Schema's UnorderedDiffs() existing heuristic.
		// Now, some pairs of statements should be permutated, but some others will have absolutely no advantage to permutate.
		// For example, a DROP VIEW and CREATE VIEW: there's no advantage to permutate the two. If the initial order is
		// inapplicable, then so will be the permutated order.
		// The next section identifies some no-brainers conditions for skipping swapping of elements.
		// There could be even more fine grained scenarios, which we can deal with in the future.
		iIsCreateDropView := false
		iIsTable := false
		switch a[i].(type) {
		case *DropViewEntityDiff, *CreateViewEntityDiff:
			iIsCreateDropView = true
		case *DropTableEntityDiff, *AlterTableEntityDiff, *CreateTableEntityDiff:
			iIsTable = true
		}

		jIsCreateDropView := false
		jIsTable := false
		switch a[j].(type) {
		case *DropViewEntityDiff, *CreateViewEntityDiff:
			jIsCreateDropView = true
		case *DropTableEntityDiff, *AlterTableEntityDiff, *CreateTableEntityDiff:
			jIsTable = true
		}

		if iIsCreateDropView && jIsCreateDropView {
			continue
		}
		if iIsCreateDropView && jIsTable {
			continue
		}
		if iIsTable && jIsCreateDropView {
			continue
		}
		// End of optimization
		a[i], a[j] = a[j], a[i]
		if brk, err := permDiff(ctx, a, callback, i+1); brk {
			return true, err
		}
		a[i], a[j] = a[j], a[i]
	}
	return false, nil
}

// SchemaDiff is a rich diff between two schemas. It includes the following:
// - The source schema (on which the diff would operate)
// - A list of SQL diffs (e.g. CREATE VIEW, ALTER TABLE, ...)
// - A map of dependencies between the diffs
// Operations on SchemaDiff are not concurrency-safe.
type SchemaDiff struct {
	schema *Schema
	diffs  []EntityDiff

	diffMap      map[string]EntityDiff // key is diff's CanonicalStatementString()
	dependencies map[string]*DiffDependency

	r *mathutil.EquivalenceRelation // internal structure to help determine diffs
}

func NewSchemaDiff(schema *Schema) *SchemaDiff {
	return &SchemaDiff{
		schema:       schema,
		dependencies: make(map[string]*DiffDependency),
		diffMap:      make(map[string]EntityDiff),
		r:            mathutil.NewEquivalenceRelation(),
	}
}

// loadDiffs loads a list of diffs, as generated by Schema.Diff(other) function. It explodes all subsequent diffs
// into distinct diffs (which then have no subsequent diffs). Thus, the list of diffs loaded can be longer than the
// list of diffs received.
func (d *SchemaDiff) loadDiffs(diffs []EntityDiff) {
	for _, diff := range diffs {
		allSubsequent := AllSubsequent(diff)
		for i, sdiff := range allSubsequent {
			d.diffs = append(d.diffs, sdiff)
			d.diffMap[sdiff.CanonicalStatementString()] = sdiff
			if i > 0 {
				// So this is a 2nd, 3rd etc. diff operating on same table
				// Two migrations on same entity (table in our case) must run sequentially.
				d.addDep(sdiff, allSubsequent[0], DiffDependencySequentialExecution)
			}
			d.r.Add(sdiff.CanonicalStatementString())
			// since we've exploded the subsequent diffs, we now clear any subsequent diffs
			// so that they do not auto-Apply() when we compute a valid path.
			sdiff.SetSubsequentDiff(nil)
		}
	}
}

// addDep adds a dependency: `dependentDiff` depends on `diff`, with given `depType`. If there's an
// already existing dependency between the two diffs, then we compare the dependency type; if the new
// type has a higher order (ie stricter) then we replace the existing dependency with the new one.
func (d *SchemaDiff) addDep(diff EntityDiff, dependentDiff EntityDiff, typ DiffDependencyType) *DiffDependency {
	_, _ = d.r.Relate(diff.CanonicalStatementString(), dependentDiff.CanonicalStatementString())
	diffDep := NewDiffDependency(diff, dependentDiff, typ)
	if existingDep, ok := d.dependencies[diffDep.hashKey()]; ok {
		if existingDep.typ >= diffDep.typ {
			// nothing new here, the new dependency is weaker or equals to an existing dependency
			return existingDep
		}
	}
	// Either the dep wasn't found, or we've just introduced a dep with a more severe type
	d.dependencies[diffDep.hashKey()] = diffDep
	return diffDep
}

// diffByStatementString is a utility function that returns a diff by its canonical statement string
func (d *SchemaDiff) diffByStatementString(s string) (EntityDiff, bool) {
	diff, ok := d.diffMap[s]
	return diff, ok
}

// diffsByEntityName returns all diffs that apply to a given entity (table/view)
func (d *SchemaDiff) diffsByEntityName(name string) (diffs []EntityDiff) {
	for _, diff := range d.diffs {
		if diff.EntityName() == name {
			diffs = append(diffs, diff)
		}
	}
	return diffs
}

// Empty returns 'true' when there are no diff entries
func (d *SchemaDiff) Empty() bool {
	return len(d.diffs) == 0
}

// UnorderedDiffs returns all the diffs. These are not sorted by dependencies. These are basically
// the original diffs, but "flattening" any subsequent diffs they may have. as result:
// - Diffs in the returned slice have no subsequent diffs
// - The returned slice may be longer than the number of diffs supplied by loadDiffs()
func (d *SchemaDiff) UnorderedDiffs() []EntityDiff {
	return d.diffs
}

// AllDependenciess returns all known dependencies
func (d *SchemaDiff) AllDependenciess() (deps []*DiffDependency) {
	for _, dep := range d.dependencies {
		deps = append(deps, dep)
	}
	return deps
}

// HasDependencies returns `true` if there is at least one known diff dependency.
// If this function returns `false` then that means there is no restriction whatsoever to the order of diffs.
func (d *SchemaDiff) HasDependencies() bool {
	return len(d.dependencies) > 0
}

// AllSequentialExecutionDependencies returns all diffs that are of "sequential execution" type.
func (d *SchemaDiff) AllSequentialExecutionDependencies() (deps []*DiffDependency) {
	for _, dep := range d.dependencies {
		if dep.typ >= DiffDependencySequentialExecution {
			deps = append(deps, dep)
		}
	}
	return deps
}

// HasSequentialExecutionDependencies return `true` if there is at least one "subsequential execution" type diff.
// If not, that means all diffs can be applied in parallel.
func (d *SchemaDiff) HasSequentialExecutionDependencies() bool {
	for _, dep := range d.dependencies {
		if dep.typ >= DiffDependencySequentialExecution {
			return true
		}
	}
	return false
}

// OrderedDiffs returns the list of diff in applicable order, if possible. This is a linearized representation
// where diffs may be applied in-order one after another, keeping the schema in valid state at all times.
func (d *SchemaDiff) OrderedDiffs(ctx context.Context) ([]EntityDiff, error) {
	lastGoodSchema := d.schema.copy()
	var orderedDiffs []EntityDiff
	m := d.r.Map()

	unorderedDiffsMap := map[string]int{}
	for i, diff := range d.UnorderedDiffs() {
		unorderedDiffsMap[diff.CanonicalStatementString()] = i
	}
	// The order of classes in the equivalence relation is, generally speaking, loyal to the order of original diffs.
	for _, class := range d.r.OrderedClasses() {
		classDiffs := []EntityDiff{}
		// Which diffs are in this equivalence class?
		for _, statementString := range m[class] {
			diff, ok := d.diffByStatementString(statementString)
			if !ok {
				return nil, fmt.Errorf("unexpected error: cannot find diff: %v", statementString)
			}
			classDiffs = append(classDiffs, diff)
		}
		sort.SliceStable(classDiffs, func(i, j int) bool {
			return unorderedDiffsMap[classDiffs[i].CanonicalStatementString()] < unorderedDiffsMap[classDiffs[j].CanonicalStatementString()]
		})

		// We will now permutate the diffs in this equivalence class, and hopefully find
		// a valid permutation (one where if we apply the diffs in-order, the schema remains valid throughout the process)
		foundValidPathForClass, err := permutateDiffs(ctx, classDiffs, func(permutatedDiffs []EntityDiff) bool {
			permutationSchema := lastGoodSchema.copy()
			// We want to apply the changes one by one, and validate the schema after each change
			for i := range permutatedDiffs {
				// apply inline
				if err := permutationSchema.apply(permutatedDiffs[i : i+1]); err != nil {
					// permutation is invalid
					return false // continue searching
				}
			}
			// Good news, we managed to apply all of the permutations!
			orderedDiffs = append(orderedDiffs, permutatedDiffs...)
			lastGoodSchema = permutationSchema
			return true // early break! No need to keep searching
		})
		if err != nil {
			return nil, err
		}
		if !foundValidPathForClass {
			// In this equivalence class, there is no valid permutation. We cannot linearize the diffs.
			return nil, &ImpossibleApplyDiffOrderError{
				UnorderedDiffs:   d.UnorderedDiffs(),
				ConflictingDiffs: classDiffs,
			}
		}

		// Done taking care of this equivalence class.
	}
	return orderedDiffs, nil
}

// InstantDDLCapability returns an overall summary of the ability of the diffs to run with ALGORITHM=INSTANT.
// It is a convenience method, whose logic anyone can reimplement.
func (d *SchemaDiff) InstantDDLCapability() InstantDDLCapability {
	// The general logic: we return "InstantDDLCapabilityPossible" if there is one or more diffs that is capable of
	// ALGORITHM=INSTANT, and zero or more diffs that are irrelevant, and no diffs that are impossible to run with
	// ALGORITHM=INSTANT.
	capability := InstantDDLCapabilityIrrelevant
	for _, diff := range d.UnorderedDiffs() {
		switch diff.InstantDDLCapability() {
		case InstantDDLCapabilityUnknown:
			return InstantDDLCapabilityUnknown // Early break
		case InstantDDLCapabilityImpossible:
			return InstantDDLCapabilityImpossible // Early break
		case InstantDDLCapabilityPossible:
			capability = InstantDDLCapabilityPossible
		case InstantDDLCapabilityIrrelevant:
			// do nothing
		}
	}
	return capability
}
