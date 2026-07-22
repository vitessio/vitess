/*
Copyright 2025 The Vitess Authors.

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

package predicates

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// Tracker is used to track expressions in join predicates and their IDs.
	// It is used to update predicates after being pushed down when the
	// join they belong to changes - like being pushed under a route
	Tracker struct {
		lastID      ID
		expressions map[ID]sqlparser.Expr

		// children tracks per-source copies of a predicate, such as the copies
		// created when a predicate is pushed into every source of a UNION.
		// When the original predicate is restored or skipped, the copies must
		// be skipped as well - they cannot be restored to column form inside
		// their source, and after a merge no one produces their arguments.
		children map[ID][]ID
	}

	// ID is a unique key that references the current expression a join predicate represents.
	ID int
)

func NewTracker() *Tracker {
	return &Tracker{
		expressions: make(map[ID]sqlparser.Expr),
		children:    make(map[ID][]ID),
	}
}

func (t *Tracker) NewJoinPredicate(org sqlparser.Expr) *JoinPredicate {
	nextID := t.nextID()
	t.expressions[nextID] = org
	return &JoinPredicate{
		ID:      nextID,
		tracker: t,
	}
}

// NewChildJoinPredicate creates a new JoinPredicate that is tracked as a copy of the
// given parent predicate, so that Skip/restore operations on the parent cascade to it.
func (t *Tracker) NewChildJoinPredicate(parent *JoinPredicate, org sqlparser.Expr) *JoinPredicate {
	jp := t.NewJoinPredicate(org)
	t.children[parent.ID] = append(t.children[parent.ID], jp.ID)
	return jp
}

// DescendantIDs returns the IDs of all transitive copies of the given predicate.
func (t *Tracker) DescendantIDs(id ID) (ids []ID) {
	for _, child := range t.children[id] {
		ids = append(ids, child)
		ids = append(ids, t.DescendantIDs(child)...)
	}
	return
}

func (t *Tracker) nextID() ID {
	id := t.lastID
	t.lastID++
	return id
}

func (t *Tracker) Set(id ID, expr sqlparser.Expr) {
	t.expressions[id] = expr
}

func (t *Tracker) Get(id ID) (sqlparser.Expr, error) {
	expr, found := t.expressions[id]
	if !found {
		return nil, vterrors.VT13001("expression not found")
	}
	return expr, nil
}

func (t *Tracker) Skip(id ID) {
	t.expressions[id] = nil
}

// SkipWithDescendants skips the given predicate and all its transitive copies.
func (t *Tracker) SkipWithDescendants(id ID) {
	t.Skip(id)
	for _, child := range t.children[id] {
		t.SkipWithDescendants(child)
	}
}
