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
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	// Tracker is used to track expressions in join predicates and their IDs.
	// It is used to update predicates after being pushed down when the
	// join they belong to changes - like being pushed under a route
	Tracker struct {
		lastID      ID
		expressions map[ID]sqlparser.Expr
	}

	// ID is a unique key that references the current expression a join predicate represents.
	ID int
)

func NewTracker() *Tracker {
	return &Tracker{
		expressions: make(map[ID]sqlparser.Expr),
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
