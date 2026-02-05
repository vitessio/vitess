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

package semantics

import (
	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	// TransitiveClosures expression equality, so we can rewrite expressions efficiently
	// It allows for transitive closures (e.g., if a == b and b == c, then a == c).
	TransitiveClosures struct {
		// ExprIDs stores the IDs of the expressions.
		// The ID can then be used to get other equivalent expressions from the Equalities slice
		exprIDs map[sqlparser.Expr]int

		// notMapKeys stores expressions that are not valid as map keys
		notMapKeys []ExprWithID

		// Both the above fields are used to find the offset into this slice
		equalities [][]sqlparser.Expr
	}

	// ExprWithID is used to keep track of expressions that are not valid as map keys
	ExprWithID struct {
		Expr sqlparser.Expr
		ID   int
	}

	SemanticColName struct {
		Name      sqlparser.IdentifierCI
		Qualifier sqlparser.TableName
	}
)

// NewTransitiveClosures creates a new TransitiveClosures
func NewTransitiveClosures() *TransitiveClosures {
	return &TransitiveClosures{
		exprIDs: make(map[sqlparser.Expr]int),
	}
}

func (tc *TransitiveClosures) getID(e sqlparser.Expr, comparer func(a, b sqlparser.Expr) bool) (int, bool) {
	if !ValidAsMapKey(e) {
		for _, id := range tc.notMapKeys {
			if comparer(id.Expr, e) {
				return id.ID, true
			}
		}

		return 0, false
	}

	// first let's try to find the expression in the map
	id, found := tc.exprIDs[e]
	if found {
		return id, found
	}

	// we might still have a match in the map, just not by reference
	for expr, id := range tc.exprIDs {
		if comparer(expr, e) {
			defer func() {
				// we do this in the defer so we don't mutate the map while iterating over it
				tc.exprIDs[e] = id
			}()
			return id, true
		}
	}

	return 0, false
}

// setID should be called after updating the equalities slice, so that the ID we are setting actually exists.
// e.g. call Add adds to the slice and then calls setID.
func (tc *TransitiveClosures) setID(in sqlparser.Expr, id int, comparer func(a, b sqlparser.Expr) bool) {
	if ValidAsMapKey(in) {
		// we need to use the comparer to find the expression in the map
		for expr := range tc.exprIDs {
			if comparer(expr, in) {
				defer func() {
					// we do this in the defer so we don't mutate the map while iterating over it
					tc.exprIDs[expr] = id
				}()
			}
		}

		tc.exprIDs[in] = id
		return
	}

	var foundMatch bool
	for idx, notMap := range tc.notMapKeys {
		if comparer(notMap.Expr, in) {
			foundMatch = true
			tc.notMapKeys[idx].ID = id
		}
	}
	if !foundMatch {
		tc.notMapKeys = append(tc.notMapKeys, ExprWithID{Expr: in, ID: id})
	}
}

// Add adds a new equality to the TransitiveClosures
func (tc *TransitiveClosures) Add(lhs, rhs sqlparser.Expr, comparer func(a, b sqlparser.Expr) bool) {
	lhsID, leftKnown := tc.getID(lhs, comparer)
	rhsID, rightKnown := tc.getID(rhs, comparer)
	if !leftKnown && !rightKnown {
		// neither expression is known
		id := len(tc.equalities)
		tc.equalities = append(tc.equalities, []sqlparser.Expr{lhs, rhs})
		tc.setID(lhs, id, comparer)
		tc.setID(rhs, id, comparer)
		return
	}

	if !leftKnown {
		// lhs is not known
		tc.equalities[rhsID] = append(tc.equalities[rhsID], lhs)
		tc.setID(lhs, rhsID, comparer)
		return
	}

	if !rightKnown {
		// rhs is not known
		tc.equalities[lhsID] = append(tc.equalities[lhsID], rhs)
		tc.setID(rhs, lhsID, comparer)
		return
	}

	// merge the two sets
	if lhsID != rhsID {
		var smallerID, largerID int
		var smaller, larger []sqlparser.Expr

		if len(tc.equalities[lhsID]) < len(tc.equalities[rhsID]) {
			smallerID, largerID = lhsID, rhsID
		} else {
			smallerID, largerID = rhsID, lhsID
		}
		smaller = tc.equalities[smallerID]
		larger = tc.equalities[largerID]

		tc.equalities[largerID] = append(larger, smaller...)
		// we don't want to shuffle any elements around, so we just set the smaller set to nil
		tc.equalities[smallerID] = nil

		for _, expr := range smaller {
			tc.setID(expr, largerID, comparer)
		}
	}
}

// Foreach calls the given function for all expressions that are equal to the given expression
func (tc *TransitiveClosures) Foreach(expr sqlparser.Expr, comparer func(a, b sqlparser.Expr) bool, f func(expr sqlparser.Expr) error) error {
	// always start with the given expression
	if err := f(expr); err != nil {
		return err
	}

	id, found := tc.getID(expr, comparer)
	if !found {
		return nil
	}

	for _, e := range tc.equalities[id] {
		err := f(e)
		if err != nil {
			return err
		}
	}
	return nil
}
