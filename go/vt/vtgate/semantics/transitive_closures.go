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

		comparer func(a, b sqlparser.Expr) bool
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

func (ce *TransitiveClosures) getID(e sqlparser.Expr, comparer func(a, b sqlparser.Expr) bool) (int, bool) {
	if !ValidAsMapKey(e) {
		for _, id := range ce.notMapKeys {
			if comparer(id.Expr, e) {
				return id.ID, true
			}
		}

		return 0, false
	}

	// first let's try to find the expression in the map
	id, found := ce.exprIDs[e]
	if found {
		return id, found
	}

	// we might still have a match in the map, just not by reference
	for expr, id := range ce.exprIDs {
		if comparer(expr, e) {
			defer func() {
				// we do this in the defer so we don't mutate the map while iterating over it
				ce.exprIDs[e] = id
			}()
			return id, true
		}
	}

	return 0, false
}

func (ce *TransitiveClosures) setID(in sqlparser.Expr, id int, comparer func(a, b sqlparser.Expr) bool) {
	if ValidAsMapKey(in) {
		// we need to use the comparer to find the expression in the map
		for expr := range ce.exprIDs {
			if comparer(expr, in) {
				defer func() {
					// we do this in the defer so we don't mutate the map while iterating over it
					ce.exprIDs[expr] = id
				}()
			}
		}

		ce.exprIDs[in] = id
		return
	}

	for idx, notMap := range ce.notMapKeys {
		if comparer(notMap.Expr, in) {
			ce.notMapKeys[idx].ID = id
			return
		}
	}
	ce.notMapKeys = append(ce.notMapKeys, ExprWithID{Expr: in, ID: id})
}

// Add adds a new equality to the TransitiveClosures
func (ce *TransitiveClosures) Add(lhs, rhs sqlparser.Expr, comparer func(a, b sqlparser.Expr) bool) {
	lhsID, lok := ce.getID(lhs, comparer)
	rhsID, rok := ce.getID(rhs, comparer)
	if !lok && !rok {
		// neither expression is known
		id := len(ce.equalities)
		ce.equalities = append(ce.equalities, []sqlparser.Expr{lhs, rhs})
		ce.setID(lhs, id, comparer)
		ce.setID(rhs, id, comparer)
		return
	}

	if !lok {
		// lhs is not known
		ce.equalities[rhsID] = append(ce.equalities[rhsID], lhs)
		ce.setID(lhs, rhsID, comparer)
		return
	}

	if !rok {
		// rhs is not known
		ce.equalities[lhsID] = append(ce.equalities[lhsID], rhs)
		ce.setID(rhs, lhsID, comparer)
	}

	// merge the two sets
	if lhsID != rhsID {
		var smallerID, largerID int
		var smaller, larger []sqlparser.Expr

		if len(ce.equalities[lhsID]) < len(ce.equalities[rhsID]) {
			smallerID = lhsID
			largerID = rhsID
			smaller = ce.equalities[lhsID]
			larger = ce.equalities[rhsID]
		} else {
			smallerID = rhsID
			largerID = lhsID
			smaller = ce.equalities[rhsID]
			larger = ce.equalities[lhsID]
		}

		ce.equalities[largerID] = append(larger, smaller...)
		// we don't want to shuffle any elements around, so we just set the smaller set to nil
		ce.equalities[smallerID] = nil

		for _, expr := range smaller {
			ce.setID(expr, largerID, comparer)
		}
	}
}

// Get returns all expressions that are equal to the given expression
func (ce *TransitiveClosures) Get(expr sqlparser.Expr, comparer func(a, b sqlparser.Expr) bool) []sqlparser.Expr {
	id, found := ce.getID(expr, comparer)
	if !found {
		return []sqlparser.Expr{expr}
	}

	return ce.equalities[id]
}

// Foreach calls the given function for all expressions that are equal to the given expression
func (ce *TransitiveClosures) Foreach(expr sqlparser.Expr, comparer func(a, b sqlparser.Expr) bool, f func(expr sqlparser.Expr) error) error {
	// always start with the given expression
	if err := f(expr); err != nil {
		return err
	}

	id, found := ce.getID(expr, comparer)
	if !found {
		return f(expr)
	}

	for _, e := range ce.equalities[id] {
		err := f(e)
		if err != nil {
			return err
		}
	}
	return nil
}
