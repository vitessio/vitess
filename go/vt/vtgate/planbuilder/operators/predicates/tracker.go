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
	"sync"

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	// Tracker manages a global mapping of expression IDs to their "Shape".
	// This allows the same logical expression to take different forms (shapes)
	// depending on pushdown or join strategies. We lock around 'lastID' to ensure
	// unique IDs in concurrent planning contexts.
	Tracker struct {
		mu          sync.Mutex
		lastID      ID
		Expressions map[ID]sqlparser.Expr
	}

	// ID is a unique key that references the shape of a single expression.
	// We use it so multiple references to an expression can share the same shape entry.
	ID int
)

func NewTracker() *Tracker {
	return &Tracker{
		Expressions: make(map[ID]sqlparser.Expr),
	}
}

func (t *Tracker) NewJoinPredicate(org sqlparser.Expr) *JoinPredicate {
	nextID := t.NextID()
	t.Expressions[nextID] = org
	return &JoinPredicate{
		ID:      nextID,
		tracker: t,
	}
}

func (t *Tracker) NextID() ID {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := t.lastID
	t.lastID++
	return id
}

func (t *Tracker) Set(id ID, expr sqlparser.Expr) {
	t.Expressions[id] = expr
}

func (t *Tracker) Skip(id ID) {
	t.Expressions[id] = nil
}
