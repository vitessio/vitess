/*
Copyright 2021 The Vitess Authors.

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

package abstract

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// OuterJoin represents an outerjoin.
type OuterJoin struct {
	Inner, Outer Operator
	Predicate    sqlparser.Expr
}

// PushPredicate implements the Operator interface
func (oj *OuterJoin) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	deps := semTable.Dependencies(expr)
	if deps.IsSolvedBy(oj.Inner.TableID()) {
		return oj.Inner.PushPredicate(expr, semTable)
	} else if deps.IsSolvedBy(oj.Outer.TableID()) {
		return oj.Outer.PushPredicate(expr, semTable)
	} else {
		return semantics.Gen4NotSupportedF("what the what!?")
	}
}

// TableID implements the Operator interface
func (oj *OuterJoin) TableID() semantics.TableSet {
	return oj.Outer.TableID().Merge(oj.Inner.TableID())
}
