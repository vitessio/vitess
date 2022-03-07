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

// Join represents an join. If we have a predicate, this is an inner join. If no predicate exists, it is a cross join
type Join struct {
	LHS, RHS Operator
	Exp      sqlparser.Expr
}

// PushPredicate implements the Operator interface
func (j *Join) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	deps := semTable.Dependencies(expr)
	switch {
	case deps.IsSolvedBy(j.LHS.TableID()):
		return j.LHS.PushPredicate(expr, semTable)
	case deps.IsSolvedBy(j.RHS.TableID()):
		return j.RHS.PushPredicate(expr, semTable)
	case deps.IsSolvedBy(j.LHS.TableID().Merge(j.RHS.TableID())):
		j.Exp = sqlparser.AndExpressions(j.Exp, expr)
		return nil
	}

	return semantics.Gen4NotSupportedF("still not sure what to do with this predicate")
}

// TableID implements the Operator interface
func (j *Join) TableID() semantics.TableSet {
	return j.RHS.TableID().Merge(j.LHS.TableID())
}
