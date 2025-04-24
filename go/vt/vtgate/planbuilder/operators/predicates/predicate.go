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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// JoinPredicate represents a condition relating two parts of the query,
// typically used when expressions span multiple tables. We track it in
// different shapes so we can rewrite or push it differently during plan generation.
type JoinPredicate struct {
	ID      ID
	tracker *Tracker
}

func (j *JoinPredicate) Clone(inner sqlparser.SQLNode) sqlparser.SQLNode {
	expr, ok := inner.(sqlparser.Expr)
	if !ok {
		panic(vterrors.VT13001("unexpected type in JoinPredicate.Clone"))
	}
	j.tracker.expressions[j.ID] = expr
	return &JoinPredicate{
		ID:      j.ID,
		tracker: j.tracker,
	}
}

var _ sqlparser.Expr = (*JoinPredicate)(nil)
var _ sqlparser.Visitable = (*JoinPredicate)(nil)

func (j *JoinPredicate) VisitThis() sqlparser.SQLNode {
	return j.Current()
}

func (j *JoinPredicate) Current() sqlparser.Expr {
	return j.tracker.expressions[j.ID]
}

func (j *JoinPredicate) IsExpr() {}

func (j *JoinPredicate) Format(buf *sqlparser.TrackedBuffer) {
	j.Current().Format(buf)
}

func (j *JoinPredicate) FormatFast(buf *sqlparser.TrackedBuffer) {
	fmt.Fprintf(buf, "JP(%d):", j.ID)
	j.Current().FormatFast(buf)
}
