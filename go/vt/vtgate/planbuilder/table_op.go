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

package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type tableOp struct {
	qtable  *abstract.QueryTable
	vtable  *vindexes.Table
	columns []*sqlparser.ColName
}

var _ abstract.PhysicalOperator = (*tableOp)(nil)

// IPhysical implements the PhysicalOperator interface
func (to *tableOp) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (to *tableOp) Cost() int {
	return 0
}

// Clone implements the PhysicalOperator interface
func (to *tableOp) Clone() abstract.PhysicalOperator {
	return to
}

// TableID implements the PhysicalOperator interface
func (to *tableOp) TableID() semantics.TableSet {
	return to.qtable.ID
}

// PushPredicate implements the PhysicalOperator interface
func (to *tableOp) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "we should not push predicates into a tableOp. It is meant to be immutable")
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (to *tableOp) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	panic("implement me")
}

// CheckValid implements the PhysicalOperator interface
func (to *tableOp) CheckValid() error {
	return nil
}

// Compact implements the PhysicalOperator interface
func (to *tableOp) Compact(semTable *semantics.SemTable) (abstract.Operator, error) {
	return to, nil
}
