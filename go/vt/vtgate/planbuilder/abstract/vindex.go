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
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	// Vindex stores the information about the vindex query
	Vindex struct {
		OpCode engine.VindexOpcode
		Table  VindexTable
		Vindex vindexes.Vindex
		Value  sqltypes.PlanValue
	}

	// VindexTable contains information about the vindex table we want to query
	VindexTable struct {
		TableID    semantics.TableSet
		Alias      *sqlparser.AliasedTableExpr
		Table      sqlparser.TableName
		Predicates []sqlparser.Expr
		VTable     *vindexes.Table
	}
)

var _ Operator = (*Vindex)(nil)

// TableID implements the Operator interface
func (v *Vindex) TableID() semantics.TableSet {
	return v.Table.TableID
}

const vindexUnsupported = "unsupported: where clause for vindex function must be of the form id = <val>"

// PushPredicate implements the Operator interface
func (v *Vindex) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	for _, e := range sqlparser.SplitAndExpression(nil, expr) {
		deps := semTable.RecursiveDeps(e)
		if deps.NumberOfTables() > 1 {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vindexUnsupported+" (multiple tables involved)")
		}
		// check if we already have a predicate
		if v.OpCode != engine.VindexNone {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (multiple filters)")
		}

		// check LHS
		comparison, ok := e.(*sqlparser.ComparisonExpr)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (not a comparison)")
		}
		if comparison.Operator != sqlparser.EqualOp {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (not equality)")
		}
		colname, ok := comparison.Left.(*sqlparser.ColName)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (lhs is not a column)")
		}
		if !colname.Name.EqualString("id") {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (lhs is not id)")
		}

		// check RHS
		if !sqlparser.IsValue(comparison.Right) {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (rhs is not a value)")
		}
		var err error
		v.Value, err = sqlparser.NewPlanValue(comparison.Right)
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+": %v", err)
		}
		v.OpCode = engine.VindexMap
		v.Table.Predicates = append(v.Table.Predicates, e)
	}
	return nil
}

// UnsolvedPredicates implements the Operator interface
func (v *Vindex) UnsolvedPredicates(*semantics.SemTable) []sqlparser.Expr {
	return nil
}

// CheckValid implements the Operator interface
func (v *Vindex) CheckValid() error {
	if len(v.Table.Predicates) == 0 {
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: where clause for vindex function must be of the form id = <val> (where clause missing)")
	}

	return nil
}

// Compact implements the Operator interface
func (v *Vindex) Compact() Operator {
	return v
}
