/*
Copyright 2019 The Vitess Authors.

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
	"fmt"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/sqlparser"
)

func buildUnionPlan(string) stmtPlanner {
	return func(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
		union := stmt.(*sqlparser.Union)
		if union.With != nil {
			return nil, vterrors.VT12001("WITH expression in UNION statement")
		}
		// For unions, create a pb with anonymous scope.
		pb := newPrimitiveBuilder(vschema, newJointab(reservedVars))
		if err := pb.processUnion(union, reservedVars, nil); err != nil {
			return nil, err
		}
		if err := pb.plan.Wireup(pb.plan, pb.jt); err != nil {
			return nil, err
		}
		return newPlanResult(pb.plan.Primitive()), nil
	}
}

func (pb *primitiveBuilder) processUnion(union *sqlparser.Union, reservedVars *sqlparser.ReservedVars, outer *symtab) error {
	if err := pb.processPart(union.Left, reservedVars, outer); err != nil {
		return err
	}

	rpb := newPrimitiveBuilder(pb.vschema, pb.jt)
	if err := rpb.processPart(union.Right, reservedVars, outer); err != nil {
		return err
	}
	err := unionRouteMerge(pb.plan, rpb.plan, union)
	if err != nil {
		// we are merging between two routes - let's check if we can see so that we have the same amount of columns on both sides of the union
		lhsCols := len(pb.plan.ResultColumns())
		rhsCols := len(rpb.plan.ResultColumns())
		if lhsCols != rhsCols {
			return &mysql.SQLError{
				Num:     mysql.ERWrongNumberOfColumnsInSelect,
				State:   "21000",
				Message: "The used SELECT statements have a different number of columns",
				Query:   sqlparser.String(union),
			}
		}

		pb.plan = &concatenate{
			lhs: pb.plan,
			rhs: rpb.plan,
		}

		if union.Distinct {
			pb.plan = newDistinctV3(pb.plan)
		}
	}
	pb.st.Outer = outer

	if err := setLock(pb.plan, union.Lock); err != nil {
		return err
	}

	if err := pb.pushOrderBy(union.OrderBy); err != nil {
		return err
	}
	return pb.pushLimit(union.Limit)
}

func (pb *primitiveBuilder) processPart(part sqlparser.SelectStatement, reservedVars *sqlparser.ReservedVars, outer *symtab) error {
	switch part := part.(type) {
	case *sqlparser.Union:
		return pb.processUnion(part, reservedVars, outer)
	case *sqlparser.Select:
		if part.SQLCalcFoundRows {
			return vterrors.VT12001("SQL_CALC_FOUND_ROWS not supported with UNION")
		}
		return pb.processSelect(part, reservedVars, outer, "")
	}
	return vterrors.VT13001(fmt.Sprintf("unexpected SELECT type: %T", part))
}

// TODO (systay) we never use this as an actual error. we should rethink the return type
func unionRouteMerge(left, right logicalPlan, us *sqlparser.Union) error {
	lroute, ok := left.(*route)
	if !ok {
		return vterrors.VT12001("SELECT of UNION is non-trivial")
	}
	rroute, ok := right.(*route)
	if !ok {
		return vterrors.VT12001("SELECT of UNION is non-trivial")
	}
	mergeSuccess := lroute.MergeUnion(rroute, us.Distinct)
	if !mergeSuccess {
		return vterrors.VT12001("execute UNION as a single route")
	}

	lroute.Select = &sqlparser.Union{Left: lroute.Select, Right: us.Right, Distinct: us.Distinct}

	return nil
}

// planLock pushes "FOR UPDATE", "LOCK IN SHARE MODE" down to all routes
func setLock(in logicalPlan, lock sqlparser.Lock) error {
	_, err := visit(in, func(plan logicalPlan) (bool, logicalPlan, error) {
		switch node := in.(type) {
		case *route:
			node.Select.SetLock(lock)
			return false, node, nil
		case *sqlCalcFoundRows, *vindexFunc:
			return false, nil, vterrors.VT13001(fmt.Sprintf("unreachable %T.locking", in))
		}
		return true, plan, nil
	})
	if err != nil {
		return err
	}
	return nil
}
