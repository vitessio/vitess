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
	"errors"
	"fmt"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildUnionPlan(stmt sqlparser.Statement, vschema ContextVSchema) (engine.Primitive, error) {
	union := stmt.(*sqlparser.Union)
	// For unions, create a pb with anonymous scope.
	pb := newPrimitiveBuilder(vschema, newJointab(sqlparser.GetBindvars(union)))
	if err := pb.processUnion(union, nil); err != nil {
		return nil, err
	}
	if err := pb.bldr.Wireup(pb.bldr, pb.jt); err != nil {
		return nil, err
	}
	return pb.bldr.Primitive(), nil
}

func (pb *primitiveBuilder) processUnion(union *sqlparser.Union, outer *symtab) error {
	if err := pb.processPart(union.FirstStatement, outer, false); err != nil {
		return err
	}
	for _, us := range union.UnionSelects {
		rpb := newPrimitiveBuilder(pb.vschema, pb.jt)
		if err := rpb.processPart(us.Statement, outer, false); err != nil {
			return err
		}
		err := unionRouteMerge(pb.bldr, rpb.bldr, us)
		if err != nil {
			if us.Type != sqlparser.UnionAllStr {
				return err
			}

			// we are merging between two routes - let's check if we can see so that we have the same amount of columns on both sides of the union
			lhsCols := len(pb.bldr.ResultColumns())
			rhsCols := len(rpb.bldr.ResultColumns())
			if lhsCols != rhsCols {
				return &mysql.SQLError{
					Num:     mysql.ERWrongNumberOfColumnsInSelect,
					State:   "21000",
					Message: "The used SELECT statements have a different number of columns",
					Query:   sqlparser.String(union),
				}
			}

			pb.bldr = &concatenate{
				lhs: pb.bldr,
				rhs: rpb.bldr,
			}
		}
		pb.st.Outer = outer
	}
	pb.bldr.PushLock(union.Lock)

	if err := pb.pushOrderBy(union.OrderBy); err != nil {
		return err
	}
	return pb.pushLimit(union.Limit)
}

func (pb *primitiveBuilder) processPart(part sqlparser.SelectStatement, outer *symtab, hasParens bool) error {
	switch part := part.(type) {
	case *sqlparser.Union:
		return pb.processUnion(part, outer)
	case *sqlparser.Select:
		if !hasParens {
			err := checkOrderByAndLimit(part)
			if err != nil {
				return err
			}
		}
		return pb.processSelect(part, outer)
	case *sqlparser.ParenSelect:
		err := pb.processPart(part.Select, outer, true)
		if err != nil {
			return err
		}
		// TODO: This is probably not a great idea. If we ended up with something other than a route, we'll lose the parens
		routeOp, ok := pb.bldr.(*route)
		if ok {
			routeOp.Select = &sqlparser.ParenSelect{Select: routeOp.Select}
		}
		return nil
	}
	return fmt.Errorf("BUG: unexpected SELECT type: %T", part)
}

func checkOrderByAndLimit(part *sqlparser.Select) error {
	if part.OrderBy != nil {
		return &mysql.SQLError{
			Num:     mysql.ERWrongUsage,
			State:   "21000",
			Message: "Incorrect usage of UNION and ORDER BY - add parens to disambiguate your query",
		}
	}
	if part.Limit != nil {
		return &mysql.SQLError{
			Num:     mysql.ERWrongUsage,
			State:   "21000",
			Message: "Incorrect usage of UNION and LIMIT - add parens to disambiguate your query",
		}
	}
	return nil
}

func unionRouteMerge(left, right builder, us *sqlparser.UnionSelect) error {
	lroute, ok := left.(*route)
	if !ok {
		return errors.New("unsupported: SELECT of UNION is non-trivial")
	}
	rroute, ok := right.(*route)
	if !ok {
		return errors.New("unsupported: SELECT of UNION is non-trivial")
	}
	if !lroute.MergeUnion(rroute) {
		return errors.New("unsupported: UNION cannot be executed as a single route")
	}

	switch n := lroute.Select.(type) {
	case *sqlparser.Union:
		n.UnionSelects = append(n.UnionSelects, us)
	default:
		lroute.Select = &sqlparser.Union{FirstStatement: lroute.Select, UnionSelects: []*sqlparser.UnionSelect{us}}
	}

	return nil
}
