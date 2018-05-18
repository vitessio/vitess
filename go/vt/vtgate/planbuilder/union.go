/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package planbuilder

import (
	"errors"
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildUnionPlan(union *sqlparser.Union, vschema ContextVSchema) (primitive engine.Primitive, err error) {
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
	lpb := newPrimitiveBuilder(pb.vschema, pb.jt)
	if err := lpb.processPart(union.Left, outer); err != nil {
		return err
	}
	rpb := newPrimitiveBuilder(pb.vschema, pb.jt)
	if err := rpb.processPart(union.Right, outer); err != nil {
		return err
	}

	var err error
	pb.bldr, pb.st, err = unionRouteMerge(union, lpb.bldr, rpb.bldr)
	if err != nil {
		return err
	}
	pb.st.Outer = outer

	if err := pb.pushOrderBy(union.OrderBy); err != nil {
		return err
	}
	return pb.pushLimit(union.Limit)
}

func (pb *primitiveBuilder) processPart(part sqlparser.SelectStatement, outer *symtab) error {
	switch part := part.(type) {
	case *sqlparser.Union:
		return pb.processUnion(part, outer)
	case *sqlparser.Select:
		return pb.processSelect(part, outer)
	case *sqlparser.ParenSelect:
		return pb.processPart(part.Select, outer)
	}
	panic(fmt.Sprintf("BUG: unexpected SELECT type: %T", part))
}

func unionRouteMerge(union *sqlparser.Union, left, right builder) (builder, *symtab, error) {
	lroute, ok := left.(*route)
	if !ok {
		return nil, nil, errors.New("unsupported construct: SELECT of UNION is non-trivial")
	}
	rroute, ok := right.(*route)
	if !ok {
		return nil, nil, errors.New("unsupported construct: SELECT of UNION is non-trivial")
	}
	if err := lroute.UnionCanMerge(rroute); err != nil {
		return nil, nil, err
	}
	rb, st := newRoute(
		&sqlparser.Union{Type: union.Type, Left: union.Left, Right: union.Right, Lock: union.Lock},
		lroute.ERoute,
		lroute.condition,
	)
	lroute.Redirect = rb
	rroute.Redirect = rb
	return rb, st, nil
}
