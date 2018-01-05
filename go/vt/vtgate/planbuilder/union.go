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

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
)

func buildUnionPlan(union *sqlparser.Union, vschema VSchema) (primitive engine.Primitive, err error) {
	bindvars := sqlparser.GetBindvars(union)
	bldr, err := processUnion(union, vschema, nil)
	if err != nil {
		return nil, err
	}
	jt := newJointab(bindvars)
	err = bldr.Wireup(bldr, jt)
	if err != nil {
		return nil, err
	}
	return bldr.Primitive(), nil
}

func processUnion(union *sqlparser.Union, vschema VSchema, outer builder) (builder, error) {
	lbldr, err := processPart(union.Left, vschema, outer)
	if err != nil {
		return nil, err
	}
	rbldr, err := processPart(union.Right, vschema, outer)
	if err != nil {
		return nil, err
	}
	bldr, err := unionRouteMerge(union, lbldr, rbldr, vschema)
	if err != nil {
		return nil, err
	}
	if outer != nil {
		bldr.Symtab().Outer = outer.Symtab()
	}
	err = pushOrderBy(union.OrderBy, bldr)
	if err != nil {
		return nil, err
	}
	bldr, err = pushLimit(union.Limit, bldr)
	if err != nil {
		return nil, err
	}
	return bldr, nil
}

func processPart(part sqlparser.SelectStatement, vschema VSchema, outer builder) (builder, error) {
	var err error
	var bldr builder
	switch part := part.(type) {
	case *sqlparser.Union:
		bldr, err = processUnion(part, vschema, outer)
	case *sqlparser.Select:
		bldr, err = processSelect(part, vschema, outer)
	case *sqlparser.ParenSelect:
		bldr, err = processPart(part.Select, vschema, outer)
	default:
		panic(fmt.Sprintf("BUG: unexpected SELECT type: %T", part))
	}
	if err != nil {
		return nil, err
	}
	return bldr, nil
}

func unionRouteMerge(union *sqlparser.Union, left, right builder, vschema VSchema) (builder, error) {
	lroute, ok := left.(*route)
	if !ok {
		return nil, errors.New("unsupported construct: SELECT of UNION is non-trivial")
	}
	rroute, ok := right.(*route)
	if !ok {
		return nil, errors.New("unsupported construct: SELECT of UNION is non-trivial")
	}
	if err := lroute.UnionCanMerge(rroute); err != nil {
		return nil, err
	}
	rb := newRoute(
		&sqlparser.Union{Type: union.Type, Left: union.Left, Right: union.Right, Lock: union.Lock},
		lroute.ERoute,
		lroute.condition,
		vschema,
	)
	lroute.Redirect = rb
	rroute.Redirect = rb
	return rb, nil
}
