// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func findRoute(expr sqlparser.Expr, syms *symtab) (route *routeBuilder, err error) {
	highestRoute := syms.FirstRoute
	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			newRoute, err := syms.Find(node, true)
			if err != nil {
				return false, err
			}
			if newRoute.Order() > highestRoute.Order() {
				highestRoute = newRoute
			}
		case *sqlparser.Subquery:
			// TODO(sougou): implement.
			return false, errors.New("subqueries not supported yet")
		}
		return true, nil
	}, expr)
	if err != nil {
		return nil, err
	}
	return highestRoute, nil
}
