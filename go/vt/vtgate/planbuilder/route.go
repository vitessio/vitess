// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// planBuilder represents any object that's used to
// build a plan. The top-level planBuilder will be a
// tree that points to other planBuilder objects.
// Currently, joinBuilder and routeBuilder are the
// only two supported planBuilder objects. More will be
// added as we extend the functionality.
// Each Builder object builds a Plan object, and they
// will mirror the same tree. Once all the plans are built,
// the builder objects will be discarded, and only
// the Plan objects will remain. Because of the near-equivalent
// meaning of a planBuilder object and its plan, the variable
// names are overloaded. The separation exists only because the
// information in the planBuilder objects are not ultimately
// required externally.
// For example, a route variable usually refers to a
// routeBuilder object, which in turn, has a Route field.
// This should not cause confusion because we almost never
// reference the inner Route directly.
type planBuilder interface {
	// Symtab returns the associated symtab.
	Symtab() *symtab
	// Order is a number that signifies execution order.
	// A lower Order number Route is executed before a
	// higher one. For a node that contains other nodes,
	// the Order represents the highest order of the leaf
	// nodes. This function is used to travel from a root
	// node to a target node.
	Order() int
	// SupplyCol will be used for the wire-up process. This function
	// takes a column reference as input, changes the plan
	// to supply the requested column and returns the column number of
	// the result for it. The request is passed down recursively
	// as needed.
	SupplyCol(col *sqlparser.ColName) int
}

// routeBuilder is used to build a Route primitive.
// It's used to build one of the Select routes like
// SelectScatter, etc. Portions of the original Select AST
// are moved into this node, which will be used to build
// the final SQL for this route.
type routeBuilder struct {
	// Redirect may point to another route if this route
	// was merged with it. The Resolve function chases
	// this pointer till the last un-redirected route.
	Redirect *routeBuilder
	// IsRHS is true if the routeBuilder is the RHS of a
	// LEFT JOIN. If so, many restrictions come into play.
	IsRHS bool
	// Select is the AST for the query fragment that will be
	// executed by this route.
	Select sqlparser.Select
	order  int
	symtab *symtab
	// Colsyms represent the columns returned by this route.
	Colsyms []*colsym
	// Route is the plan object being built. It will contain all the
	// information necessary to execute the route operation.
	Route *Route
}

// Resolve resolves redirects, and returns the last
// un-redirected route.
func (rtb *routeBuilder) Resolve() *routeBuilder {
	for rtb.Redirect != nil {
		rtb = rtb.Redirect
	}
	return rtb
}

// Symtab returns the associated symtab.
func (rtb *routeBuilder) Symtab() *symtab {
	return rtb.symtab
}

// Order returns the order of the node.
func (rtb *routeBuilder) Order() int {
	return rtb.order
}

// SupplyCol changes the router to supply the requested column
// name, and returns the result column number. If the column
// is already in the list, it's reused.
func (rtb *routeBuilder) SupplyCol(col *sqlparser.ColName) int {
	switch meta := col.Metadata.(type) {
	case *colsym:
		for i, colsym := range rtb.Colsyms {
			if meta == colsym {
				return i
			}
		}
		panic("unexpected")
	case *tableAlias:
		ref := newColref(col)
		for i, colsym := range rtb.Colsyms {
			if colsym.Underlying == ref {
				return i
			}
		}
		rtb.Colsyms = append(rtb.Colsyms, &colsym{
			Alias:      sqlparser.SQLName(sqlparser.String(col)),
			Underlying: ref,
		})
		rtb.Select.SelectExprs = append(
			rtb.Select.SelectExprs,
			&sqlparser.NonStarExpr{
				Expr: &sqlparser.ColName{
					Metadata:  col.Metadata,
					Qualifier: meta.Alias,
					Name:      col.Name,
				},
			},
		)
		return len(rtb.Colsyms) - 1
	}
	panic("unexpected")
}

// MarshalJSON marshals routeBuilder into a readable form.
// It's used for testing and diagnostics. The representation
// cannot be used to reconstruct a routeBuilder.
func (rtb *routeBuilder) MarshalJSON() ([]byte, error) {
	marshalRoute := struct {
		IsRHS  bool   `json:",omitempty"`
		Select string `json:",omitempty"`
		Order  int
		Route  *Route
	}{
		IsRHS:  rtb.IsRHS,
		Select: sqlparser.String(&rtb.Select),
		Order:  rtb.order,
		Route:  rtb.Route,
	}
	return json.Marshal(marshalRoute)
}

// IsSingle returns true if the route targets only one database.
func (rtb *routeBuilder) IsSingle() bool {
	return rtb.Route.PlanID == SelectUnsharded || rtb.Route.PlanID == SelectEqualUnique
}
