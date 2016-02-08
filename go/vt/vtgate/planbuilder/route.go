// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"
	"fmt"

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
// the Plan objects will remain.
type planBuilder interface {
	// Order is a number that signifies execution order.
	// A lower Order number Route is executed before a
	// higher one. For a node that contains other nodes,
	// the Order represents the highest order of the leaf
	// nodes.
	Order() int
	SupplyCol(col *sqlparser.ColName) int
}

// routeBuilder is used to build a Route primitive.
// It's used to build one of the Select routes like
// SelectScatter, etc. Portions of the original Select AST
// are moved into this node, which will be used to build
// the final SQL for this route.
// TODO(sougou): struct is incomplete.
type routeBuilder struct {
	// IsRHS is true if the routeBuilder is the RHS of a
	// LEFT JOIN. If so, many restrictions come into play.
	IsRHS bool
	// Select is the AST for the query fragment that will be
	// executed by this route.
	Select  sqlparser.Select
	order   int
	Colsyms []*colsym
	// Route is the plan object being built. It will contain all the
	// information necessary to execute the route operation.
	Route *Route
}

// Order returns the order of the node.
func (rtb *routeBuilder) Order() int {
	return rtb.order
}

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

// Route is a Plan object that represents a route.
// It can be any one of the Select primitives from PlanID.
// Some plan ids correspond to a multi-shard query,
// and some are for a single-shard query. The rules
// of what can be merged, or what can be pushed down
// depend on the PlanID. They're explained in code
// where such decisions are made.
// TODO(sougou): struct is incomplete.
// TODO(sougou): integrate with the older v3 Plan.
type Route struct {
	// PlanID will be one of the Select IDs from PlanID.
	PlanID PlanID
	Query  string
	// Keypsace represents the keyspace to which
	// the query will be sent.
	Keyspace *Keyspace
	// Vindex represents the vindex that will be used
	// to resolve the route.
	Vindex Vindex
	// Values can be a single value or a list of
	// values that will be used as input to the Vindex
	// to compute the target shard(s) where the query must
	// be sent.
	// TODO(sougou): explain contents of Values.
	Values  interface{}
	UseVars map[string]struct{}
}

// MarshalJSON marshals Route into a readable form.
// It's used for testing and diagnostics. The representation
// cannot be used to reconstruct a Route.
func (rt *Route) MarshalJSON() ([]byte, error) {
	var vindexName string
	if rt.Vindex != nil {
		vindexName = rt.Vindex.String()
	}
	marshalRoute := struct {
		PlanID   PlanID              `json:",omitempty"`
		Query    string              `json:",omitempty"`
		Keyspace *Keyspace           `json:",omitempty"`
		Vindex   string              `json:",omitempty"`
		Values   string              `json:",omitempty"`
		UseVars  map[string]struct{} `json:",omitempty"`
	}{
		PlanID:   rt.PlanID,
		Query:    rt.Query,
		Keyspace: rt.Keyspace,
		Vindex:   vindexName,
		Values:   prettyValue(rt.Values),
		UseVars:  rt.UseVars,
	}
	return json.Marshal(marshalRoute)
}

// SetPlan updates the plan info for the route.
func (rt *Route) SetPlan(planID PlanID, vindex Vindex, values interface{}) {
	rt.PlanID = planID
	rt.Vindex = vindex
	rt.Values = values
}

// prettyValue converts the Values to a readable form.
// This function is used for testing and diagnostics.
func prettyValue(value interface{}) string {
	switch value := value.(type) {
	case nil:
		return ""
	case sqlparser.SQLNode:
		return sqlparser.String(value)
	case []byte:
		return string(value)
	}
	return fmt.Sprintf("%v", value)
}
