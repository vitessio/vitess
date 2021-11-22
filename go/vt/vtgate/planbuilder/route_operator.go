package planbuilder

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Route struct {
	Source      abstract.Operator
	RouteOpCode engine.RouteOpcode
	Keyspace    *vindexes.Keyspace

	// The following two fields are used when routing information_schema queries
	SysTableTableSchema []evalengine.Expr
	SysTableTableName   map[string]evalengine.Expr

	// here we store the possible vindexes we can use so that when we add predicates to the plan,
	// we can quickly check if the new predicates enables any new vindex options
	vindexPreds []*vindexPlusPredicates

	// the best option available is stored here
	selected *vindexOption
}

var _ abstract.Operator = (*Route)(nil)

func (r *Route) TableID() semantics.TableSet {
	return r.Source.TableID()
}

func (r *Route) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	return r.Source.PushPredicate(expr, semTable)
}

func (r *Route) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return r.Source.UnsolvedPredicates(semTable)
}

func (r *Route) CheckValid() error {
	return r.CheckValid()
}

func (r *Route) Compact(semTable *semantics.SemTable) (abstract.Operator, error) {
	return r.Compact(semTable)
}

func (r *Route) Inspect(ctx *planningContext, predicates []sqlparser.Expr) error {
	if r.canImprove() {
		newVindexFound, err := r.searchForNewVindexes(ctx, predicates)
		if err != nil {
			return err
		}

		// if we didn't open up any new vindex options, no need to enter here
		if newVindexFound {
			r.pickBestAvailableVindex()
		}
	}

	return nil
}

func (r *Route) canImprove() bool {
	return r.RouteOpCode != engine.SelectNone
}

func (r *Route) searchForNewVindexes(ctx *planningContext, predicates []sqlparser.Expr) (bool, error) {
	newVindexFound := false
	for _, filter := range predicates {
		switch node := filter.(type) {
		case *sqlparser.ExtractedSubquery:
			originalCmp, ok := node.Original.(*sqlparser.ComparisonExpr)
			if !ok {
				break
			}

			// using the node.subquery which is the rewritten version of our subquery
			cmp := &sqlparser.ComparisonExpr{
				Left:     node.OtherSide,
				Right:    &sqlparser.Subquery{Select: node.Subquery.Select},
				Operator: originalCmp.Operator,
			}
			found, exitEarly, err := r.planComparison(ctx, cmp)
			if err != nil || exitEarly {
				return false, err
			}
			newVindexFound = newVindexFound || found
		case *sqlparser.ComparisonExpr:
			found, exitEarly, err := r.planComparison(ctx, node)
			if err != nil || exitEarly {
				return false, err
			}
			newVindexFound = newVindexFound || found
		case *sqlparser.IsExpr:
			found, err := r.planIsExpr(ctx, node)
			if err != nil {
				return false, err
			}
			newVindexFound = newVindexFound || found
		}
	}
	return newVindexFound, nil
}

func (r *Route) planComparison(ctx *planningContext, node *sqlparser.ComparisonExpr) (bool, bool, error) {
	if sqlparser.IsNull(node.Left) || sqlparser.IsNull(node.Right) {
		// we are looking at ANDed predicates in the WHERE clause.
		// since we know that nothing returns true when compared to NULL,
		// so we can safely bail out here
		r.RouteOpCode = engine.SelectNone
		return false, true, nil
	}

	switch node.Operator {
	case sqlparser.EqualOp:
		found, err := r.planEqualOp(ctx, node)
		if err != nil {
			return false, false, err
		}
		return found, false, nil
	case sqlparser.InOp:
		if r.isImpossibleIN(node) {
			return false, true, nil
		}
		found, err := r.planInOp(ctx, node)
		if err != nil {
			return false, false, err
		}
		return found, false, nil
	case sqlparser.NotInOp:
		// NOT IN is always a scatter, except when we can be sure it would return nothing
		if r.isImpossibleNotIN(node) {
			return false, true, nil
		}
	case sqlparser.LikeOp:
		found, err := r.planLikeOp(ctx, node)
		if err != nil {
			return false, false, err
		}
		return found, false, nil
	}
	return false, false, nil
}

func (r *Route) isImpossibleIN(node *sqlparser.ComparisonExpr) bool {
	switch nodeR := node.Right.(type) {
	case sqlparser.ValTuple:
		// WHERE col IN (null)
		if len(nodeR) == 1 && sqlparser.IsNull(nodeR[0]) {
			r.RouteOpCode = engine.SelectNone
			return true
		}
	}
	return false
}

func (r *Route) isImpossibleNotIN(node *sqlparser.ComparisonExpr) bool {
	switch node := node.Right.(type) {
	case sqlparser.ValTuple:
		for _, n := range node {
			if sqlparser.IsNull(n) {
				r.RouteOpCode = engine.SelectNone
				return true
			}
		}
	}

	return false
}

func (r *Route) planEqualOp(ctx *planningContext, node *sqlparser.ComparisonExpr) (bool, error) {
	column, ok := node.Left.(*sqlparser.ColName)
	other := node.Right
	vdValue := other
	if !ok {
		column, ok = node.Right.(*sqlparser.ColName)
		if !ok {
			// either the LHS or RHS have to be a column to be useful for the vindex
			return false, nil
		}
		vdValue = node.Left
	}
	val, err := makePlanValue(ctx, vdValue)
	if err != nil || val == nil {
		return false, err
	}

	return r.haveMatchingVindex(ctx, node, vdValue, column, *val, equalOrEqualUnique, justTheVindex), err
}

func (r *Route) planSimpleInOp(ctx *planningContext, node *sqlparser.ComparisonExpr, left *sqlparser.ColName) (bool, error) {
	vdValue := node.Right
	value, err := makePlanValue(ctx, vdValue)
	if err != nil || value == nil {
		return false, err
	}
	switch nodeR := vdValue.(type) {
	case sqlparser.ValTuple:
		if len(nodeR) == 1 && sqlparser.IsNull(nodeR[0]) {
			r.RouteOpCode = engine.SelectNone
			return false, nil
		}
	}
	opcode := func(*vindexes.ColumnVindex) engine.RouteOpcode { return engine.SelectIN }
	return r.haveMatchingVindex(ctx, node, vdValue, left, *value, opcode, justTheVindex), err
}

func (r *Route) planCompositeInOp(ctx *planningContext, node *sqlparser.ComparisonExpr, left sqlparser.ValTuple) (bool, error) {
	right, rightIsValTuple := node.Right.(sqlparser.ValTuple)
	if !rightIsValTuple {
		return false, nil
	}
	return r.planCompositeInOpRecursive(ctx, node, left, right, nil)
}

func (r *Route) planCompositeInOpRecursive(ctx *planningContext, node *sqlparser.ComparisonExpr, left, right sqlparser.ValTuple, coordinates []int) (bool, error) {
	foundVindex := false
	cindex := len(coordinates)
	coordinates = append(coordinates, 0)
	for i, expr := range left {
		coordinates[cindex] = i
		switch expr := expr.(type) {
		case sqlparser.ValTuple:
			ok, err := r.planCompositeInOpRecursive(ctx, node, expr, right, coordinates)
			if err != nil {
				return false, err
			}
			return ok || foundVindex, nil
		case *sqlparser.ColName:
			// check if left col is a vindex
			if !r.hasVindex(expr) {
				continue
			}

			rightVals := make(sqlparser.ValTuple, len(right))
			for j, currRight := range right {
				switch currRight := currRight.(type) {
				case sqlparser.ValTuple:
					val := tupleAccess(currRight, coordinates)
					if val == nil {
						return false, nil
					}
					rightVals[j] = val
				default:
					return false, nil
				}
			}
			newPlanValues, err := makePlanValue(ctx, rightVals)
			if newPlanValues == nil || err != nil {
				return false, err
			}

			opcode := func(*vindexes.ColumnVindex) engine.RouteOpcode { return engine.SelectMultiEqual }
			newVindex := r.haveMatchingVindex(ctx, node, rightVals, expr, *newPlanValues, opcode, justTheVindex)
			foundVindex = newVindex || foundVindex
		}
	}
	return foundVindex, nil
}

func (r *Route) planInOp(ctx *planningContext, node *sqlparser.ComparisonExpr) (bool, error) {
	switch left := node.Left.(type) {
	case *sqlparser.ColName:
		return r.planSimpleInOp(ctx, node, left)
	case sqlparser.ValTuple:
		return r.planCompositeInOp(ctx, node, left)
	}
	return false, nil
}

func (r *Route) planLikeOp(ctx *planningContext, node *sqlparser.ComparisonExpr) (bool, error) {
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false, nil
	}

	vdValue := node.Right
	val, err := makePlanValue(ctx, vdValue)
	if err != nil || val == nil {
		return false, err
	}

	selectEqual := func(*vindexes.ColumnVindex) engine.RouteOpcode { return engine.SelectEqual }
	vdx := func(vindex *vindexes.ColumnVindex) vindexes.Vindex {
		if prefixable, ok := vindex.Vindex.(vindexes.Prefixable); ok {
			return prefixable.PrefixVindex()
		}

		// if we can't use the vindex as a prefix-vindex, we can't use this vindex at all
		return nil
	}

	return r.haveMatchingVindex(ctx, node, vdValue, column, *val, selectEqual, vdx), err
}

func (r *Route) planIsExpr(ctx *planningContext, node *sqlparser.IsExpr) (bool, error) {
	// we only handle IS NULL correct. IsExpr can contain other expressions as well
	if node.Right != sqlparser.IsNullOp {
		return false, nil
	}
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false, nil
	}
	vdValue := &sqlparser.NullVal{}
	val, err := makePlanValue(ctx, vdValue)
	if err != nil || val == nil {
		return false, err
	}

	return r.haveMatchingVindex(ctx, node, vdValue, column, *val, equalOrEqualUnique, justTheVindex), err
}

func (r *Route) haveMatchingVindex(
	ctx *planningContext,
	node sqlparser.Expr,
	valueExpr sqlparser.Expr,
	column *sqlparser.ColName,
	value sqltypes.PlanValue,
	opcode func(*vindexes.ColumnVindex) engine.RouteOpcode,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) bool {
	newVindexFound := false
	for _, v := range r.vindexPreds {
		// check that the
		if !ctx.semTable.DirectDeps(column).IsSolvedBy(v.tableID) {
			continue
		}
		// Ignore MultiColumn vindexes for finding matching Vindex.
		if _, isSingleCol := v.colVindex.Vindex.(vindexes.SingleColumn); !isSingleCol {
			continue
		}

		col := v.colVindex.Columns[0]
		if column.Name.Equal(col) {
			// single column vindex - just add the option
			routeOpcode := opcode(v.colVindex)
			vindex := vfunc(v.colVindex)
			v.options = append(v.options, &vindexOption{
				values:      []sqltypes.PlanValue{value},
				valueExprs:  []sqlparser.Expr{valueExpr},
				predicates:  []sqlparser.Expr{node},
				opcode:      routeOpcode,
				foundVindex: vindex,
				cost:        costFor(vindex, routeOpcode),
				ready:       true,
			})
			newVindexFound = true
		}
	}
	return newVindexFound
}

func (r *Route) pickBestAvailableVindex() {
	for _, v := range r.vindexPreds {
		option := v.bestOption()
		if option != nil && (r.selected == nil || less(option.cost, r.selected.cost)) {
			r.selected = option
			r.RouteOpCode = option.opcode
		}
	}
}

func (r *Route) hasVindex(column *sqlparser.ColName) bool {
	for _, v := range r.vindexPreds {
		for _, col := range v.colVindex.Columns {
			if column.Name.Equal(col) {
				return true
			}
		}
	}
	return false
}
