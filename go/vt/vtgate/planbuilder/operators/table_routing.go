package operators

import (
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type ShardedRouting struct {
	// here we store the possible vindexes we can use so that when we add predicates to the plan,
	// we can quickly check if the new predicates enables any new vindex Options
	VindexPreds []*VindexPlusPredicates

	// the best option available is stored here
	Selected *VindexOption

	keyspace *vindexes.Keyspace

	RouteOpCode engine.Opcode

	// SeenPredicates contains all the predicates that have had a chance to influence routing.
	// If we need to replan routing, we'll use this list
	SeenPredicates []sqlparser.Expr

	// Alternates contains alternate routes to equivalent sources in
	// other keyspaces.
	Alternates map[*vindexes.Keyspace]*Route
}

var _ Routing = (*ShardedRouting)(nil)

func newShardedRouting(vtable *vindexes.Table, id semantics.TableSet) Routing {
	routing := &ShardedRouting{
		RouteOpCode: engine.Scatter,
		keyspace:    vtable.Keyspace,
	}

	if vtable.Pinned != nil {
		// Pinned tables have their keyspace ids already assigned.
		// Use the Binary vindex, which is the identity function
		// for keyspace id.
		routing.RouteOpCode = engine.EqualUnique
		vindex, _ := vindexes.NewBinary("binary", nil)
		routing.Selected = &VindexOption{
			Ready:       true,
			Values:      []evalengine.Expr{evalengine.NewLiteralString(vtable.Pinned, collations.TypedCollation{})},
			ValueExprs:  nil,
			Predicates:  nil,
			OpCode:      engine.EqualUnique,
			FoundVindex: vindex,
			Cost: Cost{
				OpCode: engine.EqualUnique,
			},
		}

	}
	for _, columnVindex := range vtable.ColumnVindexes {
		routing.VindexPreds = append(routing.VindexPreds, &VindexPlusPredicates{ColVindex: columnVindex, TableID: id})
	}
	return routing
}

func (tr *ShardedRouting) isScatter() bool {
	return tr.RouteOpCode == engine.Scatter
}

func (tr *ShardedRouting) tryImprove(ctx *plancontext.PlanningContext, queryTable *QueryTable) (Routing, error) {
	oldPredicates := queryTable.Predicates
	queryTable.Predicates = nil
	tr.SeenPredicates = nil
	var routing Routing = tr
	var err error
	for _, pred := range oldPredicates {
		rewritten := sqlparser.RewritePredicate(pred)
		predicates := sqlparser.SplitAndExpression(nil, rewritten.(sqlparser.Expr))
		for _, predicate := range predicates {
			queryTable.Predicates = append(queryTable.Predicates, predicate)

			routing, err = routing.UpdateRoutingLogic(ctx, predicate)
			if err != nil {
				return nil, err
			}
		}
	}

	var ok bool
	tr, ok = routing.(*ShardedRouting)
	if !ok {
		return routing, nil
	}

	if tr.isScatter() {
		// if we _still_ haven't found a better route, we can run this additional rewrite on any ORs we have
		for _, expr := range queryTable.Predicates {
			or, ok := expr.(*sqlparser.OrExpr)
			if !ok {
				continue
			}
			for _, predicate := range sqlparser.ExtractINFromOR(or) {
				routing, err = routing.UpdateRoutingLogic(ctx, predicate)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return routing, nil
}

func (tr *ShardedRouting) UpdateRoutingParams(rp *engine.RoutingParameters) {
	rp.Opcode = tr.RouteOpCode
	rp.Keyspace = tr.keyspace
	if tr.Selected != nil {
		rp.Vindex = tr.Selected.FoundVindex
		rp.Values = tr.Selected.Values
	}
}

func (tr *ShardedRouting) Clone() Routing {
	var selected *VindexOption
	if tr.Selected != nil {
		t := *tr.Selected
		selected = &t
	}
	return &ShardedRouting{
		VindexPreds:    slices.Clone(tr.VindexPreds),
		Selected:       selected,
		keyspace:       tr.keyspace,
		RouteOpCode:    tr.RouteOpCode,
		SeenPredicates: slices.Clone(tr.SeenPredicates),
		Alternates:     maps.Clone(tr.Alternates),
	}
}

func (tr *ShardedRouting) UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	tr.SeenPredicates = append(tr.SeenPredicates, expr)

	newRouting, newVindexFound, err := tr.searchForNewVindexes(ctx, expr)
	if err != nil {
		return nil, err
	}

	if newRouting != nil {
		// we found something that we can route with something other than ShardedRouting
		return newRouting, nil
	}

	// if we didn't open up any new vindex Options, no need to enter here
	if newVindexFound {
		tr.PickBestAvailableVindex()
	}

	return tr, nil
}

func (tr *ShardedRouting) searchForNewVindexes(ctx *plancontext.PlanningContext, predicate sqlparser.Expr) (Routing, bool, error) {
	newVindexFound := false
	switch node := predicate.(type) {
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
		return tr.planComparison(ctx, cmp)
	case *sqlparser.ComparisonExpr:
		return tr.planComparison(ctx, node)

	case *sqlparser.IsExpr:
		found := tr.planIsExpr(ctx, node)
		newVindexFound = newVindexFound || found
	}

	return nil, newVindexFound, nil
}

func (tr *ShardedRouting) planComparison(ctx *plancontext.PlanningContext, cmp *sqlparser.ComparisonExpr) (routing Routing, foundNew bool, err error) {
	if cmp.Operator != sqlparser.NullSafeEqualOp && (sqlparser.IsNull(cmp.Left) || sqlparser.IsNull(cmp.Right)) {
		// we are looking at ANDed predicates in the WHERE clause.
		// since we know that nothing returns true when compared to NULL,
		// so we can safely bail out here
		return &NoneRouting{keyspace: tr.keyspace}, false, nil
	}

	switch cmp.Operator {
	case sqlparser.EqualOp:
		found := tr.planEqualOp(ctx, cmp)
		return nil, found, nil
	case sqlparser.InOp:
		if isImpossibleIN(cmp) {
			return &NoneRouting{keyspace: tr.keyspace}, true, nil
		}
		found := tr.planInOp(ctx, cmp)
		return nil, found, nil
	case sqlparser.NotInOp:
		// NOT IN is always a scatter, except when we can be sure it would return nothing
		if isImpossibleNotIN(cmp) {
			return &NoneRouting{keyspace: tr.keyspace}, true, nil
		}
	case sqlparser.LikeOp:
		found := tr.planLikeOp(ctx, cmp)
		return nil, found, nil

	}
	return nil, false, nil
}

func (tr *ShardedRouting) planIsExpr(ctx *plancontext.PlanningContext, node *sqlparser.IsExpr) bool {
	// we only handle IS NULL correct. IsExpr can contain other expressions as well
	if node.Right != sqlparser.IsNullOp {
		return false
	}
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false
	}
	vdValue := &sqlparser.NullVal{}
	val := makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}
	opcodeF := func(vindex *vindexes.ColumnVindex) engine.Opcode {
		if _, ok := vindex.Vindex.(vindexes.Lookup); ok {
			return engine.Scatter
		}
		return equalOrEqualUnique(vindex)
	}

	return tr.haveMatchingVindex(ctx, node, vdValue, column, val, opcodeF, justTheVindex)
}

func isImpossibleIN(node *sqlparser.ComparisonExpr) bool {
	tuples, ok := node.Right.(sqlparser.ValTuple)
	if !ok {
		return false
	}
	return len(tuples) == 1 && sqlparser.IsNull(tuples[0])
}

func isImpossibleNotIN(node *sqlparser.ComparisonExpr) bool {
	tuples, ok := node.Right.(sqlparser.ValTuple)
	if !ok {
		return false
	}

	for _, n := range tuples {
		if sqlparser.IsNull(n) {
			return true
		}
	}

	return false
}

func (tr *ShardedRouting) planInOp(ctx *plancontext.PlanningContext, cmp *sqlparser.ComparisonExpr) bool {
	switch left := cmp.Left.(type) {
	case *sqlparser.ColName:
		vdValue := cmp.Right

		valTuple, isTuple := vdValue.(sqlparser.ValTuple)
		if isTuple && len(valTuple) == 1 {
			return tr.planEqualOp(ctx, &sqlparser.ComparisonExpr{Left: left, Right: valTuple[0], Operator: sqlparser.EqualOp})
		}

		value := makeEvalEngineExpr(ctx, vdValue)
		if value == nil {
			return false
		}
		opcode := func(*vindexes.ColumnVindex) engine.Opcode { return engine.IN }
		return tr.haveMatchingVindex(ctx, cmp, vdValue, left, value, opcode, justTheVindex)
	case sqlparser.ValTuple:
		right, rightIsValTuple := cmp.Right.(sqlparser.ValTuple)
		if !rightIsValTuple {
			return false
		}
		return tr.planCompositeInOpRecursive(ctx, cmp, left, right, nil)
	}

	return false
}

func (tr *ShardedRouting) planLikeOp(ctx *plancontext.PlanningContext, node *sqlparser.ComparisonExpr) bool {
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false
	}

	vdValue := node.Right
	val := makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}
	selectEqual := func(*vindexes.ColumnVindex) engine.Opcode { return engine.Equal }
	vdx := func(vindex *vindexes.ColumnVindex) vindexes.Vindex {
		if prefixable, ok := vindex.Vindex.(vindexes.Prefixable); ok {
			return prefixable.PrefixVindex()
		}

		// if we can't use the vindex as a prefix-vindex, we can't use this vindex at all
		return nil
	}
	return tr.haveMatchingVindex(ctx, node, vdValue, column, val, selectEqual, vdx)
}

func (tr *ShardedRouting) Cost() int {
	switch tr.RouteOpCode {
	case engine.EqualUnique:
		return 1
	case engine.Equal:
		return 5
	case engine.IN:
		return 10
	case engine.MultiEqual:
		return 10
	case engine.Scatter:
		return 20
	default:
		panic("this switch should be exhaustive")
	}
}

func (tr *ShardedRouting) OpCode() engine.Opcode {
	return tr.RouteOpCode
}

func (tr *ShardedRouting) Keyspace() *vindexes.Keyspace {
	return tr.keyspace
}

// PickBestAvailableVindex goes over the available vindexes for this route and picks the best one available.
func (tr *ShardedRouting) PickBestAvailableVindex() {
	for _, v := range tr.VindexPreds {
		option := v.bestOption()
		if option != nil && (tr.Selected == nil || less(option.Cost, tr.Selected.Cost)) {
			tr.Selected = option
			tr.RouteOpCode = option.OpCode
		}
	}
}

func (tr *ShardedRouting) haveMatchingVindex(
	ctx *plancontext.PlanningContext,
	node sqlparser.Expr,
	valueExpr sqlparser.Expr,
	column *sqlparser.ColName,
	value evalengine.Expr,
	opcode func(*vindexes.ColumnVindex) engine.Opcode,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) bool {
	newVindexFound := false
	for _, v := range tr.VindexPreds {
		// check that the
		if !ctx.SemTable.DirectDeps(column).IsSolvedBy(v.TableID) {
			continue
		}
		switch v.ColVindex.Vindex.(type) {
		case vindexes.SingleColumn:
			col := v.ColVindex.Columns[0]
			if column.Name.Equal(col) {
				// single column vindex - just add the option
				routeOpcode := opcode(v.ColVindex)
				vindex := vfunc(v.ColVindex)
				if vindex == nil || routeOpcode == engine.Scatter {
					continue
				}
				v.Options = append(v.Options, &VindexOption{
					Values:      []evalengine.Expr{value},
					ValueExprs:  []sqlparser.Expr{valueExpr},
					Predicates:  []sqlparser.Expr{node},
					OpCode:      routeOpcode,
					FoundVindex: vindex,
					Cost:        costFor(v.ColVindex, routeOpcode),
					Ready:       true,
				})
				newVindexFound = true
			}
		case vindexes.MultiColumn:
			colLoweredName := ""
			indexOfCol := -1
			for idx, col := range v.ColVindex.Columns {
				if column.Name.Equal(col) {
					colLoweredName = column.Name.Lowered()
					indexOfCol = idx
					break
				}
			}
			if colLoweredName == "" {
				break
			}

			var newOption []*VindexOption
			for _, op := range v.Options {
				if op.Ready {
					continue
				}
				_, isPresent := op.ColsSeen[colLoweredName]
				if isPresent {
					continue
				}
				option := copyOption(op)
				optionReady := option.updateWithNewColumn(colLoweredName, valueExpr, indexOfCol, value, node, v.ColVindex, opcode)
				if optionReady {
					newVindexFound = true
				}
				newOption = append(newOption, option)
			}
			v.Options = append(v.Options, newOption...)

			// multi column vindex - just always add as new option
			option := createOption(v.ColVindex, vfunc)
			optionReady := option.updateWithNewColumn(colLoweredName, valueExpr, indexOfCol, value, node, v.ColVindex, opcode)
			if optionReady {
				newVindexFound = true
			}
			v.Options = append(v.Options, option)
		}
	}
	return newVindexFound
}

func (tr *ShardedRouting) planEqualOp(ctx *plancontext.PlanningContext, node *sqlparser.ComparisonExpr) bool {
	column, ok := node.Left.(*sqlparser.ColName)
	other := node.Right
	vdValue := other
	if !ok {
		column, ok = node.Right.(*sqlparser.ColName)
		if !ok {
			// either the LHS or RHS have to be a column to be useful for the vindex
			return false
		}
		vdValue = node.Left
	}
	val := makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}

	return tr.haveMatchingVindex(ctx, node, vdValue, column, val, equalOrEqualUnique, justTheVindex)
}

func (tr *ShardedRouting) planCompositeInOpRecursive(
	ctx *plancontext.PlanningContext,
	cmp *sqlparser.ComparisonExpr,
	left, right sqlparser.ValTuple,
	coordinates []int,
) bool {
	foundVindex := false
	cindex := len(coordinates)
	coordinates = append(coordinates, 0)
	for i, expr := range left {
		coordinates[cindex] = i
		switch expr := expr.(type) {
		case sqlparser.ValTuple:
			ok := tr.planCompositeInOpRecursive(ctx, cmp, expr, right, coordinates)
			return ok || foundVindex
		case *sqlparser.ColName:
			// check if left col is a vindex
			if !tr.hasVindex(expr) {
				continue
			}

			rightVals := make(sqlparser.ValTuple, len(right))
			for j, currRight := range right {
				switch currRight := currRight.(type) {
				case sqlparser.ValTuple:
					val := tupleAccess(currRight, coordinates)
					if val == nil {
						return false
					}
					rightVals[j] = val
				default:
					return false
				}
			}
			newPlanValues := makeEvalEngineExpr(ctx, rightVals)
			if newPlanValues == nil {
				return false
			}

			opcode := func(*vindexes.ColumnVindex) engine.Opcode { return engine.MultiEqual }
			newVindex := tr.haveMatchingVindex(ctx, cmp, rightVals, expr, newPlanValues, opcode, justTheVindex)
			foundVindex = newVindex || foundVindex
		}
	}
	return foundVindex
}

func (tr *ShardedRouting) hasVindex(column *sqlparser.ColName) bool {
	for _, v := range tr.VindexPreds {
		for _, col := range v.ColVindex.Columns {
			if column.Name.Equal(col) {
				return true
			}
		}
	}
	return false
}

// Reset all vindex predicates on this route and re-build their options from
// the list of seen routing predicates.
func (tr *ShardedRouting) resetRoutingSelections(ctx *plancontext.PlanningContext) error {
	tr.RouteOpCode = engine.Scatter
	tr.Selected = nil
	for i, vp := range tr.VindexPreds {
		tr.VindexPreds[i] = &VindexPlusPredicates{ColVindex: vp.ColVindex, TableID: vp.TableID}
	}

	var routing Routing
	for _, predicate := range tr.SeenPredicates {
		var err error
		routing, err = tr.UpdateRoutingLogic(ctx, predicate)
		if err != nil {
			return err
		}
	}
	if routing != tr {
		return vterrors.VT13001("uh-oh. we ended up with a different type of routing")
	}
	return nil
}

func (tr *ShardedRouting) SelectedVindex() vindexes.Vindex {
	if tr.Selected == nil {
		return nil
	}
	return tr.Selected.FoundVindex
}

func (tr *ShardedRouting) VindexExpressions() []sqlparser.Expr {
	if tr.Selected == nil {
		return nil
	}
	return tr.Selected.ValueExprs
}

// makePlanValue transforms the given sqlparser.Expr into a sqltypes.PlanValue.
// If the given sqlparser.Expr is an argument and can be found in the r.argToReplaceBySelect then the
// method will stops and return nil values.
// Otherwise, the method will try to apply makePlanValue for any equality the sqlparser.Expr n has.
// The first PlanValue that is successfully produced will be returned.
func makeEvalEngineExpr(ctx *plancontext.PlanningContext, n sqlparser.Expr) evalengine.Expr {
	if ctx.IsSubQueryToReplace(n) {
		return nil
	}

	for _, expr := range ctx.SemTable.GetExprAndEqualities(n) {
		if subq, isSubq := expr.(*sqlparser.Subquery); isSubq {
			extractedSubquery := ctx.SemTable.FindSubqueryReference(subq)
			if extractedSubquery == nil {
				continue
			}
			switch engine.PulloutOpcode(extractedSubquery.OpCode) {
			case engine.PulloutIn, engine.PulloutNotIn:
				expr = sqlparser.NewListArg(extractedSubquery.GetArgName())
			case engine.PulloutValue, engine.PulloutExists:
				expr = sqlparser.NewArgument(extractedSubquery.GetArgName())
			}
		}
		pv, _ := evalengine.Translate(expr, ctx.SemTable)
		if pv != nil {
			return pv
		}
	}

	return nil
}
