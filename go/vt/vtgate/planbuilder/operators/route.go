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

package operators

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	Route struct {
		Source ops.Operator

		// Routes that have been merged into this one.
		MergedWith []*Route

		Routing Routing

		Ordering []RouteOrdering

		ResultColumns int
	}

	RouteOrdering struct {
		AST sqlparser.Expr
		// Offset and WOffset will contain the offset to the column (and the weightstring column). -1 if it's missing
		Offset, WOffset int
		Direction       sqlparser.OrderDirection
	}

	// VindexPlusPredicates is a struct used to store all the predicates that the vindex can be used to query
	VindexPlusPredicates struct {
		TableID   semantics.TableSet
		ColVindex *vindexes.ColumnVindex

		// during planning, we store the alternatives found for this route in this slice
		Options []*VindexOption
	}

	// VindexOption stores the information needed to know if we have all the information needed to use a vindex
	VindexOption struct {
		Ready  bool
		Values []evalengine.Expr
		// Columns that we have seen so far. Used only for multi-column vindexes so that we can track how many Columns part of the vindex we have seen
		ColsSeen    map[string]any
		ValueExprs  []sqlparser.Expr
		Predicates  []sqlparser.Expr
		OpCode      engine.Opcode
		FoundVindex vindexes.Vindex
		Cost        Cost
	}

	// Cost is used to make it easy to compare the Cost of two plans with each other
	Cost struct {
		VindexCost int
		IsUnique   bool
		OpCode     engine.Opcode
	}

	// Routing is used for the routing and merging logic of `Route`s. Every Route has a Routing object, and
	// this object is updated when predicates are found, and when merging `Route`s together
	Routing interface {
		// UpdateRoutingParams allows a Routing to control the routing params that will be used by the engine Route
		// OpCode is already set, and the default keyspace is set for read queries
		UpdateRoutingParams(ctx *plancontext.PlanningContext, rp *engine.RoutingParameters) error

		// Clone returns a copy of the routing. Since we are trying different variation of merging,
		// one Routing can be used in different constellations.
		// We don't want these different alternatives to influence each other, and cloning allows this
		Clone() Routing

		// Cost returns the cost of this Route.
		Cost() int
		OpCode() engine.Opcode
		Keyspace() *vindexes.Keyspace // note that all routings do not have a keyspace, so this method can return nil

		// updateRoutingLogic updates the routing to take predicates into account. This can be used for routing
		// using vindexes or for figuring out which keyspace an information_schema query should be sent to.
		updateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error)
	}
)

// UpdateRoutingLogic first checks if we are dealing with a predicate that
func UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr, r Routing) (Routing, error) {
	ks := r.Keyspace()
	if ks == nil {
		var err error
		ks, err = ctx.VSchema.AnyKeyspace()
		if err != nil {
			return nil, err
		}
	}
	nr := &NoneRouting{keyspace: ks}

	if isConstantFalse(expr) {
		return nr, nil
	}

	exit := func() (Routing, error) {
		return r.updateRoutingLogic(ctx, expr)
	}

	// For some expressions, even if we can't evaluate them, we know that they will always return false or null
	cmp, ok := expr.(*sqlparser.ComparisonExpr)
	if !ok {
		return exit()
	}

	if cmp.Operator != sqlparser.NullSafeEqualOp && (sqlparser.IsNull(cmp.Left) || sqlparser.IsNull(cmp.Right)) {
		// any comparison against a literal null, except a null safe equality (<=>), will return null
		return nr, nil
	}

	tuples, ok := cmp.Right.(sqlparser.ValTuple)
	if !ok {
		return exit()
	}

	switch cmp.Operator {
	case sqlparser.NotInOp:
		for _, n := range tuples {
			// If any of the values in the tuple is a literal null, we know that this comparison will always return NULL
			if sqlparser.IsNull(n) {
				return nr, nil
			}
		}
	case sqlparser.InOp:
		// WHERE col IN (null)
		if len(tuples) == 1 && sqlparser.IsNull(tuples[0]) {
			return nr, nil
		}
	}

	return exit()
}

// isConstantFalse checks whether this predicate can be evaluated at plan-time. If it returns `false` or `null`,
// we know that the query will not return anything, and this can be used to produce better plans
func isConstantFalse(expr sqlparser.Expr) bool {
	eenv := evalengine.EmptyExpressionEnv()
	eexpr, err := evalengine.Translate(expr, nil)
	if err != nil {
		return false
	}
	eres, err := eenv.Evaluate(eexpr)
	if err != nil {
		return false
	}
	if eres.Value().IsNull() {
		return false
	}
	b, err := eres.ToBooleanStrict()
	if err != nil {
		return false
	}
	return !b
}

// Cost implements the Operator interface
func (r *Route) Cost() int {
	return r.Routing.Cost()
}

// Clone implements the Operator interface
func (r *Route) Clone(inputs []ops.Operator) ops.Operator {
	cloneRoute := *r
	cloneRoute.Source = inputs[0]
	cloneRoute.Routing = r.Routing.Clone()
	return &cloneRoute
}

// Inputs implements the Operator interface
func (r *Route) Inputs() []ops.Operator {
	return []ops.Operator{r.Source}
}

// SetInputs implements the Operator interface
func (r *Route) SetInputs(ops []ops.Operator) {
	r.Source = ops[0]
}

func createOption(
	colVindex *vindexes.ColumnVindex,
	vfunc func(*vindexes.ColumnVindex) vindexes.Vindex,
) *VindexOption {
	values := make([]evalengine.Expr, len(colVindex.Columns))
	predicates := make([]sqlparser.Expr, len(colVindex.Columns))
	vindex := vfunc(colVindex)

	return &VindexOption{
		Values:      values,
		Predicates:  predicates,
		ColsSeen:    map[string]any{},
		FoundVindex: vindex,
	}
}

func copyOption(orig *VindexOption) *VindexOption {
	colsSeen := make(map[string]any, len(orig.ColsSeen))
	valueExprs := make([]sqlparser.Expr, len(orig.ValueExprs))
	values := make([]evalengine.Expr, len(orig.Values))
	predicates := make([]sqlparser.Expr, len(orig.Predicates))

	copy(values, orig.Values)
	copy(valueExprs, orig.ValueExprs)
	copy(predicates, orig.Predicates)
	for k, v := range orig.ColsSeen {
		colsSeen[k] = v
	}
	vo := &VindexOption{
		Values:      values,
		ColsSeen:    colsSeen,
		ValueExprs:  valueExprs,
		Predicates:  predicates,
		OpCode:      orig.OpCode,
		FoundVindex: orig.FoundVindex,
		Cost:        orig.Cost,
	}
	return vo
}

func (option *VindexOption) updateWithNewColumn(
	colLoweredName string,
	valueExpr sqlparser.Expr,
	indexOfCol int,
	value evalengine.Expr,
	node sqlparser.Expr,
	colVindex *vindexes.ColumnVindex,
	opcode func(*vindexes.ColumnVindex) engine.Opcode,
) bool {
	option.ColsSeen[colLoweredName] = true
	option.ValueExprs = append(option.ValueExprs, valueExpr)
	option.Values[indexOfCol] = value
	option.Predicates[indexOfCol] = node
	option.Ready = len(option.ColsSeen) == len(colVindex.Columns)
	routeOpcode := opcode(colVindex)
	if option.OpCode < routeOpcode {
		option.OpCode = routeOpcode
		option.Cost = costFor(colVindex, routeOpcode)
	}
	return option.Ready
}

func (r *Route) IsSingleShard() bool {
	switch r.Routing.OpCode() {
	case engine.Unsharded, engine.DBA, engine.Next, engine.EqualUnique, engine.Reference:
		return true
	}
	return false
}

func tupleAccess(expr sqlparser.Expr, coordinates []int) sqlparser.Expr {
	tuple, _ := expr.(sqlparser.ValTuple)
	for _, idx := range coordinates {
		if idx >= len(tuple) {
			return nil
		}
		expr = tuple[idx]
		tuple, _ = expr.(sqlparser.ValTuple)
	}
	return expr
}

func equalOrEqualUnique(vindex *vindexes.ColumnVindex) engine.Opcode {
	if vindex.IsPartialVindex() {
		return engine.SubShard
	}
	if vindex.IsUnique() {
		return engine.EqualUnique
	}

	return engine.Equal
}

func justTheVindex(vindex *vindexes.ColumnVindex) vindexes.Vindex {
	return vindex.Vindex
}

// costFor returns a cost struct to make route choices easier to compare
func costFor(foundVindex *vindexes.ColumnVindex, opcode engine.Opcode) Cost {
	switch opcode {
	// For these opcodes, we should not have a vindex, so we just return the opcode as the cost
	case engine.Unsharded, engine.Next, engine.DBA, engine.Reference, engine.None, engine.Scatter:
		return Cost{
			OpCode: opcode,
		}
	}

	return Cost{
		VindexCost: foundVindex.Cost(),
		IsUnique:   foundVindex.IsUnique(),
		OpCode:     opcode,
	}
}

// less compares two costs and returns true if the first cost is cheaper than the second
func less(c1, c2 Cost) bool {
	switch {
	case c1.OpCode != c2.OpCode:
		return c1.OpCode < c2.OpCode
	case c1.IsUnique == c2.IsUnique:
		return c1.VindexCost <= c2.VindexCost
	default:
		return c1.IsUnique
	}
}

func (vpp *VindexPlusPredicates) bestOption() *VindexOption {
	var best *VindexOption
	var keepOptions []*VindexOption
	for _, option := range vpp.Options {
		if option.Ready {
			if best == nil || less(option.Cost, best.Cost) {
				best = option
			}
		} else {
			keepOptions = append(keepOptions, option)
		}
	}
	if best != nil {
		keepOptions = append(keepOptions, best)
	}
	vpp.Options = keepOptions
	return best
}

// createRoute returns either an information_schema route, or else consults the
// VSchema to find a suitable table, and then creates a route from that.
func createRoute(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	solves semantics.TableSet,
) (ops.Operator, error) {
	if queryTable.IsInfSchema {
		return createInfSchemaRoute(ctx, queryTable)
	}
	return findVSchemaTableAndCreateRoute(ctx, queryTable, queryTable.Table, solves, true /*planAlternates*/)
}

// findVSchemaTableAndCreateRoute consults the VSchema to find a suitable
// table, and then creates a route from that.
func findVSchemaTableAndCreateRoute(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	tableName sqlparser.TableName,
	solves semantics.TableSet,
	planAlternates bool,
) (*Route, error) {
	vschemaTable, _, _, _, target, err := ctx.VSchema.FindTableOrVindex(tableName)
	if target != nil {
		return nil, vterrors.VT12001("SELECT with a target destination")
	}
	if err != nil {
		return nil, err
	}

	return createRouteFromVSchemaTable(
		ctx,
		queryTable,
		vschemaTable,
		solves,
		planAlternates,
	)
}

// createRouteFromTable creates a route from the given VSchema table.
func createRouteFromVSchemaTable(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	vschemaTable *vindexes.Table,
	solves semantics.TableSet,
	planAlternates bool,
) (*Route, error) {
	if vschemaTable.Name.String() != queryTable.Table.Name.String() {
		// we are dealing with a routed table
		queryTable = queryTable.Clone()
		name := queryTable.Table.Name
		queryTable.Table.Name = vschemaTable.Name
		astTable, ok := queryTable.Alias.Expr.(sqlparser.TableName)
		if !ok {
			return nil, vterrors.VT13001("a derived table should never be a routed table")
		}
		realTableName := sqlparser.NewIdentifierCS(vschemaTable.Name.String())
		astTable.Name = realTableName
		if queryTable.Alias.As.IsEmpty() {
			// if the user hasn't specified an alias, we'll insert one here so the old table name still works
			queryTable.Alias.As = sqlparser.NewIdentifierCS(name.String())
		}
	}
	plan := &Route{
		Source: &Table{
			QTable: queryTable,
			VTable: vschemaTable,
		},
	}

	// We create the appropiate Routing struct here, depending on the type of table we are dealing with.
	routing := createRoutingForVTable(vschemaTable, solves)
	for _, predicate := range queryTable.Predicates {
		var err error
		routing, err = UpdateRoutingLogic(ctx, predicate, routing)
		if err != nil {
			return nil, err
		}
	}

	plan.Routing = routing

	switch routing := routing.(type) {
	case *ShardedRouting:
		if routing.isScatter() && len(queryTable.Predicates) > 0 {
			var err error
			// If we have a scatter query, it's worth spending a little extra time seeing if we can't improve it
			plan.Routing, err = routing.tryImprove(ctx, queryTable)
			if err != nil {
				return nil, err
			}
		}
	case *AnyShardRouting:
		if planAlternates {
			alternates, err := createAlternateRoutesFromVSchemaTable(ctx, queryTable, vschemaTable, solves)
			if err != nil {
				return nil, err
			}
			routing.Alternates = alternates
		}
	}

	return plan, nil
}

func createRoutingForVTable(vschemaTable *vindexes.Table, id semantics.TableSet) Routing {
	switch {
	case vschemaTable.Type == vindexes.TypeSequence:
		return &SequenceRouting{keyspace: vschemaTable.Keyspace}
	case vschemaTable.Type == vindexes.TypeReference && vschemaTable.Name.String() == "dual":
		return &DualRouting{}
	case vschemaTable.Type == vindexes.TypeReference || !vschemaTable.Keyspace.Sharded:
		return &AnyShardRouting{keyspace: vschemaTable.Keyspace}
	default:
		return newShardedRouting(vschemaTable, id)
	}
}

func createAlternateRoutesFromVSchemaTable(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	vschemaTable *vindexes.Table,
	solves semantics.TableSet,
) (map[*vindexes.Keyspace]*Route, error) {
	routes := make(map[*vindexes.Keyspace]*Route)

	switch vschemaTable.Type {
	case "", vindexes.TypeReference:
		for ksName, referenceTable := range vschemaTable.ReferencedBy {
			route, err := findVSchemaTableAndCreateRoute(
				ctx,
				queryTable,
				sqlparser.TableName{
					Name:      referenceTable.Name,
					Qualifier: sqlparser.NewIdentifierCS(ksName),
				},
				solves,
				false, /*planAlternates*/
			)
			if err != nil {
				return nil, err
			}
			routes[referenceTable.Keyspace] = route
		}

		if vschemaTable.Source != nil {
			route, err := findVSchemaTableAndCreateRoute(
				ctx,
				queryTable,
				vschemaTable.Source.TableName,
				solves,
				false, /*planAlternates*/
			)
			if err != nil {
				return nil, err
			}
			keyspace := route.Routing.Keyspace()
			if keyspace != nil {
				routes[keyspace] = route
			}
		}
	}

	return routes, nil
}

func (r *Route) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	// first we see if the predicate changes how we route
	newRouting, err := UpdateRoutingLogic(ctx, expr, r.Routing)
	if err != nil {
		return nil, err
	}
	r.Routing = newRouting

	// we also need to push the predicate down into the query
	newSrc, err := r.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	r.Source = newSrc
	return r, err
}

func createProjection(src ops.Operator) (*Projection, error) {
	proj := &Projection{Source: src}
	cols, err := src.GetColumns()
	if err != nil {
		return nil, err
	}
	for _, col := range cols {
		proj.addUnexploredExpr(col, col.Expr)
	}
	return proj, nil
}

func (r *Route) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, _, addToGroupBy bool) (ops.Operator, int, error) {
	removeKeyspaceFromSelectExpr(expr)

	// check if columns is already added.
	cols, err := r.GetColumns()
	if err != nil {
		return nil, 0, err
	}
	colAsExpr := func(e *sqlparser.AliasedExpr) sqlparser.Expr {
		return e.Expr
	}
	if offset, found := canReuseColumn(ctx, cols, expr.Expr, colAsExpr); found {
		return r, offset, nil
	}

	// if column is not already present, we check if we can easily find a projection
	// or aggregation in our source that we can add to
	if ok, offset := addColumnToInput(r.Source, expr, addToGroupBy); ok {
		return r, offset, nil
	}

	// If no-one could be found, we probably don't have one yet, so we add one here
	src, err := createProjection(r.Source)
	if err != nil {
		return nil, 0, err
	}
	r.Source = src

	// And since we are under the route, we don't need to continue pushing anything further down
	offset := src.addNoPushCol(expr, false)
	if err != nil {
		return nil, 0, err
	}
	return r, offset, nil
}

type selectExpressions interface {
	addNoPushCol(expr *sqlparser.AliasedExpr, addToGroupBy bool) int
	isDerived() bool
}

func addColumnToInput(operator ops.Operator, expr *sqlparser.AliasedExpr, addToGroupBy bool) (bool, int) {
	switch op := operator.(type) {
	case *CorrelatedSubQueryOp:
		return addColumnToInput(op.Outer, expr, addToGroupBy)
	case *Limit:
		return addColumnToInput(op.Source, expr, addToGroupBy)
	case *Ordering:
		return addColumnToInput(op.Source, expr, addToGroupBy)
	case selectExpressions:
		if op.isDerived() {
			// if the only thing we can push to is a derived table,
			// we have to add a new projection and can't build on this one
			return false, 0
		}
		offset := op.addNoPushCol(expr, addToGroupBy)
		return true, offset
	default:
		return false, 0
	}
}

func (r *Route) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	return r.Source.GetColumns()
}

func (r *Route) GetOrdering() ([]ops.OrderBy, error) {
	return r.Source.GetOrdering()
}

// TablesUsed returns tables used by MergedWith routes, which are not included
// in Inputs() and thus not a part of the operator tree
func (r *Route) TablesUsed() []string {
	addString, collect := collectSortedUniqueStrings()
	for _, mw := range r.MergedWith {
		for _, u := range TablesUsed(mw) {
			addString(u)
		}
	}
	return collect()
}
func isSpecialOrderBy(o ops.OrderBy) bool {
	if sqlparser.IsNull(o.Inner.Expr) {
		return true
	}
	f, isFunction := o.Inner.Expr.(*sqlparser.FuncExpr)
	return isFunction && f.Name.Lowered() == "rand"
}

func (r *Route) planOffsets(ctx *plancontext.PlanningContext) (err error) {
	// if operator is returning data from a single shard, we don't need to do anything more
	if r.IsSingleShard() {
		return nil
	}

	// if we are getting results from multiple shards, we need to do a merge-sort
	// between them to get the final output correctly sorted
	ordering, err := r.Source.GetOrdering()
	if err != nil || len(ordering) == 0 {
		return err
	}

	columns, err := r.Source.GetColumns()
	if err != nil {
		return err
	}

	for _, order := range ordering {
		if isSpecialOrderBy(order) {
			continue
		}
		offset, err := r.getOffsetFor(ctx, order, columns)
		if err != nil {
			return err
		}

		if err != nil {
			return err
		}
		o := RouteOrdering{
			AST:       order.Inner.Expr,
			Offset:    offset,
			WOffset:   -1,
			Direction: order.Inner.Direction,
		}
		if ctx.SemTable.NeedsWeightString(order.SimplifiedExpr) {
			wrap := aeWrap(weightStringFor(order.SimplifiedExpr))
			_, offset, err = r.AddColumn(ctx, wrap, true, false)
			if err != nil {
				return err
			}
			o.WOffset = offset
		}
		r.Ordering = append(r.Ordering, o)
	}

	return nil
}

func weightStringFor(expr sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.WeightStringFuncExpr{Expr: expr}
}

func (r *Route) getOffsetFor(ctx *plancontext.PlanningContext, order ops.OrderBy, columns []*sqlparser.AliasedExpr) (int, error) {
	for idx, column := range columns {
		if sqlparser.Equals.Expr(order.SimplifiedExpr, column.Expr) {
			return idx, nil
		}
	}

	_, offset, err := r.AddColumn(ctx, aeWrap(order.Inner.Expr), true, false)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (r *Route) Description() ops.OpDescription {
	return ops.OpDescription{
		OperatorType: "Route",
		Other: map[string]any{
			"OpCode":   r.Routing.OpCode(),
			"Keyspace": r.Routing.Keyspace(),
		},
	}
}

func (r *Route) ShortDescription() string {
	first := r.Routing.OpCode().String()

	ks := r.Routing.Keyspace()
	if ks != nil {
		first = fmt.Sprintf("%s on %s", r.Routing.OpCode().String(), ks.Name)
	}

	orderBy, err := r.Source.GetOrdering()
	if err != nil {
		return first
	}

	ordering := ""
	if len(orderBy) > 0 {
		var oo []string
		for _, o := range orderBy {
			oo = append(oo, sqlparser.String(o.Inner))
		}
		ordering = " order by " + strings.Join(oo, ",")
	}

	return first + ordering
}

func (r *Route) setTruncateColumnCount(offset int) {
	r.ResultColumns = offset
}
