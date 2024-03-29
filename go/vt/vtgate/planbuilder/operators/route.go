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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	Route struct {
		Source Operator

		// Routes that have been merged into this one.
		MergedWith []*Route

		Routing Routing

		Ordering []RouteOrdering

		Comments *sqlparser.ParsedComments
		Lock     sqlparser.Lock

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
		UpdateRoutingParams(ctx *plancontext.PlanningContext, rp *engine.RoutingParameters)

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
		updateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Routing
	}
)

// UpdateRoutingLogic first checks if we are dealing with a predicate that
func UpdateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr, r Routing) Routing {
	ks := r.Keyspace()
	if ks == nil {
		var err error
		ks, err = ctx.VSchema.AnyKeyspace()
		if err != nil {
			panic(err)
		}
	}
	nr := &NoneRouting{keyspace: ks}

	if isConstantFalse(ctx.VSchema.Environment(), expr, ctx.VSchema.ConnCollation()) {
		return nr
	}

	exit := func() Routing {
		return r.updateRoutingLogic(ctx, expr)
	}

	// For some expressions, even if we can't evaluate them, we know that they will always return false or null
	cmp, ok := expr.(*sqlparser.ComparisonExpr)
	if !ok {
		return exit()
	}

	if cmp.Operator != sqlparser.NullSafeEqualOp && (sqlparser.IsNull(cmp.Left) || sqlparser.IsNull(cmp.Right)) {
		// any comparison against a literal null, except a null safe equality (<=>), will return null
		return nr
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
				return nr
			}
		}
	case sqlparser.InOp:
		// WHERE col IN (null)
		if len(tuples) == 1 && sqlparser.IsNull(tuples[0]) {
			return nr
		}
	}

	return exit()
}

// isConstantFalse checks whether this predicate can be evaluated at plan-time. If it returns `false` or `null`,
// we know that the query will not return anything, and this can be used to produce better plans
func isConstantFalse(env *vtenv.Environment, expr sqlparser.Expr, collation collations.ID) bool {
	eenv := evalengine.EmptyExpressionEnv(env)
	eexpr, err := evalengine.Translate(expr, &evalengine.Config{
		Collation:   collation,
		Environment: env,
	})
	if err != nil {
		return false
	}
	eres, err := eenv.Evaluate(eexpr)
	if err != nil {
		return false
	}
	if eres.Value(collation).IsNull() {
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
func (r *Route) Clone(inputs []Operator) Operator {
	cloneRoute := *r
	cloneRoute.Source = inputs[0]
	cloneRoute.Routing = r.Routing.Clone()
	return &cloneRoute
}

// Inputs implements the Operator interface
func (r *Route) Inputs() []Operator {
	return []Operator{r.Source}
}

// SetInputs implements the Operator interface
func (r *Route) SetInputs(ops []Operator) {
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
	if valueExpr != nil {
		option.ValueExprs = append(option.ValueExprs, valueExpr)
	}
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

func (r *Route) IsSingleShardOrByDestination() bool {
	switch r.Routing.OpCode() {
	case engine.Unsharded, engine.DBA, engine.Next, engine.EqualUnique, engine.Reference, engine.ByDestination:
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
) Operator {
	if queryTable.IsInfSchema {
		return createInfSchemaRoute(ctx, queryTable)
	}
	return findVSchemaTableAndCreateRoute(ctx, queryTable, queryTable.Table, true /*planAlternates*/)
}

// findVSchemaTableAndCreateRoute consults the VSchema to find a suitable
// table, and then creates a route from that.
func findVSchemaTableAndCreateRoute(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	tableName sqlparser.TableName,
	planAlternates bool,
) *Route {
	vschemaTable, _, _, tabletType, target, err := ctx.VSchema.FindTableOrVindex(tableName)
	if err != nil {
		panic(err)
	}

	targeted := createTargetedRouting(ctx, target, tabletType, vschemaTable)

	return createRouteFromVSchemaTable(
		ctx,
		queryTable,
		vschemaTable,
		planAlternates,
		targeted,
	)
}

func createTargetedRouting(ctx *plancontext.PlanningContext, target key.Destination, tabletType topodatapb.TabletType, vschemaTable *vindexes.Table) Routing {
	switch ctx.Statement.(type) {
	case *sqlparser.Update:
		if tabletType != topodatapb.TabletType_PRIMARY {
			panic(vterrors.VT09002("update"))
		}
	case *sqlparser.Delete:
		if tabletType != topodatapb.TabletType_PRIMARY {
			panic(vterrors.VT09002("delete"))
		}
	case *sqlparser.Insert:
		if tabletType != topodatapb.TabletType_PRIMARY {
			panic(vterrors.VT09002("insert"))
		}
		if target != nil {
			panic(vterrors.VT09017("INSERT with a target destination is not allowed"))
		}
	case sqlparser.SelectStatement:
		if target != nil {
			panic(vterrors.VT09017("SELECT with a target destination is not allowed"))
		}
	}

	if target != nil {
		return &TargetedRouting{
			keyspace:          vschemaTable.Keyspace,
			TargetDestination: target,
		}
	}
	return nil
}

// createRouteFromTable creates a route from the given VSchema table.
func createRouteFromVSchemaTable(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	vschemaTable *vindexes.Table,
	planAlternates bool,
	targeted Routing,
) *Route {
	if vschemaTable.Name.String() != queryTable.Table.Name.String() {
		// we are dealing with a routed table
		queryTable = queryTable.Clone()
		name := queryTable.Table.Name
		queryTable.Table.Name = vschemaTable.Name
		astTable, ok := queryTable.Alias.Expr.(sqlparser.TableName)
		if !ok {
			panic(vterrors.VT13001("a derived table should never be a routed table"))
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

	// We create the appropriate Routing struct here, depending on the type of table we are dealing with.
	var routing Routing
	if targeted != nil {
		routing = targeted
	} else {
		routing = createRoutingForVTable(ctx, vschemaTable, queryTable.ID)
	}

	for _, predicate := range queryTable.Predicates {
		routing = UpdateRoutingLogic(ctx, predicate, routing)
	}

	plan.Routing = routing

	switch routing := routing.(type) {
	case *ShardedRouting:
		if routing.isScatter() && len(queryTable.Predicates) > 0 {
			// If we have a scatter query, it's worth spending a little extra time seeing if we can't improve it
			plan.Routing = routing.tryImprove(ctx, queryTable)
		}
	case *AnyShardRouting:
		if planAlternates {
			routing.Alternates = createAlternateRoutesFromVSchemaTable(ctx, queryTable, vschemaTable)
		}
	}

	return plan
}

func createRoutingForVTable(ctx *plancontext.PlanningContext, vschemaTable *vindexes.Table, id semantics.TableSet) Routing {
	switch {
	case vschemaTable.Type == vindexes.TypeSequence:
		return &SequenceRouting{keyspace: vschemaTable.Keyspace}
	case vschemaTable.Type == vindexes.TypeReference && vschemaTable.Name.String() == "dual":
		return &DualRouting{}
	case vschemaTable.Type == vindexes.TypeReference || !vschemaTable.Keyspace.Sharded:
		return &AnyShardRouting{keyspace: vschemaTable.Keyspace}
	default:
		return newShardedRouting(ctx, vschemaTable, id)
	}
}

func createAlternateRoutesFromVSchemaTable(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	vschemaTable *vindexes.Table,
) map[*vindexes.Keyspace]*Route {
	routes := make(map[*vindexes.Keyspace]*Route)

	switch vschemaTable.Type {
	case "", vindexes.TypeReference:
		for ksName, referenceTable := range vschemaTable.ReferencedBy {
			route := findVSchemaTableAndCreateRoute(
				ctx,
				queryTable,
				sqlparser.TableName{
					Name:      referenceTable.Name,
					Qualifier: sqlparser.NewIdentifierCS(ksName),
				},
				false, /*planAlternates*/
			)
			routes[referenceTable.Keyspace] = route
		}

		if vschemaTable.Source != nil {
			route := findVSchemaTableAndCreateRoute(
				ctx,
				queryTable,
				vschemaTable.Source.TableName,
				false, /*planAlternates*/
			)
			keyspace := route.Routing.Keyspace()
			if keyspace != nil {
				routes[keyspace] = route
			}
		}
	}

	return routes
}

func (r *Route) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	// first we see if the predicate changes how we route
	newRouting := UpdateRoutingLogic(ctx, expr, r.Routing)
	r.Routing = newRouting

	// we also need to push the predicate down into the query
	r.Source = r.Source.AddPredicate(ctx, expr)
	return r
}

func createProjection(ctx *plancontext.PlanningContext, src Operator, derivedName string) *Projection {
	proj := newAliasedProjection(src)
	cols := src.GetColumns(ctx)
	for _, col := range cols {
		if derivedName == "" {
			proj.addUnexploredExpr(col, col.Expr)
			continue
		}

		// for derived tables, we want to use the exposed colname
		tableName := sqlparser.NewTableName(derivedName)
		columnName := col.ColumnName()
		colName := sqlparser.NewColNameWithQualifier(columnName, tableName)
		ctx.SemTable.CopySemanticInfo(col.Expr, colName)
		proj.addUnexploredExpr(aeWrap(colName), colName)
	}
	return proj
}

func (r *Route) AddColumn(ctx *plancontext.PlanningContext, reuse bool, gb bool, expr *sqlparser.AliasedExpr) int {
	removeKeyspaceFromSelectExpr(expr)

	if reuse {
		offset := r.FindCol(ctx, expr.Expr, true)
		if offset != -1 {
			return offset
		}
	}

	// if at least one column is not already present, we check if we can easily find a projection
	// or aggregation in our source that we can add to
	derived, op, ok, offsets := addMultipleColumnsToInput(ctx, r.Source, reuse, []bool{gb}, []*sqlparser.AliasedExpr{expr})
	r.Source = op
	if ok {
		return offsets[0]
	}

	// If no-one could be found, we probably don't have one yet, so we add one here
	src := createProjection(ctx, r.Source, derived)
	r.Source = src

	offsets = src.addColumnsWithoutPushing(ctx, reuse, []bool{gb}, []*sqlparser.AliasedExpr{expr})
	return offsets[0]
}

type selectExpressions interface {
	Operator
	addColumnWithoutPushing(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, addToGroupBy bool) int
	addColumnsWithoutPushing(ctx *plancontext.PlanningContext, reuse bool, addToGroupBy []bool, exprs []*sqlparser.AliasedExpr) []int
	derivedName() string
}

// addColumnToInput adds columns to an operator without pushing them down
func addMultipleColumnsToInput(
	ctx *plancontext.PlanningContext,
	operator Operator,
	reuse bool,
	addToGroupBy []bool,
	exprs []*sqlparser.AliasedExpr,
) (derivedName string, // if we found a derived table, this will contain its name
	projection Operator, // if an operator needed to be built, it will be returned here
	found bool, // whether a matching op was found or not
	offsets []int, // the offsets the expressions received
) {
	switch op := operator.(type) {
	case *SubQuery:
		derivedName, src, added, offset := addMultipleColumnsToInput(ctx, op.Outer, reuse, addToGroupBy, exprs)
		if added {
			op.Outer = src
		}
		return derivedName, op, added, offset

	case *Distinct:
		derivedName, src, added, offset := addMultipleColumnsToInput(ctx, op.Source, reuse, addToGroupBy, exprs)
		if added {
			op.Source = src
		}
		return derivedName, op, added, offset

	case *Limit:
		derivedName, src, added, offset := addMultipleColumnsToInput(ctx, op.Source, reuse, addToGroupBy, exprs)
		if added {
			op.Source = src
		}
		return derivedName, op, added, offset

	case *Ordering:
		derivedName, src, added, offset := addMultipleColumnsToInput(ctx, op.Source, reuse, addToGroupBy, exprs)
		if added {
			op.Source = src
		}
		return derivedName, op, added, offset

	case *LockAndComment:
		derivedName, src, added, offset := addMultipleColumnsToInput(ctx, op.Source, reuse, addToGroupBy, exprs)
		if added {
			op.Source = src
		}
		return derivedName, op, added, offset

	case *Horizon:
		// if the horizon has an alias, then it is a derived table,
		// we have to add a new projection and can't build on this one
		return op.Alias, op, false, nil

	case selectExpressions:
		name := op.derivedName()
		if name != "" {
			// if the only thing we can push to is a derived table,
			// we have to add a new projection and can't build on this one
			return name, op, false, nil
		}
		offset := op.addColumnsWithoutPushing(ctx, reuse, addToGroupBy, exprs)
		return "", op, true, offset

	case *Union:
		tableID := semantics.SingleTableSet(len(ctx.SemTable.Tables))
		ctx.SemTable.Tables = append(ctx.SemTable.Tables, nil)
		unionColumns := op.GetColumns(ctx)
		proj := &Projection{
			Source:  op,
			Columns: AliasedProjections(slice.Map(unionColumns, newProjExpr)),
			DT: &DerivedTable{
				TableID: tableID,
				Alias:   "dt",
			},
		}
		return addMultipleColumnsToInput(ctx, proj, reuse, addToGroupBy, exprs)
	default:
		return "", op, false, nil
	}
}

func (r *Route) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, _ bool) int {
	return r.Source.FindCol(ctx, expr, true)
}

func (r *Route) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return r.Source.GetColumns(ctx)
}

func (r *Route) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return r.Source.GetSelectExprs(ctx)
}

func (r *Route) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return r.Source.GetOrdering(ctx)
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

func isSpecialOrderBy(o OrderBy) bool {
	if sqlparser.IsNull(o.Inner.Expr) {
		return true
	}
	f, isFunction := o.Inner.Expr.(*sqlparser.FuncExpr)
	return isFunction && f.Name.Lowered() == "rand"
}

func (r *Route) planOffsets(ctx *plancontext.PlanningContext) Operator {
	// if operator is returning data from a single shard, we don't need to do anything more
	if r.IsSingleShard() {
		return nil
	}

	// if we are getting results from multiple shards, we need to do a merge-sort
	// between them to get the final output correctly sorted
	ordering := r.Source.GetOrdering(ctx)
	if len(ordering) == 0 {
		return nil
	}

	for _, order := range ordering {
		if isSpecialOrderBy(order) {
			continue
		}
		offset := r.AddColumn(ctx, true, false, aeWrap(order.SimplifiedExpr))

		o := RouteOrdering{
			AST:       order.Inner.Expr,
			Offset:    offset,
			WOffset:   -1,
			Direction: order.Inner.Direction,
		}
		if ctx.SemTable.NeedsWeightString(order.SimplifiedExpr) {
			ws := weightStringFor(order.SimplifiedExpr)
			offset := r.AddColumn(ctx, true, false, aeWrap(ws))
			o.WOffset = offset
		}
		r.Ordering = append(r.Ordering, o)
	}
	return nil
}

func weightStringFor(expr sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.WeightStringFuncExpr{Expr: expr}
}

func (r *Route) ShortDescription() string {
	first := r.Routing.OpCode().String()

	ks := r.Routing.Keyspace()
	if ks != nil {
		first = fmt.Sprintf("%s on %s", r.Routing.OpCode().String(), ks.Name)
	}

	type extraInfo interface {
		extraInfo() string
	}
	if info, ok := r.Routing.(extraInfo); ok {
		first += " " + info.extraInfo()
	}

	comments := ""
	if r.Comments != nil {
		comments = " comments: " + sqlparser.String(r.Comments)
	}
	lock := ""
	if r.Lock != sqlparser.NoLock {
		lock = " lock: " + r.Lock.ToString()
	}
	return first + comments + lock
}

func (r *Route) setTruncateColumnCount(offset int) {
	r.ResultColumns = offset
}

func (r *Route) introducesTableID() semantics.TableSet {
	id := semantics.EmptyTableSet()
	for _, route := range r.MergedWith {
		id = id.Merge(TableID(route))
	}
	return id
}
