/*
Copyright 2024 The Vitess Authors.

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

package sqlparser

// nodePool is a chunk-based allocator for a single AST node type. Instead of
// allocating each node individually on the heap, it allocates a chunk of nodes
// at once (a single []T allocation for chunkSize nodes), then hands out
// pointers to individual elements. This is GC-safe because the Go runtime
// knows the element type of []T and will properly scan interior pointers.
type nodePool[T any] struct {
	chunks [][]T
	cur    []T
	idx    int
}

const nodePoolChunkSize = 32

func (p *nodePool[T]) new() *T {
	if p.idx >= len(p.cur) {
		chunk := make([]T, nodePoolChunkSize)
		p.chunks = append(p.chunks, chunk)
		p.cur = chunk
		p.idx = 0
	}
	ptr := &p.cur[p.idx]
	p.idx++
	return ptr
}

// reset rewinds the pool so that existing chunks can be reused. It zeroes
// all elements that were handed out to clear stale references, then resets
// the allocation cursor to the first chunk.
func (p *nodePool[T]) reset() {
	if len(p.chunks) == 0 {
		return
	}
	// Zero all used elements in all chunks except the last one (fully used).
	for i := 0; i < len(p.chunks)-1; i++ {
		var zero T
		for j := range p.chunks[i] {
			p.chunks[i][j] = zero
		}
	}
	// Zero only the used elements in the last (current) chunk.
	var zero T
	for j := 0; j < p.idx; j++ {
		p.cur[j] = zero
	}
	// Keep only the first chunk to limit memory retention.
	p.cur = p.chunks[0]
	p.chunks = p.chunks[:1]
	p.idx = 0
}

// Arena is a collection of typed node pools for AST node allocation during
// parsing. It batches allocations by allocating chunks of nodes at a time,
// greatly reducing the number of individual heap allocations. Nodes allocated
// from an Arena are GC-safe: the Go runtime correctly scans interior pointers
// because each pool uses typed slices ([]T), not raw []byte.
//
// An Arena is not safe for concurrent use.
type Arena struct {
	aliasedExprs      nodePool[AliasedExpr]
	aliasedTableExprs nodePool[AliasedTableExpr]
	andExprs          nodePool[AndExpr]
	binaryExprs       nodePool[BinaryExpr]
	caseExprs         nodePool[CaseExpr]
	colNames          nodePool[ColName]
	collateExprs      nodePool[CollateExpr]
	columns           nodePool[ColumnDefinition]
	comparisonExprs   nodePool[ComparisonExpr]
	convertTypes      nodePool[ConvertType]
	curTimeFuncExprs  nodePool[CurTimeFuncExpr]
	deletes           nodePool[Delete]
	derivedTables     nodePool[DerivedTable]
	existsExprs       nodePool[ExistsExpr]
	funcExprs         nodePool[FuncExpr]
	groupConcatExprs  nodePool[GroupConcatExpr]
	indexHints        nodePool[IndexHint]
	indexOptions      nodePool[IndexOption]
	inserts           nodePool[Insert]
	intervalExprs     nodePool[IntervalExpr]
	isExprs           nodePool[IsExpr]
	joinConditions    nodePool[JoinCondition]
	joinTableExprs    nodePool[JoinTableExpr]
	limits            nodePool[Limit]
	literals          nodePool[Literal]
	matchExprs        nodePool[MatchExpr]
	notExprs          nodePool[NotExpr]
	nullVals          nodePool[NullVal]
	orders            nodePool[Order]
	orExprs           nodePool[OrExpr]
	overClauses       nodePool[OverClause]
	parenTableExprs   nodePool[ParenTableExpr]
	selects           nodePool[Select]
	selectIntos       nodePool[SelectInto]
	setExprs          nodePool[SetExpr]
	shows             nodePool[Show]
	starExprs         nodePool[StarExpr]
	subqueries        nodePool[Subquery]
	tableOptions      nodePool[TableOption]
	unaryExprs        nodePool[UnaryExpr]
	unions            nodePool[Union]
	updateExprs       nodePool[UpdateExpr]
	updates           nodePool[Update]
	whens             nodePool[When]
	wheres            nodePool[Where]
	withs             nodePool[With]
	xorExprs          nodePool[XorExpr]
}

// newArena creates a new Arena.
func newArena() *Arena {
	return &Arena{}
}

// Reset clears all pools so the arena can be reused. Existing chunk memory is
// retained (one chunk per pool) to avoid re-allocation, but all element data
// is zeroed to release references to old AST nodes.
func (a *Arena) Reset() {
	a.aliasedExprs.reset()
	a.aliasedTableExprs.reset()
	a.andExprs.reset()
	a.binaryExprs.reset()
	a.caseExprs.reset()
	a.colNames.reset()
	a.collateExprs.reset()
	a.columns.reset()
	a.comparisonExprs.reset()
	a.convertTypes.reset()
	a.curTimeFuncExprs.reset()
	a.deletes.reset()
	a.derivedTables.reset()
	a.existsExprs.reset()
	a.funcExprs.reset()
	a.groupConcatExprs.reset()
	a.indexHints.reset()
	a.indexOptions.reset()
	a.inserts.reset()
	a.intervalExprs.reset()
	a.isExprs.reset()
	a.joinConditions.reset()
	a.joinTableExprs.reset()
	a.limits.reset()
	a.literals.reset()
	a.matchExprs.reset()
	a.notExprs.reset()
	a.nullVals.reset()
	a.orders.reset()
	a.orExprs.reset()
	a.overClauses.reset()
	a.parenTableExprs.reset()
	a.selects.reset()
	a.selectIntos.reset()
	a.setExprs.reset()
	a.shows.reset()
	a.starExprs.reset()
	a.subqueries.reset()
	a.tableOptions.reset()
	a.unaryExprs.reset()
	a.unions.reset()
	a.updateExprs.reset()
	a.updates.reset()
	a.whens.reset()
	a.wheres.reset()
	a.withs.reset()
	a.xorExprs.reset()
}

// Typed allocation methods. Each returns a pointer to a zero-valued struct
// allocated from the arena's chunk pool. If the arena is nil, falls back to
// heap allocation.

// Value-copy allocation methods. Each accepts a struct by value, copies it
// into the arena pool, and returns a pointer. Nil-receiver safe: if the arena
// is nil, falls back to regular heap allocation. This allows grammar rules in
// sql.y to convert `&Type{...}` to `yyrcvr.Arena.newTypeV(Type{...})` with
// minimal syntactic change.

func (a *Arena) newAliasedExprV(v AliasedExpr) *AliasedExpr {
	if a == nil { p := v; return &p }; p := a.aliasedExprs.new(); *p = v; return p
}
func (a *Arena) newAliasedTableExprV(v AliasedTableExpr) *AliasedTableExpr {
	if a == nil { p := v; return &p }; p := a.aliasedTableExprs.new(); *p = v; return p
}
func (a *Arena) newAndExprV(v AndExpr) *AndExpr {
	if a == nil { p := v; return &p }; p := a.andExprs.new(); *p = v; return p
}
func (a *Arena) newBinaryExprV(v BinaryExpr) *BinaryExpr {
	if a == nil { p := v; return &p }; p := a.binaryExprs.new(); *p = v; return p
}
func (a *Arena) newCaseExprV(v CaseExpr) *CaseExpr {
	if a == nil { p := v; return &p }; p := a.caseExprs.new(); *p = v; return p
}
func (a *Arena) newColNameV(v ColName) *ColName {
	if a == nil { p := v; return &p }; p := a.colNames.new(); *p = v; return p
}
func (a *Arena) newCollateExprV(v CollateExpr) *CollateExpr {
	if a == nil { p := v; return &p }; p := a.collateExprs.new(); *p = v; return p
}
func (a *Arena) newColumnDefinitionV(v ColumnDefinition) *ColumnDefinition {
	if a == nil { p := v; return &p }; p := a.columns.new(); *p = v; return p
}
func (a *Arena) newComparisonExprV(v ComparisonExpr) *ComparisonExpr {
	if a == nil { p := v; return &p }; p := a.comparisonExprs.new(); *p = v; return p
}
func (a *Arena) newConvertTypeV(v ConvertType) *ConvertType {
	if a == nil { p := v; return &p }; p := a.convertTypes.new(); *p = v; return p
}
func (a *Arena) newCurTimeFuncExprV(v CurTimeFuncExpr) *CurTimeFuncExpr {
	if a == nil { p := v; return &p }; p := a.curTimeFuncExprs.new(); *p = v; return p
}
func (a *Arena) newDeleteV(v Delete) *Delete {
	if a == nil { p := v; return &p }; p := a.deletes.new(); *p = v; return p
}
func (a *Arena) newDerivedTableV(v DerivedTable) *DerivedTable {
	if a == nil { p := v; return &p }; p := a.derivedTables.new(); *p = v; return p
}
func (a *Arena) newExistsExprV(v ExistsExpr) *ExistsExpr {
	if a == nil { p := v; return &p }; p := a.existsExprs.new(); *p = v; return p
}
func (a *Arena) newFuncExprV(v FuncExpr) *FuncExpr {
	if a == nil { p := v; return &p }; p := a.funcExprs.new(); *p = v; return p
}
func (a *Arena) newGroupConcatExprV(v GroupConcatExpr) *GroupConcatExpr {
	if a == nil { p := v; return &p }; p := a.groupConcatExprs.new(); *p = v; return p
}
func (a *Arena) newIndexHintV(v IndexHint) *IndexHint {
	if a == nil { p := v; return &p }; p := a.indexHints.new(); *p = v; return p
}
func (a *Arena) newIndexOptionV(v IndexOption) *IndexOption {
	if a == nil { p := v; return &p }; p := a.indexOptions.new(); *p = v; return p
}
func (a *Arena) newInsertV(v Insert) *Insert {
	if a == nil { p := v; return &p }; p := a.inserts.new(); *p = v; return p
}
func (a *Arena) newIntervalExprV(v IntervalExpr) *IntervalExpr {
	if a == nil { p := v; return &p }; p := a.intervalExprs.new(); *p = v; return p
}
func (a *Arena) newIsExprV(v IsExpr) *IsExpr {
	if a == nil { p := v; return &p }; p := a.isExprs.new(); *p = v; return p
}
func (a *Arena) newJoinConditionV(v JoinCondition) *JoinCondition {
	if a == nil { p := v; return &p }; p := a.joinConditions.new(); *p = v; return p
}
func (a *Arena) newJoinTableExprV(v JoinTableExpr) *JoinTableExpr {
	if a == nil { p := v; return &p }; p := a.joinTableExprs.new(); *p = v; return p
}
func (a *Arena) newLimitV(v Limit) *Limit {
	if a == nil { p := v; return &p }; p := a.limits.new(); *p = v; return p
}
func (a *Arena) newLiteralV(v Literal) *Literal {
	if a == nil { p := v; return &p }; p := a.literals.new(); *p = v; return p
}
func (a *Arena) newMatchExprV(v MatchExpr) *MatchExpr {
	if a == nil { p := v; return &p }; p := a.matchExprs.new(); *p = v; return p
}
func (a *Arena) newNotExprV(v NotExpr) *NotExpr {
	if a == nil { p := v; return &p }; p := a.notExprs.new(); *p = v; return p
}
func (a *Arena) newNullValV(v NullVal) *NullVal {
	if a == nil { p := v; return &p }; p := a.nullVals.new(); *p = v; return p
}
func (a *Arena) newOrderV(v Order) *Order {
	if a == nil { p := v; return &p }; p := a.orders.new(); *p = v; return p
}
func (a *Arena) newOrExprV(v OrExpr) *OrExpr {
	if a == nil { p := v; return &p }; p := a.orExprs.new(); *p = v; return p
}
func (a *Arena) newOverClauseV(v OverClause) *OverClause {
	if a == nil { p := v; return &p }; p := a.overClauses.new(); *p = v; return p
}
func (a *Arena) newParenTableExprV(v ParenTableExpr) *ParenTableExpr {
	if a == nil { p := v; return &p }; p := a.parenTableExprs.new(); *p = v; return p
}
func (a *Arena) newSelectV(v Select) *Select {
	if a == nil { p := v; return &p }; p := a.selects.new(); *p = v; return p
}
func (a *Arena) newSelectIntoV(v SelectInto) *SelectInto {
	if a == nil { p := v; return &p }; p := a.selectIntos.new(); *p = v; return p
}
func (a *Arena) newSetExprV(v SetExpr) *SetExpr {
	if a == nil { p := v; return &p }; p := a.setExprs.new(); *p = v; return p
}
func (a *Arena) newShowV(v Show) *Show {
	if a == nil { p := v; return &p }; p := a.shows.new(); *p = v; return p
}
func (a *Arena) newStarExprV(v StarExpr) *StarExpr {
	if a == nil { p := v; return &p }; p := a.starExprs.new(); *p = v; return p
}
func (a *Arena) newSubqueryV(v Subquery) *Subquery {
	if a == nil { p := v; return &p }; p := a.subqueries.new(); *p = v; return p
}
func (a *Arena) newTableOptionV(v TableOption) *TableOption {
	if a == nil { p := v; return &p }; p := a.tableOptions.new(); *p = v; return p
}
func (a *Arena) newUnaryExprV(v UnaryExpr) *UnaryExpr {
	if a == nil { p := v; return &p }; p := a.unaryExprs.new(); *p = v; return p
}
func (a *Arena) newUnionV(v Union) *Union {
	if a == nil { p := v; return &p }; p := a.unions.new(); *p = v; return p
}
func (a *Arena) newUpdateExprV(v UpdateExpr) *UpdateExpr {
	if a == nil { p := v; return &p }; p := a.updateExprs.new(); *p = v; return p
}
func (a *Arena) newUpdateV(v Update) *Update {
	if a == nil { p := v; return &p }; p := a.updates.new(); *p = v; return p
}
func (a *Arena) newWhenV(v When) *When {
	if a == nil { p := v; return &p }; p := a.whens.new(); *p = v; return p
}
func (a *Arena) newWhereV(v Where) *Where {
	if a == nil { p := v; return &p }; p := a.wheres.new(); *p = v; return p
}
func (a *Arena) newWithV(v With) *With {
	if a == nil { p := v; return &p }; p := a.withs.new(); *p = v; return p
}
func (a *Arena) newXorExprV(v XorExpr) *XorExpr {
	if a == nil { p := v; return &p }; p := a.xorExprs.new(); *p = v; return p
}
