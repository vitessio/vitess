/*
Copyright 2026 The Vitess Authors.

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

import "strings"

const nodeSlabSize = 32

// nodeAllocator batch-allocates AST nodes in slabs to reduce the number of
// heap allocations during parsing. Each slab holds nodeSlabSize nodes and
// nodes are handed out one at a time from the front of the slab.
type nodeAllocator struct {
	selectSlab         []Select
	insertSlab         []Insert
	updateSlab         []Update
	deleteSlab         []Delete
	aliasedExprSlab    []AliasedExpr
	aliasedTableSlab   []AliasedTableExpr
	colNameSlab        []ColName
	literalSlab        []Literal
	whereSlab          []Where
	comparisonExprSlab []ComparisonExpr
	binaryExprSlab     []BinaryExpr
	unaryExprSlab      []UnaryExpr
	funcExprSlab       []FuncExpr
	andExprSlab        []AndExpr
	orExprSlab         []OrExpr
	notExprSlab        []NotExpr
	isExprSlab         []IsExpr
	xorExprSlab        []XorExpr
	joinTableExprSlab  []JoinTableExpr
	orderSlab          []Order
	limitSlab          []Limit
	subquerySlab       []Subquery
	groupBySlab        []GroupBy
	selectExprsSlab    []SelectExprs

	// Slice backing array slabs
	selectExprSliceSlab []SelectExpr
	exprSliceSlab       []Expr
	tableExprSliceSlab  []TableExpr
	identifierCISlab    []IdentifierCI
	orderPtrSlab        []*Order
	updateExprPtrSlab   []*UpdateExpr
	setExprPtrSlab      []*SetExpr
	valTupleSlab        []ValTuple

	// Slice header slabs for pointer-indirect union storage
	exprSliceHdrSlab      [][]Expr
	tableExprSliceHdrSlab []TableExprs
	columnsHdrSlab        []Columns
	valuesHdrSlab         []Values
	valTupleHdrSlab       []ValTuple
	orderByHdrSlab        []OrderBy
	updateExprsHdrSlab    []UpdateExprs
	setExprsHdrSlab       []SetExprs
	partitionsHdrSlab     []Partitions
	tableNamesHdrSlab     []TableNames
}

// na extracts the nodeAllocator from the lexer for use in grammar actions.
func na(yylex yyLexer) *nodeAllocator {
	return &yylex.(*Tokenizer).alloc
}

func (a *nodeAllocator) allocSelect() *Select {
	if len(a.selectSlab) == 0 {
		a.selectSlab = make([]Select, nodeSlabSize)
	}
	p := &a.selectSlab[0]
	a.selectSlab = a.selectSlab[1:]
	return p
}

func (a *nodeAllocator) allocInsert() *Insert {
	if len(a.insertSlab) == 0 {
		a.insertSlab = make([]Insert, nodeSlabSize)
	}
	p := &a.insertSlab[0]
	a.insertSlab = a.insertSlab[1:]
	return p
}

func (a *nodeAllocator) allocUpdate() *Update {
	if len(a.updateSlab) == 0 {
		a.updateSlab = make([]Update, nodeSlabSize)
	}
	p := &a.updateSlab[0]
	a.updateSlab = a.updateSlab[1:]
	return p
}

func (a *nodeAllocator) allocDelete() *Delete {
	if len(a.deleteSlab) == 0 {
		a.deleteSlab = make([]Delete, nodeSlabSize)
	}
	p := &a.deleteSlab[0]
	a.deleteSlab = a.deleteSlab[1:]
	return p
}

func (a *nodeAllocator) allocAliasedExpr() *AliasedExpr {
	if len(a.aliasedExprSlab) == 0 {
		a.aliasedExprSlab = make([]AliasedExpr, nodeSlabSize)
	}
	p := &a.aliasedExprSlab[0]
	a.aliasedExprSlab = a.aliasedExprSlab[1:]
	return p
}

func (a *nodeAllocator) allocAliasedTableExpr() *AliasedTableExpr {
	if len(a.aliasedTableSlab) == 0 {
		a.aliasedTableSlab = make([]AliasedTableExpr, nodeSlabSize)
	}
	p := &a.aliasedTableSlab[0]
	a.aliasedTableSlab = a.aliasedTableSlab[1:]
	return p
}

func (a *nodeAllocator) allocColName() *ColName {
	if len(a.colNameSlab) == 0 {
		a.colNameSlab = make([]ColName, nodeSlabSize)
	}
	p := &a.colNameSlab[0]
	a.colNameSlab = a.colNameSlab[1:]
	return p
}

func (a *nodeAllocator) allocLiteral() *Literal {
	if len(a.literalSlab) == 0 {
		a.literalSlab = make([]Literal, nodeSlabSize)
	}
	p := &a.literalSlab[0]
	a.literalSlab = a.literalSlab[1:]
	return p
}

func (a *nodeAllocator) allocWhere() *Where {
	if len(a.whereSlab) == 0 {
		a.whereSlab = make([]Where, nodeSlabSize)
	}
	p := &a.whereSlab[0]
	a.whereSlab = a.whereSlab[1:]
	return p
}

func (a *nodeAllocator) allocComparisonExpr() *ComparisonExpr {
	if len(a.comparisonExprSlab) == 0 {
		a.comparisonExprSlab = make([]ComparisonExpr, nodeSlabSize)
	}
	p := &a.comparisonExprSlab[0]
	a.comparisonExprSlab = a.comparisonExprSlab[1:]
	return p
}

func (a *nodeAllocator) allocBinaryExpr() *BinaryExpr {
	if len(a.binaryExprSlab) == 0 {
		a.binaryExprSlab = make([]BinaryExpr, nodeSlabSize)
	}
	p := &a.binaryExprSlab[0]
	a.binaryExprSlab = a.binaryExprSlab[1:]
	return p
}

func (a *nodeAllocator) allocUnaryExpr() *UnaryExpr {
	if len(a.unaryExprSlab) == 0 {
		a.unaryExprSlab = make([]UnaryExpr, nodeSlabSize)
	}
	p := &a.unaryExprSlab[0]
	a.unaryExprSlab = a.unaryExprSlab[1:]
	return p
}

func (a *nodeAllocator) allocFuncExpr() *FuncExpr {
	if len(a.funcExprSlab) == 0 {
		a.funcExprSlab = make([]FuncExpr, nodeSlabSize)
	}
	p := &a.funcExprSlab[0]
	a.funcExprSlab = a.funcExprSlab[1:]
	return p
}

func (a *nodeAllocator) allocAndExpr() *AndExpr {
	if len(a.andExprSlab) == 0 {
		a.andExprSlab = make([]AndExpr, nodeSlabSize)
	}
	p := &a.andExprSlab[0]
	a.andExprSlab = a.andExprSlab[1:]
	return p
}

func (a *nodeAllocator) allocOrExpr() *OrExpr {
	if len(a.orExprSlab) == 0 {
		a.orExprSlab = make([]OrExpr, nodeSlabSize)
	}
	p := &a.orExprSlab[0]
	a.orExprSlab = a.orExprSlab[1:]
	return p
}

func (a *nodeAllocator) allocNotExpr() *NotExpr {
	if len(a.notExprSlab) == 0 {
		a.notExprSlab = make([]NotExpr, nodeSlabSize)
	}
	p := &a.notExprSlab[0]
	a.notExprSlab = a.notExprSlab[1:]
	return p
}

func (a *nodeAllocator) allocIsExpr() *IsExpr {
	if len(a.isExprSlab) == 0 {
		a.isExprSlab = make([]IsExpr, nodeSlabSize)
	}
	p := &a.isExprSlab[0]
	a.isExprSlab = a.isExprSlab[1:]
	return p
}

func (a *nodeAllocator) allocXorExpr() *XorExpr {
	if len(a.xorExprSlab) == 0 {
		a.xorExprSlab = make([]XorExpr, nodeSlabSize)
	}
	p := &a.xorExprSlab[0]
	a.xorExprSlab = a.xorExprSlab[1:]
	return p
}

func (a *nodeAllocator) allocJoinTableExpr() *JoinTableExpr {
	if len(a.joinTableExprSlab) == 0 {
		a.joinTableExprSlab = make([]JoinTableExpr, nodeSlabSize)
	}
	p := &a.joinTableExprSlab[0]
	a.joinTableExprSlab = a.joinTableExprSlab[1:]
	return p
}

func (a *nodeAllocator) allocOrder() *Order {
	if len(a.orderSlab) == 0 {
		a.orderSlab = make([]Order, nodeSlabSize)
	}
	p := &a.orderSlab[0]
	a.orderSlab = a.orderSlab[1:]
	return p
}

func (a *nodeAllocator) allocLimit() *Limit {
	if len(a.limitSlab) == 0 {
		a.limitSlab = make([]Limit, nodeSlabSize)
	}
	p := &a.limitSlab[0]
	a.limitSlab = a.limitSlab[1:]
	return p
}

func (a *nodeAllocator) allocSubquery() *Subquery {
	if len(a.subquerySlab) == 0 {
		a.subquerySlab = make([]Subquery, nodeSlabSize)
	}
	p := &a.subquerySlab[0]
	a.subquerySlab = a.subquerySlab[1:]
	return p
}

func (a *nodeAllocator) allocGroupBy() *GroupBy {
	if len(a.groupBySlab) == 0 {
		a.groupBySlab = make([]GroupBy, nodeSlabSize)
	}
	p := &a.groupBySlab[0]
	a.groupBySlab = a.groupBySlab[1:]
	return p
}

func (a *nodeAllocator) allocSelectExprs() *SelectExprs {
	if len(a.selectExprsSlab) == 0 {
		a.selectExprsSlab = make([]SelectExprs, nodeSlabSize)
	}
	p := &a.selectExprsSlab[0]
	a.selectExprsSlab = a.selectExprsSlab[1:]
	return p
}

// Allocator-aware constructor functions for use in grammar rules.
// These replace the heap-allocating public constructors on the hot parse path.

func (a *nodeAllocator) newWhere(typ WhereType, expr Expr) *Where {
	if expr == nil {
		return nil
	}
	w := a.allocWhere()
	w.Type = typ
	w.Expr = expr
	return w
}

func (a *nodeAllocator) newStrLiteral(val string) *Literal {
	lit := a.allocLiteral()
	lit.Type = StrVal
	lit.Val = val
	return lit
}

func (a *nodeAllocator) newIntLiteral(val string) *Literal {
	lit := a.allocLiteral()
	lit.Type = IntVal
	lit.Val = val
	return lit
}

func (a *nodeAllocator) newDecimalLiteral(val string) *Literal {
	lit := a.allocLiteral()
	lit.Type = DecimalVal
	lit.Val = val
	return lit
}

func (a *nodeAllocator) newFloatLiteral(val string) *Literal {
	lit := a.allocLiteral()
	lit.Type = FloatVal
	lit.Val = val
	return lit
}

func (a *nodeAllocator) newHexNumLiteral(val string) *Literal {
	lit := a.allocLiteral()
	lit.Type = HexNum
	lit.Val = val
	return lit
}

func (a *nodeAllocator) newHexLiteral(val string) *Literal {
	lit := a.allocLiteral()
	lit.Type = HexVal
	lit.Val = val
	return lit
}

func (a *nodeAllocator) newBitLiteral(val string) *Literal {
	lit := a.allocLiteral()
	lit.Type = BitNum
	lit.Val = val
	return lit
}

func (a *nodeAllocator) newDateLiteral(val string) *Literal {
	lit := a.allocLiteral()
	lit.Type = DateVal
	lit.Val = val
	return lit
}

func (a *nodeAllocator) newTimeLiteral(val string) *Literal {
	lit := a.allocLiteral()
	lit.Type = TimeVal
	lit.Val = val
	return lit
}

func (a *nodeAllocator) newSelect(
	comments Comments,
	exprs *SelectExprs,
	selectOptions []string,
	into *SelectInto,
	from TableExprs,
	where *Where,
	groupBy *GroupBy,
	having *Where,
	windows NamedWindows,
) *Select {
	sel := a.allocSelect()
	sel.Comments = comments.Parsed()
	sel.SelectExprs = exprs
	sel.Into = into
	sel.From = from
	sel.Where = where
	sel.GroupBy = groupBy
	sel.Having = having
	sel.Windows = windows
	for _, option := range selectOptions {
		switch strings.ToLower(option) {
		case DistinctStr:
			sel.Distinct = true
		case SQLCacheStr:
			truth := true
			sel.Cache = &truth
		case SQLNoCacheStr:
			truth := false
			sel.Cache = &truth
		case HighPriorityStr:
			sel.HighPriority = true
		case StraightJoinHint:
			sel.StraightJoinHint = true
		case SQLSmallResultStr:
			sel.SQLSmallResult = true
		case SQLBigResultStr:
			sel.SQLBigResult = true
		case SQLBufferResultStr:
			sel.SQLBufferResult = true
		case SQLCalcFoundRowsStr:
			sel.SQLCalcFoundRows = true
		}
	}
	return sel
}

func (a *nodeAllocator) getAliasedTableExprFromTableName(tblName TableName) *AliasedTableExpr {
	ate := a.allocAliasedTableExpr()
	ate.Expr = tblName
	return ate
}

// Slice slab methods: return slices backed by slab memory.
// When append grows beyond the slab-backed capacity, Go allocates
// a new backing array as normal. The savings come from avoiding the
// initial heap allocation for short lists (the common case in SQL).

func (a *nodeAllocator) makeSelectExprSlice(n int) []SelectExpr {
	if len(a.selectExprSliceSlab) < n {
		a.selectExprSliceSlab = make([]SelectExpr, max(nodeSlabSize, n))
	}
	s := a.selectExprSliceSlab[:n:n]
	a.selectExprSliceSlab = a.selectExprSliceSlab[n:]
	return s
}

func (a *nodeAllocator) makeExprSlice(n int) []Expr {
	if len(a.exprSliceSlab) < n {
		a.exprSliceSlab = make([]Expr, max(nodeSlabSize, n))
	}
	s := a.exprSliceSlab[:n:n]
	a.exprSliceSlab = a.exprSliceSlab[n:]
	return s
}

func (a *nodeAllocator) makeTableExprSlice(n int) TableExprs {
	if len(a.tableExprSliceSlab) < n {
		a.tableExprSliceSlab = make([]TableExpr, max(nodeSlabSize, n))
	}
	s := a.tableExprSliceSlab[:n:n]
	a.tableExprSliceSlab = a.tableExprSliceSlab[n:]
	return s
}

func (a *nodeAllocator) makeColumns(n int) Columns {
	if len(a.identifierCISlab) < n {
		a.identifierCISlab = make([]IdentifierCI, max(nodeSlabSize, n))
	}
	s := a.identifierCISlab[:n:n]
	a.identifierCISlab = a.identifierCISlab[n:]
	return s
}

func (a *nodeAllocator) makeOrderSlice(n int) OrderBy {
	if len(a.orderPtrSlab) < n {
		a.orderPtrSlab = make([]*Order, max(nodeSlabSize, n))
	}
	s := a.orderPtrSlab[:n:n]
	a.orderPtrSlab = a.orderPtrSlab[n:]
	return s
}

func (a *nodeAllocator) makeUpdateExprSlice(n int) UpdateExprs {
	if len(a.updateExprPtrSlab) < n {
		a.updateExprPtrSlab = make([]*UpdateExpr, max(nodeSlabSize, n))
	}
	s := a.updateExprPtrSlab[:n:n]
	a.updateExprPtrSlab = a.updateExprPtrSlab[n:]
	return s
}

func (a *nodeAllocator) makeSetExprSlice(n int) SetExprs {
	if len(a.setExprPtrSlab) < n {
		a.setExprPtrSlab = make([]*SetExpr, max(nodeSlabSize, n))
	}
	s := a.setExprPtrSlab[:n:n]
	a.setExprPtrSlab = a.setExprPtrSlab[n:]
	return s
}

func (a *nodeAllocator) makeValuesSlice(n int) Values {
	if len(a.valTupleSlab) < n {
		a.valTupleSlab = make([]ValTuple, max(nodeSlabSize, n))
	}
	s := a.valTupleSlab[:n:n]
	a.valTupleSlab = a.valTupleSlab[n:]
	return s
}

// Nil-safe dereference helpers for pointer-indirect union types.
// These are inlined by the compiler.

func derefExprs(p *[]Expr) []Expr {
	if p == nil {
		return nil
	}
	return *p
}

func derefTableExprs(p *TableExprs) TableExprs {
	if p == nil {
		return nil
	}
	return *p
}

func derefColumns(p *Columns) Columns {
	if p == nil {
		return nil
	}
	return *p
}

func derefValues(p *Values) Values {
	if p == nil {
		return nil
	}
	return *p
}

func derefValTuple(p *ValTuple) ValTuple {
	if p == nil {
		return nil
	}
	return *p
}

func derefOrderBy(p *OrderBy) OrderBy {
	if p == nil {
		return nil
	}
	return *p
}

func derefUpdateExprs(p *UpdateExprs) UpdateExprs {
	if p == nil {
		return nil
	}
	return *p
}

func derefSetExprs(p *SetExprs) SetExprs {
	if p == nil {
		return nil
	}
	return *p
}

func derefPartitions(p *Partitions) Partitions {
	if p == nil {
		return nil
	}
	return *p
}

func derefTableNames(p *TableNames) TableNames {
	if p == nil {
		return nil
	}
	return *p
}

// Pointer-indirect slice allocators: return *[]T for boxing-free union storage.
// The slice header lives in a slab, so the pointer is stable.

func (a *nodeAllocator) makeExprSlicePtr(n int) *[]Expr {
	if len(a.exprSliceHdrSlab) == 0 {
		a.exprSliceHdrSlab = make([][]Expr, nodeSlabSize)
	}
	p := &a.exprSliceHdrSlab[0]
	a.exprSliceHdrSlab = a.exprSliceHdrSlab[1:]
	*p = a.makeExprSlice(n)
	return p
}

func (a *nodeAllocator) makeTableExprSlicePtr(n int) *TableExprs {
	if len(a.tableExprSliceHdrSlab) == 0 {
		a.tableExprSliceHdrSlab = make([]TableExprs, nodeSlabSize)
	}
	p := &a.tableExprSliceHdrSlab[0]
	a.tableExprSliceHdrSlab = a.tableExprSliceHdrSlab[1:]
	*p = a.makeTableExprSlice(n)
	return p
}

func (a *nodeAllocator) makeColumnsPtr(n int) *Columns {
	if len(a.columnsHdrSlab) == 0 {
		a.columnsHdrSlab = make([]Columns, nodeSlabSize)
	}
	p := &a.columnsHdrSlab[0]
	a.columnsHdrSlab = a.columnsHdrSlab[1:]
	*p = a.makeColumns(n)
	return p
}

func (a *nodeAllocator) makeValuesSlicePtr(n int) *Values {
	if len(a.valuesHdrSlab) == 0 {
		a.valuesHdrSlab = make([]Values, nodeSlabSize)
	}
	p := &a.valuesHdrSlab[0]
	a.valuesHdrSlab = a.valuesHdrSlab[1:]
	*p = a.makeValuesSlice(n)
	return p
}

func (a *nodeAllocator) makeValTuplePtr() *ValTuple {
	if len(a.valTupleHdrSlab) == 0 {
		a.valTupleHdrSlab = make([]ValTuple, nodeSlabSize)
	}
	p := &a.valTupleHdrSlab[0]
	a.valTupleHdrSlab = a.valTupleHdrSlab[1:]
	return p
}

func (a *nodeAllocator) makeOrderByPtr(n int) *OrderBy {
	if len(a.orderByHdrSlab) == 0 {
		a.orderByHdrSlab = make([]OrderBy, nodeSlabSize)
	}
	p := &a.orderByHdrSlab[0]
	a.orderByHdrSlab = a.orderByHdrSlab[1:]
	*p = a.makeOrderSlice(n)
	return p
}

func (a *nodeAllocator) makeUpdateExprsPtr(n int) *UpdateExprs {
	if len(a.updateExprsHdrSlab) == 0 {
		a.updateExprsHdrSlab = make([]UpdateExprs, nodeSlabSize)
	}
	p := &a.updateExprsHdrSlab[0]
	a.updateExprsHdrSlab = a.updateExprsHdrSlab[1:]
	*p = a.makeUpdateExprSlice(n)
	return p
}

func (a *nodeAllocator) makeSetExprsPtr(n int) *SetExprs {
	if len(a.setExprsHdrSlab) == 0 {
		a.setExprsHdrSlab = make([]SetExprs, nodeSlabSize)
	}
	p := &a.setExprsHdrSlab[0]
	a.setExprsHdrSlab = a.setExprsHdrSlab[1:]
	*p = a.makeSetExprSlice(n)
	return p
}

func (a *nodeAllocator) makePartitionsPtr() *Partitions {
	if len(a.partitionsHdrSlab) == 0 {
		a.partitionsHdrSlab = make([]Partitions, nodeSlabSize)
	}
	p := &a.partitionsHdrSlab[0]
	a.partitionsHdrSlab = a.partitionsHdrSlab[1:]
	return p
}

func (a *nodeAllocator) makeTableNamesPtr() *TableNames {
	if len(a.tableNamesHdrSlab) == 0 {
		a.tableNamesHdrSlab = make([]TableNames, nodeSlabSize)
	}
	p := &a.tableNamesHdrSlab[0]
	a.tableNamesHdrSlab = a.tableNamesHdrSlab[1:]
	return p
}
