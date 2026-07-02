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
	parsedCommentsSlab []ParsedComments
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
	whenPtrSlab         []*When
	colNamePtrSlab      []*ColName
	ctePtrSlab          []*CommonTableExpr
	variablePtrSlab     []*Variable
	statementSlab       []Statement
	stringSlab          []string
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
	sel.Comments = a.parsedComments(comments)
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

func (a *nodeAllocator) parsedComments(c Comments) *ParsedComments {
	if len(c) == 0 {
		return nil
	}
	if len(a.parsedCommentsSlab) == 0 {
		a.parsedCommentsSlab = make([]ParsedComments, nodeSlabSize)
	}
	p := &a.parsedCommentsSlab[0]
	a.parsedCommentsSlab = a.parsedCommentsSlab[1:]
	p.comments = c
	return p
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

// sliceCapacity returns the capacity to give a slab-backed slice.
// We grant extra capacity (up to 4 elements) so that append in
// grammar list rules can grow without heap-allocating a new backing
// array. Most SQL lists have ≤4 elements, so this covers the common
// case. The extra slots are consumed from the slab.
const sliceExtraCap = 3

func sliceCapacity(n, remaining int) int {
	return min(n+sliceExtraCap, remaining)
}

func (a *nodeAllocator) makeSelectExprSlice(n int) []SelectExpr {
	if len(a.selectExprSliceSlab) < n {
		a.selectExprSliceSlab = make([]SelectExpr, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.selectExprSliceSlab))
	s := a.selectExprSliceSlab[:n:c]
	a.selectExprSliceSlab = a.selectExprSliceSlab[c:]
	return s
}

func (a *nodeAllocator) makeExprSlice(n int) []Expr {
	if len(a.exprSliceSlab) < n {
		a.exprSliceSlab = make([]Expr, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.exprSliceSlab))
	s := a.exprSliceSlab[:n:c]
	a.exprSliceSlab = a.exprSliceSlab[c:]
	return s
}

func (a *nodeAllocator) makeTableExprSlice(n int) TableExprs {
	if len(a.tableExprSliceSlab) < n {
		a.tableExprSliceSlab = make([]TableExpr, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.tableExprSliceSlab))
	s := a.tableExprSliceSlab[:n:c]
	a.tableExprSliceSlab = a.tableExprSliceSlab[c:]
	return s
}

func (a *nodeAllocator) makeColumns(n int) Columns {
	if len(a.identifierCISlab) < n {
		a.identifierCISlab = make([]IdentifierCI, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.identifierCISlab))
	s := a.identifierCISlab[:n:c]
	a.identifierCISlab = a.identifierCISlab[c:]
	return s
}

func (a *nodeAllocator) makeOrderSlice(n int) OrderBy {
	if len(a.orderPtrSlab) < n {
		a.orderPtrSlab = make([]*Order, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.orderPtrSlab))
	s := a.orderPtrSlab[:n:c]
	a.orderPtrSlab = a.orderPtrSlab[c:]
	return s
}

func (a *nodeAllocator) makeUpdateExprSlice(n int) UpdateExprs {
	if len(a.updateExprPtrSlab) < n {
		a.updateExprPtrSlab = make([]*UpdateExpr, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.updateExprPtrSlab))
	s := a.updateExprPtrSlab[:n:c]
	a.updateExprPtrSlab = a.updateExprPtrSlab[c:]
	return s
}

func (a *nodeAllocator) makeSetExprSlice(n int) SetExprs {
	if len(a.setExprPtrSlab) < n {
		a.setExprPtrSlab = make([]*SetExpr, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.setExprPtrSlab))
	s := a.setExprPtrSlab[:n:c]
	a.setExprPtrSlab = a.setExprPtrSlab[c:]
	return s
}

func (a *nodeAllocator) makeValuesSlice(n int) Values {
	if len(a.valTupleSlab) < n {
		a.valTupleSlab = make([]ValTuple, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.valTupleSlab))
	s := a.valTupleSlab[:n:c]
	a.valTupleSlab = a.valTupleSlab[c:]
	return s
}

func (a *nodeAllocator) makeWhenPtrSlice(n int) []*When {
	if len(a.whenPtrSlab) < n {
		a.whenPtrSlab = make([]*When, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.whenPtrSlab))
	s := a.whenPtrSlab[:n:c]
	a.whenPtrSlab = a.whenPtrSlab[c:]
	return s
}

func (a *nodeAllocator) makeColNamePtrSlice(n int) []*ColName {
	if len(a.colNamePtrSlab) < n {
		a.colNamePtrSlab = make([]*ColName, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.colNamePtrSlab))
	s := a.colNamePtrSlab[:n:c]
	a.colNamePtrSlab = a.colNamePtrSlab[c:]
	return s
}

func (a *nodeAllocator) makeCTEPtrSlice(n int) []*CommonTableExpr {
	if len(a.ctePtrSlab) < n {
		a.ctePtrSlab = make([]*CommonTableExpr, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.ctePtrSlab))
	s := a.ctePtrSlab[:n:c]
	a.ctePtrSlab = a.ctePtrSlab[c:]
	return s
}

func (a *nodeAllocator) makeVariablePtrSlice(n int) []*Variable {
	if len(a.variablePtrSlab) < n {
		a.variablePtrSlab = make([]*Variable, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.variablePtrSlab))
	s := a.variablePtrSlab[:n:c]
	a.variablePtrSlab = a.variablePtrSlab[c:]
	return s
}

func (a *nodeAllocator) makeStatementSlice(n int) []Statement {
	if len(a.statementSlab) < n {
		a.statementSlab = make([]Statement, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.statementSlab))
	s := a.statementSlab[:n:c]
	a.statementSlab = a.statementSlab[c:]
	return s
}

func (a *nodeAllocator) makeStringSlice(n int) []string {
	if len(a.stringSlab) < n {
		a.stringSlab = make([]string, max(nodeSlabSize, n))
	}
	c := sliceCapacity(n, len(a.stringSlab))
	s := a.stringSlab[:n:c]
	a.stringSlab = a.stringSlab[c:]
	return s
}
