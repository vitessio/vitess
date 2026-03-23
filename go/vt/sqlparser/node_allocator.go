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
