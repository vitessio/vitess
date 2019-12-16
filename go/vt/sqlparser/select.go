/*
Copyright 2019 The Vitess Authors.

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

// AddOrder adds an order by element
func (node *Select) AddOrder(order *Order) {
	node.OrderBy = append(node.OrderBy, order)
}

// SetLimit sets the limit clause
func (node *Select) SetLimit(limit *Limit) {
	node.Limit = limit
}

// AddWhere adds the boolean expression to the
// WHERE clause as an AND condition. If the expression
// is an OR clause, it parenthesizes it. Currently,
// the OR operator is the only one that's lower precedence
// than AND.
func (node *Select) AddWhere(expr Expr) {
	if _, ok := expr.(*OrExpr); ok {
		expr = &ParenExpr{Expr: expr}
	}
	if node.Where == nil {
		node.Where = &Where{
			Type: WhereStr,
			Expr: expr,
		}
		return
	}
	node.Where.Expr = &AndExpr{
		Left:  node.Where.Expr,
		Right: expr,
	}
}

// AddHaving adds the boolean expression to the
// HAVING clause as an AND condition. If the expression
// is an OR clause, it parenthesizes it. Currently,
// the OR operator is the only one that's lower precedence
// than AND.
func (node *Select) AddHaving(expr Expr) {
	if _, ok := expr.(*OrExpr); ok {
		expr = &ParenExpr{Expr: expr}
	}
	if node.Having == nil {
		node.Having = &Where{
			Type: HavingStr,
			Expr: expr,
		}
		return
	}
	node.Having.Expr = &AndExpr{
		Left:  node.Having.Expr,
		Right: expr,
	}
}

// AddOrder adds an order by element
func (node *ParenSelect) AddOrder(order *Order) {
	panic("unreachable")
}

// SetLimit sets the limit clause
func (node *ParenSelect) SetLimit(limit *Limit) {
	panic("unreachable")
}

// AddOrder adds an order by element
func (node *Union) AddOrder(order *Order) {
	node.OrderBy = append(node.OrderBy, order)
}

// SetLimit sets the limit clause
func (node *Union) SetLimit(limit *Limit) {
	node.Limit = limit
}
