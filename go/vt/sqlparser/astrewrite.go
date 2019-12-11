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

import (
	"unsafe"
)

// WalkFunc describes a function to be called for each node during a Walk. The
// returned node can be used to rewrite the AST. Walking stops if the returned
// bool is false.
type WalkFunc func(SQLNode) (SQLNode, bool)

// VisitFunc describes a function to be called for each node during a Walk.
// If the function returns false, walking will stop.
type VisitFunc func(SQLNode) bool

// Identity is a function that will return the same AST after passing through every element in it
var Identity = func(in SQLNode) (SQLNode, bool) {
	return in, true
}

// VisitAll will visit all elements of the AST until one of them returns false
func VisitAll(root SQLNode, fn VisitFunc) {
	Walk(root, func(node SQLNode) (SQLNode, bool) {
		return node, fn(node)
	})
}

// do nothing
func (node *Begin) walk(_ WalkFunc) SQLNode      { return node }
func (node BoolVal) walk(_ WalkFunc) SQLNode     { return node }
func (node Comments) walk(_ WalkFunc) SQLNode    { return node }
func (node *Commit) walk(_ WalkFunc) SQLNode     { return node }
func (node ColIdent) walk(_ WalkFunc) SQLNode    { return node }
func (node *DBDDL) walk(_ WalkFunc) SQLNode      { return node }
func (node *Default) walk(_ WalkFunc) SQLNode    { return node }
func (node ListArg) walk(_ WalkFunc) SQLNode     { return node }
func (node *NullVal) walk(_ WalkFunc) SQLNode    { return node }
func (node *OtherRead) walk(_ WalkFunc) SQLNode  { return node }
func (node *OtherAdmin) walk(_ WalkFunc) SQLNode { return node }
func (node *SQLVal) walk(_ WalkFunc) SQLNode     { return node }
func (node *Rollback) walk(_ WalkFunc) SQLNode   { return node }
func (node TableIdent) walk(_ WalkFunc) SQLNode  { return node }

func (node *AliasedExpr) walk(fn WalkFunc) SQLNode {
	node.As = Walk(node.As, fn).(ColIdent)
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	return node
}

func (node *AliasedTableExpr) walk(fn WalkFunc) SQLNode {
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(SimpleTableExpr)
	}
	if node.Partitions != nil {
		node.Partitions = Walk(node.Partitions, fn).(Partitions)
	}
	node.As = Walk(node.As, fn).(TableIdent)
	if node.Hints != nil {
		node.Hints = Walk(node.Hints, fn).(*IndexHints)
	}
	return node
}

func (node *AndExpr) walk(fn WalkFunc) SQLNode {
	if node.Left != nil {
		node.Left = Walk(node.Left, fn).(Expr)
	}
	if node.Right != nil {
		node.Right = Walk(node.Right, fn).(Expr)
	}
	return node
}

func (node *AutoIncSpec) walk(fn WalkFunc) SQLNode {
	node.Column = Walk(node.Column, fn).(ColIdent)
	node.Sequence = Walk(node.Sequence, fn).(TableName)
	return node
}

func (node *BinaryExpr) walk(fn WalkFunc) SQLNode {
	if node.Left != nil {
		node.Left = Walk(node.Left, fn).(Expr)
	}
	if node.Right != nil {
		node.Right = Walk(node.Right, fn).(Expr)
	}
	return node
}

func (node *CaseExpr) walk(fn WalkFunc) SQLNode {
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	for i, p := range node.Whens {
		node.Whens[i] = Walk(p, fn).(*When)
	}
	if node.Else != nil {
		node.Else = Walk(node.Else, fn).(Expr)
	}
	return node
}

func (node *ColName) walk(fn WalkFunc) SQLNode {
	node.Name = Walk(node.Name, fn).(ColIdent)
	node.Qualifier = Walk(node.Qualifier, fn).(TableName)
	return node
}

func (node *CollateExpr) walk(fn WalkFunc) SQLNode {
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	return node
}

func (col *ColumnDefinition) walk(fn WalkFunc) SQLNode {
	col.Name = Walk(col.Name, fn).(ColIdent)
	return col
}

func (ct *ColumnType) walk(fn WalkFunc) SQLNode {
	ct.NotNull = Walk(ct.NotNull, fn).(BoolVal)
	ct.Autoincrement = Walk(ct.Autoincrement, fn).(BoolVal)
	if ct.Default != nil {
		ct.Default = Walk(ct.Default, fn).(Expr)
	}
	if ct.OnUpdate != nil {
		ct.OnUpdate = Walk(ct.OnUpdate, fn).(Expr)
	}
	if ct.Comment != nil {
		ct.Comment = Walk(ct.Comment, fn).(*SQLVal)
	}
	if ct.Length != nil {
		ct.Length = Walk(ct.Length, fn).(*SQLVal)
	}
	ct.Unsigned = Walk(ct.Unsigned, fn).(BoolVal)
	ct.Zerofill = Walk(ct.Zerofill, fn).(BoolVal)
	if ct.Scale != nil {
		ct.Scale = Walk(ct.Scale, fn).(*SQLVal)
	}
	return ct
}

func (node Columns) walk(fn WalkFunc) SQLNode {
	for i, p := range node {
		node[i] = Walk(p, fn).(ColIdent)
	}
	return node
}

func (node *ComparisonExpr) walk(fn WalkFunc) SQLNode {
	if node.Left != nil {
		node.Left = Walk(node.Left, fn).(Expr)
	}
	if node.Right != nil {
		node.Right = Walk(node.Right, fn).(Expr)
	}
	if node.Escape != nil {
		node.Escape = Walk(node.Escape, fn).(Expr)
	}
	return node
}

func (c *ConstraintDefinition) walk(fn WalkFunc) SQLNode {
	c.Details = Walk(c.Details, fn).(ConstraintInfo)
	return c
}

func (node *ConvertExpr) walk(fn WalkFunc) SQLNode {
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	if node.Type != nil {
		node.Type = Walk(node.Type, fn).(*ConvertType)
	}
	return node
}

func (node *ConvertType) walk(fn WalkFunc) SQLNode {
	if node.Length != nil {
		node.Length = Walk(node.Length, fn).(*SQLVal)
	}
	if node.Scale != nil {
		node.Scale = Walk(node.Scale, fn).(*SQLVal)
	}
	return node
}

func (node *ConvertUsingExpr) walk(fn WalkFunc) SQLNode {
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	return node
}

func (node *CurTimeFuncExpr) walk(fn WalkFunc) SQLNode {
	node.Name = Walk(node.Name, fn).(ColIdent)
	if node.Fsp != nil {
		node.Fsp = Walk(node.Fsp, fn).(Expr)
	}
	return node
}

func (node *Delete) walk(fn WalkFunc) SQLNode {
	if node.Comments != nil {
		node.Comments = Walk(node.Comments, fn).(Comments)
	}
	if node.Targets != nil {
		node.Targets = Walk(node.Targets, fn).(TableNames)
	}
	if node.TableExprs != nil {
		node.TableExprs = Walk(node.TableExprs, fn).(TableExprs)
	}
	if node.Partitions != nil {
		node.Partitions = Walk(node.Partitions, fn).(Partitions)
	}
	if node.Where != nil {
		node.Where = Walk(node.Where, fn).(*Where)
	}
	if node.OrderBy != nil {
		node.OrderBy = Walk(node.OrderBy, fn).(OrderBy)
	}
	if node.Limit != nil {
		node.Limit = Walk(node.Limit, fn).(*Limit)
	}
	return node
}

func (node *DDL) walk(fn WalkFunc) SQLNode {
	if node.FromTables != nil {
		node.FromTables = Walk(node.FromTables, fn).(TableNames)
	}
	if node.ToTables != nil {
		node.ToTables = Walk(node.ToTables, fn).(TableNames)
	}
	node.Table = Walk(node.Table, fn).(TableName)
	if node.TableSpec != nil {
		node.TableSpec = Walk(node.TableSpec, fn).(*TableSpec)
	}
	if node.OptLike != nil {
		node.OptLike = Walk(node.OptLike, fn).(*OptLike)
	}
	if node.PartitionSpec != nil {
		node.PartitionSpec = Walk(node.PartitionSpec, fn).(*PartitionSpec)
	}
	if node.VindexSpec != nil {
		node.VindexSpec = Walk(node.VindexSpec, fn).(*VindexSpec)
	}
	if node.AutoIncSpec != nil {
		node.AutoIncSpec = Walk(node.AutoIncSpec, fn).(*AutoIncSpec)
	}
	for i, c := range node.VindexCols {
		node.VindexCols[i] = Walk(c, fn).(ColIdent)
	}
	return node
}

func (node *GroupConcatExpr) walk(fn WalkFunc) SQLNode {
	if node.Exprs != nil {
		node.Exprs = Walk(node.Exprs, fn).(SelectExprs)
	}
	if node.OrderBy != nil {
		node.OrderBy = Walk(node.OrderBy, fn).(OrderBy)
	}
	return node
}

func (node *ExistsExpr) walk(fn WalkFunc) SQLNode {
	//node.Subquery = Walk(node.Subquery, fn).(*Subquery)
	// TODO - Keep this and document why or rewrite like everything else
	// We don't descend into the subquery to keep parity with old behaviour.
	if node.Subquery != nil && !isNilValue(node.Subquery) {
		_, _ = fn(node.Subquery)
	}
	return node
}

func (node Exprs) walk(fn WalkFunc) SQLNode {
	for i, p := range node {
		node[i] = Walk(p, fn).(Expr)
	}

	return node
}

func (node *FuncExpr) walk(fn WalkFunc) SQLNode {
	node.Qualifier = Walk(node.Qualifier, fn).(TableIdent)
	node.Name = Walk(node.Name, fn).(ColIdent)
	if node.Exprs != nil {
		node.Exprs = Walk(node.Exprs, fn).(SelectExprs)
	}
	return node
}

func (f *ForeignKeyDefinition) walk(fn WalkFunc) SQLNode {
	if f.Source != nil {
		f.Source = Walk(f.Source, fn).(Columns)
	}
	f.ReferencedTable = Walk(f.ReferencedTable, fn).(TableName)
	if f.ReferencedColumns != nil {
		f.ReferencedColumns = Walk(f.ReferencedColumns, fn).(Columns)
	}
	return f
}

func (node GroupBy) walk(fn WalkFunc) SQLNode {
	for i, e := range node {
		node[i] = Walk(e, fn).(Expr)
	}
	return node
}
func (idx *IndexDefinition) walk(fn WalkFunc) SQLNode {
	if idx.Info != nil {
		idx.Info = Walk(idx.Info, fn).(*IndexInfo)
	}
	return idx
}

func (ii *IndexInfo) walk(fn WalkFunc) SQLNode {
	ii.Name = Walk(ii.Name, fn).(ColIdent)
	return ii
}

func (node *IndexHints) walk(fn WalkFunc) SQLNode {
	for i, p := range node.Indexes {
		node.Indexes[i] = Walk(p, fn).(ColIdent)
	}
	return node
}

func (node *IntervalExpr) walk(fn WalkFunc) SQLNode {
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	return node
}

func (node *Insert) walk(fn WalkFunc) SQLNode {
	if node.Comments != nil {
		node.Comments = Walk(node.Comments, fn).(Comments)
	}
	node.Table = Walk(node.Table, fn).(TableName)
	if node.Partitions != nil {
		node.Partitions = Walk(node.Partitions, fn).(Partitions)
	}
	if node.Columns != nil {
		node.Columns = Walk(node.Columns, fn).(Columns)
	}
	if node.Rows != nil {
		node.Rows = Walk(node.Rows, fn).(InsertRows)
	}
	if node.OnDup != nil {
		node.OnDup = Walk(node.OnDup, fn).(OnDup)
	}
	return node
}

func (node *IsExpr) walk(fn WalkFunc) SQLNode {
	node.Expr = Walk(node.Expr, fn).(Expr)
	return node
}

func (node *JoinTableExpr) walk(fn WalkFunc) SQLNode {
	if node.LeftExpr != nil {
		node.LeftExpr = Walk(node.LeftExpr, fn).(TableExpr)
	}
	if node.RightExpr != nil {
		node.RightExpr = Walk(node.RightExpr, fn).(TableExpr)
	}
	node.Condition = Walk(node.Condition, fn).(JoinCondition)
	return node
}

func (node JoinCondition) walk(fn WalkFunc) SQLNode {
	if node.On != nil {
		node.On = Walk(node.On, fn).(Expr)
	}
	if node.Using != nil {
		node.Using = Walk(node.Using, fn).(Columns)
	}
	return node
}

func (node *Limit) walk(fn WalkFunc) SQLNode {
	if node.Offset != nil {
		node.Offset = Walk(node.Offset, fn).(Expr)
	}
	if node.Rowcount != nil {
		node.Rowcount = Walk(node.Rowcount, fn).(Expr)
	}
	return node
}

func (node *MatchExpr) walk(fn WalkFunc) SQLNode {
	if node.Columns != nil {
		node.Columns = Walk(node.Columns, fn).(SelectExprs)
	}
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	return node
}

func (node Nextval) walk(fn WalkFunc) SQLNode {
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	return node
}

func (node *NotExpr) walk(fn WalkFunc) SQLNode {
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	return node
}

func (node *OptLike) walk(fn WalkFunc) SQLNode {
	node.LikeTable = Walk(node.LikeTable, fn).(TableName)
	return node
}

func (node *Order) walk(fn WalkFunc) SQLNode {
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	return node
}

func (node OnDup) walk(fn WalkFunc) SQLNode {
	for i, t := range node {
		node[i] = Walk(t, fn).(*UpdateExpr)
	}
	return node
}

func (node OrderBy) walk(fn WalkFunc) SQLNode {
	for i, p := range node {
		node[i] = Walk(p, fn).(*Order)
	}
	return node
}

func (node *OrExpr) walk(fn WalkFunc) SQLNode {
	if node.Left != nil {
		node.Left = Walk(node.Left, fn).(Expr)
	}
	if node.Right != nil {
		node.Right = Walk(node.Right, fn).(Expr)
	}
	return node
}

func (node *ParenExpr) walk(fn WalkFunc) SQLNode {
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	return node
}

func (node *ParenSelect) walk(fn WalkFunc) SQLNode {
	node.Select = Walk(node.Select, fn).(SelectStatement)
	return node
}

func (node *ParenTableExpr) walk(fn WalkFunc) SQLNode {
	if node.Exprs != nil {
		node.Exprs = Walk(node.Exprs, fn).(TableExprs)
	}
	return node
}

func (node Partitions) walk(fn WalkFunc) SQLNode {
	for i, p := range node {
		node[i] = Walk(p, fn).(ColIdent)
	}
	return node
}

func (node *PartitionSpec) walk(fn WalkFunc) SQLNode {
	node.Name = Walk(node.Name, fn).(ColIdent)
	for i, pd := range node.Definitions {
		node.Definitions[i] = Walk(pd, fn).(*PartitionDefinition)
	}
	return node
}

func (node *PartitionDefinition) walk(fn WalkFunc) SQLNode {
	node.Name = Walk(node.Name, fn).(ColIdent)
	if node.Limit != nil {
		node.Limit = Walk(node.Limit, fn).(Expr)
	}
	return node
}

func (node *Set) walk(fn WalkFunc) SQLNode {
	if node.Comments != nil {
		node.Comments = Walk(node.Comments, fn).(Comments)
	}
	if node.Exprs != nil {
		node.Exprs = Walk(node.Exprs, fn).(SetExprs)
	}
	return node
}

func (node *SetExpr) walk(fn WalkFunc) SQLNode {
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	node.Name = Walk(node.Name, fn).(ColIdent)
	return node
}

func (node *Select) walk(fn WalkFunc) SQLNode {
	if node.Comments != nil {
		node.Comments = Walk(node.Comments, fn).(Comments)
	}
	if node.SelectExprs != nil {
		node.SelectExprs = Walk(node.SelectExprs, fn).(SelectExprs)
	}
	if node.From != nil {
		node.From = Walk(node.From, fn).(TableExprs)
	}
	if node.Where != nil {
		node.Where = Walk(node.Where, fn).(*Where)
	}
	if node.GroupBy != nil {
		node.GroupBy = Walk(node.GroupBy, fn).(GroupBy)
	}
	if node.Having != nil {
		node.Having = Walk(node.Having, fn).(*Where)
	}
	if node.OrderBy != nil {
		node.OrderBy = Walk(node.OrderBy, fn).(OrderBy)
	}
	if node.Limit != nil {
		node.Limit = Walk(node.Limit, fn).(*Limit)
	}
	return node
}

func (node SelectExprs) walk(fn WalkFunc) SQLNode {
	for i, e := range node {
		node[i] = Walk(e, fn).(SelectExpr)
	}
	return node
}

func (node SetExprs) walk(fn WalkFunc) SQLNode {
	for i, e := range node {
		node[i] = Walk(e, fn).(*SetExpr)
	}
	return node
}

func (node *Show) walk(fn WalkFunc) SQLNode {
	node.OnTable = Walk(node.OnTable, fn).(TableName)
	node.Table = Walk(node.Table, fn).(TableName)
	if node.ShowCollationFilterOpt != nil {
		node.ShowCollationFilterOpt = Walk(node.ShowCollationFilterOpt, fn).(Expr)
	}
	return node
}

func (node *ShowFilter) walk(fn WalkFunc) SQLNode {
	if node.Filter != nil {
		node.Filter = Walk(node.Filter, fn).(Expr)
	}
	return node
}

func (node *StarExpr) walk(fn WalkFunc) SQLNode {
	node.TableName = Walk(node.TableName, fn).(TableName)
	return node
}

func (node *Stream) walk(fn WalkFunc) SQLNode {
	if node.Comments != nil {
		node.Comments = Walk(node.Comments, fn).(Comments)
	}
	if node.SelectExpr != nil {
		node.SelectExpr = Walk(node.SelectExpr, fn).(SelectExpr)
	}
	node.Table = Walk(node.Table, fn).(TableName)
	return node
}

func (node *Subquery) walk(fn WalkFunc) SQLNode {
	if node.Select != nil {
		node.Select = Walk(node.Select, fn).(SelectStatement)
	}
	return node
}

func (node *SubstrExpr) walk(fn WalkFunc) SQLNode {
	if node.Name != nil {
		node.Name = Walk(node.Name, fn).(*ColName)
	}
	if node.StrVal != nil {
		node.StrVal = Walk(node.StrVal, fn).(*SQLVal)
	}
	if node.From != nil {
		node.From = Walk(node.From, fn).(Expr)
	}
	if node.To != nil {
		node.To = Walk(node.To, fn).(Expr)
	}
	return node
}

func (node *RangeCond) walk(fn WalkFunc) SQLNode {
	if node.Left != nil {
		node.Left = Walk(node.Left, fn).(Expr)
	}
	if node.From != nil {
		node.From = Walk(node.From, fn).(Expr)
	}
	if node.To != nil {
		node.To = Walk(node.To, fn).(Expr)
	}
	return node
}

func (node TableExprs) walk(fn WalkFunc) SQLNode {
	for i, t := range node {
		node[i] = Walk(t, fn).(TableExpr)
	}
	return node
}

func (node TableNames) walk(fn WalkFunc) SQLNode {
	for i, t := range node {
		node[i] = Walk(t, fn).(TableName)
	}
	return node
}

func (node TableName) walk(fn WalkFunc) SQLNode {
	node.Name = Walk(node.Name, fn).(TableIdent)
	node.Qualifier = Walk(node.Qualifier, fn).(TableIdent)
	return node
}

func (ts *TableSpec) walk(fn WalkFunc) SQLNode {
	for i, o := range ts.Columns {
		ts.Columns[i] = Walk(o, fn).(*ColumnDefinition)
	}
	for i, o := range ts.Indexes {
		ts.Indexes[i] = Walk(o, fn).(*IndexDefinition)
	}
	for i, o := range ts.Constraints {
		ts.Constraints[i] = Walk(o, fn).(*ConstraintDefinition)
	}
	return ts
}

func (node *TimestampFuncExpr) walk(fn WalkFunc) SQLNode {
	if node.Expr1 != nil {
		node.Expr1 = Walk(node.Expr1, fn).(Expr)
	}
	if node.Expr2 != nil {
		node.Expr2 = Walk(node.Expr2, fn).(Expr)
	}
	return node
}

func (node *UnaryExpr) walk(fn WalkFunc) SQLNode {
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	return node
}

func (node *Union) walk(fn WalkFunc) SQLNode {
	if node.Left != nil {
		node.Left = Walk(node.Left, fn).(SelectStatement)
	}
	if node.Right != nil {
		node.Right = Walk(node.Right, fn).(SelectStatement)
	}
	if node.OrderBy != nil {
		node.OrderBy = Walk(node.OrderBy, fn).(OrderBy)
	}
	if node.Limit != nil {
		node.Limit = Walk(node.Limit, fn).(*Limit)
	}
	return node
}

func (node *Update) walk(fn WalkFunc) SQLNode {
	if node.Comments != nil {
		node.Comments = Walk(node.Comments, fn).(Comments)
	}
	if node.TableExprs != nil {
		node.TableExprs = Walk(node.TableExprs, fn).(TableExprs)
	}
	if node.Exprs != nil {
		node.Exprs = Walk(node.Exprs, fn).(UpdateExprs)
	}
	if node.Where != nil {
		node.Where = Walk(node.Where, fn).(*Where)
	}
	if node.OrderBy != nil {
		node.OrderBy = Walk(node.OrderBy, fn).(OrderBy)
	}
	if node.Limit != nil {
		node.Limit = Walk(node.Limit, fn).(*Limit)
	}
	return node
}

func (node *UpdateExpr) walk(fn WalkFunc) SQLNode {
	node.Name = Walk(node.Name, fn).(*ColName)
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	return node
}

func (node UpdateExprs) walk(fn WalkFunc) SQLNode {
	for i, t := range node {
		node[i] = Walk(t, fn).(*UpdateExpr)
	}
	return node
}

func (node *Use) walk(fn WalkFunc) SQLNode {
	node.DBName = Walk(node.DBName, fn).(TableIdent)
	return node
}

func (node Values) walk(fn WalkFunc) SQLNode {
	for i, v := range node {
		node[i] = Walk(v, fn).(ValTuple)
	}
	return node
}

func (node *ValuesFuncExpr) walk(fn WalkFunc) SQLNode {
	if node.Name != nil {
		node.Name = Walk(node.Name, fn).(*ColName)
	}
	return node
}

func (node ValTuple) walk(fn WalkFunc) SQLNode {
	for i, v := range node {
		node[i] = Walk(v, fn).(Expr)
	}
	return node
}

func (node *VindexSpec) walk(fn WalkFunc) SQLNode {
	node.Name = Walk(node.Name, fn).(ColIdent)
	node.Type = Walk(node.Type, fn).(ColIdent)
	return node
}

func (node *When) walk(fn WalkFunc) SQLNode {
	if node.Cond != nil {
		node.Cond = Walk(node.Cond, fn).(Expr)
	}
	if node.Val != nil {
		node.Val = Walk(node.Val, fn).(Expr)
	}
	return node
}

func (node *Where) walk(fn WalkFunc) SQLNode {
	if node.Expr != nil {
		node.Expr = Walk(node.Expr, fn).(Expr)
	}
	return node
}

// Walk traverses an AST in depth-first order: It starts by calling
// fn(node); node must not be nil. It returns the rewritten node. If fn returns
// true, Walk invokes fn recursively for each of the non-nil children of node,
// followed by a call of fn(nil). The returned node of fn can be used to
// rewrite the passed node to fn. Panics if the returned type is not the same
// type as the original one.
func Walk(root SQLNode, fn WalkFunc) SQLNode {
	if root == nil || isNilValue(root) {
		return nil
	}

	rewritten, continueDown := fn(root)
	if !continueDown {
		return rewritten
	}

	return rewritten.walk(fn)
}

// This hack is for speed and less code.
// It allows us to not have to do explicit nil checks in every single `walk` implementation in this file
func isNilValue(i interface{}) bool {
	return (*[2]uintptr)(unsafe.Pointer(&i))[1] == 0
}
