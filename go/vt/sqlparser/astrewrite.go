package sqlparser

import (
	"fmt"
	"reflect"
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

// Walk traverses an AST in depth-first order: It starts by calling
// fn(node); node must not be nil. It returns the rewritten node. If fn returns
// true, Walk invokes fn recursively for each of the non-nil children of node,
// followed by a call of fn(nil). The returned node of fn can be used to
// rewrite the passed node to fn. Panics if the returned type is not the same
// type as the original one.
func Walk(node SQLNode, fn WalkFunc) SQLNode {
	if node == nil {
		return nil
	}

	rewritten, continueDown := fn(node)
	if !continueDown {
		return rewritten
	}

	switch n := rewritten.(type) {
	// nothing to do
	case *Begin:
	case BoolVal:
	case *Comments:
	case Comments:
	case *Commit:
	case ColIdent:
	case *DBDDL:
	case *Default:
	case ListArg:
	case *NullVal:
	case *OtherRead:
	case *OtherAdmin:
	case *SQLVal:
	case *Rollback:
	case TableIdent:

	case *AliasedExpr:
		n.As = Walk(n.As, fn).(ColIdent)
		n.Expr = Walk(n.Expr, fn).(Expr)

	case *AliasedTableExpr:
		n.Expr = Walk(n.Expr, fn).(SimpleTableExpr)
		for i, p := range n.Partitions {
			n.Partitions[i] = Walk(p, fn).(ColIdent)
		}
		n.As = Walk(n.As, fn).(TableIdent)
		if n.Hints != nil {
			n.Hints = Walk(n.Hints, fn).(*IndexHints)
		}

	case *AndExpr:
		n.Left = Walk(n.Left, fn).(Expr)
		n.Right = Walk(n.Right, fn).(Expr)

	case *AutoIncSpec:
		n.Column = Walk(n.Column, fn).(ColIdent)
		n.Sequence = Walk(n.Sequence, fn).(TableName)

	case *BinaryExpr:
		n.Left = Walk(n.Left, fn).(Expr)
		n.Right = Walk(n.Right, fn).(Expr)

	case *CaseExpr:
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}
		for i, p := range n.Whens {
			n.Whens[i] = Walk(p, fn).(*When)
		}
		if n.Else != nil {
			n.Else = Walk(n.Else, fn).(Expr)
		}

	case *ColName:
		n.Name = Walk(n.Name, fn).(ColIdent)
		n.Qualifier = Walk(n.Qualifier, fn).(TableName)

	case *CollateExpr:
		n.Expr = Walk(n.Expr, fn).(Expr)

	case *ColumnDefinition:
		n.Name = Walk(n.Name, fn).(ColIdent)

	case *ComparisonExpr:
		n.Left = Walk(n.Left, fn).(Expr)
		n.Right = Walk(n.Right, fn).(Expr)
		if n.Escape != nil {
			n.Escape = Walk(n.Escape, fn).(Expr)
		}

	case *ConvertExpr:
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}
		if n.Type != nil {
			n.Type = Walk(n.Type, fn).(*ConvertType)
		}

	case *ConvertType:
		if n.Length != nil {
			n.Length = Walk(n.Length, fn).(*SQLVal)
		}
		if n.Scale != nil {
			n.Scale = Walk(n.Scale, fn).(*SQLVal)
		}

	case *ConvertUsingExpr:
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *CurTimeFuncExpr:
		n.Name = Walk(n.Name, fn).(ColIdent)
		if n.Fsp != nil {
			n.Fsp = Walk(n.Fsp, fn).(Expr)
		}

	case *Delete:
		if n.Comments != nil {
			n.Comments = Walk(n.Comments, fn).(Comments)
		}
		for i, e := range n.Targets {
			n.Targets[i] = Walk(e, fn).(TableName)
		}
		for i, from := range n.TableExprs {
			n.TableExprs[i] = Walk(from, fn).(TableExpr)
		}
		for i, p := range n.Partitions {
			n.Partitions[i] = Walk(p, fn).(ColIdent)
		}
		if n.Where != nil {
			n.Where = Walk(n.Where, fn).(*Where)
		}
		for i, e := range n.OrderBy {
			n.OrderBy[i] = Walk(e, fn).(*Order)
		}
		if n.Limit != nil {
			n.Limit = Walk(n.Limit, fn).(*Limit)
		}

	case *DDL:
		if n.FromTables != nil {
			n.FromTables = Walk(n.FromTables, fn).(TableNames)
		}
		if n.ToTables != nil {
			n.ToTables = Walk(n.ToTables, fn).(TableNames)
		}
		n.Table = Walk(n.Table, fn).(TableName)
		if n.TableSpec != nil {
			n.TableSpec = Walk(n.TableSpec, fn).(*TableSpec)
		}
		if n.OptLike != nil {
			n.OptLike = Walk(n.OptLike, fn).(*OptLike)
		}
		if n.PartitionSpec != nil {
			n.PartitionSpec = Walk(n.PartitionSpec, fn).(*PartitionSpec)
		}
		if n.VindexSpec != nil {
			n.VindexSpec = Walk(n.VindexSpec, fn).(*VindexSpec)
		}
		if n.AutoIncSpec != nil {
			n.AutoIncSpec = Walk(n.AutoIncSpec, fn).(*AutoIncSpec)
		}
		for i, c := range n.VindexCols {
			n.VindexCols[i] = Walk(c, fn).(ColIdent)
		}

	case *GroupConcatExpr:
		for i, e := range n.Exprs {
			n.Exprs[i] = Walk(e, fn).(SelectExpr)
		}
		for i, e := range n.OrderBy {
			n.OrderBy[i] = Walk(e, fn).(*Order)
		}

	case *ExistsExpr:
		n.Subquery = Walk(n.Subquery, fn).(*Subquery)

	case *FuncExpr:
		n.Qualifier = Walk(n.Qualifier, fn).(TableIdent)
		n.Name = Walk(n.Name, fn).(ColIdent)
		for i, e := range n.Exprs {
			n.Exprs[i] = Walk(e, fn).(SelectExpr)
		}

	case *IndexHints:
		for i, p := range n.Indexes {
			n.Indexes[i] = Walk(p, fn).(ColIdent)
		}

	case *IntervalExpr:
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *Insert:
		if n.Comments != nil {
			n.Comments = Walk(n.Comments, fn).(Comments)
		}
		n.Table = Walk(n.Table, fn).(TableName)
		for i, p := range n.Partitions {
			n.Partitions[i] = Walk(p, fn).(ColIdent)
		}
		for i, p := range n.Columns {
			n.Columns[i] = Walk(p, fn).(ColIdent)
		}
		if n.Rows != nil {
			n.Rows = Walk(n.Rows, fn).(InsertRows)
		}
		for i, p := range n.OnDup {
			n.OnDup[i] = Walk(p, fn).(*UpdateExpr)
		}

	case *IsExpr:
		n.Expr = Walk(n.Expr, fn).(Expr)

	case *JoinTableExpr:
		if n.LeftExpr != nil {
			n.LeftExpr = Walk(n.LeftExpr, fn).(TableExpr)
		}
		if n.RightExpr != nil {
			n.RightExpr = Walk(n.RightExpr, fn).(TableExpr)
		}
		n.Condition = Walk(n.Condition, fn).(JoinCondition)

	case JoinCondition:
		if n.On != nil {
			n.On = Walk(n.On, fn).(Expr)
		}
		for i, c := range n.Using {
			n.Using[i] = Walk(c, fn).(ColIdent)
		}

	case *Limit:
		if n.Offset != nil {
			n.Offset = Walk(n.Offset, fn).(Expr)
		}
		if n.Rowcount != nil {
			n.Rowcount = Walk(n.Rowcount, fn).(Expr)
		}

	case *MatchExpr:
		for i, p := range n.Columns {
			n.Columns[i] = Walk(p, fn).(SelectExpr)
		}
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case Nextval:
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *NotExpr:
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *Order:
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *OrExpr:
		n.Left = Walk(n.Left, fn).(Expr)
		n.Right = Walk(n.Right, fn).(Expr)

	case *ParenExpr:
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *ParenSelect:
		n.Select = Walk(n.Select, fn).(SelectStatement)

	case *ParenTableExpr:
		for i, e := range n.Exprs {
			n.Exprs[i] = Walk(e, fn).(TableExpr)
		}

	case *PartitionSpec:
		n.Name = Walk(n.Name, fn).(ColIdent)
		for i, pd := range n.Definitions {
			n.Definitions[i] = Walk(pd, fn).(*PartitionDefinition)
		}

	case *PartitionDefinition:
		n.Name = Walk(n.Name, fn).(ColIdent)
		if n.Limit != nil {
			n.Limit = Walk(n.Limit, fn).(Expr)
		}

	case *Set:
		if n.Comments != nil {
			n.Comments = Walk(n.Comments, fn).(Comments)
		}
		for i, e := range n.Exprs {
			n.Exprs[i] = Walk(e, fn).(*SetExpr)
		}

	case *SetExpr:
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}
		n.Name = Walk(n.Name, fn).(ColIdent)

	case *Select:
		if n.Comments != nil {
			n.Comments = Walk(n.Comments, fn).(Comments)
		}
		for i, e := range n.SelectExprs {
			n.SelectExprs[i] = Walk(e, fn).(SelectExpr)
		}
		for i, from := range n.From {
			n.From[i] = Walk(from, fn).(TableExpr)
		}
		if n.Where != nil {
			n.Where = Walk(n.Where, fn).(*Where)
		}
		for i, e := range n.GroupBy {
			n.GroupBy[i] = Walk(e, fn).(Expr)
		}
		if n.Having != nil {
			n.Having = Walk(n.Having, fn).(*Where)
		}
		for i, e := range n.OrderBy {
			n.OrderBy[i] = Walk(e, fn).(*Order)
		}
		if n.Limit != nil {
			n.Limit = Walk(n.Limit, fn).(*Limit)
		}

	case *Show:
		n.OnTable = Walk(n.OnTable, fn).(TableName)
		n.Table = Walk(n.Table, fn).(TableName)
		if n.ShowCollationFilterOpt != nil {
			x := Walk(*n.ShowCollationFilterOpt, fn).(Expr)
			n.ShowCollationFilterOpt = &x
		}

	case *StarExpr:
		n.TableName = Walk(n.TableName, fn).(TableName)

	case *Stream:
		if n.Comments != nil {
			n.Comments = Walk(n.Comments, fn).(Comments)
		}
		if n.SelectExpr != nil {
			n.SelectExpr = Walk(n.SelectExpr, fn).(SelectExpr)
		}
		n.Table = Walk(n.Table, fn).(TableName)

	case *Subquery:
		if n.Select != nil {
			n.Select = Walk(n.Select, fn).(SelectStatement)
		}

	case *SubstrExpr:
		if n.Name != nil {
			n.Name = Walk(n.Name, fn).(*ColName)
		}
		if n.StrVal != nil {
			n.StrVal = Walk(n.StrVal, fn).(*SQLVal)
		}
		if n.From != nil {
			n.From = Walk(n.From, fn).(Expr)
		}
		if n.To != nil {
			n.To = Walk(n.To, fn).(Expr)
		}

	case *RangeCond:
		if n.Left != nil {
			n.Left = Walk(n.Left, fn).(Expr)
		}
		if n.From != nil {
			n.From = Walk(n.From, fn).(Expr)
		}
		if n.To != nil {
			n.To = Walk(n.To, fn).(Expr)
		}

	case TableNames:
		for i, t := range n {
			n[i] = Walk(t, fn).(TableName)
		}

	case TableName:
		n.Name = Walk(n.Name, fn).(TableIdent)
		n.Qualifier = Walk(n.Qualifier, fn).(TableIdent)

	case *TableSpec:
		for i, o := range n.Columns {
			n.Columns[i] = Walk(o, fn).(*ColumnDefinition)
		}
		for i, o := range n.Indexes {
			n.Indexes[i] = Walk(o, fn).(*IndexDefinition)
		}
		for i, o := range n.Constraints {
			n.Constraints[i] = Walk(o, fn).(*ConstraintDefinition)
		}

	case *TimestampFuncExpr:
		if n.Expr1 != nil {
			n.Expr1 = Walk(n.Expr1, fn).(Expr)
		}
		if n.Expr2 != nil {
			n.Expr2 = Walk(n.Expr2, fn).(Expr)
		}

	case *UnaryExpr:
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *Union:
		n.Left = Walk(n.Left, fn).(SelectStatement)
		n.Right = Walk(n.Right, fn).(SelectStatement)
		for i, o := range n.OrderBy {
			n.OrderBy[i] = Walk(o, fn).(*Order)
		}
		if n.Limit != nil {
			n.Limit = Walk(n.Limit, fn).(*Limit)
		}

	case *Update:
		if n.Comments != nil {
			n.Comments = Walk(n.Comments, fn).(Comments)
		}
		for i, t := range n.TableExprs {
			n.TableExprs[i] = Walk(t, fn).(TableExpr)
		}
		for i, e := range n.Exprs {
			n.Exprs[i] = Walk(e, fn).(*UpdateExpr)
		}
		if n.Where != nil {
			n.Where = Walk(n.Where, fn).(*Where)
		}
		for i, o := range n.OrderBy {
			n.OrderBy[i] = Walk(o, fn).(*Order)
		}
		if n.Limit != nil {
			n.Limit = Walk(n.Limit, fn).(*Limit)
		}

	case *UpdateExpr:
		n.Name = Walk(n.Name, fn).(*ColName)
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *Use:
		n.DBName = Walk(n.DBName, fn).(TableIdent)

	case Values:
		for i, v := range n {
			n[i] = Walk(v, fn).(ValTuple)
		}

	case *ValuesFuncExpr:
		if n.Name != nil {
			n.Name = Walk(n.Name, fn).(*ColName)
		}

	case ValTuple:
		for i, v := range n {
			n[i] = Walk(v, fn).(Expr)
		}

	case *VindexSpec:
		n.Name = Walk(n.Name, fn).(ColIdent)
		n.Type = Walk(n.Type, fn).(ColIdent)

	case *When:
		n.Cond = Walk(n.Cond, fn).(Expr)
		n.Val = Walk(n.Val, fn).(Expr)

	case *Where:
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	default:
		fmt.Println(fmt.Sprintf("unknown AST object: %v of type %s", node, reflect.TypeOf(node)))

	}
	return rewritten
}
