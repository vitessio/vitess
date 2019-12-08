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
		if n == nil {
			break
		}
		n.As = Walk(n.As, fn).(ColIdent)
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *AliasedTableExpr:
		if n == nil {
			break
		}
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(SimpleTableExpr)
		}
		if n.Partitions != nil {
			n.Partitions = Walk(n.Partitions, fn).(Partitions)
		}
		n.As = Walk(n.As, fn).(TableIdent)
		if n.Hints != nil {
			n.Hints = Walk(n.Hints, fn).(*IndexHints)
		}

	case *AndExpr:
		if n == nil {
			break
		}
		if n.Left != nil {
			n.Left = Walk(n.Left, fn).(Expr)
		}
		if n.Right != nil {
			n.Right = Walk(n.Right, fn).(Expr)
		}

	case *AutoIncSpec:
		if n == nil {
			break
		}
		n.Column = Walk(n.Column, fn).(ColIdent)
		n.Sequence = Walk(n.Sequence, fn).(TableName)

	case *BinaryExpr:
		if n == nil {
			break
		}
		if n.Left != nil {
			n.Left = Walk(n.Left, fn).(Expr)
		}
		if n.Right != nil {
			n.Right = Walk(n.Right, fn).(Expr)
		}

	case *CaseExpr:
		if n == nil {
			break
		}
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
		if n == nil {
			break
		}
		n.Name = Walk(n.Name, fn).(ColIdent)
		n.Qualifier = Walk(n.Qualifier, fn).(TableName)

	case *CollateExpr:
		if n == nil {
			break
		}
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *ColumnDefinition:
		if n == nil {
			break
		}
		n.Name = Walk(n.Name, fn).(ColIdent)

	case *ComparisonExpr:
		if n == nil {
			break
		}
		if n.Left != nil {
			n.Left = Walk(n.Left, fn).(Expr)
		}
		if n.Right != nil {
			n.Right = Walk(n.Right, fn).(Expr)
		}
		if n.Escape != nil {
			n.Escape = Walk(n.Escape, fn).(Expr)
		}

	case *ConvertExpr:
		if n == nil {
			break
		}
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}
		if n.Type != nil {
			n.Type = Walk(n.Type, fn).(*ConvertType)
		}

	case *ConvertType:
		if n == nil {
			break
		}
		if n.Length != nil {
			n.Length = Walk(n.Length, fn).(*SQLVal)
		}
		if n.Scale != nil {
			n.Scale = Walk(n.Scale, fn).(*SQLVal)
		}

	case *ConvertUsingExpr:
		if n == nil {
			break
		}
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *CurTimeFuncExpr:
		if n == nil {
			break
		}
		n.Name = Walk(n.Name, fn).(ColIdent)
		if n.Fsp != nil {
			n.Fsp = Walk(n.Fsp, fn).(Expr)
		}

	case *Delete:
		if n == nil {
			break
		}
		if n.Comments != nil {
			n.Comments = Walk(n.Comments, fn).(Comments)
		}
		if n.Targets != nil {
			n.Targets = Walk(n.Targets, fn).(TableNames)
		}
		if n.TableExprs != nil {
			n.TableExprs = Walk(n.TableExprs, fn).(TableExprs)
		}
		if n.Partitions != nil {
			n.Partitions = Walk(n.Partitions, fn).(Partitions)
		}
		if n.Where != nil {
			n.Where = Walk(n.Where, fn).(*Where)
		}
		if n.OrderBy != nil {
			n.OrderBy = Walk(n.OrderBy, fn).(OrderBy)
		}
		if n.Limit != nil {
			n.Limit = Walk(n.Limit, fn).(*Limit)
		}

	case *DDL:
		if n == nil {
			break
		}
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
		if n == nil {
			break
		}
		if n.Exprs != nil {
			n.Exprs = Walk(n.Exprs, fn).(SelectExprs)
		}
		if n.OrderBy != nil {
			n.OrderBy = Walk(n.OrderBy, fn).(OrderBy)
		}

	case *ExistsExpr:
		if n == nil {
			break
		}
		//n.Subquery = Walk(n.Subquery, fn).(*Subquery)
		// TODO - Keep this and document why or rewrite like everything else
		// We don't descend into the subquery to keep parity with old behaviour.
		_, _ = fn(n.Subquery)

	case *FuncExpr:
		if n == nil {
			break
		}
		n.Qualifier = Walk(n.Qualifier, fn).(TableIdent)
		n.Name = Walk(n.Name, fn).(ColIdent)
		if n.Exprs != nil {
			n.Exprs = Walk(n.Exprs, fn).(SelectExprs)
		}

	case *IndexHints:
		if n == nil {
			break
		}
		for i, p := range n.Indexes {
			n.Indexes[i] = Walk(p, fn).(ColIdent)
		}

	case *IntervalExpr:
		if n == nil {
			break
		}
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *Insert:
		if n == nil {
			break
		}
		if n.Comments != nil {
			n.Comments = Walk(n.Comments, fn).(Comments)
		}
		n.Table = Walk(n.Table, fn).(TableName)
		if n.Partitions != nil {
			n.Partitions = Walk(n.Partitions, fn).(Partitions)
		}
		if n.Columns != nil {
			n.Columns = Walk(n.Columns, fn).(Columns)
		}
		if n.Rows != nil {
			n.Rows = Walk(n.Rows, fn).(InsertRows)
		}
		if n.OnDup != nil {
			n.OnDup = Walk(n.OnDup, fn).(OnDup)
		}

	case *IsExpr:
		if n == nil {
			break
		}
		n.Expr = Walk(n.Expr, fn).(Expr)

	case *JoinTableExpr:
		if n == nil {
			break
		}
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
		if n.Using != nil {
			n.Using = Walk(n.Using, fn).(Columns)
		}

	case *Limit:
		if n == nil {
			break
		}
		if n.Offset != nil {
			n.Offset = Walk(n.Offset, fn).(Expr)
		}
		if n.Rowcount != nil {
			n.Rowcount = Walk(n.Rowcount, fn).(Expr)
		}

	case *MatchExpr:
		if n == nil {
			break
		}
		if n.Columns != nil {
			n.Columns = Walk(n.Columns, fn).(SelectExprs)
		}
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case Nextval:
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *NotExpr:
		if n == nil {
			break
		}
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *Order:
		if n == nil {
			break
		}
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case OrderBy:
		for i, p := range n {
			n[i] = Walk(p, fn).(*Order)
		}

	case *OrExpr:
		if n == nil {
			break
		}
		if n.Left != nil {
			n.Left = Walk(n.Left, fn).(Expr)
		}
		if n.Right != nil {
			n.Right = Walk(n.Right, fn).(Expr)
		}

	case *ParenExpr:
		if n == nil {
			break
		}
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *ParenSelect:
		if n == nil {
			break
		}
		n.Select = Walk(n.Select, fn).(SelectStatement)

	case *ParenTableExpr:
		if n == nil {
			break
		}
		if n.Exprs != nil {
			n.Exprs = Walk(n.Exprs, fn).(TableExprs)
		}

	case *PartitionSpec:
		if n == nil {
			break
		}
		n.Name = Walk(n.Name, fn).(ColIdent)
		for i, pd := range n.Definitions {
			n.Definitions[i] = Walk(pd, fn).(*PartitionDefinition)
		}

	case *PartitionDefinition:
		if n == nil {
			break
		}
		n.Name = Walk(n.Name, fn).(ColIdent)
		if n.Limit != nil {
			n.Limit = Walk(n.Limit, fn).(Expr)
		}

	case *Set:
		if n == nil {
			break
		}
		if n.Comments != nil {
			n.Comments = Walk(n.Comments, fn).(Comments)
		}
		if n.Exprs != nil {
			n.Exprs = Walk(n.Exprs, fn).(SetExprs)
		}

	case *SetExpr:
		if n == nil {
			break
		}
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}
		n.Name = Walk(n.Name, fn).(ColIdent)

	case *Select:
		if n == nil {
			break
		}
		if n.Comments != nil {
			n.Comments = Walk(n.Comments, fn).(Comments)
		}
		if n.SelectExprs != nil {
			n.SelectExprs = Walk(n.SelectExprs, fn).(SelectExprs)
		}
		if n.From != nil {
			n.From = Walk(n.From, fn).(TableExprs)
		}
		if n.Where != nil {
			n.Where = Walk(n.Where, fn).(*Where)
		}
		if n.GroupBy != nil {
			n.GroupBy = Walk(n.GroupBy, fn).(GroupBy)
		}
		if n.Having != nil {
			n.Having = Walk(n.Having, fn).(*Where)
		}
		if n.OrderBy != nil {
			n.OrderBy = Walk(n.OrderBy, fn).(OrderBy)
		}
		if n.Limit != nil {
			n.Limit = Walk(n.Limit, fn).(*Limit)
		}

	case SelectExprs:
		for i, e := range n {
			n[i] = Walk(e, fn).(SelectExpr)
		}

	case *Show:
		if n == nil {
			break
		}
		n.OnTable = Walk(n.OnTable, fn).(TableName)
		n.Table = Walk(n.Table, fn).(TableName)
		if n.ShowCollationFilterOpt != nil {
			n.ShowCollationFilterOpt = Walk(n.ShowCollationFilterOpt, fn).(Expr)
		}

	case *StarExpr:
		if n == nil {
			break
		}
		n.TableName = Walk(n.TableName, fn).(TableName)

	case *Stream:
		if n == nil {
			break
		}
		if n.Comments != nil {
			n.Comments = Walk(n.Comments, fn).(Comments)
		}
		if n.SelectExpr != nil {
			n.SelectExpr = Walk(n.SelectExpr, fn).(SelectExpr)
		}
		n.Table = Walk(n.Table, fn).(TableName)

	case *Subquery:
		if n == nil {
			break
		}
		if n.Select != nil {
			n.Select = Walk(n.Select, fn).(SelectStatement)
		}

	case *SubstrExpr:
		if n == nil {
			break
		}
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
		if n == nil {
			break
		}
		if n.Left != nil {
			n.Left = Walk(n.Left, fn).(Expr)
		}
		if n.From != nil {
			n.From = Walk(n.From, fn).(Expr)
		}
		if n.To != nil {
			n.To = Walk(n.To, fn).(Expr)
		}

	case TableExprs:
		for i, t := range n {
			n[i] = Walk(t, fn).(TableExpr)
		}

	case TableNames:
		for i, t := range n {
			n[i] = Walk(t, fn).(TableName)
		}

	case TableName:
		n.Name = Walk(n.Name, fn).(TableIdent)
		n.Qualifier = Walk(n.Qualifier, fn).(TableIdent)

	case *TableSpec:
		if n == nil {
			break
		}
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
		if n == nil {
			break
		}
		if n.Expr1 != nil {
			n.Expr1 = Walk(n.Expr1, fn).(Expr)
		}
		if n.Expr2 != nil {
			n.Expr2 = Walk(n.Expr2, fn).(Expr)
		}

	case *UnaryExpr:
		if n == nil {
			break
		}
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case *Union:
		if n == nil {
			break
		}
		if n.Left != nil {
			n.Left = Walk(n.Left, fn).(SelectStatement)
		}
		if n.Right != nil {
			n.Right = Walk(n.Right, fn).(SelectStatement)
		}
		if n.OrderBy != nil {
			n.OrderBy = Walk(n.OrderBy, fn).(OrderBy)
		}
		if n.Limit != nil {
			n.Limit = Walk(n.Limit, fn).(*Limit)
		}

	case *Update:
		if n == nil {
			break
		}
		if n.Comments != nil {
			n.Comments = Walk(n.Comments, fn).(Comments)
		}
		if n.TableExprs != nil {
			n.TableExprs = Walk(n.TableExprs, fn).(TableExprs)
		}
		if n.Exprs != nil {
			n.Exprs = Walk(n.Exprs, fn).(UpdateExprs)
		}
		if n.Where != nil {
			n.Where = Walk(n.Where, fn).(*Where)
		}
		if n.OrderBy != nil {
			n.OrderBy = Walk(n.OrderBy, fn).(OrderBy)
		}
		if n.Limit != nil {
			n.Limit = Walk(n.Limit, fn).(*Limit)
		}

	case *UpdateExpr:
		if n == nil {
			break
		}
		n.Name = Walk(n.Name, fn).(*ColName)
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	case UpdateExprs:
		for i, t := range n {
			n[i] = Walk(t, fn).(*UpdateExpr)
		}

	case *Use:
		if n == nil {
			break
		}
		n.DBName = Walk(n.DBName, fn).(TableIdent)

	case Values:
		for i, v := range n {
			n[i] = Walk(v, fn).(ValTuple)
		}

	case *ValuesFuncExpr:
		if n == nil {
			break
		}
		if n.Name != nil {
			n.Name = Walk(n.Name, fn).(*ColName)
		}

	case ValTuple:
		for i, v := range n {
			n[i] = Walk(v, fn).(Expr)
		}

	case *VindexSpec:
		if n == nil {
			break
		}
		n.Name = Walk(n.Name, fn).(ColIdent)
		n.Type = Walk(n.Type, fn).(ColIdent)

	case *When:
		if n == nil {
			break
		}
		if n.Cond != nil {
			n.Cond = Walk(n.Cond, fn).(Expr)
		}
		if n.Val != nil {
			n.Val = Walk(n.Val, fn).(Expr)
		}

	case *Where:
		if n == nil {
			break
		}
		if n.Expr != nil {
			n.Expr = Walk(n.Expr, fn).(Expr)
		}

	default:
		panic(fmt.Sprintf("unknown AST object: %v of type %s", node, reflect.TypeOf(node)))

	}
	return rewritten
}
