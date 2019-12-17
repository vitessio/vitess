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
	"fmt"
	"strings"

	"vitess.io/vitess/go/sqltypes"
)

// This file contains all the Format implementations for AST nodes

// Format formats the node.
func (node *Select) Format(buf *TrackedBuffer) {
	buf.Myprintf("select %v%s%s%s%v from %v%v%v%v%v%v%s",
		node.Comments, node.Cache, node.Distinct, node.Hints, node.SelectExprs,
		node.From, node.Where,
		node.GroupBy, node.Having, node.OrderBy,
		node.Limit, node.Lock)
}

// Format formats the node.
func (node *ParenSelect) Format(buf *TrackedBuffer) {
	buf.Myprintf("(%v)", node.Select)
}

// Format formats the node.
func (node *Union) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v %s %v%v%v%s", node.Left, node.Type, node.Right,
		node.OrderBy, node.Limit, node.Lock)
}

// Format formats the node.
func (node *Stream) Format(buf *TrackedBuffer) {
	buf.Myprintf("stream %v%v from %v",
		node.Comments, node.SelectExpr, node.Table)
}

// Format formats the node.
func (node *Insert) Format(buf *TrackedBuffer) {
	buf.Myprintf("%s %v%sinto %v%v%v %v%v",
		node.Action,
		node.Comments, node.Ignore,
		node.Table, node.Partitions, node.Columns, node.Rows, node.OnDup)
}

// Format formats the node.
func (node *Update) Format(buf *TrackedBuffer) {
	buf.Myprintf("update %v%s%v set %v%v%v%v",
		node.Comments, node.Ignore, node.TableExprs,
		node.Exprs, node.Where, node.OrderBy, node.Limit)
}

// Format formats the node.
func (node *Delete) Format(buf *TrackedBuffer) {
	buf.Myprintf("delete %v", node.Comments)
	if node.Targets != nil {
		buf.Myprintf("%v ", node.Targets)
	}
	buf.Myprintf("from %v%v%v%v%v", node.TableExprs, node.Partitions, node.Where, node.OrderBy, node.Limit)
}

// Format formats the node.
func (node *Set) Format(buf *TrackedBuffer) {
	if node.Scope == "" {
		buf.Myprintf("set %v%v", node.Comments, node.Exprs)
	} else {
		buf.Myprintf("set %v%s %v", node.Comments, node.Scope, node.Exprs)
	}
}

// Format formats the node.
func (node *DBDDL) Format(buf *TrackedBuffer) {
	switch node.Action {
	case CreateStr:
		buf.WriteString(fmt.Sprintf("%s database %s", node.Action, node.DBName))
	case DropStr:
		exists := ""
		if node.IfExists {
			exists = " if exists"
		}
		buf.WriteString(fmt.Sprintf("%s database%s %v", node.Action, exists, node.DBName))
	}
}

// Format formats the node.
func (node *DDL) Format(buf *TrackedBuffer) {
	switch node.Action {
	case CreateStr:
		if node.OptLike != nil {
			buf.Myprintf("%s table %v %v", node.Action, node.Table, node.OptLike)
		} else if node.TableSpec != nil {
			buf.Myprintf("%s table %v %v", node.Action, node.Table, node.TableSpec)
		} else {
			buf.Myprintf("%s table %v", node.Action, node.Table)
		}
	case DropStr:
		exists := ""
		if node.IfExists {
			exists = " if exists"
		}
		buf.Myprintf("%s table%s %v", node.Action, exists, node.FromTables)
	case RenameStr:
		buf.Myprintf("%s table %v to %v", node.Action, node.FromTables[0], node.ToTables[0])
		for i := 1; i < len(node.FromTables); i++ {
			buf.Myprintf(", %v to %v", node.FromTables[i], node.ToTables[i])
		}
	case AlterStr:
		if node.PartitionSpec != nil {
			buf.Myprintf("%s table %v %v", node.Action, node.Table, node.PartitionSpec)
		} else {
			buf.Myprintf("%s table %v", node.Action, node.Table)
		}
	case FlushStr:
		buf.Myprintf("%s", node.Action)
	case CreateVindexStr:
		buf.Myprintf("alter vschema create vindex %v %v", node.Table, node.VindexSpec)
	case DropVindexStr:
		buf.Myprintf("alter vschema drop vindex %v", node.Table)
	case AddVschemaTableStr:
		buf.Myprintf("alter vschema add table %v", node.Table)
	case DropVschemaTableStr:
		buf.Myprintf("alter vschema drop table %v", node.Table)
	case AddColVindexStr:
		buf.Myprintf("alter vschema on %v add vindex %v (", node.Table, node.VindexSpec.Name)
		for i, col := range node.VindexCols {
			if i != 0 {
				buf.Myprintf(", %v", col)
			} else {
				buf.Myprintf("%v", col)
			}
		}
		buf.Myprintf(")")
		if node.VindexSpec.Type.String() != "" {
			buf.Myprintf(" %v", node.VindexSpec)
		}
	case DropColVindexStr:
		buf.Myprintf("alter vschema on %v drop vindex %v", node.Table, node.VindexSpec.Name)
	case AddSequenceStr:
		buf.Myprintf("alter vschema add sequence %v", node.Table)
	case AddAutoIncStr:
		buf.Myprintf("alter vschema on %v add auto_increment %v", node.Table, node.AutoIncSpec)
	default:
		buf.Myprintf("%s table %v", node.Action, node.Table)
	}
}

// Format formats the node.
func (node *OptLike) Format(buf *TrackedBuffer) {
	buf.Myprintf("like %v", node.LikeTable)
}

// Format formats the node.
func (node *PartitionSpec) Format(buf *TrackedBuffer) {
	switch node.Action {
	case ReorganizeStr:
		buf.Myprintf("%s %v into (", node.Action, node.Name)
		var prefix string
		for _, pd := range node.Definitions {
			buf.Myprintf("%s%v", prefix, pd)
			prefix = ", "
		}
		buf.Myprintf(")")
	default:
		panic("unimplemented")
	}
}

// Format formats the node
func (node *PartitionDefinition) Format(buf *TrackedBuffer) {
	if !node.Maxvalue {
		buf.Myprintf("partition %v values less than (%v)", node.Name, node.Limit)
	} else {
		buf.Myprintf("partition %v values less than (maxvalue)", node.Name)
	}
}

// Format formats the node.
func (ts *TableSpec) Format(buf *TrackedBuffer) {
	buf.Myprintf("(\n")
	for i, col := range ts.Columns {
		if i == 0 {
			buf.Myprintf("\t%v", col)
		} else {
			buf.Myprintf(",\n\t%v", col)
		}
	}
	for _, idx := range ts.Indexes {
		buf.Myprintf(",\n\t%v", idx)
	}
	for _, c := range ts.Constraints {
		buf.Myprintf(",\n\t%v", c)
	}

	buf.Myprintf("\n)%s", strings.Replace(ts.Options, ", ", ",\n  ", -1))
}

// Format formats the node.
func (col *ColumnDefinition) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v %v", col.Name, &col.Type)
}

// Format returns a canonical string representation of the type and all relevant options
func (ct *ColumnType) Format(buf *TrackedBuffer) {
	buf.Myprintf("%s", ct.Type)

	if ct.Length != nil && ct.Scale != nil {
		buf.Myprintf("(%v,%v)", ct.Length, ct.Scale)

	} else if ct.Length != nil {
		buf.Myprintf("(%v)", ct.Length)
	}

	if ct.EnumValues != nil {
		buf.Myprintf("(%s)", strings.Join(ct.EnumValues, ", "))
	}

	opts := make([]string, 0, 16)
	if ct.Unsigned {
		opts = append(opts, keywordStrings[UNSIGNED])
	}
	if ct.Zerofill {
		opts = append(opts, keywordStrings[ZEROFILL])
	}
	if ct.Charset != "" {
		opts = append(opts, keywordStrings[CHARACTER], keywordStrings[SET], ct.Charset)
	}
	if ct.Collate != "" {
		opts = append(opts, keywordStrings[COLLATE], ct.Collate)
	}
	if ct.NotNull {
		opts = append(opts, keywordStrings[NOT], keywordStrings[NULL])
	}
	if ct.Default != nil {
		opts = append(opts, keywordStrings[DEFAULT], String(ct.Default))
	}
	if ct.OnUpdate != nil {
		opts = append(opts, keywordStrings[ON], keywordStrings[UPDATE], String(ct.OnUpdate))
	}
	if ct.Autoincrement {
		opts = append(opts, keywordStrings[AUTO_INCREMENT])
	}
	if ct.Comment != nil {
		opts = append(opts, keywordStrings[COMMENT_KEYWORD], String(ct.Comment))
	}
	if ct.KeyOpt == colKeyPrimary {
		opts = append(opts, keywordStrings[PRIMARY], keywordStrings[KEY])
	}
	if ct.KeyOpt == colKeyUnique {
		opts = append(opts, keywordStrings[UNIQUE])
	}
	if ct.KeyOpt == colKeyUniqueKey {
		opts = append(opts, keywordStrings[UNIQUE], keywordStrings[KEY])
	}
	if ct.KeyOpt == colKeySpatialKey {
		opts = append(opts, keywordStrings[SPATIAL], keywordStrings[KEY])
	}
	if ct.KeyOpt == colKey {
		opts = append(opts, keywordStrings[KEY])
	}

	if len(opts) != 0 {
		buf.Myprintf(" %s", strings.Join(opts, " "))
	}
}

// Format formats the node.
func (idx *IndexDefinition) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v (", idx.Info)
	for i, col := range idx.Columns {
		if i != 0 {
			buf.Myprintf(", %v", col.Column)
		} else {
			buf.Myprintf("%v", col.Column)
		}
		if col.Length != nil {
			buf.Myprintf("(%v)", col.Length)
		}
	}
	buf.Myprintf(")")

	for _, opt := range idx.Options {
		buf.Myprintf(" %s", opt.Name)
		if opt.Using != "" {
			buf.Myprintf(" %s", opt.Using)
		} else {
			buf.Myprintf(" %v", opt.Value)
		}
	}
}

// Format formats the node.
func (ii *IndexInfo) Format(buf *TrackedBuffer) {
	if ii.Primary {
		buf.Myprintf("%s", ii.Type)
	} else {
		buf.Myprintf("%s", ii.Type)
		if !ii.Name.IsEmpty() {
			buf.Myprintf(" %v", ii.Name)
		}
	}
}

// Format formats the node.
func (node *AutoIncSpec) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v ", node.Column)
	buf.Myprintf("using %v", node.Sequence)
}

// Format formats the node. The "CREATE VINDEX" preamble was formatted in
// the containing DDL node Format, so this just prints the type, any
// parameters, and optionally the owner
func (node *VindexSpec) Format(buf *TrackedBuffer) {
	buf.Myprintf("using %v", node.Type)

	numParams := len(node.Params)
	if numParams != 0 {
		buf.Myprintf(" with ")
		for i, p := range node.Params {
			if i != 0 {
				buf.Myprintf(", ")
			}
			buf.Myprintf("%v", p)
		}
	}
}

// Format formats the node.
func (node VindexParam) Format(buf *TrackedBuffer) {
	buf.Myprintf("%s=%s", node.Key.String(), node.Val)
}

// Format formats the node.
func (c *ConstraintDefinition) Format(buf *TrackedBuffer) {
	if c.Name != "" {
		buf.Myprintf("constraint %s ", c.Name)
	}
	c.Details.Format(buf)
}

// Format formats the node.
func (a ReferenceAction) Format(buf *TrackedBuffer) {
	switch a {
	case Restrict:
		buf.WriteString("restrict")
	case Cascade:
		buf.WriteString("cascade")
	case NoAction:
		buf.WriteString("no action")
	case SetNull:
		buf.WriteString("set null")
	case SetDefault:
		buf.WriteString("set default")
	}
}

// Format formats the node.
func (f *ForeignKeyDefinition) Format(buf *TrackedBuffer) {
	buf.Myprintf("foreign key %v references %v %v", f.Source, f.ReferencedTable, f.ReferencedColumns)
	if f.OnDelete != DefaultAction {
		buf.Myprintf(" on delete %v", f.OnDelete)
	}
	if f.OnUpdate != DefaultAction {
		buf.Myprintf(" on update %v", f.OnUpdate)
	}
}

// Format formats the node.
func (node *Show) Format(buf *TrackedBuffer) {
	if (node.Type == "tables" || node.Type == "columns" || node.Type == "fields") && node.ShowTablesOpt != nil {
		opt := node.ShowTablesOpt
		buf.Myprintf("show %s%s", opt.Full, node.Type)
		if (node.Type == "columns" || node.Type == "fields") && node.HasOnTable() {
			buf.Myprintf(" from %v", node.OnTable)
		}
		if opt.DbName != "" {
			buf.Myprintf(" from %s", opt.DbName)
		}
		buf.Myprintf("%v", opt.Filter)
		return
	}
	if node.Scope == "" {
		buf.Myprintf("show %s", node.Type)
	} else {
		buf.Myprintf("show %s %s", node.Scope, node.Type)
	}
	if node.HasOnTable() {
		buf.Myprintf(" on %v", node.OnTable)
	}
	if node.Type == "collation" && node.ShowCollationFilterOpt != nil {
		buf.Myprintf(" where %v", *node.ShowCollationFilterOpt)
	}
	if node.HasTable() {
		buf.Myprintf(" %v", node.Table)
	}
}

// Format formats the node.
func (node *ShowFilter) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	if node.Like != "" {
		buf.Myprintf(" like '%s'", node.Like)
	} else {
		buf.Myprintf(" where %v", node.Filter)
	}
}

// Format formats the node.
func (node *Use) Format(buf *TrackedBuffer) {
	if node.DBName.v != "" {
		buf.Myprintf("use %v", node.DBName)
	} else {
		buf.Myprintf("use")
	}
}

// Format formats the node.
func (node *Commit) Format(buf *TrackedBuffer) {
	buf.WriteString("commit")
}

// Format formats the node.
func (node *Begin) Format(buf *TrackedBuffer) {
	buf.WriteString("begin")
}

// Format formats the node.
func (node *Rollback) Format(buf *TrackedBuffer) {
	buf.WriteString("rollback")
}

// Format formats the node.
func (node *OtherRead) Format(buf *TrackedBuffer) {
	buf.WriteString("otherread")
}

// Format formats the node.
func (node *OtherAdmin) Format(buf *TrackedBuffer) {
	buf.WriteString("otheradmin")
}

// Format formats the node.
func (node Comments) Format(buf *TrackedBuffer) {
	for _, c := range node {
		buf.Myprintf("%s ", c)
	}
}

// Format formats the node.
func (node *StarExpr) Format(buf *TrackedBuffer) {
	if !node.TableName.IsEmpty() {
		buf.Myprintf("%v.", node.TableName)
	}
	buf.Myprintf("*")
}

// Format formats the node.
func (node *AliasedExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v", node.Expr)
	if !node.As.IsEmpty() {
		buf.Myprintf(" as %v", node.As)
	}
}

// Format formats the node.
func (node Nextval) Format(buf *TrackedBuffer) {
	buf.Myprintf("next %v values", node.Expr)
}

// Format formats the node.
func (node Columns) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	prefix := "("
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = ", "
	}
	buf.WriteString(")")
}

// Format formats the node
func (node Partitions) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	prefix := " partition ("
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = ", "
	}
	buf.WriteString(")")
}

// Format formats the node.
func (node TableExprs) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *AliasedTableExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v%v", node.Expr, node.Partitions)
	if !node.As.IsEmpty() {
		buf.Myprintf(" as %v", node.As)
	}
	if node.Hints != nil {
		// Hint node provides the space padding.
		buf.Myprintf("%v", node.Hints)
	}
}

// Format formats the node.
func (node TableNames) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node TableName) Format(buf *TrackedBuffer) {
	if node.IsEmpty() {
		return
	}
	if !node.Qualifier.IsEmpty() {
		buf.Myprintf("%v.", node.Qualifier)
	}
	buf.Myprintf("%v", node.Name)
}

// Format formats the node.
func (node *ParenTableExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("(%v)", node.Exprs)
}

// Format formats the node.
func (node JoinCondition) Format(buf *TrackedBuffer) {
	if node.On != nil {
		buf.Myprintf(" on %v", node.On)
	}
	if node.Using != nil {
		buf.Myprintf(" using %v", node.Using)
	}
}

// Format formats the node.
func (node *JoinTableExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v %s %v%v", node.LeftExpr, node.Join, node.RightExpr, node.Condition)
}

// Format formats the node.
func (node *IndexHints) Format(buf *TrackedBuffer) {
	buf.Myprintf(" %sindex ", node.Type)
	prefix := "("
	for _, n := range node.Indexes {
		buf.Myprintf("%s%v", prefix, n)
		prefix = ", "
	}
	buf.Myprintf(")")
}

// Format formats the node.
func (node *Where) Format(buf *TrackedBuffer) {
	if node == nil || node.Expr == nil {
		return
	}
	buf.Myprintf(" %s %v", node.Type, node.Expr)
}

// Format formats the node.
func (node Exprs) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *AndExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v and %v", node.Left, node.Right)
}

// Format formats the node.
func (node *OrExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v or %v", node.Left, node.Right)
}

// Format formats the node.
func (node *NotExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("not %v", node.Expr)
}

// Format formats the node.
func (node *ParenExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("(%v)", node.Expr)
}

// Format formats the node.
func (node *ComparisonExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v %s %v", node.Left, node.Operator, node.Right)
	if node.Escape != nil {
		buf.Myprintf(" escape %v", node.Escape)
	}
}

// Format formats the node.
func (node *RangeCond) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v %s %v and %v", node.Left, node.Operator, node.From, node.To)
}

// Format formats the node.
func (node *IsExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v %s", node.Expr, node.Operator)
}

// Format formats the node.
func (node *ExistsExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("exists %v", node.Subquery)
}

// Format formats the node.
func (node *SQLVal) Format(buf *TrackedBuffer) {
	switch node.Type {
	case StrVal:
		sqltypes.MakeTrusted(sqltypes.VarBinary, node.Val).EncodeSQL(buf)
	case IntVal, FloatVal, HexNum:
		buf.Myprintf("%s", []byte(node.Val))
	case HexVal:
		buf.Myprintf("X'%s'", []byte(node.Val))
	case BitVal:
		buf.Myprintf("B'%s'", []byte(node.Val))
	case ValArg:
		buf.WriteArg(string(node.Val))
	default:
		panic("unexpected")
	}
}

// Format formats the node.
func (node *NullVal) Format(buf *TrackedBuffer) {
	buf.Myprintf("null")
}

// Format formats the node.
func (node BoolVal) Format(buf *TrackedBuffer) {
	if node {
		buf.Myprintf("true")
	} else {
		buf.Myprintf("false")
	}
}

// Format formats the node.
func (node *ColName) Format(buf *TrackedBuffer) {
	if !node.Qualifier.IsEmpty() {
		buf.Myprintf("%v.", node.Qualifier)
	}
	buf.Myprintf("%v", node.Name)
}

// Format formats the node.
func (node ValTuple) Format(buf *TrackedBuffer) {
	buf.Myprintf("(%v)", Exprs(node))
}

// Format formats the node.
func (node *Subquery) Format(buf *TrackedBuffer) {
	buf.Myprintf("(%v)", node.Select)
}

// Format formats the node.
func (node ListArg) Format(buf *TrackedBuffer) {
	buf.WriteArg(string(node))
}

// Format formats the node.
func (node *BinaryExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v %s %v", node.Left, node.Operator, node.Right)
}

// Format formats the node.
func (node *UnaryExpr) Format(buf *TrackedBuffer) {
	if _, unary := node.Expr.(*UnaryExpr); unary {
		buf.Myprintf("%s %v", node.Operator, node.Expr)
		return
	}
	buf.Myprintf("%s%v", node.Operator, node.Expr)
}

// Format formats the node.
func (node *IntervalExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("interval %v %s", node.Expr, node.Unit)
}

// Format formats the node.
func (node *TimestampFuncExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%s(%s, %v, %v)", node.Name, node.Unit, node.Expr1, node.Expr2)
}

// Format formats the node.
func (node *CurTimeFuncExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%s(%v)", node.Name.String(), node.Fsp)
}

// Format formats the node.
func (node *CollateExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v collate %s", node.Expr, node.Charset)
}

// Format formats the node.
func (node *FuncExpr) Format(buf *TrackedBuffer) {
	var distinct string
	if node.Distinct {
		distinct = "distinct "
	}
	if !node.Qualifier.IsEmpty() {
		buf.Myprintf("%v.", node.Qualifier)
	}
	// Function names should not be back-quoted even
	// if they match a reserved word. So, print the
	// name as is.
	buf.Myprintf("%s(%s%v)", node.Name.String(), distinct, node.Exprs)
}

// Format formats the node
func (node *GroupConcatExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("group_concat(%s%v%v%s)", node.Distinct, node.Exprs, node.OrderBy, node.Separator)
}

// Format formats the node.
func (node *ValuesFuncExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("values(%v)", node.Name)
}

// Format formats the node.
func (node *SubstrExpr) Format(buf *TrackedBuffer) {
	var val interface{}
	if node.Name != nil {
		val = node.Name
	} else {
		val = node.StrVal
	}

	if node.To == nil {
		buf.Myprintf("substr(%v, %v)", val, node.From)
	} else {
		buf.Myprintf("substr(%v, %v, %v)", val, node.From, node.To)
	}
}

// Format formats the node.
func (node *ConvertExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("convert(%v, %v)", node.Expr, node.Type)
}

// Format formats the node.
func (node *ConvertUsingExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("convert(%v using %s)", node.Expr, node.Type)
}

// Format formats the node.
func (node *ConvertType) Format(buf *TrackedBuffer) {
	buf.Myprintf("%s", node.Type)
	if node.Length != nil {
		buf.Myprintf("(%v", node.Length)
		if node.Scale != nil {
			buf.Myprintf(", %v", node.Scale)
		}
		buf.Myprintf(")")
	}
	if node.Charset != "" {
		buf.Myprintf("%s %s", node.Operator, node.Charset)
	}
}

// Format formats the node
func (node *MatchExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("match(%v) against (%v%s)", node.Columns, node.Expr, node.Option)
}

// Format formats the node.
func (node *CaseExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("case ")
	if node.Expr != nil {
		buf.Myprintf("%v ", node.Expr)
	}
	for _, when := range node.Whens {
		buf.Myprintf("%v ", when)
	}
	if node.Else != nil {
		buf.Myprintf("else %v ", node.Else)
	}
	buf.Myprintf("end")
}

// Format formats the node.
func (node *Default) Format(buf *TrackedBuffer) {
	buf.Myprintf("default")
	if node.ColName != "" {
		buf.Myprintf("(%s)", node.ColName)
	}
}

// Format formats the node.
func (node *When) Format(buf *TrackedBuffer) {
	buf.Myprintf("when %v then %v", node.Cond, node.Val)
}

// Format formats the node.
func (node GroupBy) Format(buf *TrackedBuffer) {
	prefix := " group by "
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node OrderBy) Format(buf *TrackedBuffer) {
	prefix := " order by "
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *Order) Format(buf *TrackedBuffer) {
	if node, ok := node.Expr.(*NullVal); ok {
		buf.Myprintf("%v", node)
		return
	}
	if node, ok := node.Expr.(*FuncExpr); ok {
		if node.Name.Lowered() == "rand" {
			buf.Myprintf("%v", node)
			return
		}
	}

	buf.Myprintf("%v %s", node.Expr, node.Direction)
}

// Format formats the node.
func (node *Limit) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.Myprintf(" limit ")
	if node.Offset != nil {
		buf.Myprintf("%v, ", node.Offset)
	}
	buf.Myprintf("%v", node.Rowcount)
}

// Format formats the node.
func (node Values) Format(buf *TrackedBuffer) {
	prefix := "values "
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node UpdateExprs) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *UpdateExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v = %v", node.Name, node.Expr)
}

// Format formats the node.
func (node SetExprs) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *SetExpr) Format(buf *TrackedBuffer) {
	// We don't have to backtick set variable names.
	if node.Name.EqualString("charset") || node.Name.EqualString("names") {
		buf.Myprintf("%s %v", node.Name.String(), node.Expr)
	} else if node.Name.EqualString(TransactionStr) {
		sqlVal := node.Expr.(*SQLVal)
		buf.Myprintf("%s %s", node.Name.String(), strings.ToLower(string(sqlVal.Val)))
	} else {
		buf.Myprintf("%s = %v", node.Name.String(), node.Expr)
	}
}

// Format formats the node.
func (node OnDup) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.Myprintf(" on duplicate key update %v", UpdateExprs(node))
}

// Format formats the node.
func (node ColIdent) Format(buf *TrackedBuffer) {
	formatID(buf, node.val, node.Lowered())
}

// Format formats the node.
func (node TableIdent) Format(buf *TrackedBuffer) {
	formatID(buf, node.v, strings.ToLower(node.v))
}
