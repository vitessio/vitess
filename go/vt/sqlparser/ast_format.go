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

package sqlparser

import (
	"strings"

	"vitess.io/vitess/go/sqltypes"
)

// Format formats the node.
func (node *Select) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "select %v", node.Comments)

	if node.Distinct {
		buf.WriteString(DistinctStr)
	}
	if node.Cache != nil {
		if *node.Cache {
			buf.WriteString(SQLCacheStr)
		} else {
			buf.WriteString(SQLNoCacheStr)
		}
	}
	if node.StraightJoinHint {
		buf.WriteString(StraightJoinHint)
	}
	if node.SQLCalcFoundRows {
		buf.WriteString(SQLCalcFoundRowsStr)
	}

	buf.astPrintf(node, "%v from %v%v%v%v%v%v%s%v",
		node.SelectExprs,
		node.From, node.Where,
		node.GroupBy, node.Having, node.OrderBy,
		node.Limit, node.Lock.ToString(), node.Into)
}

// Format formats the node.
func (node *ParenSelect) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "(%v)", node.Select)
}

// Format formats the node.
func (node *Union) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v", node.FirstStatement)
	for _, us := range node.UnionSelects {
		buf.astPrintf(node, "%v", us)
	}
	buf.astPrintf(node, "%v%v%s", node.OrderBy, node.Limit, node.Lock.ToString())
}

// Format formats the node.
func (node *UnionSelect) Format(buf *TrackedBuffer) {
	if node.Distinct {
		buf.astPrintf(node, " %s %v", UnionStr, node.Statement)
	} else {
		buf.astPrintf(node, " %s %v", UnionAllStr, node.Statement)
	}
}

// Format formats the node.
func (node *VStream) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "vstream %v%v from %v",
		node.Comments, node.SelectExpr, node.Table)
}

// Format formats the node.
func (node *Stream) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "stream %v%v from %v",
		node.Comments, node.SelectExpr, node.Table)
}

// Format formats the node.
func (node *Insert) Format(buf *TrackedBuffer) {
	switch node.Action {
	case InsertAct:
		buf.astPrintf(node, "%s %v%sinto %v%v%v %v%v",
			InsertStr,
			node.Comments, node.Ignore.ToString(),
			node.Table, node.Partitions, node.Columns, node.Rows, node.OnDup)
	case ReplaceAct:
		buf.astPrintf(node, "%s %v%sinto %v%v%v %v%v",
			ReplaceStr,
			node.Comments, node.Ignore.ToString(),
			node.Table, node.Partitions, node.Columns, node.Rows, node.OnDup)
	default:
		buf.astPrintf(node, "%s %v%sinto %v%v%v %v%v",
			"Unkown Insert Action",
			node.Comments, node.Ignore.ToString(),
			node.Table, node.Partitions, node.Columns, node.Rows, node.OnDup)
	}

}

// Format formats the node.
func (node *Update) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "update %v%s%v set %v%v%v%v",
		node.Comments, node.Ignore.ToString(), node.TableExprs,
		node.Exprs, node.Where, node.OrderBy, node.Limit)
}

// Format formats the node.
func (node *Delete) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "delete %v", node.Comments)
	if node.Ignore {
		buf.WriteString("ignore ")
	}
	if node.Targets != nil {
		buf.astPrintf(node, "%v ", node.Targets)
	}
	buf.astPrintf(node, "from %v%v%v%v%v", node.TableExprs, node.Partitions, node.Where, node.OrderBy, node.Limit)
}

// Format formats the node.
func (node *Set) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "set %v%v", node.Comments, node.Exprs)
}

// Format formats the node.
func (node *SetTransaction) Format(buf *TrackedBuffer) {
	if node.Scope == ImplicitScope {
		buf.astPrintf(node, "set %vtransaction ", node.Comments)
	} else {
		buf.astPrintf(node, "set %v%s transaction ", node.Comments, node.Scope.ToString())
	}

	for i, char := range node.Characteristics {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.astPrintf(node, "%v", char)
	}
}

// Format formats the node.
func (node *DropDatabase) Format(buf *TrackedBuffer) {
	exists := ""
	if node.IfExists {
		exists = "if exists "
	}
	buf.astPrintf(node, "%s database %v%s%v", DropStr, node.Comments, exists, node.DBName)
}

// Format formats the node.
func (node *Flush) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s", FlushStr)
	if node.IsLocal {
		buf.WriteString(" local")
	}
	if len(node.FlushOptions) != 0 {
		prefix := " "
		for _, option := range node.FlushOptions {
			buf.astPrintf(node, "%s%s", prefix, option)
			prefix = ", "
		}
	} else {
		buf.WriteString(" tables")
		if len(node.TableNames) != 0 {
			buf.astPrintf(node, " %v", node.TableNames)
		}
		if node.ForExport {
			buf.WriteString(" for export")
		}
		if node.WithLock {
			buf.WriteString(" with read lock")
		}
	}
}

// Format formats the node.
func (node *AlterVschema) Format(buf *TrackedBuffer) {
	switch node.Action {
	case CreateVindexDDLAction:
		buf.astPrintf(node, "alter vschema create vindex %v %v", node.Table, node.VindexSpec)
	case DropVindexDDLAction:
		buf.astPrintf(node, "alter vschema drop vindex %v", node.Table)
	case AddVschemaTableDDLAction:
		buf.astPrintf(node, "alter vschema add table %v", node.Table)
	case DropVschemaTableDDLAction:
		buf.astPrintf(node, "alter vschema drop table %v", node.Table)
	case AddColVindexDDLAction:
		buf.astPrintf(node, "alter vschema on %v add vindex %v (", node.Table, node.VindexSpec.Name)
		for i, col := range node.VindexCols {
			if i != 0 {
				buf.astPrintf(node, ", %v", col)
			} else {
				buf.astPrintf(node, "%v", col)
			}
		}
		buf.astPrintf(node, ")")
		if node.VindexSpec.Type.String() != "" {
			buf.astPrintf(node, " %v", node.VindexSpec)
		}
	case DropColVindexDDLAction:
		buf.astPrintf(node, "alter vschema on %v drop vindex %v", node.Table, node.VindexSpec.Name)
	case AddSequenceDDLAction:
		buf.astPrintf(node, "alter vschema add sequence %v", node.Table)
	case AddAutoIncDDLAction:
		buf.astPrintf(node, "alter vschema on %v add auto_increment %v", node.Table, node.AutoIncSpec)
	default:
		buf.astPrintf(node, "%s table %v", node.Action.ToString(), node.Table)
	}
}

// Format formats the node.
func (node *AlterMigration) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "alter vitess_migration")
	if node.UUID != "" {
		buf.astPrintf(node, " '%s'", node.UUID)
	}
	var alterType string
	switch node.Type {
	case RetryMigrationType:
		alterType = "retry"
	case CompleteMigrationType:
		alterType = "complete"
	case CancelMigrationType:
		alterType = "cancel"
	case CancelAllMigrationType:
		alterType = "cancel all"
	}
	buf.astPrintf(node, " %s", alterType)
}

// Format formats the node.
func (node *RevertMigration) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "revert vitess_migration '%s'", node.UUID)
}

// Format formats the node.
func (node *OptLike) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "like %v", node.LikeTable)
}

// Format formats the node.
func (node *PartitionSpec) Format(buf *TrackedBuffer) {
	switch node.Action {
	case ReorganizeAction:
		buf.astPrintf(node, "%s ", ReorganizeStr)
		for i, n := range node.Names {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.astPrintf(node, "%v", n)
		}
		buf.WriteString(" into (")
		for i, pd := range node.Definitions {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.astPrintf(node, "%v", pd)
		}
		buf.astPrintf(node, ")")
	case AddAction:
		buf.astPrintf(node, "%s (%v)", AddStr, node.Definitions[0])
	case DropAction:
		buf.astPrintf(node, "%s ", DropPartitionStr)
		for i, n := range node.Names {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.astPrintf(node, "%v", n)
		}
	case DiscardAction:
		buf.astPrintf(node, "%s ", DiscardStr)
		if node.IsAll {
			buf.WriteString("all")
		} else {
			prefix := ""
			for _, n := range node.Names {
				buf.astPrintf(node, "%s%v", prefix, n)
				prefix = ", "
			}
		}
		buf.WriteString(" tablespace")
	case ImportAction:
		buf.astPrintf(node, "%s ", ImportStr)
		if node.IsAll {
			buf.WriteString("all")
		} else {
			prefix := ""
			for _, n := range node.Names {
				buf.astPrintf(node, "%s%v", prefix, n)
				prefix = ", "
			}
		}
		buf.WriteString(" tablespace")
	case TruncateAction:
		buf.astPrintf(node, "%s ", TruncatePartitionStr)
		if node.IsAll {
			buf.WriteString("all")
		} else {
			prefix := ""
			for _, n := range node.Names {
				buf.astPrintf(node, "%s%v", prefix, n)
				prefix = ", "
			}
		}
	case CoalesceAction:
		buf.astPrintf(node, "%s %v", CoalesceStr, node.Number)
	case ExchangeAction:
		buf.astPrintf(node, "%s %v with table %v", ExchangeStr, node.Names[0], node.TableName)
		if node.WithoutValidation {
			buf.WriteString(" without validation")
		}
	case AnalyzeAction:
		buf.astPrintf(node, "%s ", AnalyzePartitionStr)
		if node.IsAll {
			buf.WriteString("all")
		} else {
			prefix := ""
			for _, n := range node.Names {
				buf.astPrintf(node, "%s%v", prefix, n)
				prefix = ", "
			}
		}
	case CheckAction:
		buf.astPrintf(node, "%s ", CheckStr)
		if node.IsAll {
			buf.WriteString("all")
		} else {
			prefix := ""
			for _, n := range node.Names {
				buf.astPrintf(node, "%s%v", prefix, n)
				prefix = ", "
			}
		}
	case OptimizeAction:
		buf.astPrintf(node, "%s ", OptimizeStr)
		if node.IsAll {
			buf.WriteString("all")
		} else {
			prefix := ""
			for _, n := range node.Names {
				buf.astPrintf(node, "%s%v", prefix, n)
				prefix = ", "
			}
		}
	case RebuildAction:
		buf.astPrintf(node, "%s ", RebuildStr)
		if node.IsAll {
			buf.WriteString("all")
		} else {
			prefix := ""
			for _, n := range node.Names {
				buf.astPrintf(node, "%s%v", prefix, n)
				prefix = ", "
			}
		}
	case RepairAction:
		buf.astPrintf(node, "%s ", RepairStr)
		if node.IsAll {
			buf.WriteString("all")
		} else {
			prefix := ""
			for _, n := range node.Names {
				buf.astPrintf(node, "%s%v", prefix, n)
				prefix = ", "
			}
		}
	case RemoveAction:
		buf.WriteString(RemoveStr)
	case UpgradeAction:
		buf.WriteString(UpgradeStr)
	default:
		panic("unimplemented")
	}
}

// Format formats the node
func (node *PartitionDefinition) Format(buf *TrackedBuffer) {
	if !node.Maxvalue {
		buf.astPrintf(node, "partition %v values less than (%v)", node.Name, node.Limit)
	} else {
		buf.astPrintf(node, "partition %v values less than (maxvalue)", node.Name)
	}
}

// Format formats the node.
func (ts *TableSpec) Format(buf *TrackedBuffer) {
	buf.astPrintf(ts, "(\n")
	for i, col := range ts.Columns {
		if i == 0 {
			buf.astPrintf(ts, "\t%v", col)
		} else {
			buf.astPrintf(ts, ",\n\t%v", col)
		}
	}
	for _, idx := range ts.Indexes {
		buf.astPrintf(ts, ",\n\t%v", idx)
	}
	for _, c := range ts.Constraints {
		buf.astPrintf(ts, ",\n\t%v", c)
	}

	buf.astPrintf(ts, "\n)")
	for i, opt := range ts.Options {
		if i != 0 {
			buf.WriteString(",\n ")
		}
		buf.astPrintf(ts, " %s", opt.Name)
		if opt.String != "" {
			buf.astPrintf(ts, " %s", opt.String)
		} else if opt.Value != nil {
			buf.astPrintf(ts, " %v", opt.Value)
		} else {
			buf.astPrintf(ts, " (%v)", opt.Tables)
		}
	}
}

// Format formats the node.
func (col *ColumnDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(col, "%v %v", col.Name, &col.Type)
}

// Format returns a canonical string representation of the type and all relevant options
func (ct *ColumnType) Format(buf *TrackedBuffer) {
	buf.astPrintf(ct, "%s", ct.Type)

	if ct.Length != nil && ct.Scale != nil {
		buf.astPrintf(ct, "(%v,%v)", ct.Length, ct.Scale)

	} else if ct.Length != nil {
		buf.astPrintf(ct, "(%v)", ct.Length)
	}

	if ct.EnumValues != nil {
		buf.astPrintf(ct, "(%s)", strings.Join(ct.EnumValues, ", "))
	}

	if ct.Unsigned {
		buf.astPrintf(ct, " %s", keywordStrings[UNSIGNED])
	}
	if ct.Zerofill {
		buf.astPrintf(ct, " %s", keywordStrings[ZEROFILL])
	}
	if ct.Charset != "" {
		buf.astPrintf(ct, " %s %s %s", keywordStrings[CHARACTER], keywordStrings[SET], ct.Charset)
	}
	if ct.Collate != "" {
		buf.astPrintf(ct, " %s %s", keywordStrings[COLLATE], ct.Collate)
	}
	if ct.Options.Null != nil {
		if *ct.Options.Null {
			buf.astPrintf(ct, " %s", keywordStrings[NULL])
		} else {
			buf.astPrintf(ct, " %s %s", keywordStrings[NOT], keywordStrings[NULL])
		}
	}
	if ct.Options.Default != nil {
		buf.astPrintf(ct, " %s %v", keywordStrings[DEFAULT], ct.Options.Default)
	}
	if ct.Options.OnUpdate != nil {
		buf.astPrintf(ct, " %s %s %v", keywordStrings[ON], keywordStrings[UPDATE], ct.Options.OnUpdate)
	}
	if ct.Options.Autoincrement {
		buf.astPrintf(ct, " %s", keywordStrings[AUTO_INCREMENT])
	}
	if ct.Options.Comment != nil {
		buf.astPrintf(ct, " %s %v", keywordStrings[COMMENT_KEYWORD], ct.Options.Comment)
	}
	if ct.Options.KeyOpt == colKeyPrimary {
		buf.astPrintf(ct, " %s %s", keywordStrings[PRIMARY], keywordStrings[KEY])
	}
	if ct.Options.KeyOpt == colKeyUnique {
		buf.astPrintf(ct, " %s", keywordStrings[UNIQUE])
	}
	if ct.Options.KeyOpt == colKeyUniqueKey {
		buf.astPrintf(ct, " %s %s", keywordStrings[UNIQUE], keywordStrings[KEY])
	}
	if ct.Options.KeyOpt == colKeySpatialKey {
		buf.astPrintf(ct, " %s %s", keywordStrings[SPATIAL], keywordStrings[KEY])
	}
	if ct.Options.KeyOpt == colKeyFulltextKey {
		buf.astPrintf(ct, " %s %s", keywordStrings[FULLTEXT], keywordStrings[KEY])
	}
	if ct.Options.KeyOpt == colKey {
		buf.astPrintf(ct, " %s", keywordStrings[KEY])
	}
}

// Format formats the node.
func (idx *IndexDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(idx, "%v (", idx.Info)
	for i, col := range idx.Columns {
		if i != 0 {
			buf.astPrintf(idx, ", %v", col.Column)
		} else {
			buf.astPrintf(idx, "%v", col.Column)
		}
		if col.Length != nil {
			buf.astPrintf(idx, "(%v)", col.Length)
		}
		if col.Direction == DescOrder {
			buf.astPrintf(idx, " desc")
		}
	}
	buf.astPrintf(idx, ")")

	for _, opt := range idx.Options {
		buf.astPrintf(idx, " %s", opt.Name)
		if opt.String != "" {
			buf.astPrintf(idx, " %s", opt.String)
		} else {
			buf.astPrintf(idx, " %v", opt.Value)
		}
	}
}

// Format formats the node.
func (ii *IndexInfo) Format(buf *TrackedBuffer) {
	if !ii.ConstraintName.IsEmpty() {
		buf.astPrintf(ii, "constraint %v ", ii.ConstraintName)
	}
	if ii.Primary {
		buf.astPrintf(ii, "%s", ii.Type)
	} else {
		buf.astPrintf(ii, "%s", ii.Type)
		if !ii.Name.IsEmpty() {
			buf.astPrintf(ii, " %v", ii.Name)
		}
	}
}

// Format formats the node.
func (node *AutoIncSpec) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v ", node.Column)
	buf.astPrintf(node, "using %v", node.Sequence)
}

// Format formats the node. The "CREATE VINDEX" preamble was formatted in
// the containing DDL node Format, so this just prints the type, any
// parameters, and optionally the owner
func (node *VindexSpec) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "using %v", node.Type)

	numParams := len(node.Params)
	if numParams != 0 {
		buf.astPrintf(node, " with ")
		for i, p := range node.Params {
			if i != 0 {
				buf.astPrintf(node, ", ")
			}
			buf.astPrintf(node, "%v", p)
		}
	}
}

// Format formats the node.
func (node VindexParam) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s=%s", node.Key.String(), node.Val)
}

// Format formats the node.
func (c *ConstraintDefinition) Format(buf *TrackedBuffer) {
	if !c.Name.IsEmpty() {
		buf.astPrintf(c, "constraint %v ", c.Name)
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
	buf.astPrintf(f, "foreign key %v references %v %v", f.Source, f.ReferencedTable, f.ReferencedColumns)
	if f.OnDelete != DefaultAction {
		buf.astPrintf(f, " on delete %v", f.OnDelete)
	}
	if f.OnUpdate != DefaultAction {
		buf.astPrintf(f, " on update %v", f.OnUpdate)
	}
}

// Format formats the node.
func (c *CheckConstraintDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(c, "check (%v)", c.Expr)
	if !c.Enforced {
		buf.astPrintf(c, " not enforced")
	}
}

// Format formats the node.
func (node *Show) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v", node.Internal)
}

// Format formats the node.
func (node *ShowLegacy) Format(buf *TrackedBuffer) {
	nodeType := strings.ToLower(node.Type)
	if (nodeType == "tables" || nodeType == "columns" || nodeType == "fields" || nodeType == "index" || nodeType == "keys" || nodeType == "indexes" ||
		nodeType == "databases" || nodeType == "schemas" || nodeType == "keyspaces" || nodeType == "vitess_keyspaces" || nodeType == "vitess_shards" || nodeType == "vitess_tablets") && node.ShowTablesOpt != nil {
		opt := node.ShowTablesOpt
		if node.Extended != "" {
			buf.astPrintf(node, "show %s%s", node.Extended, nodeType)
		} else {
			buf.astPrintf(node, "show %s%s", opt.Full, nodeType)
		}
		if (nodeType == "columns" || nodeType == "fields") && node.HasOnTable() {
			buf.astPrintf(node, " from %v", node.OnTable)
		}
		if (nodeType == "index" || nodeType == "keys" || nodeType == "indexes") && node.HasOnTable() {
			buf.astPrintf(node, " from %v", node.OnTable)
		}
		if opt.DbName != "" {
			buf.astPrintf(node, " from %s", opt.DbName)
		}
		buf.astPrintf(node, "%v", opt.Filter)
		return
	}
	if node.Scope == ImplicitScope {
		buf.astPrintf(node, "show %s", nodeType)
	} else {
		buf.astPrintf(node, "show %s %s", node.Scope.ToString(), nodeType)
	}
	if node.HasOnTable() {
		buf.astPrintf(node, " on %v", node.OnTable)
	}
	if nodeType == "collation" && node.ShowCollationFilterOpt != nil {
		buf.astPrintf(node, " where %v", node.ShowCollationFilterOpt)
	}
	if nodeType == "charset" && node.ShowTablesOpt != nil {
		buf.astPrintf(node, "%v", node.ShowTablesOpt.Filter)
	}
	if node.HasTable() {
		buf.astPrintf(node, " %v", node.Table)
	}
}

// Format formats the node.
func (node *ShowFilter) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	if node.Like != "" {
		buf.astPrintf(node, " like ")
		sqltypes.BufEncodeStringSQL(buf.Builder, node.Like)
	} else {
		buf.astPrintf(node, " where %v", node.Filter)
	}
}

// Format formats the node.
func (node *Use) Format(buf *TrackedBuffer) {
	if node.DBName.v != "" {
		buf.astPrintf(node, "use %v", node.DBName)
	} else {
		buf.astPrintf(node, "use")
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
func (node *SRollback) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "rollback to %v", node.Name)
}

// Format formats the node.
func (node *Savepoint) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "savepoint %v", node.Name)
}

// Format formats the node.
func (node *Release) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "release savepoint %v", node.Name)
}

// Format formats the node.
func (node *ExplainStmt) Format(buf *TrackedBuffer) {
	format := ""
	switch node.Type {
	case EmptyType:
	case AnalyzeType:
		format = AnalyzeStr + " "
	default:
		format = "format = " + node.Type.ToString() + " "
	}
	buf.astPrintf(node, "explain %s%v", format, node.Statement)
}

// Format formats the node.
func (node *ExplainTab) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "explain %v", node.Table)
	if node.Wild != "" {
		buf.astPrintf(node, " %s", node.Wild)
	}
}

// Format formats the node.
func (node *CallProc) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "call %v(%v)", node.Name, node.Params)
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
		buf.astPrintf(node, "%s ", c)
	}
}

// Format formats the node.
func (node SelectExprs) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *StarExpr) Format(buf *TrackedBuffer) {
	if !node.TableName.IsEmpty() {
		buf.astPrintf(node, "%v.", node.TableName)
	}
	buf.astPrintf(node, "*")
}

// Format formats the node.
func (node *AliasedExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v", node.Expr)
	if !node.As.IsEmpty() {
		buf.astPrintf(node, " as %v", node.As)
	}
}

// Format formats the node.
func (node *Nextval) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "next %v values", node.Expr)
}

// Format formats the node.
func (node Columns) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	prefix := "("
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
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
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
	buf.WriteString(")")
}

// Format formats the node.
func (node TableExprs) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *AliasedTableExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v%v", node.Expr, node.Partitions)
	if !node.As.IsEmpty() {
		buf.astPrintf(node, " as %v", node.As)
	}
	if node.Hints != nil {
		// Hint node provides the space padding.
		buf.astPrintf(node, "%v", node.Hints)
	}
}

// Format formats the node.
func (node TableNames) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node TableName) Format(buf *TrackedBuffer) {
	if node.IsEmpty() {
		return
	}
	if !node.Qualifier.IsEmpty() {
		buf.astPrintf(node, "%v.", node.Qualifier)
	}
	buf.astPrintf(node, "%v", node.Name)
}

// Format formats the node.
func (node *ParenTableExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "(%v)", node.Exprs)
}

// Format formats the node.
func (node JoinCondition) Format(buf *TrackedBuffer) {
	if node.On != nil {
		buf.astPrintf(node, " on %v", node.On)
	}
	if node.Using != nil {
		buf.astPrintf(node, " using %v", node.Using)
	}
}

// Format formats the node.
func (node *JoinTableExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v %s %v%v", node.LeftExpr, node.Join.ToString(), node.RightExpr, node.Condition)
}

// Format formats the node.
func (node *IndexHints) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, " %sindex ", node.Type.ToString())
	if len(node.Indexes) == 0 {
		buf.astPrintf(node, "()")
	} else {
		prefix := "("
		for _, n := range node.Indexes {
			buf.astPrintf(node, "%s%v", prefix, n)
			prefix = ", "
		}
		buf.astPrintf(node, ")")
	}
}

// Format formats the node.
func (node *Where) Format(buf *TrackedBuffer) {
	if node == nil || node.Expr == nil {
		return
	}
	buf.astPrintf(node, " %s %v", node.Type.ToString(), node.Expr)
}

// Format formats the node.
func (node Exprs) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *AndExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%l and %r", node.Left, node.Right)
}

// Format formats the node.
func (node *OrExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%l or %r", node.Left, node.Right)
}

// Format formats the node.
func (node *XorExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%l xor %r", node.Left, node.Right)
}

// Format formats the node.
func (node *NotExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "not %v", node.Expr)
}

// Format formats the node.
func (node *ComparisonExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%l %s %r", node.Left, node.Operator.ToString(), node.Right)
	if node.Escape != nil {
		buf.astPrintf(node, " escape %v", node.Escape)
	}
}

// Format formats the node.
func (node *RangeCond) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v %s %l and %r", node.Left, node.Operator.ToString(), node.From, node.To)
}

// Format formats the node.
func (node *IsExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v %s", node.Expr, node.Operator.ToString())
}

// Format formats the node.
func (node *ExistsExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "exists %v", node.Subquery)
}

// Format formats the node.
func (node *Literal) Format(buf *TrackedBuffer) {
	switch node.Type {
	case StrVal:
		sqltypes.MakeTrusted(sqltypes.VarBinary, node.Bytes()).EncodeSQL(buf)
	case IntVal, FloatVal, HexNum:
		buf.astPrintf(node, "%s", node.Val)
	case HexVal:
		buf.astPrintf(node, "X'%s'", node.Val)
	case BitVal:
		buf.astPrintf(node, "B'%s'", node.Val)
	default:
		panic("unexpected")
	}
}

// Format formats the node.
func (node Argument) Format(buf *TrackedBuffer) {
	buf.WriteArg(string(node))
}

// Format formats the node.
func (node *NullVal) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "null")
}

// Format formats the node.
func (node BoolVal) Format(buf *TrackedBuffer) {
	if node {
		buf.astPrintf(node, "true")
	} else {
		buf.astPrintf(node, "false")
	}
}

// Format formats the node.
func (node *ColName) Format(buf *TrackedBuffer) {
	if !node.Qualifier.IsEmpty() {
		buf.astPrintf(node, "%v.", node.Qualifier)
	}
	buf.astPrintf(node, "%v", node.Name)
}

// Format formats the node.
func (node ValTuple) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "(%v)", Exprs(node))
}

// Format formats the node.
func (node *Subquery) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "(%v)", node.Select)
}

// Format formats the node.
func (node *DerivedTable) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "(%v)", node.Select)
}

// Format formats the node.
func (node ListArg) Format(buf *TrackedBuffer) {
	buf.WriteArg(string(node))
}

// Format formats the node.
func (node *BinaryExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%l %s %r", node.Left, node.Operator.ToString(), node.Right)
}

// Format formats the node.
func (node *UnaryExpr) Format(buf *TrackedBuffer) {
	if _, unary := node.Expr.(*UnaryExpr); unary {
		// They have same precedence so parenthesis is not required.
		buf.astPrintf(node, "%s %v", node.Operator.ToString(), node.Expr)
		return
	}
	buf.astPrintf(node, "%s%v", node.Operator.ToString(), node.Expr)
}

// Format formats the node.
func (node *IntervalExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "interval %v %s", node.Expr, node.Unit)
}

// Format formats the node.
func (node *TimestampFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%s, %v, %v)", node.Name, node.Unit, node.Expr1, node.Expr2)
}

// Format formats the node.
func (node *CurTimeFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v)", node.Name.String(), node.Fsp)
}

// Format formats the node.
func (node *CollateExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v collate %s", node.Expr, node.Charset)
}

// Format formats the node.
func (node *FuncExpr) Format(buf *TrackedBuffer) {
	var distinct string
	if node.Distinct {
		distinct = "distinct "
	}
	if !node.Qualifier.IsEmpty() {
		buf.astPrintf(node, "%v.", node.Qualifier)
	}
	// Function names should not be back-quoted even
	// if they match a reserved word, only if they contain illegal characters
	funcName := node.Name.String()

	if containEscapableChars(funcName, NoAt) {
		writeEscapedString(buf, funcName)
	} else {
		buf.WriteString(funcName)
	}
	buf.astPrintf(node, "(%s%v)", distinct, node.Exprs)
}

// Format formats the node
func (node *GroupConcatExpr) Format(buf *TrackedBuffer) {
	if node.Distinct {
		buf.astPrintf(node, "group_concat(%s%v%v%s%v)", DistinctStr, node.Exprs, node.OrderBy, node.Separator, node.Limit)
	} else {
		buf.astPrintf(node, "group_concat(%v%v%s%v)", node.Exprs, node.OrderBy, node.Separator, node.Limit)
	}
}

// Format formats the node.
func (node *ValuesFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "values(%v)", node.Name)
}

// Format formats the node.
func (node *SubstrExpr) Format(buf *TrackedBuffer) {
	var val SQLNode
	if node.Name != nil {
		val = node.Name
	} else {
		val = node.StrVal
	}

	if node.To == nil {
		buf.astPrintf(node, "substr(%v, %v)", val, node.From)
	} else {
		buf.astPrintf(node, "substr(%v, %v, %v)", val, node.From, node.To)
	}
}

// Format formats the node.
func (node *ConvertExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "convert(%v, %v)", node.Expr, node.Type)
}

// Format formats the node.
func (node *ConvertUsingExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "convert(%v using %s)", node.Expr, node.Type)
}

// Format formats the node.
func (node *ConvertType) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s", node.Type)
	if node.Length != nil {
		buf.astPrintf(node, "(%v", node.Length)
		if node.Scale != nil {
			buf.astPrintf(node, ", %v", node.Scale)
		}
		buf.astPrintf(node, ")")
	}
	if node.Charset != "" {
		buf.astPrintf(node, "%s %s", node.Operator.ToString(), node.Charset)
	}
}

// Format formats the node
func (node *MatchExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "match(%v) against (%v%s)", node.Columns, node.Expr, node.Option.ToString())
}

// Format formats the node.
func (node *CaseExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "case ")
	if node.Expr != nil {
		buf.astPrintf(node, "%v ", node.Expr)
	}
	for _, when := range node.Whens {
		buf.astPrintf(node, "%v ", when)
	}
	if node.Else != nil {
		buf.astPrintf(node, "else %v ", node.Else)
	}
	buf.astPrintf(node, "end")
}

// Format formats the node.
func (node *Default) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "default")
	if node.ColName != "" {
		buf.WriteString("(")
		formatID(buf, node.ColName, NoAt)
		buf.WriteString(")")
	}
}

// Format formats the node.
func (node *When) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "when %v then %v", node.Cond, node.Val)
}

// Format formats the node.
func (node GroupBy) Format(buf *TrackedBuffer) {
	prefix := " group by "
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node OrderBy) Format(buf *TrackedBuffer) {
	prefix := " order by "
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *Order) Format(buf *TrackedBuffer) {
	if node, ok := node.Expr.(*NullVal); ok {
		buf.astPrintf(node, "%v", node)
		return
	}
	if node, ok := node.Expr.(*FuncExpr); ok {
		if node.Name.Lowered() == "rand" {
			buf.astPrintf(node, "%v", node)
			return
		}
	}

	buf.astPrintf(node, "%v %s", node.Expr, node.Direction.ToString())
}

// Format formats the node.
func (node *Limit) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.astPrintf(node, " limit ")
	if node.Offset != nil {
		buf.astPrintf(node, "%v, ", node.Offset)
	}
	buf.astPrintf(node, "%v", node.Rowcount)
}

// Format formats the node.
func (node Values) Format(buf *TrackedBuffer) {
	prefix := "values "
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node UpdateExprs) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *UpdateExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v = %v", node.Name, node.Expr)
}

// Format formats the node.
func (node SetExprs) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *SetExpr) Format(buf *TrackedBuffer) {
	if node.Scope != ImplicitScope {
		buf.WriteString(node.Scope.ToString())
		buf.WriteString(" ")
	}
	// We don't have to backtick set variable names.
	switch {
	case node.Name.EqualString("charset") || node.Name.EqualString("names"):
		buf.astPrintf(node, "%s %v", node.Name.String(), node.Expr)
	case node.Name.EqualString(TransactionStr):
		literal := node.Expr.(*Literal)
		buf.astPrintf(node, "%s %s", node.Name.String(), strings.ToLower(string(literal.Val)))
	default:
		buf.astPrintf(node, "%v = %v", node.Name, node.Expr)
	}
}

// Format formats the node.
func (node OnDup) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.astPrintf(node, " on duplicate key update %v", UpdateExprs(node))
}

// Format formats the node.
func (node ColIdent) Format(buf *TrackedBuffer) {
	for i := NoAt; i < node.at; i++ {
		buf.WriteByte('@')
	}
	formatID(buf, node.val, node.at)
}

// Format formats the node.
func (node TableIdent) Format(buf *TrackedBuffer) {
	formatID(buf, node.v, NoAt)
}

// Format formats the node.
func (node IsolationLevel) Format(buf *TrackedBuffer) {
	buf.WriteString("isolation level ")
	switch node {
	case ReadUncommitted:
		buf.WriteString(ReadUncommittedStr)
	case ReadCommitted:
		buf.WriteString(ReadCommittedStr)
	case RepeatableRead:
		buf.WriteString(RepeatableReadStr)
	case Serializable:
		buf.WriteString(SerializableStr)
	default:
		buf.WriteString("Unknown Isolation level value")
	}
}

// Format formats the node.
func (node AccessMode) Format(buf *TrackedBuffer) {
	if node == ReadOnly {
		buf.WriteString(TxReadOnly)
	} else {
		buf.WriteString(TxReadWrite)
	}
}

// Format formats the node.
func (node *Load) Format(buf *TrackedBuffer) {
	buf.WriteString("AST node missing for Load type")
}

// Format formats the node.
func (node *ShowBasic) Format(buf *TrackedBuffer) {
	buf.WriteString("show")
	if node.Full {
		buf.WriteString(" full")
	}
	buf.astPrintf(node, "%s", node.Command.ToString())
	if !node.Tbl.IsEmpty() {
		buf.astPrintf(node, " from %v", node.Tbl)
	}
	if !node.DbName.IsEmpty() {
		buf.astPrintf(node, " from %v", node.DbName)
	}
	buf.astPrintf(node, "%v", node.Filter)
}

// Format formats the node.
func (node *ShowCreate) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "show%s %v", node.Command.ToString(), node.Op)
}

// Format formats the node.
func (node *SelectInto) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.astPrintf(node, "%s%s", node.Type.ToString(), node.FileName)
	if node.Charset != "" {
		buf.astPrintf(node, " character set %s", node.Charset)
	}
	buf.astPrintf(node, "%s%s%s%s", node.FormatOption, node.ExportOption, node.Manifest, node.Overwrite)
}

// Format formats the node.
func (node *CreateDatabase) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "create database %v", node.Comments)
	if node.IfNotExists {
		buf.WriteString("if not exists ")
	}
	buf.astPrintf(node, "%v", node.DBName)
	if node.CreateOptions != nil {
		for _, createOption := range node.CreateOptions {
			if createOption.IsDefault {
				buf.WriteString(" default")
			}
			buf.WriteString(createOption.Type.ToString())
			buf.WriteString(" " + createOption.Value)
		}
	}
}

// Format formats the node.
func (node *AlterDatabase) Format(buf *TrackedBuffer) {
	buf.WriteString("alter database")
	if !node.DBName.IsEmpty() {
		buf.astPrintf(node, " %v", node.DBName)
	}
	if node.UpdateDataDirectory {
		buf.WriteString(" upgrade data directory name")
	}
	if node.AlterOptions != nil {
		for _, createOption := range node.AlterOptions {
			if createOption.IsDefault {
				buf.WriteString(" default")
			}
			buf.WriteString(createOption.Type.ToString())
			buf.WriteString(" " + createOption.Value)
		}
	}
}

// Format formats the node.
func (node *CreateTable) Format(buf *TrackedBuffer) {
	buf.WriteString("create ")
	if node.Temp {
		buf.WriteString("temporary ")
	}
	buf.WriteString("table ")

	if node.IfNotExists {
		buf.WriteString("if not exists ")
	}
	buf.astPrintf(node, "%v", node.Table)

	if node.OptLike != nil {
		buf.astPrintf(node, " %v", node.OptLike)
	}
	if node.TableSpec != nil {
		buf.astPrintf(node, " %v", node.TableSpec)
	}
}

// Format formats the node.
func (node *CreateView) Format(buf *TrackedBuffer) {
	buf.WriteString("create")
	if node.IsReplace {
		buf.WriteString(" or replace")
	}
	if node.Algorithm != "" {
		buf.astPrintf(node, " algorithm = %s", node.Algorithm)
	}
	if node.Definer != "" {
		buf.astPrintf(node, " definer = %s", node.Definer)
	}
	if node.Security != "" {
		buf.astPrintf(node, " sql security %s", node.Security)
	}
	buf.astPrintf(node, " view %v", node.ViewName)
	buf.astPrintf(node, "%v as %v", node.Columns, node.Select)
	if node.CheckOption != "" {
		buf.astPrintf(node, " with %s check option", node.CheckOption)
	}
}

// Format formats the LockTables node.
func (node *LockTables) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "lock tables %v %s", node.Tables[0].Table, node.Tables[0].Lock.ToString())
	for i := 1; i < len(node.Tables); i++ {
		buf.astPrintf(node, ", %v %s", node.Tables[i].Table, node.Tables[i].Lock.ToString())
	}
}

// Format formats the UnlockTables node.
func (node *UnlockTables) Format(buf *TrackedBuffer) {
	buf.WriteString("unlock tables")
}

// Format formats the node.
func (node *AlterView) Format(buf *TrackedBuffer) {
	buf.WriteString("alter")
	if node.Algorithm != "" {
		buf.astPrintf(node, " algorithm = %s", node.Algorithm)
	}
	if node.Definer != "" {
		buf.astPrintf(node, " definer = %s", node.Definer)
	}
	if node.Security != "" {
		buf.astPrintf(node, " sql security %s", node.Security)
	}
	buf.astPrintf(node, " view %v", node.ViewName)
	buf.astPrintf(node, "%v as %v", node.Columns, node.Select)
	if node.CheckOption != "" {
		buf.astPrintf(node, " with %s check option", node.CheckOption)
	}
}

// Format formats the node.
func (node *DropTable) Format(buf *TrackedBuffer) {
	temp := ""
	if node.Temp {
		temp = " temporary"
	}
	exists := ""
	if node.IfExists {
		exists = " if exists"
	}
	buf.astPrintf(node, "drop%s table%s %v", temp, exists, node.FromTables)
}

// Format formats the node.
func (node *DropView) Format(buf *TrackedBuffer) {
	exists := ""
	if node.IfExists {
		exists = " if exists"
	}
	buf.astPrintf(node, "drop view%s %v", exists, node.FromTables)
}

// Format formats the AlterTable node.
func (node *AlterTable) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "alter table %v", node.Table)
	prefix := ""
	for i, option := range node.AlterOptions {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.astPrintf(node, " %v", option)
		if node.PartitionSpec != nil && node.PartitionSpec.Action != RemoveAction {
			prefix = ","
		}
	}
	if node.PartitionSpec != nil {
		buf.astPrintf(node, "%s %v", prefix, node.PartitionSpec)
	}
}

// Format formats the node.
func (node *AddConstraintDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "add %v", node.ConstraintDefinition)
}

// Format formats the node.
func (node *AddIndexDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "add %v", node.IndexDefinition)
}

// Format formats the node.
func (node *AddColumns) Format(buf *TrackedBuffer) {

	if len(node.Columns) == 1 {
		buf.astPrintf(node, "add column %v", node.Columns[0])
		if node.First != nil {
			buf.astPrintf(node, " first %v", node.First)
		}
		if node.After != nil {
			buf.astPrintf(node, " after %v", node.After)
		}
	} else {
		for i, col := range node.Columns {
			if i == 0 {
				buf.astPrintf(node, "add column (%v", col)
			} else {
				buf.astPrintf(node, ", %v", col)
			}
		}
		buf.WriteString(")")
	}
}

// Format formats the node.
func (node AlgorithmValue) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "algorithm = %s", string(node))
}

// Format formats the node
func (node *AlterColumn) Format(buf *TrackedBuffer) {
	if node.DropDefault {
		buf.astPrintf(node, "alter column %v drop default", node.Column)
	} else {
		buf.astPrintf(node, "alter column %v set default", node.Column)
		buf.astPrintf(node, " %v", node.DefaultVal)
	}
}

// Format formats the node
func (node *ChangeColumn) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "change column %v %v", node.OldColumn, node.NewColDefinition)
	if node.First != nil {
		buf.astPrintf(node, " first %v", node.First)
	}
	if node.After != nil {
		buf.astPrintf(node, " after %v", node.After)
	}
}

// Format formats the node
func (node *ModifyColumn) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "modify column %v", node.NewColDefinition)
	if node.First != nil {
		buf.astPrintf(node, " first %v", node.First)
	}
	if node.After != nil {
		buf.astPrintf(node, " after %v", node.After)
	}
}

// Format formats the node
func (node *AlterCharset) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "convert to character set %s", node.CharacterSet)
	if node.Collate != "" {
		buf.astPrintf(node, " collate %s", node.Collate)
	}
}

// Format formats the node
func (node *KeyState) Format(buf *TrackedBuffer) {
	if node.Enable {
		buf.WriteString("enable keys")
	} else {
		buf.WriteString("disable keys")
	}

}

// Format formats the node
func (node *TablespaceOperation) Format(buf *TrackedBuffer) {
	if node.Import {
		buf.WriteString("import tablespace")
	} else {
		buf.WriteString("discard tablespace")
	}
}

// Format formats the node
func (node *DropColumn) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "drop column %v", node.Name)
}

// Format formats the node
func (node *DropKey) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "drop %s", node.Type.ToString())
	if !node.Name.IsEmpty() {
		buf.astPrintf(node, " %v", node.Name)
	}
}

// Format formats the node
func (node *Force) Format(buf *TrackedBuffer) {
	buf.WriteString("force")
}

// Format formats the node
func (node *LockOption) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "lock %s", node.Type.ToString())
}

// Format formats the node
func (node *OrderByOption) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "order by ")
	prefix := ""
	for _, n := range node.Cols {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node
func (node *RenameTableName) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "rename %v", node.Table)
}

// Format formats the node
func (node *RenameIndex) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "rename index %v to %v", node.OldName, node.NewName)
}

// Format formats the node
func (node *Validation) Format(buf *TrackedBuffer) {
	if node.With {
		buf.WriteString("with validation")
	} else {
		buf.WriteString("without validation")
	}
}

// Format formats the node
func (node TableOptions) Format(buf *TrackedBuffer) {
	for i, option := range node {
		if i != 0 {
			buf.WriteString(" ")
		}
		buf.astPrintf(node, "%s", option.Name)
		if option.String != "" {
			buf.astPrintf(node, " %s", option.String)
		} else if option.Value != nil {
			buf.astPrintf(node, " %v", option.Value)
		} else {
			buf.astPrintf(node, " (%v)", option.Tables)
		}
	}
}

// Format formats the node
func (node *TruncateTable) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "truncate table %v", node.Table)
}

// Format formats the node.
func (node *RenameTable) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "rename table")
	prefix := " "
	for _, pair := range node.TablePairs {
		buf.astPrintf(node, "%s%v to %v", prefix, pair.FromTable, pair.ToTable)
		prefix = ", "
	}
}
