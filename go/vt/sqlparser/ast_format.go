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
	"vitess.io/vitess/go/sqltypes"
)

// Format formats the node.
func (node *Select) Format(buf *TrackedBuffer) {
	if node.With != nil {
		buf.astPrintf(node, "%v", node.With)
	}
	buf.astPrintf(node, "select %v", node.Comments)

	if node.Distinct {
		buf.literal(DistinctStr)
	}
	if node.Cache != nil {
		if *node.Cache {
			buf.literal(SQLCacheStr)
		} else {
			buf.literal(SQLNoCacheStr)
		}
	}
	if node.StraightJoinHint {
		buf.literal(StraightJoinHint)
	}
	if node.SQLCalcFoundRows {
		buf.literal(SQLCalcFoundRowsStr)
	}

	buf.astPrintf(node, "%v from ", node.SelectExprs)

	prefix := ""
	for _, expr := range node.From {
		buf.astPrintf(node, "%s%v", prefix, expr)
		prefix = ", "
	}

	buf.astPrintf(node, "%v%v%v",
		node.Where,
		node.GroupBy, node.Having)

	if node.Windows != nil {
		buf.astPrintf(node, " %v", node.Windows)
	}

	buf.astPrintf(node, "%v%v%s%v",
		node.OrderBy,
		node.Limit, node.Lock.ToString(), node.Into)
}

// Format formats the node.
func (node *CommentOnly) Format(buf *TrackedBuffer) {
	for _, comment := range node.Comments {
		buf.WriteString(comment)
	}
}

// Format formats the node.
func (node *Union) Format(buf *TrackedBuffer) {
	if requiresParen(node.Left) {
		buf.astPrintf(node, "(%v)", node.Left)
	} else {
		buf.astPrintf(node, "%v", node.Left)
	}

	buf.WriteByte(' ')
	if node.Distinct {
		buf.literal(UnionStr)
	} else {
		buf.literal(UnionAllStr)
	}
	buf.WriteByte(' ')

	if requiresParen(node.Right) {
		buf.astPrintf(node, "(%v)", node.Right)
	} else {
		buf.astPrintf(node, "%v", node.Right)
	}

	buf.astPrintf(node, "%v%v%s", node.OrderBy, node.Limit, node.Lock.ToString())
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
			node.Table.Expr, node.Partitions, node.Columns, node.Rows, node.OnDup)
	case ReplaceAct:
		buf.astPrintf(node, "%s %v%sinto %v%v%v %v%v",
			ReplaceStr,
			node.Comments, node.Ignore.ToString(),
			node.Table.Expr, node.Partitions, node.Columns, node.Rows, node.OnDup)
	default:
		buf.astPrintf(node, "%s %v%sinto %v%v%v %v%v",
			"Unkown Insert Action",
			node.Comments, node.Ignore.ToString(),
			node.Table.Expr, node.Partitions, node.Columns, node.Rows, node.OnDup)
	}

}

// Format formats the node.
func (node *With) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "with ")

	if node.Recursive {
		buf.astPrintf(node, "recursive ")
	}
	ctesLength := len(node.ctes)
	for i := 0; i < ctesLength-1; i++ {
		buf.astPrintf(node, "%v, ", node.ctes[i])
	}
	buf.astPrintf(node, "%v", node.ctes[ctesLength-1])
}

// Format formats the node.
func (node *CommonTableExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v%v as %v ", node.ID, node.Columns, node.Subquery)
}

// Format formats the node.
func (node *Update) Format(buf *TrackedBuffer) {
	if node.With != nil {
		buf.astPrintf(node, "%v", node.With)
	}
	buf.astPrintf(node, "update %v%s%v set %v%v%v%v",
		node.Comments, node.Ignore.ToString(), node.TableExprs,
		node.Exprs, node.Where, node.OrderBy, node.Limit)
}

// Format formats the node.
func (node *Delete) Format(buf *TrackedBuffer) {
	if node.With != nil {
		buf.astPrintf(node, "%v", node.With)
	}
	buf.astPrintf(node, "delete %v", node.Comments)
	if node.Ignore {
		buf.literal("ignore ")
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
func (node *DropDatabase) Format(buf *TrackedBuffer) {
	exists := ""
	if node.IfExists {
		exists = "if exists "
	}
	buf.astPrintf(node, "%s %vdatabase %s%v", DropStr, node.Comments, exists, node.DBName)
}

// Format formats the node.
func (node *Flush) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s", FlushStr)
	if node.IsLocal {
		buf.literal(" local")
	}
	if len(node.FlushOptions) != 0 {
		prefix := " "
		for _, option := range node.FlushOptions {
			buf.astPrintf(node, "%s%s", prefix, option)
			prefix = ", "
		}
	} else {
		buf.literal(" tables")
		if len(node.TableNames) != 0 {
			buf.astPrintf(node, " %v", node.TableNames)
		}
		if node.ForExport {
			buf.literal(" for export")
		}
		if node.WithLock {
			buf.literal(" with read lock")
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
		buf.astPrintf(node, " '%#s'", node.UUID)
	}
	var alterType string
	switch node.Type {
	case RetryMigrationType:
		alterType = "retry"
	case CleanupMigrationType:
		alterType = "cleanup"
	case LaunchMigrationType:
		alterType = "launch"
	case LaunchAllMigrationType:
		alterType = "launch all"
	case CompleteMigrationType:
		alterType = "complete"
	case CompleteAllMigrationType:
		alterType = "complete all"
	case CancelMigrationType:
		alterType = "cancel"
	case CancelAllMigrationType:
		alterType = "cancel all"
	case ThrottleMigrationType:
		alterType = "throttle"
	case ThrottleAllMigrationType:
		alterType = "throttle all"
	case UnthrottleMigrationType:
		alterType = "unthrottle"
	case UnthrottleAllMigrationType:
		alterType = "unthrottle all"
	}
	buf.astPrintf(node, " %#s", alterType)
	if node.Expire != "" {
		buf.astPrintf(node, " expire '%#s'", node.Expire)
	}
	if node.Ratio != nil {
		buf.astPrintf(node, " ratio %v", node.Ratio)
	}
	if node.Shards != "" {
		buf.astPrintf(node, " vitess_shards '%#s'", node.Shards)
	}
}

// Format formats the node.
func (node *RevertMigration) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "revert %vvitess_migration '%#s'", node.Comments, node.UUID)
}

// Format formats the node.
func (node *ShowMigrationLogs) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "show vitess_migration '%#s' logs", node.UUID)
}

// Format formats the node.
func (node *ShowThrottledApps) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "show vitess_throttled_apps")
}

// Format formats the node.
func (node *ShowThrottlerStatus) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "show vitess_throttler status")
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
				buf.literal(", ")
			}
			buf.astPrintf(node, "%v", n)
		}
		buf.literal(" into (")
		for i, pd := range node.Definitions {
			if i != 0 {
				buf.literal(", ")
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
				buf.literal(", ")
			}
			buf.astPrintf(node, "%v", n)
		}
	case DiscardAction:
		buf.astPrintf(node, "%s ", DiscardStr)
		if node.IsAll {
			buf.literal("all")
		} else {
			prefix := ""
			for _, n := range node.Names {
				buf.astPrintf(node, "%s%v", prefix, n)
				prefix = ", "
			}
		}
		buf.literal(" tablespace")
	case ImportAction:
		buf.astPrintf(node, "%s ", ImportStr)
		if node.IsAll {
			buf.literal("all")
		} else {
			prefix := ""
			for _, n := range node.Names {
				buf.astPrintf(node, "%s%v", prefix, n)
				prefix = ", "
			}
		}
		buf.literal(" tablespace")
	case TruncateAction:
		buf.astPrintf(node, "%s ", TruncatePartitionStr)
		if node.IsAll {
			buf.literal("all")
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
			buf.literal(" without validation")
		}
	case AnalyzeAction:
		buf.astPrintf(node, "%s ", AnalyzePartitionStr)
		if node.IsAll {
			buf.literal("all")
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
			buf.literal("all")
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
			buf.literal("all")
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
			buf.literal("all")
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
			buf.literal("all")
		} else {
			prefix := ""
			for _, n := range node.Names {
				buf.astPrintf(node, "%s%v", prefix, n)
				prefix = ", "
			}
		}
	case RemoveAction:
		buf.literal(RemoveStr)
	case UpgradeAction:
		buf.literal(UpgradeStr)
	default:
		panic("unimplemented")
	}
}

// Format formats the node
func (node *PartitionDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "partition %v", node.Name)
	buf.astPrintf(node, "%v", node.Options)
}

// Format formats the node
func (node *PartitionDefinitionOptions) Format(buf *TrackedBuffer) {
	if node.ValueRange != nil {
		buf.astPrintf(node, " %v", node.ValueRange)
	}
	if node.Engine != nil {
		buf.astPrintf(node, " %v", node.Engine)
	}
	if node.Comment != nil {
		buf.astPrintf(node, " comment %v", node.Comment)
	}
	if node.DataDirectory != nil {
		buf.astPrintf(node, " data directory %v", node.DataDirectory)
	}
	if node.IndexDirectory != nil {
		buf.astPrintf(node, " index directory %v", node.IndexDirectory)
	}
	if node.MaxRows != nil {
		buf.astPrintf(node, " max_rows %d", *node.MaxRows)
	}
	if node.MinRows != nil {
		buf.astPrintf(node, " min_rows %d", *node.MinRows)
	}
	if node.TableSpace != "" {
		buf.astPrintf(node, " tablespace %#s", node.TableSpace)
	}
	if node.SubPartitionDefinitions != nil {
		buf.astPrintf(node, " (%v)", node.SubPartitionDefinitions)
	}
}

// Format formats the node
func (node SubPartitionDefinitions) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node
func (node *SubPartitionDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "subpartition %v", node.Name)
	buf.astPrintf(node, "%v", node.Options)
}

// Format formats the node
func (node *SubPartitionDefinitionOptions) Format(buf *TrackedBuffer) {
	if node.Engine != nil {
		buf.astPrintf(node, " %v", node.Engine)
	}
	if node.Comment != nil {
		buf.astPrintf(node, " comment %v", node.Comment)
	}
	if node.DataDirectory != nil {
		buf.astPrintf(node, " data directory %v", node.DataDirectory)
	}
	if node.IndexDirectory != nil {
		buf.astPrintf(node, " index directory %v", node.IndexDirectory)
	}
	if node.MaxRows != nil {
		buf.astPrintf(node, " max_rows %d", *node.MaxRows)
	}
	if node.MinRows != nil {
		buf.astPrintf(node, " min_rows %d", *node.MinRows)
	}
	if node.TableSpace != "" {
		buf.astPrintf(node, " tablespace %#s", node.TableSpace)
	}
}

// Format formats the node
func (node *PartitionValueRange) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "values %s", node.Type.ToString())
	if node.Maxvalue {
		buf.literal(" maxvalue")
	} else {
		buf.astPrintf(node, " %v", node.Range)
	}
}

// Format formats the node
func (node *PartitionEngine) Format(buf *TrackedBuffer) {
	if node.Storage {
		buf.astPrintf(node, "%s", "storage ")
	}
	buf.astPrintf(node, "%s", "engine ")
	buf.astPrintf(node, "%#s", node.Name)
}

// Format formats the node.
func (node *PartitionOption) Format(buf *TrackedBuffer) {
	buf.literal("\npartition by")
	if node.IsLinear {
		buf.literal(" linear")
	}

	switch node.Type {
	case HashType:
		buf.astPrintf(node, " hash (%v)", node.Expr)
	case KeyType:
		buf.literal(" key")
		if node.KeyAlgorithm != 0 {
			buf.astPrintf(node, " algorithm = %d", node.KeyAlgorithm)
		}
		if len(node.ColList) == 0 {
			buf.literal(" ()")
		} else {
			buf.astPrintf(node, " %v", node.ColList)
		}
	case RangeType, ListType:
		buf.astPrintf(node, " %s", node.Type.ToString())
		if node.Expr != nil {
			buf.astPrintf(node, " (%v)", node.Expr)
		} else {
			buf.astPrintf(node, " columns %v", node.ColList)
		}
	}

	if node.Partitions != -1 {
		buf.astPrintf(node, " partitions %d", node.Partitions)
	}
	if node.SubPartition != nil {
		buf.astPrintf(node, " %v", node.SubPartition)
	}
	if node.Definitions != nil {
		buf.literal("\n(")
		for i, pd := range node.Definitions {
			if i != 0 {
				buf.literal(",\n ")
			}
			buf.astPrintf(node, "%v", pd)
		}
		buf.WriteByte(')')
	}
}

// Format formats the node.
func (node *SubPartition) Format(buf *TrackedBuffer) {
	buf.literal("subpartition by")
	if node.IsLinear {
		buf.literal(" linear")
	}

	switch node.Type {
	case HashType:
		buf.astPrintf(node, " hash (%v)", node.Expr)
	case KeyType:
		buf.literal(" key")
		if node.KeyAlgorithm != 0 {
			buf.astPrintf(node, " algorithm = %d", node.KeyAlgorithm)
		}
		if len(node.ColList) == 0 {
			buf.literal(" ()")
		} else {
			buf.astPrintf(node, " %v", node.ColList)
		}
	}

	if node.SubPartitions != -1 {
		buf.astPrintf(node, " subpartitions %d", node.SubPartitions)
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
			buf.literal(",\n ")
		}
		buf.astPrintf(ts, " %s", opt.Name)
		if opt.String != "" {
			if opt.CaseSensitive {
				buf.astPrintf(ts, " %#s", opt.String)
			} else {
				buf.astPrintf(ts, " %s", opt.String)
			}
		} else if opt.Value != nil {
			buf.astPrintf(ts, " %v", opt.Value)
		} else {
			buf.astPrintf(ts, " (%v)", opt.Tables)
		}
	}
	if ts.PartitionOption != nil {
		buf.astPrintf(ts, "%v", ts.PartitionOption)
	}
}

// Format formats the node.
func (col *ColumnDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(col, "%v %v", col.Name, col.Type)
}

// Format returns a canonical string representation of the type and all relevant options
func (ct *ColumnType) Format(buf *TrackedBuffer) {
	buf.astPrintf(ct, "%#s", ct.Type)

	if ct.Length != nil && ct.Scale != nil {
		buf.astPrintf(ct, "(%v,%v)", ct.Length, ct.Scale)

	} else if ct.Length != nil {
		buf.astPrintf(ct, "(%v)", ct.Length)
	}

	if ct.EnumValues != nil {
		buf.WriteString("(")
		for i, enum := range ct.EnumValues {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.astPrintf(ct, "%#s", enum)
		}
		buf.WriteString(")")
	}

	if ct.Unsigned {
		buf.astPrintf(ct, " %#s", keywordStrings[UNSIGNED])
	}
	if ct.Zerofill {
		buf.astPrintf(ct, " %#s", keywordStrings[ZEROFILL])
	}
	if ct.Charset.Name != "" {
		buf.astPrintf(ct, " %s %s %#s", keywordStrings[CHARACTER], keywordStrings[SET], ct.Charset.Name)
	}
	if ct.Charset.Binary {
		buf.astPrintf(ct, " %#s", keywordStrings[BINARY])
	}
	if ct.Options != nil {
		if ct.Options.Collate != "" {
			buf.astPrintf(ct, " %s %#s", keywordStrings[COLLATE], ct.Options.Collate)
		}
		if ct.Options.Null != nil && ct.Options.As == nil {
			if *ct.Options.Null {
				buf.astPrintf(ct, " %s", keywordStrings[NULL])
			} else {
				buf.astPrintf(ct, " %s %s", keywordStrings[NOT], keywordStrings[NULL])
			}
		}
		if ct.Options.Default != nil {
			buf.astPrintf(ct, " %s", keywordStrings[DEFAULT])
			if defaultRequiresParens(ct) {
				buf.astPrintf(ct, " (%v)", ct.Options.Default)
			} else {
				buf.astPrintf(ct, " %v", ct.Options.Default)
			}
		}
		if ct.Options.OnUpdate != nil {
			buf.astPrintf(ct, " %s %s %v", keywordStrings[ON], keywordStrings[UPDATE], ct.Options.OnUpdate)
		}
		if ct.Options.As != nil {
			buf.astPrintf(ct, " %s (%v)", keywordStrings[AS], ct.Options.As)

			if ct.Options.Storage == VirtualStorage {
				buf.astPrintf(ct, " %s", keywordStrings[VIRTUAL])
			} else if ct.Options.Storage == StoredStorage {
				buf.astPrintf(ct, " %s", keywordStrings[STORED])
			}
			if ct.Options.Null != nil {
				if *ct.Options.Null {
					buf.astPrintf(ct, " %s", keywordStrings[NULL])
				} else {
					buf.astPrintf(ct, " %s %s", keywordStrings[NOT], keywordStrings[NULL])
				}
			}
		}
		if ct.Options.Autoincrement {
			buf.astPrintf(ct, " %s", keywordStrings[AUTO_INCREMENT])
		}
		if ct.Options.Comment != nil {
			buf.astPrintf(ct, " %s %v", keywordStrings[COMMENT_KEYWORD], ct.Options.Comment)
		}
		if ct.Options.Invisible != nil {
			if *ct.Options.Invisible {
				buf.astPrintf(ct, " %s", keywordStrings[INVISIBLE])
			} else {
				buf.astPrintf(ct, " %s", keywordStrings[VISIBLE])
			}
		}
		if ct.Options.Format != UnspecifiedFormat {
			buf.astPrintf(ct, " %s %s", keywordStrings[COLUMN_FORMAT], ct.Options.Format.ToString())
		}
		if ct.Options.EngineAttribute != nil {
			buf.astPrintf(ct, " %s %v", keywordStrings[ENGINE_ATTRIBUTE], ct.Options.EngineAttribute)
		}
		if ct.Options.SecondaryEngineAttribute != nil {
			buf.astPrintf(ct, " %s %v", keywordStrings[SECONDARY_ENGINE_ATTRIBUTE], ct.Options.SecondaryEngineAttribute)
		}
		if ct.Options.KeyOpt == ColKeyPrimary {
			buf.astPrintf(ct, " %s %s", keywordStrings[PRIMARY], keywordStrings[KEY])
		}
		if ct.Options.KeyOpt == ColKeyUnique {
			buf.astPrintf(ct, " %s", keywordStrings[UNIQUE])
		}
		if ct.Options.KeyOpt == ColKeyUniqueKey {
			buf.astPrintf(ct, " %s %s", keywordStrings[UNIQUE], keywordStrings[KEY])
		}
		if ct.Options.KeyOpt == ColKeySpatialKey {
			buf.astPrintf(ct, " %s %s", keywordStrings[SPATIAL], keywordStrings[KEY])
		}
		if ct.Options.KeyOpt == ColKeyFulltextKey {
			buf.astPrintf(ct, " %s %s", keywordStrings[FULLTEXT], keywordStrings[KEY])
		}
		if ct.Options.KeyOpt == ColKey {
			buf.astPrintf(ct, " %s", keywordStrings[KEY])
		}
		if ct.Options.Reference != nil {
			buf.astPrintf(ct, " %v", ct.Options.Reference)
		}
		if ct.Options.SRID != nil {
			buf.astPrintf(ct, " %s %v", keywordStrings[SRID], ct.Options.SRID)
		}
	}
}

// Format formats the node.
func (idx *IndexDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(idx, "%v (", idx.Info)
	for i, col := range idx.Columns {
		if i != 0 {
			buf.astPrintf(idx, ", ")
		}
		if col.Expression != nil {
			buf.astPrintf(idx, "(%v)", col.Expression)
		} else {
			buf.astPrintf(idx, "%v", col.Column)
			if col.Length != nil {
				buf.astPrintf(idx, "(%v)", col.Length)
			}
		}
		if col.Direction == DescOrder {
			buf.astPrintf(idx, " desc")
		}
	}
	buf.astPrintf(idx, ")")

	for _, opt := range idx.Options {
		buf.astPrintf(idx, " %s", opt.Name)
		if opt.String != "" {
			buf.astPrintf(idx, " %#s", opt.String)
		} else if opt.Value != nil {
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
	buf.astPrintf(node, "%#s=%#s", node.Key.String(), node.Val)
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
		buf.literal("restrict")
	case Cascade:
		buf.literal("cascade")
	case NoAction:
		buf.literal("no action")
	case SetNull:
		buf.literal("set null")
	case SetDefault:
		buf.literal("set default")
	}
}

// Format formats the node.
func (a MatchAction) Format(buf *TrackedBuffer) {
	switch a {
	case Full:
		buf.literal("full")
	case Simple:
		buf.literal("simple")
	case Partial:
		buf.literal("partial")
	}
}

// Format formats the node.
func (f *ForeignKeyDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(f, "foreign key %v%v %v", f.IndexName, f.Source, f.ReferenceDefinition)
}

// Format formats the node.
func (ref *ReferenceDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(ref, "references %v %v", ref.ReferencedTable, ref.ReferencedColumns)
	if ref.Match != DefaultMatch {
		buf.astPrintf(ref, " match %v", ref.Match)
	}
	if ref.OnDelete != DefaultAction {
		buf.astPrintf(ref, " on delete %v", ref.OnDelete)
	}
	if ref.OnUpdate != DefaultAction {
		buf.astPrintf(ref, " on update %v", ref.OnUpdate)
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
	buf.literal("commit")
}

// Format formats the node.
func (node *Begin) Format(buf *TrackedBuffer) {
	if node.TxAccessModes == nil {
		buf.literal("begin")
		return
	}
	buf.literal("start transaction")
	for idx, accessMode := range node.TxAccessModes {
		if idx == 0 {
			buf.astPrintf(node, " %s", accessMode.ToString())
			continue
		}
		buf.astPrintf(node, ", %s", accessMode.ToString())
	}

}

// Format formats the node.
func (node *Rollback) Format(buf *TrackedBuffer) {
	buf.literal("rollback")
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
	buf.astPrintf(node, "explain %v%s%v", node.Comments, format, node.Statement)
}

// Format formats the node.
func (node *VExplainStmt) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "vexplain %v%s %v", node.Comments, node.Type.ToString(), node.Statement)
}

// Format formats the node.
func (node *ExplainTab) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "explain %v", node.Table)
	if node.Wild != "" {
		buf.astPrintf(node, " %s", node.Wild)
	}
}

// Format formats the node.
func (node *PrepareStmt) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "prepare %v%v from ", node.Comments, node.Name)
	if node.Statement != nil {
		buf.astPrintf(node, "%v", node.Statement)
	}
}

// Format formats the node.
func (node *ExecuteStmt) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "execute %v%v", node.Comments, node.Name)
	if len(node.Arguments) > 0 {
		buf.literal(" using ")
	}
	var prefix string
	for _, n := range node.Arguments {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *DeallocateStmt) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "deallocate %vprepare %v", node.Comments, node.Name)
}

// Format formats the node.
func (node *CallProc) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "call %v(%v)", node.Name, node.Params)
}

// Format formats the node.
func (node *OtherRead) Format(buf *TrackedBuffer) {
	buf.literal("otherread")
}

// Format formats the node.
func (node *OtherAdmin) Format(buf *TrackedBuffer) {
	buf.literal("otheradmin")
}

// Format formats the node.
func (node *ParsedComments) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	for _, c := range node.comments {
		buf.astPrintf(node, "%#s ", c)
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
	buf.WriteByte('(')
	prefix := ""
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
	buf.WriteByte(')')
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
	buf.WriteByte(')')
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
		if len(node.Columns) != 0 {
			buf.astPrintf(node, "%v", node.Columns)
		}
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
func (node *JoinCondition) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
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
func (node IndexHints) Format(buf *TrackedBuffer) {
	for _, n := range node {
		buf.astPrintf(node, "%v", n)
	}
}

// Format formats the node.
func (node *IndexHint) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, " %sindex ", node.Type.ToString())
	if node.ForType != NoForType {
		buf.astPrintf(node, "for %s ", node.ForType.ToString())
	}
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
func (node *BetweenExpr) Format(buf *TrackedBuffer) {
	if node.IsBetween {
		buf.astPrintf(node, "%v between %l and %r", node.Left, node.From, node.To)
	} else {
		buf.astPrintf(node, "%v not between %l and %r", node.Left, node.From, node.To)
	}
}

// Format formats the node.
func (node *IsExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v %s", node.Left, node.Right.ToString())
}

// Format formats the node.
func (node *ExistsExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "exists %v", node.Subquery)
}

// Format formats the node.
func (node *AssignmentExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%l := %r", node.Left, node.Right)
}

// Format formats the node.
func (node *Literal) Format(buf *TrackedBuffer) {
	switch node.Type {
	case StrVal:
		sqltypes.MakeTrusted(sqltypes.VarBinary, node.Bytes()).EncodeSQL(buf)
	case IntVal, FloatVal, DecimalVal, HexNum:
		buf.astPrintf(node, "%#s", node.Val)
	case HexVal:
		buf.astPrintf(node, "X'%#s'", node.Val)
	case BitVal:
		buf.astPrintf(node, "B'%#s'", node.Val)
	case DateVal:
		buf.astPrintf(node, "date'%#s'", node.Val)
	case TimeVal:
		buf.astPrintf(node, "time'%#s'", node.Val)
	case TimestampVal:
		buf.astPrintf(node, "timestamp'%#s'", node.Val)
	default:
		panic("unexpected")
	}
}

// Format formats the node.
func (node *Argument) Format(buf *TrackedBuffer) {
	buf.WriteArg(":", node.Name)
	if node.Type >= 0 {
		// For bind variables that are statically typed, emit their type as an adjacent comment.
		// This comment will be ignored by older versions of Vitess (and by MySQL) but will provide
		// type safety when using the query as a cache key.
		buf.astPrintf(node, " /* %s */", node.Type.String())
	}
}

// Format formats the node.
func (node *NullVal) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "null")
}

// Format formats the node.
func (node BoolVal) Format(buf *TrackedBuffer) {
	if node {
		buf.WriteString("true")
	} else {
		buf.WriteString("false")
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
	if node.Lateral {
		buf.literal("lateral ")
	}
	buf.astPrintf(node, "(%v)", node.Select)
}

// Format formats the node.
func (node ListArg) Format(buf *TrackedBuffer) {
	buf.WriteArg("::", string(node))
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
func (node *IntroducerExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%#s %v", node.CharacterSet, node.Expr)
}

// Format formats the node.
func (node *TimestampFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%#s(%#s, %v, %v)", node.Name, node.Unit, node.Expr1, node.Expr2)
}

// Format formats the node.
func (node *ExtractFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "extract(%#s from %v)", node.IntervalTypes.ToString(), node.Expr)
}

// Format formats the node
func (node *RegexpInstrExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "regexp_instr(%v, %v", node.Expr, node.Pattern)
	if node.Position != nil {
		buf.astPrintf(node, ", %v", node.Position)
	}
	if node.Occurrence != nil {
		buf.astPrintf(node, ", %v", node.Occurrence)
	}
	if node.ReturnOption != nil {
		buf.astPrintf(node, ", %v", node.ReturnOption)
	}
	if node.MatchType != nil {
		buf.astPrintf(node, ", %v", node.MatchType)
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *RegexpLikeExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "regexp_like(%v, %v", node.Expr, node.Pattern)
	if node.MatchType != nil {
		buf.astPrintf(node, ", %v", node.MatchType)
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *RegexpReplaceExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "regexp_replace(%v, %v, %v", node.Expr, node.Pattern, node.Repl)
	if node.Position != nil {
		buf.astPrintf(node, ", %v", node.Position)
	}
	if node.Occurrence != nil {
		buf.astPrintf(node, ", %v", node.Occurrence)
	}
	if node.MatchType != nil {
		buf.astPrintf(node, ", %v", node.MatchType)
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *RegexpSubstrExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "regexp_substr(%v, %v", node.Expr, node.Pattern)
	if node.Position != nil {
		buf.astPrintf(node, ", %v", node.Position)
	}
	if node.Occurrence != nil {
		buf.astPrintf(node, ", %v", node.Occurrence)
	}
	if node.MatchType != nil {
		buf.astPrintf(node, ", %v", node.MatchType)
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *DateAddExpr) Format(buf *TrackedBuffer) {
	switch node.Type {
	case AdddateType:
		buf.astPrintf(node, "adddate(%v, ", node.Date)
		if node.Unit == IntervalUnknown {
			buf.astPrintf(node, "%v", node.Expr)
		} else {
			buf.astPrintf(node, "interval %v %#s", node.Expr, node.Unit.ToString())
		}
		buf.WriteByte(')')
	case DateAddType:
		buf.astPrintf(node, "date_add(%v, interval %v %#s)", node.Date, node.Expr, node.Unit.ToString())
	case PlusIntervalLeftType:
		buf.astPrintf(node, "interval %v %#s + %v", node.Expr, node.Unit.ToString(), node.Date)
	case PlusIntervalRightType:
		buf.astPrintf(node, "%v + interval %v %#s", node.Date, node.Expr, node.Unit.ToString())
	}
}

// Format formats the node
func (node *DateSubExpr) Format(buf *TrackedBuffer) {
	switch node.Type {
	case SubdateType:
		buf.astPrintf(node, "subdate(%v, ", node.Date)
		if node.Unit == IntervalUnknown {
			buf.astPrintf(node, "%v", node.Expr)
		} else {
			buf.astPrintf(node, "interval %v %#s", node.Expr, node.Unit.ToString())
		}
		buf.WriteByte(')')
	case DateSubType:
		buf.astPrintf(node, "date_sub(%v, interval %v %#s)", node.Date, node.Expr, node.Unit.ToString())
	case MinusIntervalRightType:
		buf.astPrintf(node, "%v - interval %v %#s", node.Date, node.Expr, node.Unit.ToString())
	}
}

// Format formats the node.
func (node *TrimFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.TrimFuncType.ToString())
	if node.TrimFuncType == NormalTrimType {
		var from bool
		if node.Type != NoTrimType {
			buf.astPrintf(node, "%s ", node.Type.ToString())
			from = true
		}
		if node.TrimArg != nil {
			buf.astPrintf(node, "%v ", node.TrimArg)
			from = true
		}

		if from {
			buf.literal("from ")
		}
	}
	buf.astPrintf(node, "%v", node.StringArg)
	buf.WriteByte(')')
}

// Format formats the node.
func (node *WeightStringFuncExpr) Format(buf *TrackedBuffer) {
	if node.As != nil {
		buf.astPrintf(node, "weight_string(%v as %v)", node.Expr, node.As)
	} else {
		buf.astPrintf(node, "weight_string(%v)", node.Expr)
	}
}

// Format formats the node.
func (node *CurTimeFuncExpr) Format(buf *TrackedBuffer) {
	if node.Fsp > 0 {
		buf.astPrintf(node, "%#s(%d)", node.Name.String(), node.Fsp)
	} else {
		buf.astPrintf(node, "%#s()", node.Name.String())
	}
}

// Format formats the node.
func (node *CollateExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v collate %#s", node.Expr, node.Collation)
}

// Format formats the node.
func (node *FuncExpr) Format(buf *TrackedBuffer) {
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
	buf.astPrintf(node, "(%v)", node.Exprs)
}

// Format formats the node
func (node *GroupConcatExpr) Format(buf *TrackedBuffer) {
	if node.Distinct {
		buf.astPrintf(node, "group_concat(%s%v%v", DistinctStr, node.Exprs, node.OrderBy)
	} else {
		buf.astPrintf(node, "group_concat(%v%v", node.Exprs, node.OrderBy)
	}
	if node.Separator != "" {
		buf.astPrintf(node, " %s %#s", keywordStrings[SEPARATOR], node.Separator)
	}
	buf.astPrintf(node, "%v)", node.Limit)
}

// Format formats the node.
func (node *ValuesFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "values(%v)", node.Name)
}

// Format formats the node
func (node *JSONPrettyExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_pretty(%v)", node.JSONVal)

}

// Format formats the node
func (node *JSONStorageFreeExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_storage_free(%v)", node.JSONVal)

}

// Format formats the node
func (node *JSONStorageSizeExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_storage_size(%v)", node.JSONVal)

}

// Format formats the node
func (node *OverClause) Format(buf *TrackedBuffer) {
	buf.WriteString("over")
	if !node.WindowName.IsEmpty() {
		buf.astPrintf(node, " %v", node.WindowName)
	}
	if node.WindowSpec != nil {
		buf.astPrintf(node, " (%v)", node.WindowSpec)
	}
}

// Format formats the node
func (node *WindowSpecification) Format(buf *TrackedBuffer) {
	if !node.Name.IsEmpty() {
		buf.astPrintf(node, " %v", node.Name)
	}
	if node.PartitionClause != nil {
		buf.astPrintf(node, " partition by %v", node.PartitionClause)
	}
	if node.OrderClause != nil {
		buf.astPrintf(node, "%v", node.OrderClause)
	}
	if node.FrameClause != nil {
		buf.astPrintf(node, "%v", node.FrameClause)
	}
}

// Format formats the node
func (node *FrameClause) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, " %s", node.Unit.ToString())
	if node.End != nil {
		buf.astPrintf(node, " between%v and%v", node.Start, node.End)
	} else {
		buf.astPrintf(node, "%v", node.Start)
	}
}

// Format formats the node
func (node *NullTreatmentClause) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, " %s", node.Type.ToString())
}

// Format formats the node
func (node *FromFirstLastClause) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, " %s", node.Type.ToString())
}

// Format formats the node
func (node *FramePoint) Format(buf *TrackedBuffer) {
	if node.Expr != nil {
		if node.Unit != IntervalUnknown {
			buf.astPrintf(node, " interval %v %#s", node.Expr, node.Unit.ToString())
		} else {
			buf.astPrintf(node, " %v", node.Expr)
		}
	}
	buf.astPrintf(node, " %s", node.Type.ToString())
}

// Format formats the node
func (node *ArgumentLessWindowExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s()", node.Type.ToString())
	if node.OverClause != nil {
		buf.astPrintf(node, " %v", node.OverClause)
	}
}

// Format formats the node
func (node *FirstOrLastValueExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v)", node.Type.ToString(), node.Expr)
	if node.NullTreatmentClause != nil {
		buf.astPrintf(node, "%v", node.NullTreatmentClause)
	}
	if node.OverClause != nil {
		buf.astPrintf(node, " %v", node.OverClause)
	}
}

// Format formats the node
func (node *NtileExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "ntile(")
	buf.astPrintf(node, "%v", node.N)
	buf.WriteString(")")
	if node.OverClause != nil {
		buf.astPrintf(node, " %v", node.OverClause)
	}
}

// Format formats the node
func (node *NTHValueExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "nth_value(%v, ", node.Expr)
	buf.astPrintf(node, "%v", node.N)
	buf.WriteString(")")
	if node.FromFirstLastClause != nil {
		buf.astPrintf(node, "%v", node.FromFirstLastClause)
	}
	if node.NullTreatmentClause != nil {
		buf.astPrintf(node, "%v", node.NullTreatmentClause)
	}
	if node.OverClause != nil {
		buf.astPrintf(node, " %v", node.OverClause)
	}
}

// Format formats the node
func (node *LagLeadExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v", node.Type.ToString(), node.Expr)
	if node.N != nil {
		buf.astPrintf(node, ", %v", node.N)
	}
	if node.Default != nil {
		buf.astPrintf(node, ", %v", node.Default)
	}
	buf.WriteString(")")
	if node.NullTreatmentClause != nil {
		buf.astPrintf(node, "%v", node.NullTreatmentClause)
	}
	if node.OverClause != nil {
		buf.astPrintf(node, " %v", node.OverClause)
	}
}

// Format formats the node
func (node *ExtractValueExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "extractvalue(%v, %v)", node.Fragment, node.XPathExpr)
}

// Format formats the node
func (node *UpdateXMLExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "updatexml(%v, %v, %v)", node.Target, node.XPathExpr, node.NewXML)
}

func (node *PerformanceSchemaFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.Type.ToString())
	if node.Argument != nil {
		buf.astPrintf(node, "%v", node.Argument)
	}
	buf.astPrintf(node, ")")
}

// Format formats the node
func (node *GTIDFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v", node.Type.ToString(), node.Set1)
	if node.Set2 != nil {
		buf.astPrintf(node, ", %v", node.Set2)
	}
	if node.Timeout != nil {
		buf.astPrintf(node, ", %v", node.Timeout)
	}
	if node.Channel != nil {
		buf.astPrintf(node, ", %v", node.Channel)
	}
	buf.astPrintf(node, ")")
}

// Format formats the node.
func (node *SubstrExpr) Format(buf *TrackedBuffer) {
	if node.To == nil {
		buf.astPrintf(node, "substr(%v, %v)", node.Name, node.From)
	} else {
		buf.astPrintf(node, "substr(%v, %v, %v)", node.Name, node.From, node.To)
	}
}

// Format formats the node.
func (node *InsertExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "insert(%v, %v, %v, %v)", node.Str, node.Pos, node.Len, node.NewStr)
}

// Format formats the node.
func (node *IntervalFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "interval(%v, %v)", node.Expr, node.Exprs)
}

// Format formats the node.
func (node *LocateExpr) Format(buf *TrackedBuffer) {
	if node.Pos != nil {
		buf.astPrintf(node, "locate(%v, %v, %v)", node.SubStr, node.Str, node.Pos)
	} else {
		buf.astPrintf(node, "locate(%v, %v)", node.SubStr, node.Str)
	}
}

// Format formats the node.
func (node *CharExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "char(%v", node.Exprs)
	if node.Charset != "" {
		buf.astPrintf(node, " using %#s", node.Charset)
	}
	buf.astPrintf(node, ")")
}

// Format formats the node.
func (node *NamedWindow) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "window %v", node.Windows)
}

// Format formats the node.
func (node NamedWindows) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *WindowDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v AS (%v)", node.Name, node.WindowSpec)
}

// Format formats the node.
func (node WindowDefinitions) Format(buf *TrackedBuffer) {
	var prefix string
	for _, n := range node {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
}

// Format formats the node.
func (node *CastExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "cast(%v as %v", node.Expr, node.Type)
	if node.Array {
		buf.astPrintf(node, " %#s", keywordStrings[ARRAY])
	}
	buf.astPrintf(node, ")")
}

// Format formats the node.
func (node *ConvertExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "convert(%v, %v)", node.Expr, node.Type)
}

// Format formats the node.
func (node *ConvertUsingExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "convert(%v using %#s)", node.Expr, node.Type)
}

// Format formats the node.
func (node *ConvertType) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%#s", node.Type)
	if node.Length != nil {
		buf.astPrintf(node, "(%v", node.Length)
		if node.Scale != nil {
			buf.astPrintf(node, ", %v", node.Scale)
		}
		buf.astPrintf(node, ")")
	}
	if node.Charset.Name != "" {
		buf.astPrintf(node, " character set %#s", node.Charset.Name)
	}
	if node.Charset.Binary {
		buf.astPrintf(node, " %#s", keywordStrings[BINARY])
	}
}

// Format formats the node
func (node *MatchExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "match(")
	for i, col := range node.Columns {
		if i != 0 {
			buf.astPrintf(node, ", %v", col)
		} else {
			buf.astPrintf(node, "%v", col)
		}
	}
	buf.astPrintf(node, ") against (%v%s)", node.Expr, node.Option.ToString())
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
		buf.WriteByte('(')
		formatID(buf, node.ColName, NoAt)
		buf.WriteByte(')')
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
	// We don't have to backtick set variable names.
	switch {
	case node.Var.Name.EqualString("charset") || node.Var.Name.EqualString("names"):
		buf.astPrintf(node, "%s %v", node.Var.Name.String(), node.Expr)
	default:
		buf.astPrintf(node, "%v = %v", node.Var, node.Expr)
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
func (node IdentifierCI) Format(buf *TrackedBuffer) {
	if node.IsEmpty() {
		return
	}
	formatID(buf, node.val, NoAt)
}

// Format formats the node.
func (node IdentifierCS) Format(buf *TrackedBuffer) {
	formatID(buf, node.v, NoAt)
}

// Format formats the node.
func (node *Load) Format(buf *TrackedBuffer) {
	buf.literal("AST node missing for Load type")
}

// Format formats the node.
func (node *ShowBasic) Format(buf *TrackedBuffer) {
	buf.literal("show")
	if node.Full {
		buf.literal(" full")
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
func (node *ShowOther) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "show %s", node.Command)
}

// Format formats the node.
func (node *SelectInto) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.astPrintf(node, "%s%#s", node.Type.ToString(), node.FileName)
	if node.Charset.Name != "" {
		buf.astPrintf(node, " character set %#s", node.Charset.Name)
	}
	buf.astPrintf(node, "%#s%#s%#s%#s", node.FormatOption, node.ExportOption, node.Manifest, node.Overwrite)
}

// Format formats the node.
func (node *CreateDatabase) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "create database %v", node.Comments)
	if node.IfNotExists {
		buf.literal("if not exists ")
	}
	buf.astPrintf(node, "%v", node.DBName)
	if node.CreateOptions != nil {
		for _, createOption := range node.CreateOptions {
			if createOption.IsDefault {
				buf.literal(" default")
			}
			buf.literal(createOption.Type.ToString())
			buf.WriteByte(' ')
			buf.literal(createOption.Value)
		}
	}
}

// Format formats the node.
func (node *AlterDatabase) Format(buf *TrackedBuffer) {
	buf.literal("alter database")
	if !node.DBName.IsEmpty() {
		buf.astPrintf(node, " %v", node.DBName)
	}
	if node.UpdateDataDirectory {
		buf.literal(" upgrade data directory name")
	}
	if node.AlterOptions != nil {
		for _, createOption := range node.AlterOptions {
			if createOption.IsDefault {
				buf.literal(" default")
			}
			buf.literal(createOption.Type.ToString())
			buf.WriteByte(' ')
			buf.literal(createOption.Value)
		}
	}
}

// Format formats the node.
func (node *CreateTable) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "create %v", node.Comments)
	if node.Temp {
		buf.literal("temporary ")
	}
	buf.literal("table ")

	if node.IfNotExists {
		buf.literal("if not exists ")
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
	buf.astPrintf(node, "create %v", node.Comments)
	if node.IsReplace {
		buf.literal("or replace ")
	}
	if node.Algorithm != "" {
		buf.astPrintf(node, "algorithm = %#s ", node.Algorithm)
	}
	if node.Definer != nil {
		buf.astPrintf(node, "definer = %v ", node.Definer)
	}
	if node.Security != "" {
		buf.astPrintf(node, "sql security %s ", node.Security)
	}
	buf.astPrintf(node, "view %v", node.ViewName)
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
	buf.literal("unlock tables")
}

// Format formats the node.
func (node *AlterView) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "alter %v", node.Comments)
	if node.Algorithm != "" {
		buf.astPrintf(node, "algorithm = %s ", node.Algorithm)
	}
	if node.Definer != nil {
		buf.astPrintf(node, "definer = %v ", node.Definer)
	}
	if node.Security != "" {
		buf.astPrintf(node, "sql security %s ", node.Security)
	}
	buf.astPrintf(node, "view %v", node.ViewName)
	buf.astPrintf(node, "%v as %v", node.Columns, node.Select)
	if node.CheckOption != "" {
		buf.astPrintf(node, " with %s check option", node.CheckOption)
	}
}

func (definer *Definer) Format(buf *TrackedBuffer) {
	buf.astPrintf(definer, "%#s", definer.Name)
	if definer.Address != "" {
		buf.astPrintf(definer, "@%#s", definer.Address)
	}
}

// Format formats the node.
func (node *DropTable) Format(buf *TrackedBuffer) {
	temp := ""
	if node.Temp {
		temp = "temporary "
	}
	exists := ""
	if node.IfExists {
		exists = " if exists"
	}
	buf.astPrintf(node, "drop %v%stable%s %v", node.Comments, temp, exists, node.FromTables)
}

// Format formats the node.
func (node *DropView) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "drop %v", node.Comments)
	exists := ""
	if node.IfExists {
		exists = " if exists"
	}
	buf.astPrintf(node, "view%s %v", exists, node.FromTables)
}

// Format formats the AlterTable node.
func (node *AlterTable) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "alter %vtable %v", node.Comments, node.Table)
	prefix := ""
	for i, option := range node.AlterOptions {
		if i != 0 {
			buf.WriteByte(',')
		}
		buf.astPrintf(node, " %v", option)
		if node.PartitionSpec != nil && node.PartitionSpec.Action != RemoveAction {
			prefix = ","
		}
	}
	if node.PartitionSpec != nil {
		buf.astPrintf(node, "%s %v", prefix, node.PartitionSpec)
	}
	if node.PartitionOption != nil {
		buf.astPrintf(node, "%s %v", prefix, node.PartitionOption)
	}
}

// Format formats the node.
func (node *AddConstraintDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "add %v", node.ConstraintDefinition)
}

func (node *AlterCheck) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "alter check %v", node.Name)
	if node.Enforced {
		buf.astPrintf(node, " %s", keywordStrings[ENFORCED])
	} else {
		buf.astPrintf(node, " %s %s", keywordStrings[NOT], keywordStrings[ENFORCED])
	}
}

// Format formats the node.
func (node *AddIndexDefinition) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "add %v", node.IndexDefinition)
}

// Format formats the node.
func (node *AddColumns) Format(buf *TrackedBuffer) {

	if len(node.Columns) == 1 {
		buf.astPrintf(node, "add column %v", node.Columns[0])
		if node.First {
			buf.astPrintf(node, " first")
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
		buf.WriteByte(')')
	}
}

// Format formats the node.
func (node AlgorithmValue) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "algorithm = %#s", string(node))
}

// Format formats the node
func (node *AlterColumn) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "alter column %v", node.Column)
	if node.DropDefault {
		buf.astPrintf(node, " drop default")
	} else if node.DefaultVal != nil {
		buf.astPrintf(node, " set default %v", node.DefaultVal)
	}
	if node.Invisible != nil {
		if *node.Invisible {
			buf.astPrintf(node, " set invisible")
		} else {
			buf.astPrintf(node, " set visible")
		}
	}
}

// Format formats the node
func (node *AlterIndex) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "alter index %v", node.Name)
	if node.Invisible {
		buf.astPrintf(node, " invisible")
	} else {
		buf.astPrintf(node, " visible")
	}
}

// Format formats the node
func (node *ChangeColumn) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "change column %v %v", node.OldColumn, node.NewColDefinition)
	if node.First {
		buf.astPrintf(node, " first")
	}
	if node.After != nil {
		buf.astPrintf(node, " after %v", node.After)
	}
}

// Format formats the node
func (node *ModifyColumn) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "modify column %v", node.NewColDefinition)
	if node.First {
		buf.astPrintf(node, " first")
	}
	if node.After != nil {
		buf.astPrintf(node, " after %v", node.After)
	}
}

// Format formats the node
func (node *RenameColumn) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "rename column %v to %v", node.OldName, node.NewName)
}

// Format formats the node
func (node *AlterCharset) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "convert to character set %#s", node.CharacterSet)
	if node.Collate != "" {
		buf.astPrintf(node, " collate %#s", node.Collate)
	}
}

// Format formats the node
func (node *KeyState) Format(buf *TrackedBuffer) {
	if node.Enable {
		buf.literal("enable keys")
	} else {
		buf.literal("disable keys")
	}

}

// Format formats the node
func (node *TablespaceOperation) Format(buf *TrackedBuffer) {
	if node.Import {
		buf.literal("import tablespace")
	} else {
		buf.literal("discard tablespace")
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
	buf.literal("force")
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
		buf.literal("with validation")
	} else {
		buf.literal("without validation")
	}
}

// Format formats the node
func (node TableOptions) Format(buf *TrackedBuffer) {
	for i, option := range node {
		if i != 0 {
			buf.WriteByte(' ')
		}
		buf.astPrintf(node, "%s", option.Name)
		switch {
		case option.String != "":
			if option.CaseSensitive {
				buf.astPrintf(node, " %#s", option.String)
			} else {
				buf.astPrintf(node, " %s", option.String)
			}
		case option.Value != nil:
			buf.astPrintf(node, " %v", option.Value)
		default:
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

// Format formats the node.
// If an extracted subquery is still in the AST when we print it,
// it will be formatted as if the subquery has been extracted, and instead
// show up like argument comparisons
func (node *ExtractedSubquery) Format(buf *TrackedBuffer) {
	node.alternative.Format(buf)
}

func (node *JSONTableExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_table(%v, %v columns(\n", node.Expr, node.Filter)
	sz := len(node.Columns)

	for i := 0; i < sz-1; i++ {
		buf.astPrintf(node, "\t%v,\n", node.Columns[i])
	}
	buf.astPrintf(node, "\t%v\n", node.Columns[sz-1])
	buf.astPrintf(node, "\t)\n) as %v", node.Alias)
}

func (node *JtColumnDefinition) Format(buf *TrackedBuffer) {
	if node.JtOrdinal != nil {
		buf.astPrintf(node, "%v for ordinality", node.JtOrdinal.Name)
	} else if node.JtNestedPath != nil {
		buf.astPrintf(node, "nested path %v columns(\n", node.JtNestedPath.Path)
		sz := len(node.JtNestedPath.Columns)

		for i := 0; i < sz-1; i++ {
			buf.astPrintf(node, "\t%v,\n", node.JtNestedPath.Columns[i])
		}
		buf.astPrintf(node, "\t%v\n)", node.JtNestedPath.Columns[sz-1])
	} else if node.JtPath != nil {
		buf.astPrintf(node, "%v %v ", node.JtPath.Name, node.JtPath.Type)
		if node.JtPath.JtColExists {
			buf.astPrintf(node, "exists ")
		}
		buf.astPrintf(node, "path %v ", node.JtPath.Path)

		if node.JtPath.EmptyOnResponse != nil {
			buf.astPrintf(node, "%v on empty ", node.JtPath.EmptyOnResponse)
		}

		if node.JtPath.ErrorOnResponse != nil {
			buf.astPrintf(node, "%v on error ", node.JtPath.ErrorOnResponse)
		}
	}
}

func (node *JtOnResponse) Format(buf *TrackedBuffer) {
	switch node.ResponseType {
	case ErrorJSONType:
		buf.astPrintf(node, "error")
	case NullJSONType:
		buf.astPrintf(node, "null")
	case DefaultJSONType:
		buf.astPrintf(node, "default %v", node.Expr)
	}
}

// Format formats the node.
func (node *Offset) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, ":%d", node.V)
}

// Format formats the node.
func (node *JSONSchemaValidFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_schema_valid(%v, %v)", node.Schema, node.Document)
}

// Format formats the node.
func (node *JSONSchemaValidationReportFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_schema_validation_report(%v, %v)", node.Schema, node.Document)
}

// Format formats the node.
func (node *JSONArrayExpr) Format(buf *TrackedBuffer) {
	buf.literal("json_array(")
	if len(node.Params) > 0 {
		var prefix string
		for _, n := range node.Params {
			buf.astPrintf(node, "%s%v", prefix, n)
			prefix = ", "
		}
	}
	buf.WriteByte(')')
}

// Format formats the node.
func (node *JSONObjectExpr) Format(buf *TrackedBuffer) {
	buf.literal("json_object(")
	if len(node.Params) > 0 {
		for i, p := range node.Params {
			if i != 0 {
				buf.astPrintf(node, ", ")

			}
			buf.astPrintf(node, "%v", p)
		}
	}
	buf.WriteByte(')')
}

// Format formats the node.
func (node *JSONObjectParam) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v, %v", node.Key, node.Value)
}

// Format formats the node.
func (node *JSONQuoteExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_quote(%v)", node.StringArg)
}

// Format formats the node
func (node *JSONContainsExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_contains(%v, %v", node.Target, node.Candidate)
	if len(node.PathList) > 0 {
		buf.literal(", ")
	}
	var prefix string
	for _, n := range node.PathList {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *JSONContainsPathExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_contains_path(%v, %v, ", node.JSONDoc, node.OneOrAll)
	var prefix string
	for _, n := range node.PathList {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *JSONExtractExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_extract(%v, ", node.JSONDoc)
	var prefix string
	for _, n := range node.PathList {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *JSONKeysExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_keys(%v", node.JSONDoc)
	if node.Path != nil {
		buf.astPrintf(node, ", %v)", node.Path)
		return
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *JSONOverlapsExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_overlaps(%v, %v)", node.JSONDoc1, node.JSONDoc2)
}

// Format formats the node
func (node *JSONSearchExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_search(%v, %v, %v", node.JSONDoc, node.OneOrAll, node.SearchStr)
	if node.EscapeChar != nil {
		buf.astPrintf(node, ", %v", node.EscapeChar)
	}
	if len(node.PathList) > 0 {
		buf.literal(", ")
	}
	var prefix string
	for _, n := range node.PathList {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *JSONValueExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_value(%v, %v", node.JSONDoc, node.Path)

	if node.ReturningType != nil {
		buf.astPrintf(node, " returning %v", node.ReturningType)
	}

	if node.EmptyOnResponse != nil {
		buf.astPrintf(node, " %v on empty", node.EmptyOnResponse)
	}

	if node.ErrorOnResponse != nil {
		buf.astPrintf(node, " %v on error", node.ErrorOnResponse)
	}

	buf.WriteByte(')')
}

// Format formats the node
func (node *MemberOfExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%v member of (%v)", node.Value, node.JSONArr)
}

// Format formats the node
func (node *JSONAttributesExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.Type.ToString())
	buf.astPrintf(node, "%v", node.JSONDoc)
	if node.Path != nil {
		buf.astPrintf(node, ", %v", node.Path)
	}
	buf.WriteString(")")
}

// Format formats the node.
func (node *JSONValueModifierExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v, ", node.Type.ToString(), node.JSONDoc)
	var prefix string
	for _, n := range node.Params {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
	buf.WriteString(")")
}

// Format formats the node.
func (node *JSONValueMergeExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v, ", node.Type.ToString(), node.JSONDoc)
	var prefix string
	for _, n := range node.JSONDocList {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
	buf.WriteString(")")
}

// Format formats the node.
func (node *JSONRemoveExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_remove(%v, ", node.JSONDoc)
	var prefix string
	for _, n := range node.PathList {
		buf.astPrintf(node, "%s%v", prefix, n)
		prefix = ", "
	}
	buf.WriteString(")")
}

// Format formats the node.
func (node *JSONUnquoteExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "json_unquote(%v", node.JSONValue)
	buf.WriteString(")")
}

func (node *Count) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	if node.Distinct {
		buf.literal(DistinctStr)
	}
	buf.astPrintf(node, "%v)", node.Args)
}

func (node *CountStar) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	buf.WriteString("*)")
}

func (node *Avg) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	if node.Distinct {
		buf.literal(DistinctStr)
	}
	buf.astPrintf(node, "%v)", node.Arg)
}

func (node *Max) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	if node.Distinct {
		buf.literal(DistinctStr)
	}
	buf.astPrintf(node, "%v)", node.Arg)
}

func (node *Min) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	if node.Distinct {
		buf.literal(DistinctStr)
	}
	buf.astPrintf(node, "%v)", node.Arg)
}

func (node *Sum) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	if node.Distinct {
		buf.literal(DistinctStr)
	}
	buf.astPrintf(node, "%v)", node.Arg)
}

func (node *BitAnd) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	buf.astPrintf(node, "%v)", node.Arg)
}

func (node *BitOr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	buf.astPrintf(node, "%v)", node.Arg)
}

func (node *BitXor) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	buf.astPrintf(node, "%v)", node.Arg)
}

func (node *Std) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	buf.astPrintf(node, "%v)", node.Arg)
}

func (node *StdDev) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	buf.astPrintf(node, "%v)", node.Arg)
}

func (node *StdPop) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	buf.astPrintf(node, "%v)", node.Arg)
}

func (node *StdSamp) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	buf.astPrintf(node, "%v)", node.Arg)
}

func (node *VarPop) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	buf.astPrintf(node, "%v)", node.Arg)
}

func (node *VarSamp) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	buf.astPrintf(node, "%v)", node.Arg)
}

func (node *Variance) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(", node.AggrName())
	buf.astPrintf(node, "%v)", node.Arg)
}

// Format formats the node.
func (node *LockingFunc) Format(buf *TrackedBuffer) {
	buf.WriteString(node.Type.ToString() + "(")
	if node.Type != ReleaseAllLocks {
		buf.astPrintf(node, "%v", node.Name)
	}
	if node.Type == GetLock {
		buf.astPrintf(node, ", %v", node.Timeout)
	}
	buf.WriteString(")")
}

// Format formats the node.
func (node *Variable) Format(buf *TrackedBuffer) {
	switch node.Scope {
	case VariableScope:
		buf.literal("@")
	case SessionScope:
		if node.Name.EqualString(TransactionIsolationStr) || node.Name.EqualString(TransactionReadOnlyStr) {
			// @@ without session have `next transaction` scope for these system variables.
			// so if they are in session scope it has to be printed explicitly.
			buf.astPrintf(node, "@@%s.", node.Scope.ToString())
			break
		}
		buf.literal("@@")
	case GlobalScope, PersistSysScope, PersistOnlySysScope:
		buf.astPrintf(node, "@@%s.", node.Scope.ToString())
	case NextTxScope:
		buf.literal("@@")
	}
	buf.astPrintf(node, "%v", node.Name)
}

// Format formats the node.
func (node *PointExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "point(%v, %v)", node.XCordinate, node.YCordinate)
}

// Format formats the node.
func (node *LineStringExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "linestring(%v)", node.PointParams)
}

// Format formats the node.
func (node *PolygonExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "polygon(%v)", node.LinestringParams)
}

// Format formats the node.
func (node *PurgeBinaryLogs) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "purge binary logs")
	if node.To != "" {
		buf.astPrintf(node, " to '%#s'", node.To)
	} else {
		buf.astPrintf(node, " before '%#s'", node.Before)
	}
}

func (node *MultiPolygonExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "multipolygon(%v)", node.PolygonParams)
}

// Format formats the node.
func (node *MultiPointExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "multipoint(%v)", node.PointParams)
}

// Format formats the node.
func (node *MultiLinestringExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "multilinestring(%v)", node.LinestringParams)
}

// Format formats the node
func (node *GeomFromTextExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v", node.Type.ToString(), node.WktText)
	if node.Srid != nil {
		buf.astPrintf(node, ", %v", node.Srid)
	}
	if node.AxisOrderOpt != nil {
		buf.astPrintf(node, ", %v", node.AxisOrderOpt)
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *GeomFromWKBExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v", node.Type.ToString(), node.WkbBlob)
	if node.Srid != nil {
		buf.astPrintf(node, ", %v", node.Srid)
	}
	if node.AxisOrderOpt != nil {
		buf.astPrintf(node, ", %v", node.AxisOrderOpt)
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *GeomFormatExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v", node.FormatType.ToString(), node.Geom)
	if node.AxisOrderOpt != nil {
		buf.astPrintf(node, ", %v", node.AxisOrderOpt)
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *GeomPropertyFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v)", node.Property.ToString(), node.Geom)
}

// Format formats the node
func (node *PointPropertyFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v", node.Property.ToString(), node.Point)
	if node.ValueToSet != nil {
		buf.astPrintf(node, ", %v", node.ValueToSet)
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *LinestrPropertyFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v", node.Property.ToString(), node.Linestring)
	if node.PropertyDefArg != nil {
		buf.astPrintf(node, ", %v", node.PropertyDefArg)
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *PolygonPropertyFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v", node.Property.ToString(), node.Polygon)
	if node.PropertyDefArg != nil {
		buf.astPrintf(node, ", %v", node.PropertyDefArg)
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *GeomCollPropertyFuncExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v", node.Property.ToString(), node.GeomColl)
	if node.PropertyDefArg != nil {
		buf.astPrintf(node, ", %v", node.PropertyDefArg)
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *GeomFromGeoHashExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "%s(%v", node.GeomType.ToString(), node.GeoHash)
	if node.SridOpt != nil {
		buf.astPrintf(node, ", %v", node.SridOpt)
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *GeoHashFromLatLongExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "st_geohash(%v, %v, %v)", node.Longitude, node.Latitude, node.MaxLength)
}

// Format formats the node
func (node *GeoHashFromPointExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "st_geohash(%v, %v)", node.Point, node.MaxLength)
}

// Format formats the node
func (node *GeoJSONFromGeomExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "st_asgeojson(%v", node.Geom)
	if node.MaxDecimalDigits != nil {
		buf.astPrintf(node, ", %v", node.MaxDecimalDigits)
	}
	if node.Bitmask != nil {
		buf.astPrintf(node, ", %v", node.Bitmask)
	}
	buf.WriteByte(')')
}

// Format formats the node
func (node *GeomFromGeoJSONExpr) Format(buf *TrackedBuffer) {
	buf.astPrintf(node, "st_geomfromgeojson(%v", node.GeoJSON)
	if node.HigherDimHandlerOpt != nil {
		buf.astPrintf(node, ", %v", node.HigherDimHandlerOpt)
	}
	if node.Srid != nil {
		buf.astPrintf(node, ", %v", node.Srid)
	}
	buf.WriteByte(')')
}
