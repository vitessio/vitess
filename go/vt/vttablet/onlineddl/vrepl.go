/*
	Original copyright by GitHub as follows. Additions by the Vitess authors as follows.
*/
/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/
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

package onlineddl

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconnpool"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/onlineddl/vrepl"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

// VReplStream represents a row in _vt.vreplication table
type VReplStream struct {
	id                   int64
	workflow             string
	source               string
	pos                  string
	timeUpdated          int64
	transactionTimestamp int64
	state                string
	message              string
	rowsCopied           int64
	bls                  *binlogdatapb.BinlogSource
}

// VRepl is an online DDL helper for VReplication based migrations (ddl_strategy="online")
type VRepl struct {
	workflow     string
	keyspace     string
	shard        string
	dbName       string
	sourceTable  string
	targetTable  string
	pos          string
	alterOptions string
	tableRows    int64

	sourceSharedColumns *vrepl.ColumnList
	targetSharedColumns *vrepl.ColumnList
	sharedColumnsMap    map[string]string
	sourceAutoIncrement uint64

	chosenSourceUniqueKey *vrepl.UniqueKey
	chosenTargetUniqueKey *vrepl.UniqueKey

	filterQuery   string
	enumToTextMap map[string]string
	bls           *binlogdatapb.BinlogSource

	parser *vrepl.AlterTableParser

	convertCharset map[string](*binlogdatapb.CharsetConversion)
}

// NewVRepl creates a VReplication handler for Online DDL
func NewVRepl(workflow, keyspace, shard, dbName, sourceTable, targetTable, alterOptions string) *VRepl {
	return &VRepl{
		workflow:       workflow,
		keyspace:       keyspace,
		shard:          shard,
		dbName:         dbName,
		sourceTable:    sourceTable,
		targetTable:    targetTable,
		alterOptions:   alterOptions,
		parser:         vrepl.NewAlterTableParser(),
		enumToTextMap:  map[string]string{},
		convertCharset: map[string](*binlogdatapb.CharsetConversion){},
	}
}

// getSharedUniqueKeys returns the unique keys shared between the two source&target tables
func getSharedUniqueKeys(sourceUniqueKeys, targetUniqueKeys [](*vrepl.UniqueKey), columnRenameMap map[string]string) (chosenSourceUniqueKey, chosenTargetUniqueKey *vrepl.UniqueKey) {
	type ukPair struct{ source, target *vrepl.UniqueKey }
	var sharedUKPairs []*ukPair
	for _, sourceUniqueKey := range sourceUniqueKeys {
		for _, targetUniqueKey := range targetUniqueKeys {
			uniqueKeyMatches := func() bool {
				// Compare two unique keys
				if sourceUniqueKey.Columns.Len() != targetUniqueKey.Columns.Len() {
					return false
				}
				// Expect same columns, same order, potentially column name mapping
				sourceUniqueKeyNames := sourceUniqueKey.Columns.Names()
				targetUniqueKeyNames := targetUniqueKey.Columns.Names()
				for i := range sourceUniqueKeyNames {
					sourceColumnName := sourceUniqueKeyNames[i]
					targetColumnName := targetUniqueKeyNames[i]
					mappedSourceColumnName := sourceColumnName
					if mapped, ok := columnRenameMap[sourceColumnName]; ok {
						mappedSourceColumnName = mapped
					}
					if !strings.EqualFold(mappedSourceColumnName, targetColumnName) {
						return false
					}
				}
				return true
			}
			if uniqueKeyMatches() {
				sharedUKPairs = append(sharedUKPairs, &ukPair{source: sourceUniqueKey, target: targetUniqueKey})
			}
		}
	}
	// Now that we know what the shared unique keys are, let's find the "best" shared one.
	// Source and target unique keys can have different name, even though they cover the exact same
	// columns and in same order.
	for _, pair := range sharedUKPairs {
		if pair.source.HasNullable {
			continue
		}
		if pair.target.HasNullable {
			continue
		}
		return pair.source, pair.target
	}
	return nil, nil
}

// getUniqueKeyCoveredByColumns returns the first unique key from given list, whose columns all appear
// in given column list.
func getUniqueKeyCoveredByColumns(uniqueKeys [](*vrepl.UniqueKey), columns *vrepl.ColumnList) (chosenUniqueKey *vrepl.UniqueKey) {
	for _, uniqueKey := range uniqueKeys {
		if uniqueKey.Columns.IsSubsetOf(columns) {
			return uniqueKey
		}
	}
	return nil
}

// readAutoIncrement reads the AUTO_INCREMENT vlaue, if any, for a give ntable
func (v *VRepl) readAutoIncrement(ctx context.Context, conn *dbconnpool.DBConnection, tableName string) (autoIncrement uint64, err error) {
	query, err := sqlparser.ParseAndBind(sqlGetAutoIncrement,
		sqltypes.StringBindVariable(v.dbName),
		sqltypes.StringBindVariable(tableName),
	)
	if err != nil {
		return 0, err
	}

	rs, err := conn.ExecuteFetch(query, math.MaxInt64, true)
	if err != nil {
		return 0, err
	}
	for _, row := range rs.Named().Rows {
		autoIncrement = row.AsUint64("AUTO_INCREMENT", 0)
	}

	return autoIncrement, nil
}

// readTableColumns reads column list from given table
func (v *VRepl) readTableColumns(ctx context.Context, conn *dbconnpool.DBConnection, tableName string) (columns *vrepl.ColumnList, virtualColumns *vrepl.ColumnList, pkColumns *vrepl.ColumnList, err error) {
	parsed := sqlparser.BuildParsedQuery(sqlShowColumnsFrom, tableName)
	rs, err := conn.ExecuteFetch(parsed.Query, math.MaxInt64, true)
	if err != nil {
		return nil, nil, nil, err
	}
	columnNames := []string{}
	virtualColumnNames := []string{}
	pkColumnNames := []string{}
	for _, row := range rs.Named().Rows {
		columnName := row.AsString("Field", "")
		columnNames = append(columnNames, columnName)

		extra := row.AsString("Extra", "")
		if strings.Contains(extra, "VIRTUAL") || strings.Contains(extra, "GENERATED") {
			virtualColumnNames = append(virtualColumnNames, columnName)
		}

		key := row.AsString("Key", "")
		if key == "PRI" {
			pkColumnNames = append(pkColumnNames, columnName)
		}
	}
	if len(columnNames) == 0 {
		return nil, nil, nil, fmt.Errorf("Found 0 columns on `%s`", tableName)
	}
	return vrepl.NewColumnList(columnNames), vrepl.NewColumnList(virtualColumnNames), vrepl.NewColumnList(pkColumnNames), nil
}

// readTableUniqueKeys reads all unique keys from a given table, by order of usefulness/performance: PRIMARY first, integers are better, non-null are better
func (v *VRepl) readTableUniqueKeys(ctx context.Context, conn *dbconnpool.DBConnection, tableName string) (uniqueKeys []*vrepl.UniqueKey, err error) {
	query, err := sqlparser.ParseAndBind(sqlSelectUniqueKeys,
		sqltypes.StringBindVariable(v.dbName),
		sqltypes.StringBindVariable(tableName),
		sqltypes.StringBindVariable(v.dbName),
		sqltypes.StringBindVariable(tableName),
	)
	if err != nil {
		return nil, err
	}
	rs, err := conn.ExecuteFetch(query, math.MaxInt64, true)
	if err != nil {
		return nil, err
	}
	for _, row := range rs.Named().Rows {
		if row.AsBool("is_float", false) {
			// float & double data types are imprecise and we cannot use them while iterating unique keys
			continue
		}
		if row.AsBool("has_nullable", false) {
			// NULLable columns in a unique key means the set of values is not really unique (two identical rows with NULLs are allowed).
			// Thus, we cannot use this unique key for iteration.
			continue
		}

		uniqueKey := &vrepl.UniqueKey{
			Name:            row.AsString("index_name", ""),
			Columns:         *vrepl.ParseColumnList(row.AsString("column_names", "")),
			HasNullable:     row.AsBool("has_nullable", false),
			IsAutoIncrement: row.AsBool("is_auto_increment", false),
		}
		uniqueKeys = append(uniqueKeys, uniqueKey)
	}
	return uniqueKeys, nil
}

// readTableStatus reads table status information
func (v *VRepl) readTableStatus(ctx context.Context, conn *dbconnpool.DBConnection, tableName string) (tableRows int64, err error) {
	parsed := sqlparser.BuildParsedQuery(sqlShowTableStatus, tableName)
	rs, err := conn.ExecuteFetch(parsed.Query, math.MaxInt64, true)
	if err != nil {
		return 0, err
	}
	row := rs.Named().Row()
	if row == nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Cannot SHOW TABLE STATUS LIKE '%s'", tableName)
	}
	tableRows, err = row.ToInt64("Rows")
	return tableRows, err
}

// applyColumnTypes
func (v *VRepl) applyColumnTypes(ctx context.Context, conn *dbconnpool.DBConnection, tableName string, columnsLists ...*vrepl.ColumnList) error {
	query, err := sqlparser.ParseAndBind(sqlSelectColumnTypes,
		sqltypes.StringBindVariable(v.dbName),
		sqltypes.StringBindVariable(tableName),
	)
	if err != nil {
		return err
	}
	rs, err := conn.ExecuteFetch(query, math.MaxInt64, true)
	if err != nil {
		return err
	}
	for _, row := range rs.Named().Rows {
		columnName := row["COLUMN_NAME"].ToString()
		columnType := row["COLUMN_TYPE"].ToString()
		columnOctetLength := row.AsUint64("CHARACTER_OCTET_LENGTH", 0)

		for _, columnsList := range columnsLists {
			column := columnsList.GetColumn(columnName)
			if column == nil {
				continue
			}

			column.Type = vrepl.UnknownColumnType
			if strings.Contains(columnType, "unsigned") {
				column.IsUnsigned = true
			}
			if strings.Contains(columnType, "mediumint") {
				column.SetTypeIfUnknown(vrepl.MediumIntColumnType)
			}
			if strings.Contains(columnType, "timestamp") {
				column.SetTypeIfUnknown(vrepl.TimestampColumnType)
			}
			if strings.Contains(columnType, "datetime") {
				column.SetTypeIfUnknown(vrepl.DateTimeColumnType)
			}
			if strings.Contains(columnType, "json") {
				column.SetTypeIfUnknown(vrepl.JSONColumnType)
			}
			if strings.Contains(columnType, "float") {
				column.SetTypeIfUnknown(vrepl.FloatColumnType)
			}
			if strings.HasPrefix(columnType, "enum") {
				column.SetTypeIfUnknown(vrepl.EnumColumnType)
				column.EnumValues = schema.ParseEnumValues(columnType)
			}
			if strings.HasPrefix(columnType, "binary") {
				column.SetTypeIfUnknown(vrepl.BinaryColumnType)
				column.BinaryOctetLength = columnOctetLength
			}
			if charset := row.AsString("CHARACTER_SET_NAME", ""); charset != "" {
				column.Charset = charset
			}
			if collation := row.AsString("COLLATION_NAME", ""); collation != "" {
				column.SetTypeIfUnknown(vrepl.StringColumnType)
				column.Collation = collation
			}
		}
	}
	return nil
}

// getSharedColumns returns the intersection of two lists of columns in same order as the first list
func (v *VRepl) getSharedColumns(sourceColumns, targetColumns *vrepl.ColumnList, sourceVirtualColumns, targetVirtualColumns *vrepl.ColumnList, columnRenameMap map[string]string) (
	sourceSharedColumns *vrepl.ColumnList, targetSharedColumns *vrepl.ColumnList, sharedColumnsMap map[string]string,
) {
	sharedColumnNames := []string{}
	for _, sourceColumn := range sourceColumns.Names() {
		isSharedColumn := false
		for _, targetColumn := range targetColumns.Names() {
			if strings.EqualFold(sourceColumn, targetColumn) {
				// both tables have this column. Good start.
				isSharedColumn = true
				break
			}
			if strings.EqualFold(columnRenameMap[sourceColumn], targetColumn) {
				// column in source is renamed in target
				isSharedColumn = true
				break
			}
		}
		for droppedColumn := range v.parser.DroppedColumnsMap() {
			if strings.EqualFold(sourceColumn, droppedColumn) {
				isSharedColumn = false
				break
			}
		}
		for _, virtualColumn := range sourceVirtualColumns.Names() {
			// virtual/generated columns on source are silently skipped
			if strings.EqualFold(sourceColumn, virtualColumn) {
				isSharedColumn = false
			}
		}
		for _, virtualColumn := range targetVirtualColumns.Names() {
			// virtual/generated columns on target are silently skipped
			if strings.EqualFold(sourceColumn, virtualColumn) {
				isSharedColumn = false
			}
		}
		if isSharedColumn {
			sharedColumnNames = append(sharedColumnNames, sourceColumn)
		}
	}
	sharedColumnsMap = map[string]string{}
	for _, columnName := range sharedColumnNames {
		if mapped, ok := columnRenameMap[columnName]; ok {
			sharedColumnsMap[columnName] = mapped
		} else {
			sharedColumnsMap[columnName] = columnName
		}
	}
	mappedSharedColumnNames := []string{}
	for _, columnName := range sharedColumnNames {
		mappedSharedColumnNames = append(mappedSharedColumnNames, sharedColumnsMap[columnName])
	}
	return vrepl.NewColumnList(sharedColumnNames), vrepl.NewColumnList(mappedSharedColumnNames), sharedColumnsMap
}

func (v *VRepl) analyzeAlter(ctx context.Context) error {
	if err := v.parser.ParseAlterStatement(v.alterOptions); err != nil {
		return err
	}
	if v.parser.IsRenameTable() {
		return fmt.Errorf("Renaming the table is not aupported in ALTER TABLE: %s", v.alterOptions)
	}
	return nil
}

func (v *VRepl) analyzeTables(ctx context.Context, conn *dbconnpool.DBConnection) (err error) {
	v.tableRows, err = v.readTableStatus(ctx, conn, v.sourceTable)
	if err != nil {
		return err
	}
	// columns:
	sourceColumns, sourceVirtualColumns, sourcePKColumns, err := v.readTableColumns(ctx, conn, v.sourceTable)
	if err != nil {
		return err
	}
	targetColumns, targetVirtualColumns, targetPKColumns, err := v.readTableColumns(ctx, conn, v.targetTable)
	if err != nil {
		return err
	}
	v.sourceSharedColumns, v.targetSharedColumns, v.sharedColumnsMap = v.getSharedColumns(sourceColumns, targetColumns, sourceVirtualColumns, targetVirtualColumns, v.parser.ColumnRenameMap())

	// unique keys
	sourceUniqueKeys, err := v.readTableUniqueKeys(ctx, conn, v.sourceTable)
	if err != nil {
		return err
	}
	if len(sourceUniqueKeys) == 0 {
		return fmt.Errorf("Found no possible unique key on `%s`", v.sourceTable)
	}
	targetUniqueKeys, err := v.readTableUniqueKeys(ctx, conn, v.targetTable)
	if err != nil {
		return err
	}
	if len(targetUniqueKeys) == 0 {
		return fmt.Errorf("Found no possible unique key on `%s`", v.targetTable)
	}
	v.chosenSourceUniqueKey, v.chosenTargetUniqueKey = getSharedUniqueKeys(sourceUniqueKeys, targetUniqueKeys, v.parser.ColumnRenameMap())
	if v.chosenSourceUniqueKey == nil {
		// VReplication supports completely different unique keys on source and target, covering
		// some/completely different columns. The condition is that the key on source
		// must use columns which all exist on target table.
		v.chosenSourceUniqueKey = getUniqueKeyCoveredByColumns(sourceUniqueKeys, v.sourceSharedColumns)
		if v.chosenSourceUniqueKey == nil {
			// Still no luck.
			return fmt.Errorf("Found no possible unique key on `%s` whose columns are in target table `%s`", v.sourceTable, v.targetTable)
		}
	}
	if v.chosenTargetUniqueKey == nil {
		// VReplication supports completely different unique keys on source and target, covering
		// some/completely different columns. The condition is that the key on target
		// must use columns which all exist on source table.
		v.chosenTargetUniqueKey = getUniqueKeyCoveredByColumns(targetUniqueKeys, v.targetSharedColumns)
		if v.chosenTargetUniqueKey == nil {
			// Still no luck.
			return fmt.Errorf("Found no possible unique key on `%s` whose columns are in source table `%s`", v.targetTable, v.sourceTable)
		}
	}
	if v.chosenSourceUniqueKey == nil || v.chosenTargetUniqueKey == nil {
		return fmt.Errorf("Found no shared, not nullable, unique keys between `%s` and `%s`", v.sourceTable, v.targetTable)
	}
	// chosen source & target unique keys have exact columns in same order
	sharedPKColumns := &v.chosenSourceUniqueKey.Columns

	if err := v.applyColumnTypes(ctx, conn, v.sourceTable, sourceColumns, sourceVirtualColumns, sourcePKColumns, v.sourceSharedColumns, sharedPKColumns); err != nil {
		return err
	}
	if err := v.applyColumnTypes(ctx, conn, v.targetTable, targetColumns, targetVirtualColumns, targetPKColumns, v.targetSharedColumns); err != nil {
		return err
	}

	for _, sourcePKColumn := range sharedPKColumns.Columns() {
		mappedColumn := v.targetSharedColumns.GetColumn(sourcePKColumn.Name)
		if sourcePKColumn.Type == vrepl.EnumColumnType && mappedColumn.Type == vrepl.EnumColumnType {
			// An ENUM as part of PRIMARY KEY. We must convert it to text because OMG that's complicated.
			// There's a scenario where a query may modify the enum value (and it's bad practice, seeing
			// that it's part of the PK, but it's still valid), and in that case we must have the string value
			// to be able to DELETE the old row
			v.targetSharedColumns.SetEnumToTextConversion(mappedColumn.Name, sourcePKColumn.EnumValues)
			v.enumToTextMap[sourcePKColumn.Name] = sourcePKColumn.EnumValues
		}
	}

	for i := range v.sourceSharedColumns.Columns() {
		sourceColumn := v.sourceSharedColumns.Columns()[i]
		mappedColumn := v.targetSharedColumns.Columns()[i]
		if sourceColumn.Type == vrepl.EnumColumnType && mappedColumn.Type != vrepl.EnumColumnType && mappedColumn.Charset != "" {
			// A column is converted from ENUM type to textual type
			v.targetSharedColumns.SetEnumToTextConversion(mappedColumn.Name, sourceColumn.EnumValues)
			v.enumToTextMap[sourceColumn.Name] = sourceColumn.EnumValues
		}
	}

	v.sourceAutoIncrement, err = v.readAutoIncrement(ctx, conn, v.sourceTable)
	if err != nil {
		return err
	}

	return nil
}

// generateFilterQuery creates a SELECT query used by vreplication as a filter. It SELECTs all
// non-generated columns between source & target tables, and takes care of column renames.
func (v *VRepl) generateFilterQuery(ctx context.Context) error {
	if v.sourceSharedColumns.Len() == 0 {
		return fmt.Errorf("Empty column list")
	}
	var sb strings.Builder
	sb.WriteString("select ")

	for i, sourceCol := range v.sourceSharedColumns.Columns() {
		name := sourceCol.Name
		targetName := v.sharedColumnsMap[name]

		if i > 0 {
			sb.WriteString(", ")
		}
		switch {
		case sourceCol.EnumToTextConversion:
			sb.WriteString(fmt.Sprintf("CONCAT(%s)", escapeName(name)))
		case sourceCol.Type == vrepl.JSONColumnType:
			sb.WriteString(fmt.Sprintf("convert(%s using utf8mb4)", escapeName(name)))
		case sourceCol.Type == vrepl.StringColumnType:
			targetCol := v.targetSharedColumns.GetColumn(targetName)
			if targetCol == nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Cannot find target column %s", targetName)
			}
			// Check source and target charset/encoding. If needed, create
			// a binlogdatapb.CharsetConversion entry (later written to vreplication)
			fromEncoding, ok := mysql.CharacterSetEncoding[sourceCol.Charset]
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Character set %s not supported for column %s", sourceCol.Charset, sourceCol.Name)
			}
			toEncoding, ok := mysql.CharacterSetEncoding[targetCol.Charset]
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Character set %s not supported for column %s", targetCol.Charset, targetCol.Name)
			}
			if fromEncoding == nil && toEncoding == nil {
				// Both source and target have trivial charsets
				sb.WriteString(escapeName(name))
			} else {
				// encoding can be nil for trivial charsets, like utf8, ascii, binary, etc.
				v.convertCharset[targetName] = &binlogdatapb.CharsetConversion{
					FromCharset: sourceCol.Charset,
					ToCharset:   targetCol.Charset,
				}
				sb.WriteString(fmt.Sprintf("convert(%s using utf8mb4)", escapeName(name)))
			}
		default:
			sb.WriteString(escapeName(name))
		}
		sb.WriteString(" as ")
		sb.WriteString(escapeName(targetName))
	}
	sb.WriteString(" from ")
	sb.WriteString(escapeName(v.sourceTable))

	v.filterQuery = sb.String()
	return nil
}

func (v *VRepl) analyzeBinlogSource(ctx context.Context) {
	bls := &binlogdatapb.BinlogSource{
		Keyspace:      v.keyspace,
		Shard:         v.shard,
		Filter:        &binlogdatapb.Filter{},
		StopAfterCopy: false,
	}

	encodeColumns := func(columns *vrepl.ColumnList) string {
		return textutil.EscapeJoin(columns.Names(), ",")
	}
	rule := &binlogdatapb.Rule{
		Match:                        v.targetTable,
		Filter:                       v.filterQuery,
		SourceUniqueKeyColumns:       encodeColumns(&v.chosenSourceUniqueKey.Columns),
		TargetUniqueKeyColumns:       encodeColumns(&v.chosenTargetUniqueKey.Columns),
		SourceUniqueKeyTargetColumns: encodeColumns(v.chosenSourceUniqueKey.Columns.MappedNamesColumnList(v.sharedColumnsMap)),
	}
	if len(v.convertCharset) > 0 {
		rule.ConvertCharset = v.convertCharset
	}
	if len(v.enumToTextMap) > 0 {
		rule.ConvertEnumToText = v.enumToTextMap
	}

	bls.Filter.Rules = append(bls.Filter.Rules, rule)
	v.bls = bls
}

func (v *VRepl) analyze(ctx context.Context, conn *dbconnpool.DBConnection) error {
	if err := v.analyzeAlter(ctx); err != nil {
		return err
	}
	if err := v.analyzeTables(ctx, conn); err != nil {
		return err
	}
	if err := v.generateFilterQuery(ctx); err != nil {
		return err
	}
	v.analyzeBinlogSource(ctx)
	return nil
}

// generateInsertStatement generates the INSERT INTO _vt.replication stataement that creates the vreplication workflow
func (v *VRepl) generateInsertStatement(ctx context.Context) (string, error) {
	ig := vreplication.NewInsertGenerator(binlogplayer.BlpStopped, v.dbName)
	ig.AddRow(v.workflow, v.bls, v.pos, "", "in_order:REPLICA,MASTER")

	return ig.String(), nil
}

// generateStartStatement Generates the statement to start VReplication running on the workflow
func (v *VRepl) generateStartStatement(ctx context.Context) (string, error) {
	return sqlparser.ParseAndBind(sqlStartVReplStream,
		sqltypes.StringBindVariable(v.dbName),
		sqltypes.StringBindVariable(v.workflow),
	)
}

func getVreplTable(ctx context.Context, s *VReplStream) (string, error) {
	// sanity checks:
	if s == nil {
		return "", vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "No vreplication stream migration %s", s.workflow)
	}
	if s.bls.Filter == nil {
		return "", vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "No binlog source filter for migration %s", s.workflow)
	}
	if len(s.bls.Filter.Rules) != 1 {
		return "", vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Cannot detect filter rules for migration/vreplication %+v", s.workflow)
	}
	vreplTable := s.bls.Filter.Rules[0].Match
	return vreplTable, nil
}

// escapeName will escape a db/table/column/... name by wrapping with backticks.
// It is not fool proof. I'm just trying to do the right thing here, not solving
// SQL injection issues, which should be irrelevant for this tool.
func escapeName(name string) string {
	if unquoted, err := strconv.Unquote(name); err == nil {
		name = unquoted
	}
	return fmt.Sprintf("`%s`", name)
}
