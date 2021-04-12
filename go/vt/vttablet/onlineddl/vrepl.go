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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconnpool"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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

	sharedPKColumns *vrepl.ColumnList

	sourceSharedColumns *vrepl.ColumnList
	targetSharedColumns *vrepl.ColumnList
	sharedColumnsMap    map[string]string

	filterQuery string
	bls         *binlogdatapb.BinlogSource

	parser *vrepl.AlterTableParser
}

// NewVRepl creates a VReplication handler for Online DDL
func NewVRepl(workflow, keyspace, shard, dbName, sourceTable, targetTable, alterOptions string) *VRepl {
	return &VRepl{
		workflow:     workflow,
		keyspace:     keyspace,
		shard:        shard,
		dbName:       dbName,
		sourceTable:  sourceTable,
		targetTable:  targetTable,
		alterOptions: alterOptions,
		parser:       vrepl.NewAlterTableParser(),
	}
}

// getCandidateUniqueKeys investigates a table and returns the list of unique keys
// candidate for chunking
func (v *VRepl) getCandidateUniqueKeys(ctx context.Context, conn *dbconnpool.DBConnection, tableName string) (uniqueKeys [](*vrepl.UniqueKey), err error) {

	query, err := sqlparser.ParseAndBind(sqlShowColumnsFrom,
		sqltypes.StringBindVariable(v.dbName),
		sqltypes.StringBindVariable(tableName),
		sqltypes.StringBindVariable(v.dbName),
		sqltypes.StringBindVariable(tableName),
	)
	if err != nil {
		return uniqueKeys, err
	}

	rs, err := conn.ExecuteFetch(query, math.MaxInt64, true)
	if err != nil {
		return nil, err
	}
	for _, row := range rs.Named().Rows {
		uniqueKey := &vrepl.UniqueKey{
			Name:            row.AsString("INDEX_NAME", ""),
			Columns:         *vrepl.ParseColumnList(row.AsString("COLUMN_NAMES", "")),
			HasNullable:     row.AsBool("has_nullable", false),
			IsAutoIncrement: row.AsBool("is_auto_increment", false),
		}
		uniqueKeys = append(uniqueKeys, uniqueKey)
	}
	return uniqueKeys, nil
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
		if strings.Contains(extra, " GENERATED") {
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

// getSharedPKColumns returns the intersection of PRIMARY KEY columns (taking renaming into consideration) between source and target tables
func (v *VRepl) getSharedPKColumns(sourcePKColumns, targetPKColumns *vrepl.ColumnList, columnRenameMap map[string]string) (
	sharedPKColumns *vrepl.ColumnList,
) {
	sharedColumnNames := []string{}
	for _, sourceColumn := range sourcePKColumns.Names() {
		isSharedColumn := false
		for _, targetColumn := range targetPKColumns.Names() {
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
		if isSharedColumn {
			sharedColumnNames = append(sharedColumnNames, sourceColumn)
		}
	}
	return vrepl.NewColumnList(sharedColumnNames)
}

// getSharedUniqueKeys returns the intersection of two given unique keys,
// testing by list of columns
func (v *VRepl) getSharedUniqueKeys(sourceUniqueKeys, targetUniqueKeys [](*vrepl.UniqueKey)) (uniqueKeys [](*vrepl.UniqueKey), err error) {
	// We actually do NOT rely on key name, just on the set of columns. This is because maybe
	// the ALTER is on the name itself...
	for _, sourceUniqueKey := range sourceUniqueKeys {
		for _, targetUniqueKey := range targetUniqueKeys {
			if sourceUniqueKey.Columns.EqualsByNames(&targetUniqueKey.Columns) {
				uniqueKeys = append(uniqueKeys, sourceUniqueKey)
			}
		}
	}
	return uniqueKeys, nil
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

func (v *VRepl) analyzeTables(ctx context.Context, conn *dbconnpool.DBConnection) error {
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

	v.sharedPKColumns = v.getSharedPKColumns(sourcePKColumns, targetPKColumns, v.parser.ColumnRenameMap())
	if v.sharedPKColumns.Len() == 0 {
		// TODO(shlomi): need to carefully examine what happens when we extend/reduce a PRIMARY KEY
		// is a column subset OK?
		return fmt.Errorf("Found no shared PRIMARY KEY columns between `%s` and `%s`", v.sourceTable, v.targetTable)
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
	for i, name := range v.sourceSharedColumns.Names() {
		targetName := v.sharedColumnsMap[name]

		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(escapeName(name))
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
	rule := &binlogdatapb.Rule{
		Match:  v.targetTable,
		Filter: v.filterQuery,
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
	ig.AddRow(v.workflow, v.bls, v.pos, "", "MASTER")

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
