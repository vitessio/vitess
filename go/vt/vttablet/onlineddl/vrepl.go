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
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/onlineddl/vrepl"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// VReplStream represents a row in _vt.vreplication table
type VReplStream struct {
	id                   int32
	workflow             string
	source               string
	pos                  string
	timeUpdated          int64
	timeHeartbeat        int64
	timeThrottled        int64
	componentThrottled   string
	transactionTimestamp int64
	state                binlogdatapb.VReplicationWorkflowState
	message              string
	rowsCopied           int64
	bls                  *binlogdatapb.BinlogSource
}

// livenessTimeIndicator returns a time indicator for last known healthy state.
// vreplication uses three indicators:
// - transaction_timestamp
// - time_heartbeat
// - time_throttled.
// Updating any of them, also updates time_updated, indicating liveness.
func (v *VReplStream) livenessTimeIndicator() int64 {
	return v.timeUpdated
}

// isRunning() returns true when the workflow is actively running
func (v *VReplStream) isRunning() bool {
	switch v.state {
	case binlogdatapb.VReplicationWorkflowState_Init, binlogdatapb.VReplicationWorkflowState_Copying, binlogdatapb.VReplicationWorkflowState_Running:
		return true
	}
	return false
}

// hasError() returns true when the workflow has failed and will not retry
func (v *VReplStream) hasError() (isTerminal bool, vreplError error) {
	switch {
	case v.state == binlogdatapb.VReplicationWorkflowState_Error:
		return true, errors.New(v.message)
	case strings.Contains(strings.ToLower(v.message), "error"):
		return false, errors.New(v.message)
	}
	return false, nil
}

// VRepl is an online DDL helper for VReplication based migrations (ddl_strategy="online")
type VRepl struct {
	workflow   string
	keyspace   string
	shard      string
	dbName     string
	pos        string
	alterQuery *sqlparser.AlterTable
	tableRows  int64

	sourceCreateTable *sqlparser.CreateTable
	targetCreateTable *sqlparser.CreateTable

	analyzeTable bool

	sourceSharedColumns              *vrepl.ColumnList
	targetSharedColumns              *vrepl.ColumnList
	droppedSourceNonGeneratedColumns *vrepl.ColumnList
	droppedNoDefaultColumnNames      []string
	expandedColumnNames              []string
	sharedColumnsMap                 map[string]string
	sourceAutoIncrement              uint64

	chosenSourceUniqueKey *vrepl.UniqueKey
	chosenTargetUniqueKey *vrepl.UniqueKey

	addedUniqueKeys        []*vrepl.UniqueKey
	removedUniqueKeys      []*vrepl.UniqueKey
	removedForeignKeyNames []string

	revertibleNotes string
	filterQuery     string
	intToEnumMap    map[string]bool
	bls             *binlogdatapb.BinlogSource

	parser *vrepl.AlterTableParser

	convertCharset map[string](*binlogdatapb.CharsetConversion)

	env *vtenv.Environment
}

// NewVRepl creates a VReplication handler for Online DDL
func NewVRepl(
	env *vtenv.Environment,
	workflow string,
	keyspace string,
	shard string,
	dbName string,
	sourceCreateTable *sqlparser.CreateTable,
	targetCreateTable *sqlparser.CreateTable,
	alterQuery *sqlparser.AlterTable,
	analyzeTable bool,
) *VRepl {
	return &VRepl{
		env:               env,
		workflow:          workflow,
		keyspace:          keyspace,
		shard:             shard,
		dbName:            dbName,
		sourceCreateTable: sourceCreateTable,
		targetCreateTable: targetCreateTable,
		alterQuery:        alterQuery,
		analyzeTable:      analyzeTable,
		parser:            vrepl.NewAlterTableParser(),
		intToEnumMap:      map[string]bool{},
		convertCharset:    map[string](*binlogdatapb.CharsetConversion){},
	}
}

// readTableColumns reads column list from given table
func readTableColumns(createTableEntity *schemadiff.CreateTableEntity) (
	columns *vrepl.ColumnList,
	virtualColumns *vrepl.ColumnList,
	pkColumns *vrepl.ColumnList,
	err error,
) {
	columnNames := []string{}
	virtualColumnNames := []string{}
	pkColumnNames := []string{}

	for _, col := range createTableEntity.ColumnDefinitionEntities() {
		columnName := col.Name()
		columnNames = append(columnNames, columnName)
		if isGenerated, _ := col.IsGenerated(); isGenerated {
			virtualColumnNames = append(virtualColumnNames, columnName)
		}
	}
	if len(columnNames) == 0 {
		return nil, nil, nil, fmt.Errorf("found 0 columns on `%s`", createTableEntity.Name())
	}
	for _, key := range createTableEntity.CreateTable.TableSpec.Indexes {
		if key.Info.Type == sqlparser.IndexTypePrimary {
			for _, col := range key.Columns {
				pkColumnNames = append(pkColumnNames, col.Column.String())
			}
		}
	}
	return vrepl.NewColumnList(columnNames), vrepl.NewColumnList(virtualColumnNames), vrepl.NewColumnList(pkColumnNames), nil
}

func (v *VRepl) sourceTableName() string {
	return v.sourceCreateTable.Table.Name.String()
}

func (v *VRepl) targetTableName() string {
	return v.targetCreateTable.Table.Name.String()
}

// readTableUniqueKeys reads all unique keys from a given table, by order of usefulness/performance: PRIMARY first, integers are better, non-null are better
func (v *VRepl) readTableUniqueKeys(ctx context.Context, createTableEntity *schemadiff.CreateTableEntity) (uniqueKeys []*vrepl.UniqueKey, err error) {
	m := createTableEntity.ColumnDefinitionEntitiesMap()
	keys := createTableEntity.CreateTable.TableSpec.Indexes
	keysMap := map[string]*sqlparser.IndexDefinition{}
	for _, key := range keys {
		keysMap[key.Info.Name.String()] = key
	}
	for _, key := range keys {
		func() {
			if !key.Info.IsUnique() {
				return
			}
			hasNullable := false
			columnNames := []string{}
			for _, col := range key.Columns {
				columnNames = append(columnNames, col.Column.String())
				colName := col.Column.Lowered()
				if m[colName].IsNullable() {
					hasNullable = true
				}
				// Conditions to make this unique key non-eligible:
				if col.Length != nil {
					// e.g. KEY (name(7))
					return
				}
				if m[colName].IsFloatingPointType() {
					return
				}
			}
			// OK, this unique key is good to go!
			uniqueKey := &vrepl.UniqueKey{
				Name:        key.Info.Name.String(),
				Columns:     *vrepl.NewColumnList(columnNames),
				HasNullable: hasNullable,
			}
			uniqueKeys = append(uniqueKeys, uniqueKey)
		}()
	}
	sort.Slice(uniqueKeys, func(i, j int) bool {
		// PRIMARY is always first
		if uniqueKeys[i].Name == "PRIMARY" {
			return true
		}
		if uniqueKeys[j].HasNullable {
			return true
		}
		iKey := keysMap[uniqueKeys[i].Name]
		jKey := keysMap[uniqueKeys[j].Name]
		iFirstColName := iKey.Columns[0].Column.Lowered()
		jFirstColName := jKey.Columns[0].Column.Lowered()
		iFirstCol := m[iFirstColName]
		jFirstCol := m[jFirstColName]
		if iFirstCol.IsIntegralType() && !jFirstCol.IsIntegralType() {
			return true
		}
		if jFirstCol.HasBlobTypeStorage() {
			return true
		}
		if !iFirstCol.IsTextual() && jFirstCol.IsTextual() {
			return true
		}
		if schemadiff.IntegralTypeStorage(iFirstCol.Type()) < schemadiff.IntegralTypeStorage(jFirstCol.Type()) {
			return true
		}
		return false
	})
	return uniqueKeys, nil
}

// isFastAnalyzeTableSupported checks if the underlying MySQL server supports 'fast_analyze_table',
// introduced by a fork of MySQL: https://github.com/planetscale/mysql-server/commit/c8a9d93686358dabfba8f3dc5cc0621e3149fe78
// When `fast_analyze_table=1`, an `ANALYZE TABLE` command only analyzes the clustering index (normally the `PRIMARY KEY`).
// This is useful when you want to get a better estimate of the number of table rows, as fast as possible.
func (v *VRepl) isFastAnalyzeTableSupported(ctx context.Context, conn *dbconnpool.DBConnection) (isSupported bool, err error) {
	rs, err := conn.ExecuteFetch(sqlShowVariablesLikeFastAnalyzeTable, -1, true)
	if err != nil {
		return false, err
	}
	return len(rs.Rows) > 0, nil
}

// executeAnalyzeTable runs an ANALYZE TABLE command
func (v *VRepl) executeAnalyzeTable(ctx context.Context, conn *dbconnpool.DBConnection, tableName string) error {
	fastAnalyzeTableSupported, err := v.isFastAnalyzeTableSupported(ctx, conn)
	if err != nil {
		return err
	}
	if fastAnalyzeTableSupported {
		// This code is only applicable when MySQL supports the 'fast_analyze_table' variable. This variable
		// does not exist in vanilla MySQL.
		// See  https://github.com/planetscale/mysql-server/commit/c8a9d93686358dabfba8f3dc5cc0621e3149fe78
		// as part of https://github.com/planetscale/mysql-server/releases/tag/8.0.34-ps1.
		if _, err := conn.ExecuteFetch(sqlEnableFastAnalyzeTable, 1, false); err != nil {
			return err
		}
		log.Infof("@@fast_analyze_table enabled")
		defer conn.ExecuteFetch(sqlDisableFastAnalyzeTable, 1, false)
	}

	parsed := sqlparser.BuildParsedQuery(sqlAnalyzeTable, tableName)
	if _, err := conn.ExecuteFetch(parsed.Query, 1, false); err != nil {
		return err
	}
	return nil
}

// readTableStatus reads table status information
func (v *VRepl) readTableStatus(ctx context.Context, conn *dbconnpool.DBConnection, tableName string) (tableRows int64, err error) {
	parsed := sqlparser.BuildParsedQuery(sqlShowTableStatus, tableName)
	rs, err := conn.ExecuteFetch(parsed.Query, -1, true)
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
func (v *VRepl) applyColumnTypes(createTableEntity *schemadiff.CreateTableEntity, columnsLists ...*vrepl.ColumnList) error {
	for _, col := range createTableEntity.ColumnDefinitionEntities() {
		col = col.Clone()
		col.SetExplicitDefaultAndNull()
		if err := col.SetExplicitCharsetCollate(); err != nil {
			return err
		}
		for _, columnsList := range columnsLists {
			column := columnsList.GetColumn(col.Name())
			if column == nil {
				column = columnsList.GetColumn(col.NameLowered())
			}
			if column == nil {
				continue
			}
			column.Entity = col
		}
	}
	return nil
}

func (v *VRepl) analyzeAlter(ctx context.Context) error {
	if v.alterQuery == nil {
		// Happens for REVERT
		return nil
	}
	v.parser.AnalyzeAlter(v.alterQuery)
	if v.parser.IsRenameTable() {
		return fmt.Errorf("renaming the table is not supported in ALTER TABLE: %s", sqlparser.CanonicalString(v.alterQuery))
	}
	return nil
}

func (v *VRepl) analyzeTables(ctx context.Context, conn *dbconnpool.DBConnection) (err error) {
	if v.analyzeTable {
		if err := v.executeAnalyzeTable(ctx, conn, v.sourceTableName()); err != nil {
			return err
		}
	}
	senv := schemadiff.NewEnv(v.env, v.env.CollationEnv().DefaultConnectionCharset())
	sourceCreateTableEntity, err := schemadiff.NewCreateTableEntity(senv, v.sourceCreateTable)
	if err != nil {
		return err
	}
	targetCreateTableEntity, err := schemadiff.NewCreateTableEntity(senv, v.targetCreateTable)
	if err != nil {
		return err
	}

	v.tableRows, err = v.readTableStatus(ctx, conn, v.sourceTableName())
	if err != nil {
		return err
	}
	// columns:
	sourceColumns, sourceVirtualColumns, sourcePKColumns, err := readTableColumns(sourceCreateTableEntity)
	if err != nil {
		return err
	}
	targetColumns, targetVirtualColumns, targetPKColumns, err := readTableColumns(targetCreateTableEntity)
	if err != nil {
		return err
	}
	v.sourceSharedColumns, v.targetSharedColumns, v.droppedSourceNonGeneratedColumns, v.sharedColumnsMap = vrepl.GetSharedColumns(sourceColumns, targetColumns, sourceVirtualColumns, targetVirtualColumns, v.parser)

	// unique keys
	sourceUniqueKeys, err := v.readTableUniqueKeys(ctx, sourceCreateTableEntity)
	if err != nil {
		return err
	}
	if len(sourceUniqueKeys) == 0 {
		return fmt.Errorf("found no possible unique key on `%s`", v.sourceTableName())
	}
	targetUniqueKeys, err := v.readTableUniqueKeys(ctx, targetCreateTableEntity)
	if err != nil {
		return err
	}
	if len(targetUniqueKeys) == 0 {
		return fmt.Errorf("found no possible unique key on `%s`", v.targetTableName())
	}
	// VReplication supports completely different unique keys on source and target, covering
	// some/completely different columns. The condition is that the key on source
	// must use columns which all exist on target table.
	eligibleSourceColumnsForUniqueKey := v.sourceSharedColumns.Union(sourceVirtualColumns)
	v.chosenSourceUniqueKey = vrepl.GetUniqueKeyCoveredByColumns(sourceUniqueKeys, eligibleSourceColumnsForUniqueKey)
	if v.chosenSourceUniqueKey == nil {
		// Still no luck.
		return fmt.Errorf("found no possible unique key on `%s` whose columns are in target table `%s`", v.sourceTableName(), v.targetTableName())
	}
	// VReplication supports completely different unique keys on source and target, covering
	// some/completely different columns. The condition is that the key on target
	// must use columns which all exist on source table.
	eligibleTargetColumnsForUniqueKey := v.targetSharedColumns.Union(targetVirtualColumns).MappedNamesColumnList(v.sharedColumnsMap)
	v.chosenTargetUniqueKey = vrepl.GetUniqueKeyCoveredByColumns(targetUniqueKeys, eligibleTargetColumnsForUniqueKey)
	if v.chosenTargetUniqueKey == nil {
		// Still no luck.
		return fmt.Errorf("found no possible unique key on `%s` whose columns are in source table `%s`; col names=%v, v.sharedColumnsMap=%v", v.targetTableName(), v.sourceTableName(), eligibleTargetColumnsForUniqueKey, v.sharedColumnsMap)
	}
	v.addedUniqueKeys = vrepl.AddedUniqueKeys(sourceUniqueKeys, targetUniqueKeys, v.parser.ColumnRenameMap())
	v.removedUniqueKeys = vrepl.RemovedUniqueKeys(sourceUniqueKeys, targetUniqueKeys, v.parser.ColumnRenameMap())
	v.removedForeignKeyNames, err = vrepl.RemovedForeignKeyNames(v.env, v.sourceCreateTable, v.targetCreateTable)
	if err != nil {
		return err
	}

	// chosen source & target unique keys have exact columns in same order
	sharedPKColumns := &v.chosenSourceUniqueKey.Columns

	if err := v.applyColumnTypes(sourceCreateTableEntity, sourceColumns, sourceVirtualColumns, sourcePKColumns, v.sourceSharedColumns, sharedPKColumns, v.droppedSourceNonGeneratedColumns); err != nil {
		return err
	}
	if err := v.applyColumnTypes(targetCreateTableEntity, targetColumns, targetVirtualColumns, targetPKColumns, v.targetSharedColumns); err != nil {
		return err
	}

	for _, sourcePKColumn := range sharedPKColumns.Columns() {
		mappedColumn := v.targetSharedColumns.GetColumn(sourcePKColumn.Entity.NameLowered())
		if mappedColumn == nil {
			mappedColumn = v.targetSharedColumns.GetColumn(sourcePKColumn.Entity.Name())
		}
		if mappedColumn == nil {
			// This can happen if the target column is virtual
			continue
		}

		if sourcePKColumn.Entity.Type() == "enum" && mappedColumn.Entity.Type() == "enum" {
			// An ENUM as part of PRIMARY KEY. We must convert it to text because OMG that's complicated.
			// There's a scenario where a query may modify the enum value (and it's bad practice, seeing
			// that it's part of the PK, but it's still valid), and in that case we must have the string value
			// to be able to DELETE the old row
			v.targetSharedColumns.SetEnumToTextConversion(mappedColumn.Name, sourcePKColumn.EnumValues)
		}
	}

	for i := range v.sourceSharedColumns.Columns() {
		sourceColumn := v.sourceSharedColumns.Columns()[i]
		mappedColumn := v.targetSharedColumns.Columns()[i]
		if sourceColumn.Entity.Type() == "enum" {
			switch {
			// Either this is an ENUM column that stays an ENUM, or it is converted to a textual type.
			// We take note of the enum values, and make it available in vreplication's Filter.Rule.ConvertEnumToText.
			// This, in turn, will be used by vplayer (in TablePlan) like so:
			// - In the binary log, enum values are integers.
			// - Upon seeing this map, PlanBuilder will convert said int to the enum's logical string value.
			// - And will apply the value as a string (`StringBindVariable`) in the query.
			// What this allows is for enum values to have different ordering in the before/after table schema,
			// so that for example you could modify an enum column:
			// - from `('red', 'green', 'blue')` to `('red', 'blue')`
			// - from `('red', 'green', 'blue')` to `('blue', 'red', 'green')`
			case mappedColumn.Entity.Type() == "enum":
			case mappedColumn.Entity.Charset() != "":
				v.targetSharedColumns.SetEnumToTextConversion(mappedColumn.Name, sourceColumn.EnumValues)
			}
		}

		if sourceColumn.Entity.IsIntegralType() && mappedColumn.Entity.Type() == "enum" {
			v.intToEnumMap[sourceColumn.Name] = true
		}
	}

	v.droppedNoDefaultColumnNames = vrepl.GetNoDefaultColumnNames(v.droppedSourceNonGeneratedColumns)
	var expandedDescriptions map[string]string
	v.expandedColumnNames, expandedDescriptions = vrepl.GetExpandedColumnNames(v.sourceSharedColumns, v.targetSharedColumns)

	v.sourceAutoIncrement, err = sourceCreateTableEntity.AutoIncrementValue()

	notes := []string{}
	for _, uk := range v.removedUniqueKeys {
		notes = append(notes, fmt.Sprintf("unique constraint removed: %s", uk.Name))
	}
	for _, name := range v.droppedNoDefaultColumnNames {
		notes = append(notes, fmt.Sprintf("column %s dropped, and had no default value", name))
	}
	for _, name := range v.expandedColumnNames {
		notes = append(notes, fmt.Sprintf("column %s: %s", name, expandedDescriptions[name]))
	}
	for _, name := range v.removedForeignKeyNames {
		notes = append(notes, fmt.Sprintf("foreign key %s dropped", name))
	}
	v.revertibleNotes = strings.Join(notes, "\n")
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

		targetCol := v.targetSharedColumns.GetColumn(targetName)
		if targetCol == nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Cannot find target column %s", targetName)
		}

		if i > 0 {
			sb.WriteString(", ")
		}
		switch {
		case sourceCol.EnumToTextConversion:
			sb.WriteString(fmt.Sprintf("CONCAT(%s)", escapeName(name)))
		case v.intToEnumMap[name]:
			sb.WriteString(fmt.Sprintf("CONCAT(%s)", escapeName(name)))
		case sourceCol.Entity.Type() == "json":
			sb.WriteString(fmt.Sprintf("convert(%s using utf8mb4)", escapeName(name)))
		case sourceCol.Entity.IsTextual():
			// Check source and target charset/encoding. If needed, create
			// a binlogdatapb.CharsetConversion entry (later written to vreplication)
			fromCollation := v.env.CollationEnv().DefaultCollationForCharset(sourceCol.Entity.Charset())
			if fromCollation == collations.Unknown {
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Character set %s not supported for column %s", sourceCol.Entity.Charset(), sourceCol.Name)
			}
			toCollation := v.env.CollationEnv().DefaultCollationForCharset(targetCol.Entity.Charset())
			// Let's see if target col is at all textual
			if targetCol.Entity.IsTextual() && toCollation == collations.Unknown {
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Character set %s not supported for column %s", targetCol.Entity.Charset(), targetCol.Name)
			}

			if trivialCharset(fromCollation) && trivialCharset(toCollation) && targetCol.Entity.Type() != "json" {
				sb.WriteString(escapeName(name))
			} else {
				v.convertCharset[targetName] = &binlogdatapb.CharsetConversion{
					FromCharset: sourceCol.Entity.Charset(),
					ToCharset:   targetCol.Entity.Charset(),
				}
				sb.WriteString(fmt.Sprintf("convert(%s using utf8mb4)", escapeName(name)))
			}
		case targetCol.Entity.Type() == "json" && sourceCol.Entity.Type() != "json":
			// Convert any type to JSON: encode the type as utf8mb4 text
			sb.WriteString(fmt.Sprintf("convert(%s using utf8mb4)", escapeName(name)))
		default:
			sb.WriteString(escapeName(name))
		}
		sb.WriteString(" as ")
		sb.WriteString(escapeName(targetName))
	}
	sb.WriteString(" from ")
	sb.WriteString(escapeName(v.sourceTableName()))

	v.filterQuery = sb.String()
	return nil
}

func trivialCharset(c collations.ID) bool {
	if c == collations.Unknown {
		return true
	}
	utf8mb4Charset := charset.Charset_utf8mb4{}
	return utf8mb4Charset.IsSuperset(colldata.Lookup(c).Charset()) || c == collations.CollationBinaryID
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
		Match:                        v.targetTableName(),
		Filter:                       v.filterQuery,
		SourceUniqueKeyColumns:       encodeColumns(&v.chosenSourceUniqueKey.Columns),
		TargetUniqueKeyColumns:       encodeColumns(&v.chosenTargetUniqueKey.Columns),
		SourceUniqueKeyTargetColumns: encodeColumns(v.chosenSourceUniqueKey.Columns.MappedNamesColumnList(v.sharedColumnsMap)),
		ForceUniqueKey:               url.QueryEscape(v.chosenSourceUniqueKey.Name),
	}
	if len(v.convertCharset) > 0 {
		rule.ConvertCharset = v.convertCharset
	}
	if len(v.intToEnumMap) > 0 {
		rule.ConvertIntToEnum = v.intToEnumMap
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

// generateInsertStatement generates the INSERT INTO _vt.replication statement that creates the vreplication workflow
func (v *VRepl) generateInsertStatement(ctx context.Context) (string, error) {
	ig := vreplication.NewInsertGenerator(binlogdatapb.VReplicationWorkflowState_Stopped, v.dbName)
	ig.AddRow(v.workflow, v.bls, v.pos, "", "in_order:REPLICA,PRIMARY",
		binlogdatapb.VReplicationWorkflowType_OnlineDDL, binlogdatapb.VReplicationWorkflowSubType_None, false)

	return ig.String(), nil
}

// generateStartStatement Generates the statement to start VReplication running on the workflow
func (v *VRepl) generateStartStatement(ctx context.Context) (string, error) {
	return sqlparser.ParseAndBind(sqlStartVReplStream,
		sqltypes.StringBindVariable(v.dbName),
		sqltypes.StringBindVariable(v.workflow),
	)
}

func getVreplTable(s *VReplStream) (string, error) {
	// sanity checks:
	if s == nil {
		return "", vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "No vreplication stream migration")
	}
	if s.bls.Filter == nil {
		return "", vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "No binlog source filter for migration %s", s.workflow)
	}
	if len(s.bls.Filter.Rules) != 1 {
		return "", vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Cannot detect filter rules for migration/vreplication %s", s.workflow)
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
