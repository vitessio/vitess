/*
Copyright 2024 The Vitess Authors.

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

package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/schematools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// lookupVindex is responsible for performing actions related to lookup vindexes.
type lookupVindex struct {
	ts  *topo.Server
	tmc tmclient.TabletManagerClient

	logger logutil.Logger
	parser *sqlparser.Parser
}

// newLookupVindex creates a new lookupVindex instance which is responsible
// for performing actions related to lookup vindexes.
func newLookupVindex(ws *Server) *lookupVindex {
	return &lookupVindex{
		ts:     ws.ts,
		tmc:    ws.tmc,
		logger: ws.Logger(),
		parser: ws.SQLParser(),
	}
}

// prepareCreate performs the preparatory steps for creating a LookupVindex.
func (lv *lookupVindex) prepareCreate(ctx context.Context, workflow, keyspace string, specs *vschemapb.Keyspace, continueAfterCopyWithOwner bool) (
	ms *vtctldatapb.MaterializeSettings, sourceVSchema, targetVSchema *topo.KeyspaceVSchemaInfo, cancelFunc func() error, err error) {
	var (
		// sourceVSchemaTable is the table info present in the vschema.
		sourceVSchemaTable *vschemapb.Table
		// sourceVindexColumns are computed from the input sourceTable.
		sourceVindexColumns []string

		// origTargetVSchema is the original target keyspace VSchema.
		// If any error occurs, we can revert back to the original VSchema.
		origTargetVSchema *topo.KeyspaceVSchemaInfo

		// Target table info.
		createDDL        string
		materializeQuery string

		targetKeyspace string
		tableSettings  []*vtctldatapb.TableMaterializeSettings
	)

	if specs == nil || len(specs.Vindexes) == 0 {
		return nil, nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "no vindex provided")
	}

	targetVSchemaChanged := false

	// If we are about to backfill multiple vindexes, we should validate if
	// all the vindexes are owned, as creating a backfilling workflow with a
	// mix of unowned and owned vindexes can be a problem while performing
	// other operations like externalize, internalize, complete and cancel.
	if len(specs.Vindexes) > 1 {
		for vindexName, vindex := range specs.Vindexes {
			if vindex.Owner == "" {
				return nil, nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "vindex(%s) has no owner", vindexName)
			}
		}
	}

	// Collect columnVindexes in a map, for faster access of source column vindexes
	// in the main loop.
	columnVindexByName := map[string]*vschemapb.ColumnVindex{}
	for _, table := range specs.Tables {
		for _, colVindex := range table.ColumnVindexes {
			columnVindexByName[colVindex.Name] = colVindex
		}
	}

	for vindexName, vindex := range specs.Vindexes {
		vInfo, err := lv.validateAndGetVindexInfo(vindexName, vindex, specs.Tables)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		if vindex.Owner == "" {
			// If the vindex is unowned, we need to search for source table
			// using a loop iterating over specs.Tables. And, as we have
			// already validated if all vindexes are owned in case of multiple
			// vindexes, so this case should be possible only when we are
			// backfilling a single vindex. So, this approach can be used.
			if len(specs.Tables) < 1 || len(specs.Tables) > 2 {
				return nil, nil, nil, nil, errors.New("one or two tables must be specified")
			}
			vInfo.sourceTable, vInfo.sourceTableName, err = getSourceTable(specs.Tables, vInfo.targetTableName, vInfo.fromCols)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			if vInfo.sourceTable == nil || vInfo.sourceTableName == "" {
				return nil, nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "source table not found for vindex %s", vInfo.name)
			}
		} else {
			var ok bool
			if vInfo.sourceTable, ok = specs.Tables[vindex.Owner]; !ok {
				return nil, nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "table owner not found for vindex %s", vInfo.name)
			}
			vInfo.sourceTableName = vindex.Owner
		}

		if len(specs.Vindexes) == 1 {
			sourceVindexColumns, err = validateSourceTableAndGetVindexColumns(vInfo, vindex, keyspace)
		} else {
			sourceVindexColumns, err = getSourceVindexColumns(vInfo, columnVindexByName[vindexName])
		}
		if err != nil {
			return nil, nil, nil, nil, err
		}

		// This should be possible only for the first iteration.
		if sourceVSchema == nil || targetVSchema == nil {
			targetKeyspace = vInfo.targetKeyspace
			sourceVSchema, targetVSchema, err = lv.getTargetAndSourceVSchema(ctx, keyspace, vInfo.targetKeyspace)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			// Save a copy of the original vschema if we modify it and need to provide
			// a cancelFunc. We do NOT want to clone the key version as we explicitly
			// want to go back in time. So we only clone the internal vschema.Keyspace.
			origTargetVSchema = &topo.KeyspaceVSchemaInfo{
				Name:     vInfo.targetKeyspace,
				Keyspace: targetVSchema.Keyspace.CloneVT(),
			}
		}

		if existing, ok := sourceVSchema.Vindexes[vInfo.name]; ok {
			if !proto.Equal(existing, vindex) { // If the exact same vindex already exists then we can re-use it
				return nil, nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "a conflicting vindex named %s already exists in the %s keyspace",
					vInfo.name, keyspace)
			}
		}

		sourceVSchemaTable = sourceVSchema.Tables[vInfo.sourceTableName]
		if sourceVSchemaTable == nil && !schema.IsInternalOperationTableName(vInfo.sourceTableName) {
			return nil, nil, nil, nil,
				vterrors.Errorf(vtrpcpb.Code_INTERNAL, "table %s not found in the %s keyspace", vInfo.sourceTableName, keyspace)
		}
		if err := validateNonConflictingColumnVindex(sourceVSchemaTable, vInfo, sourceVindexColumns, keyspace); err != nil {
			return nil, nil, nil, nil, err
		}

		// Validate against source schema.
		sourceShards, err := lv.ts.GetServingShards(ctx, keyspace)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		onesource := sourceShards[0]
		if onesource.PrimaryAlias == nil {
			return nil, nil, nil, nil,
				vterrors.Errorf(vtrpcpb.Code_INTERNAL, "source shard %s has no primary", onesource.ShardName())
		}

		req := &tabletmanagerdatapb.GetSchemaRequest{Tables: []string{vInfo.sourceTableName}}
		tableSchema, err := schematools.GetSchema(ctx, lv.ts, lv.tmc, onesource.PrimaryAlias, req)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if len(tableSchema.TableDefinitions) != 1 {
			return nil, nil, nil, nil,
				vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected number of tables (%d) returned from %s schema",
					len(tableSchema.TableDefinitions), keyspace)
		}

		// Generate "create table" statement.
		createDDL, err = lv.generateCreateDDLStatement(tableSchema, sourceVindexColumns, vInfo, vindex)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		// Generate vreplication query.
		materializeQuery = generateMaterializeQuery(vInfo, vindex, sourceVindexColumns)

		// Update targetVSchema.
		targetTable := specs.Tables[vInfo.targetTableName]
		if targetVSchema.Sharded {
			targetVindex, err := getTargetVindex(tableSchema.TableDefinitions[0], sourceVindexColumns[0], targetTable)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			targetVindexType := targetVindex.Type

			if existing, ok := targetVSchema.Vindexes[targetVindexType]; ok {
				if !proto.Equal(existing, targetVindex) {
					return nil, nil, nil, nil,
						vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "a conflicting vindex named %v already exists in the %s keyspace",
							targetVindexType, vInfo.targetKeyspace)
				}
			} else {
				targetVSchema.Vindexes[targetVindexType] = targetVindex
				targetVSchemaChanged = true
			}

			targetTable = &vschemapb.Table{
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: vInfo.fromCols[0],
					Name:   targetVindexType,
				}},
			}
		} else {
			targetTable = &vschemapb.Table{}
		}
		if existing, ok := targetVSchema.Tables[vInfo.targetTableName]; ok {
			if !proto.Equal(existing, targetTable) {
				return nil, nil, nil, nil,
					vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "a conflicting table named %s already exists in the %s vschema",
						vInfo.targetTableName, vInfo.targetKeyspace)
			}
		} else {
			targetVSchema.Tables[vInfo.targetTableName] = targetTable
			targetVSchemaChanged = true
		}

		materializeTableSettings := &vtctldatapb.TableMaterializeSettings{
			TargetTable:      vInfo.targetTableName,
			SourceExpression: materializeQuery,
			CreateDdl:        createDDL,
		}

		tableSettings = append(tableSettings, materializeTableSettings)
		// Update sourceVSchema
		sourceVSchema.Vindexes[vInfo.name] = vindex
		sourceVSchemaTable.ColumnVindexes = append(sourceVSchemaTable.ColumnVindexes, columnVindexByName[vindexName])
	}

	if targetVSchemaChanged {
		cancelFunc = func() error {
			// Restore the original target vschema.
			return lv.ts.SaveVSchema(ctx, origTargetVSchema)
		}
	}

	ms = &vtctldatapb.MaterializeSettings{
		Workflow:              workflow,
		MaterializationIntent: vtctldatapb.MaterializationIntent_CREATELOOKUPINDEX,
		SourceKeyspace:        keyspace,
		TargetKeyspace:        targetKeyspace,
		StopAfterCopy:         !continueAfterCopyWithOwner,
		TableSettings:         tableSettings,
	}

	return ms, sourceVSchema, targetVSchema, cancelFunc, nil
}

// vindexInfo holds the validated vindex configuration
type vindexInfo struct {
	name            string
	targetKeyspace  string
	targetTableName string
	fromCols        []string
	toCol           string
	ignoreNulls     bool

	// sourceTable is the supplied table info.
	sourceTable     *vschemapb.Table
	sourceTableName string
}

// validateAndGetVindex validates and extracts vindex configuration
func (lv *lookupVindex) validateAndGetVindexInfo(vindexName string, vindex *vschemapb.Vindex, tables map[string]*vschemapb.Table) (*vindexInfo, error) {
	if !strings.Contains(vindex.Type, "lookup") {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "vindex %s is not a lookup type", vindex.Type)
	}

	targetKeyspace, targetTableName, err := lv.parser.ParseTable(vindex.Params["table"])
	if err != nil || targetKeyspace == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"vindex table name (%s) must be in the form <keyspace>.<table>", vindex.Params["table"])
	}

	vindexFromCols := strings.Split(vindex.Params["from"], ",")
	for i, col := range vindexFromCols {
		vindexFromCols[i] = strings.TrimSpace(col)
	}

	if strings.Contains(vindex.Type, "unique") {
		if len(vindexFromCols) != 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unique vindex 'from' should have only one column")
		}
	}

	vindexToCol := vindex.Params["to"]
	// Make the vindex write_only. If one exists already in the vschema,
	// it will need to match this vindex exactly, including the write_only setting.
	vindex.Params["write_only"] = "true"

	// See if we can create the vindex without errors.
	if _, err := vindexes.CreateVindex(vindex.Type, vindexName, vindex.Params); err != nil {
		return nil, err
	}

	ignoreNulls := false
	if ignoreNullsStr, ok := vindex.Params["ignore_nulls"]; ok {
		// This mirrors the behavior of vindexes.boolFromMap().
		switch ignoreNullsStr {
		case "true":
			ignoreNulls = true
		case "false":
			ignoreNulls = false
		default:
			return nil,
				vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "ignore_nulls (%s) value must be 'true' or 'false'",
					ignoreNullsStr)
		}
	}

	// Validate input table.
	if len(tables) < 1 {
		return nil, errors.New("at least one table must be specified")
	}

	return &vindexInfo{
		name:            vindexName,
		targetKeyspace:  targetKeyspace,
		targetTableName: targetTableName,
		fromCols:        vindexFromCols,
		toCol:           vindexToCol,
		ignoreNulls:     ignoreNulls,
	}, nil
}

func (lv *lookupVindex) getTargetAndSourceVSchema(ctx context.Context, sourceKeyspace, targetKeyspace string) (sourceVSchema, targetVSchema *topo.KeyspaceVSchemaInfo, err error) {
	sourceVSchema, err = lv.ts.GetVSchema(ctx, sourceKeyspace)
	if err != nil {
		return nil, nil, err
	}
	if sourceVSchema.Vindexes == nil {
		sourceVSchema.Vindexes = make(map[string]*vschemapb.Vindex)
	}
	// If source and target keyspaces are the same, make vschemas point
	// to the same object.
	if sourceKeyspace == targetKeyspace {
		targetVSchema = sourceVSchema
	} else {
		targetVSchema, err = lv.ts.GetVSchema(ctx, targetKeyspace)
		if err != nil {
			return nil, nil, err
		}
	}
	if targetVSchema.Vindexes == nil {
		targetVSchema.Vindexes = make(map[string]*vschemapb.Vindex)
	}
	if targetVSchema.Tables == nil {
		targetVSchema.Tables = make(map[string]*vschemapb.Table)
	}

	return sourceVSchema, targetVSchema, nil
}

func getSourceTable(tables map[string]*vschemapb.Table, targetTableName string, fromCols []string) (sourceTable *vschemapb.Table, sourceTableName string, err error) {
	// Loop executes once or twice.
	for tableName, table := range tables {
		if len(table.ColumnVindexes) != 1 {
			return nil, "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "exactly one ColumnVindex must be specified for the %s table",
				tableName)
		}

		if tableName != targetTableName { // This is the source table.
			sourceTableName = tableName
			sourceTable = table
			continue
		}
		// This is a primary vindex definition for the target table
		// which allows you to override the vindex type used.
		var vindexCols []string
		if len(table.ColumnVindexes[0].Columns) != 0 {
			vindexCols = table.ColumnVindexes[0].Columns
		} else {
			if table.ColumnVindexes[0].Column == "" {
				return nil, "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "at least one column must be specified in ColumnVindexes for the %s table",
					tableName)
			}
			vindexCols = []string{table.ColumnVindexes[0].Column}
		}
		if !slices.Equal(vindexCols, fromCols) {
			return nil, "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "columns in the lookup table %s primary vindex (%s) don't match the 'from' columns specified (%s)",
				tableName, strings.Join(vindexCols, ","), strings.Join(fromCols, ","))
		}
	}
	return sourceTable, sourceTableName, nil
}

func (lv *lookupVindex) generateCreateDDLStatement(tableSchema *tabletmanagerdatapb.SchemaDefinition, sourceVindexColumns []string, vInfo *vindexInfo, vindex *vschemapb.Vindex) (string, error) {
	lines := strings.Split(tableSchema.TableDefinitions[0].Schema, "\n")
	if len(lines) < 3 {
		// Should never happen.
		return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "schema looks incorrect: %s, expecting at least four lines",
			tableSchema.TableDefinitions[0].Schema)
	}

	var modified []string
	modified = append(modified, strings.Replace(lines[0], vInfo.sourceTableName, vInfo.targetTableName, 1))
	for i := range sourceVindexColumns {
		line, err := generateColDef(lines, sourceVindexColumns[i], vInfo.fromCols[i])
		if err != nil {
			return "", err
		}
		modified = append(modified, line)
	}

	if vindex.Params["data_type"] == "" || strings.EqualFold(vindex.Type, "consistent_lookup_unique") || strings.EqualFold(vindex.Type, "consistent_lookup") {
		modified = append(modified, fmt.Sprintf("  %s varbinary(128),", sqlescape.EscapeID(vInfo.toCol)))
	} else {
		modified = append(modified, fmt.Sprintf("  %s %s,", sqlescape.EscapeID(vInfo.toCol), sqlescape.EscapeID(vindex.Params["data_type"])))
	}

	buf := sqlparser.NewTrackedBuffer(nil)
	fmt.Fprintf(buf, "  PRIMARY KEY (")
	prefix := ""
	for _, col := range vInfo.fromCols {
		fmt.Fprintf(buf, "%s%s", prefix, sqlescape.EscapeID(col))
		prefix = ", "
	}
	fmt.Fprintf(buf, ")")

	modified = append(modified, buf.String())
	modified = append(modified, ")")
	createDDL := strings.Join(modified, "\n")

	// Confirm that our DDL is valid before we create anything.
	if _, err := lv.parser.ParseStrictDDL(createDDL); err != nil {
		return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "error: %v; invalid lookup table definition generated: %s",
			err, createDDL)
	}

	return createDDL, nil
}

func generateMaterializeQuery(vInfo *vindexInfo, vindex *vschemapb.Vindex, sourceVindexColumns []string) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("select ")
	for i := range vInfo.fromCols {
		buf.Myprintf("%s as %s, ", sqlparser.String(sqlparser.NewIdentifierCI(sourceVindexColumns[i])), sqlparser.String(sqlparser.NewIdentifierCI(vInfo.fromCols[i])))
	}
	if strings.EqualFold(vInfo.toCol, "keyspace_id") || strings.EqualFold(vindex.Type, "consistent_lookup_unique") || strings.EqualFold(vindex.Type, "consistent_lookup") {
		buf.Myprintf("keyspace_id() as %s ", sqlparser.String(sqlparser.NewIdentifierCI(vInfo.toCol)))
	} else {
		buf.Myprintf("%s as %s ", sqlparser.String(sqlparser.NewIdentifierCI(vInfo.toCol)), sqlparser.String(sqlparser.NewIdentifierCI(vInfo.toCol)))
	}
	buf.Myprintf("from %s", sqlparser.String(sqlparser.NewIdentifierCS(vInfo.sourceTableName)))
	if vInfo.ignoreNulls {
		buf.Myprintf(" where ")
		lastValIdx := len(vInfo.fromCols) - 1
		for i := range vInfo.fromCols {
			buf.Myprintf("%s is not null", sqlparser.String(sqlparser.NewIdentifierCI(vInfo.fromCols[i])))
			if i != lastValIdx {
				buf.Myprintf(" and ")
			}
		}
	}
	if vindex.Owner != "" {
		// Only backfill.
		buf.Myprintf(" group by ")
		for i := range vInfo.fromCols {
			buf.Myprintf("%s, ", sqlparser.String(sqlparser.NewIdentifierCI(vInfo.fromCols[i])))
		}
		buf.Myprintf("%s", sqlparser.String(sqlparser.NewIdentifierCI(vInfo.toCol)))
	}
	return buf.String()
}

// validateSourceTableAndGetVindexColumns validates input table and vindex consistency, and returns sourceVindexColumns.
func validateSourceTableAndGetVindexColumns(vInfo *vindexInfo, vindex *vschemapb.Vindex, keyspace string) ([]string, error) {
	if vInfo.sourceTable == nil || len(vInfo.sourceTable.ColumnVindexes) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No ColumnVindex found for the owner table (%s) in the %s keyspace",
			vInfo.sourceTable, keyspace)
	}
	if vInfo.sourceTable.ColumnVindexes[0].Name != vInfo.name {
		return nil,
			vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "ColumnVindex name (%s) must match vindex name (%s)",
				vInfo.sourceTable.ColumnVindexes[0].Name, vInfo.name)
	}
	if vindex.Owner != "" && vindex.Owner != vInfo.sourceTableName {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "vindex owner (%s) must match table name (%s)",
			vindex.Owner, vInfo.sourceTableName)
	}

	return getSourceVindexColumns(vInfo, vInfo.sourceTable.ColumnVindexes[0])
}

func getSourceVindexColumns(vInfo *vindexInfo, colVindex *vschemapb.ColumnVindex) (sourceVindexColumns []string, err error) {
	if colVindex == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "column vindex name (%s) not found in table %s",
			vInfo.name, vInfo.sourceTableName)
	}

	if len(colVindex.Columns) != 0 {
		sourceVindexColumns = colVindex.Columns
	} else {
		if colVindex.Column == "" {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "at least one column must be specified in ColumnVindexes for the %s table",
				vInfo.sourceTableName)
		}
		sourceVindexColumns = []string{colVindex.Column}
	}
	if len(sourceVindexColumns) != len(vInfo.fromCols) {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "length of table columns (%d) differs from length of vindex columns (%d)",
			len(sourceVindexColumns), len(vInfo.fromCols))
	}
	return sourceVindexColumns, nil
}

func validateNonConflictingColumnVindex(sourceVSchemaTable *vschemapb.Table, vInfo *vindexInfo, sourceVindexColumns []string, keyspace string) error {
	for _, colVindex := range sourceVSchemaTable.ColumnVindexes {
		// For a conflict, the vindex name and column should match.
		if colVindex.Name != vInfo.name {
			continue
		}
		var colNames []string
		if len(colVindex.Columns) == 0 {
			colNames = []string{colVindex.Column}
		} else {
			colNames = colVindex.Columns
		}
		// If this is the exact same definition then we can use the existing one. If they
		// are not the same then they are two distinct conflicting vindexes and we should
		// not proceed.
		if !slices.Equal(colNames, sourceVindexColumns) {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "a conflicting ColumnVindex on column(s) %s in table %s already exists in the %s keyspace",
				strings.Join(colNames, ","), vInfo.sourceTableName, keyspace)
		}
	}
	return nil
}

func generateColDef(lines []string, sourceVindexCol, vindexFromCol string) (string, error) {
	source := sqlescape.EscapeID(sourceVindexCol)
	target := sqlescape.EscapeID(vindexFromCol)

	for _, line := range lines[1:] {
		if strings.Contains(line, source) {
			line = strings.Replace(line, source, target, 1)
			line = strings.Replace(line, " AUTO_INCREMENT", "", 1)
			line = strings.Replace(line, " DEFAULT NULL", "", 1)
			// Ensure that the column definition ends with a comma as we will
			// be appending the TO column and PRIMARY KEY definitions. If the
			// souce column here was the last entity defined in the source
			// table's definition then it will not already have the comma.
			if !strings.HasSuffix(strings.TrimSpace(line), ",") {
				line += ","
			}
			return line, nil
		}
	}
	return "", fmt.Errorf("column %s not found in schema %v", sourceVindexCol, lines)
}

// getTargetVindex returns the targetVindex. We choose a primary vindex type
// for the lookup table based on the source definition if one was not explicitly specified.
func getTargetVindex(sourceTableDefinition *tabletmanagerdatapb.TableDefinition, sourceVindexColumn string, targetTable *vschemapb.Table) (
	targetVindex *vschemapb.Vindex, err error) {
	var targetVindexType string
	for _, field := range sourceTableDefinition.Fields {
		if sourceVindexColumn == field.Name {
			if targetTable != nil && len(targetTable.ColumnVindexes) > 0 {
				targetVindexType = targetTable.ColumnVindexes[0].Name
			}
			if targetVindexType == "" {
				targetVindexType, err = vindexes.ChooseVindexForType(field.Type)
				if err != nil {
					return
				}
			}
			targetVindex = &vschemapb.Vindex{
				Type: targetVindexType,
			}
			break
		}
	}
	if targetVindex == nil {
		err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column %s not found in target schema %s",
			sourceVindexColumn, sourceTableDefinition.Schema)
		return
	}
	return
}

// validateExternalizedVindex checks if a given vindex is externalized.
// A vindex is considered externalized if it has an owner and is not in write-only mode.
func (lv *lookupVindex) validateExternalizedVindex(vindex *vschemapb.Vindex) error {
	writeOnly, ok := vindex.Params["write_only"]
	if ok && writeOnly == "true" {
		return errors.New("vindex is in write-only mode")
	}
	if vindex.Owner == "" {
		return errors.New("vindex has no owner")
	}
	return nil
}

// validateExternalized checks if the vindexes have been externalized
// and verifies the state of the VReplication workflow on the target shards.
// It ensures that all streams in the workflow are frozen.
func (lv *lookupVindex) validateExternalized(ctx context.Context, vindexByName map[string]*vschemapb.Vindex, workflowName string, targetShards []*topo.ShardInfo) error {
	for vindexName, vindex := range vindexByName {
		if err := lv.validateExternalizedVindex(vindex); err != nil {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vindex %s has not been externalized yet: %v", vindexName, err)
		}
	}

	err := forAllShards(targetShards, func(targetShard *topo.ShardInfo) error {
		targetPrimary, err := lv.ts.GetTablet(ctx, targetShard.PrimaryAlias)
		if err != nil {
			return err
		}
		res, err := lv.tmc.ReadVReplicationWorkflow(ctx, targetPrimary.Tablet, &tabletmanagerdatapb.ReadVReplicationWorkflowRequest{
			Workflow: workflowName,
		})
		if err != nil {
			return err
		}
		if res == nil || res.Workflow == "" {
			return vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "workflow %s not found on %v", workflowName, topoproto.TabletAliasString(targetPrimary.Alias))
		}
		for _, stream := range res.Streams {
			// All streams need to be frozen.
			if stream.State != binlogdatapb.VReplicationWorkflowState_Stopped || stream.Message != Frozen {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "stream %d for %v/%v is not frozen: %v, %v", stream.Id, targetShard.Keyspace(), targetShard.ShardName(), stream.State, stream.Message)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// validateInternalizedState ensures that the workflow is running if it
// backfills unowned vindex or we've requested that it shouldn't be stopped
// after copy phase completes, else it ensures that the workflow is stopped.
func (lv *lookupVindex) validateInternalizedState(ctx context.Context, workflowName string, isBackfillingOwned bool, targetShards []*topo.ShardInfo) error {
	return forAllShards(targetShards, func(targetShard *topo.ShardInfo) error {
		targetPrimary, err := lv.ts.GetTablet(ctx, targetShard.PrimaryAlias)
		if err != nil {
			return err
		}
		res, err := lv.tmc.ReadVReplicationWorkflow(ctx, targetPrimary.Tablet, &tabletmanagerdatapb.ReadVReplicationWorkflowRequest{
			Workflow: workflowName,
		})
		if err != nil {
			return err
		}
		if res == nil || res.Workflow == "" {
			return vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "workflow %s not found on %v", workflowName, topoproto.TabletAliasString(targetPrimary.Alias))
		}
		for _, stream := range res.Streams {
			if stream.Bls.Filter == nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid binlog source")
			}
			if !isBackfillingOwned || !stream.Bls.StopAfterCopy {
				// If there's no owner or we've requested that the workflow NOT be stopped
				// after the copy phase completes, then all streams need to be running.
				if stream.State != binlogdatapb.VReplicationWorkflowState_Running {
					return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "stream %d for %v.%v is not in Running state: %v", stream.Id, targetShard.Keyspace(), targetShard.ShardName(), stream.State)
				}
			} else {
				// If there is an owner, all streams need to be stopped after copy.
				if stream.State != binlogdatapb.VReplicationWorkflowState_Stopped || !strings.Contains(stream.Message, "Stopped after copy") {
					return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "stream %d for %v.%v is not in Stopped after copy state: %v, %v", stream.Id, targetShard.Keyspace(), targetShard.ShardName(), stream.State, stream.Message)
				}
			}
		}
		return nil
	})
}

// getVindexesFromWorkflowOptions reads workflow options from each target
// shard, and returns lookup vindex names found in workflow options.
func (lv *lookupVindex) getVindexesFromWorkflowOptions(ctx context.Context, workflowName string, targetShards []*topo.ShardInfo) ([]string, error) {
	var (
		options string
		mu      sync.Mutex
	)
	err := forAllShards(targetShards, func(si *topo.ShardInfo) error {
		targetPrimary, err := lv.ts.GetTablet(ctx, si.PrimaryAlias)
		if err != nil {
			return err
		}
		res, err := lv.tmc.ReadVReplicationWorkflow(ctx, targetPrimary.Tablet, &tabletmanagerdatapb.ReadVReplicationWorkflowRequest{
			Workflow: workflowName,
		})
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if options != "" && options != res.Options {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "inconsistent workflow options on target shard %s/%s",
				targetPrimary.Keyspace, targetPrimary.GetShard())
		}
		options = res.GetOptions()
		return nil
	})
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read workflow options")
	}
	workflowOptions := &vtctldatapb.WorkflowOptions{}
	if err := json.Unmarshal([]byte(options), workflowOptions); err != nil {
		return nil, vterrors.Wrapf(err, "failed to parse workflow options")
	}
	return workflowOptions.GetLookupVindexes(), nil
}

// getVindexAndVSchema gets the vindexes (from VSchema) and VSchema with the
// provided keyspace and workflow. Uses workflow options to retrieve lookup
// vindex names.
func (lv *lookupVindex) getVindexesAndVSchema(ctx context.Context, keyspace string, workflowName string, targetShards []*topo.ShardInfo) (map[string]*vschemapb.Vindex, *topo.KeyspaceVSchemaInfo, error) {
	lookupVindexes, err := lv.getVindexesFromWorkflowOptions(ctx, workflowName, targetShards)
	if err != nil {
		return nil, nil, vterrors.Wrapf(err, "failed to retrieve lookup vindex names from workflow")
	}
	if len(lookupVindexes) == 0 {
		// If lookup vindexes from workflow options were absent, we should
		// assume the vindex name to be same as workflow name. This will allow
		// us to support old behaviour. Even if this was a wrong assumption,
		// we will still error out while retrieving vindex from vschema.
		lookupVindexes = []string{workflowName}
	}

	vschema, err := lv.ts.GetVSchema(ctx, keyspace)
	if err != nil {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get vschema for the %s keyspace", keyspace)
	}
	vindexByName := make(map[string]*vschemapb.Vindex, len(lookupVindexes))
	for _, vindexName := range lookupVindexes {
		vindex := vschema.Vindexes[vindexName]
		if vindex == nil {
			return nil, nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "vindex %s not found in the %s keyspace", vindexName, keyspace)
		}
		vindexByName[vindexName] = vindex
	}
	return vindexByName, vschema, nil
}

// IsBackfillingOwnedVindexes returns if the VReplication workflow is
// backfilling owned lookup vindexes. Also, returns error in case the
// workflow backfills a mix of owned and unowned vindexes.
func IsBackfillingOwnedVindexes(vindexByName map[string]*vschemapb.Vindex) (bool, error) {
	isBackfillingOwned := false
	for vindexName, vindex := range vindexByName {
		if vindex.Owner != "" {
			isBackfillingOwned = true
		} else if isBackfillingOwned {
			// We don't allow a workflow to backfill a mix of unowned
			// and owned vindexes.
			return false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "workflow can't backfill mix of unowned and owned vindexes, vindex %s is unowned", vindexName)
		}
	}
	return isBackfillingOwned, nil
}
