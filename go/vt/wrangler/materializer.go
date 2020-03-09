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

package wrangler

import (
	"fmt"
	"strings"
	"sync"
	"text/template"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

type materializer struct {
	wr            *Wrangler
	ms            *vtctldatapb.MaterializeSettings
	targetVSchema *vindexes.KeyspaceSchema
	sourceShards  []*topo.ShardInfo
	targetShards  []*topo.ShardInfo
}

// Migrate initiates a table migration.
func (wr *Wrangler) Migrate(ctx context.Context, workflow, sourceKeyspace, targetKeyspace, tableSpecs, cell, tabletTypes string) error {
	var tables []string
	var vschema *vschemapb.Keyspace
	if strings.HasPrefix(tableSpecs, "{") {
		wrap := fmt.Sprintf(`{"tables": %s}`, tableSpecs)
		ks := &vschemapb.Keyspace{}
		if err := json2.Unmarshal([]byte(wrap), ks); err != nil {
			return err
		}
		var err error
		vschema, err = wr.ts.GetVSchema(ctx, targetKeyspace)
		if err != nil {
			return err
		}
		if vschema.Tables == nil {
			vschema.Tables = make(map[string]*vschemapb.Table)
		}
		for table, vtab := range ks.Tables {
			vschema.Tables[table] = vtab
			tables = append(tables, table)
		}
	} else {
		tables = strings.Split(tableSpecs, ",")
	}

	// Save routing rules before vschema. If we save vschema first, and routing rules
	// fails to save, we may generate duplicate table errors.
	rules, err := wr.getRoutingRules(ctx)
	if err != nil {
		return err
	}
	for _, table := range tables {
		rules[table] = []string{sourceKeyspace + "." + table}
		rules[targetKeyspace+"."+table] = []string{sourceKeyspace + "." + table}
	}
	if err := wr.saveRoutingRules(ctx, rules); err != nil {
		return err
	}
	if vschema != nil {
		// We added to the vschema.
		if err := wr.ts.SaveVSchema(ctx, targetKeyspace, vschema); err != nil {
			return err
		}
	}
	if err := wr.ts.RebuildSrvVSchema(ctx, nil); err != nil {
		return err
	}

	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       workflow,
		SourceKeyspace: sourceKeyspace,
		TargetKeyspace: targetKeyspace,
		Cell:           cell,
		TabletTypes:    tabletTypes,
	}
	for _, table := range tables {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("select * from %v", sqlparser.NewTableIdent(table))
		ms.TableSettings = append(ms.TableSettings, &vtctldatapb.TableMaterializeSettings{
			TargetTable:      table,
			SourceExpression: buf.String(),
			CreateDdl:        "copy",
		})
	}
	return wr.Materialize(ctx, ms)
}

// CreateLookupVindex creates a lookup vindex and sets up the backfill.
func (wr *Wrangler) CreateLookupVindex(ctx context.Context, keyspace string, specs *vschemapb.Keyspace, cell, tabletTypes string) error {
	ms, sourceVSchema, targetVSchema, err := wr.prepareCreateLookup(ctx, keyspace, specs)
	if err != nil {
		return err
	}
	if err := wr.ts.SaveVSchema(ctx, ms.TargetKeyspace, targetVSchema); err != nil {
		return err
	}
	ms.Cell = cell
	ms.TabletTypes = tabletTypes
	if err := wr.Materialize(ctx, ms); err != nil {
		return err
	}
	if err := wr.ts.SaveVSchema(ctx, keyspace, sourceVSchema); err != nil {
		return err
	}

	return wr.ts.RebuildSrvVSchema(ctx, nil)
}

// prepareCreateLookup performs the preparatory steps for creating a lookup vindex.
func (wr *Wrangler) prepareCreateLookup(ctx context.Context, keyspace string, specs *vschemapb.Keyspace) (ms *vtctldatapb.MaterializeSettings, sourceVSchema, targetVSchema *vschemapb.Keyspace, err error) {
	// Important variables are pulled out here.
	var (
		// lookup vindex info
		vindexName      string
		vindex          *vschemapb.Vindex
		targetKeyspace  string
		targetTableName string
		vindexFromCols  []string
		vindexToCol     string

		// source table info
		sourceTableName string
		// sourceTable is the supplied table info
		sourceTable *vschemapb.Table
		// sourceVSchemaTable is the table info present in the vschema
		sourceVSchemaTable *vschemapb.Table
		// sourceVindexColumns are computed from the input sourceTable
		sourceVindexColumns []string

		// target table info
		createDDL        string
		materializeQuery string
	)

	// Validate input vindex
	if len(specs.Vindexes) != 1 {
		return nil, nil, nil, fmt.Errorf("only one vindex must be specified in the specs: %v", specs.Vindexes)
	}
	for name, vi := range specs.Vindexes {
		vindexName = name
		vindex = vi
	}
	if !strings.Contains(vindex.Type, "lookup") {
		return nil, nil, nil, fmt.Errorf("vindex %s is not a lookup type", vindex.Type)
	}
	strs := strings.Split(vindex.Params["table"], ".")
	if len(strs) != 2 {
		return nil, nil, nil, fmt.Errorf("vindex 'table' must be <keyspace>.<table>: %v", vindex)
	}
	targetKeyspace, targetTableName = strs[0], strs[1]

	vindexFromCols = strings.Split(vindex.Params["from"], ",")
	if strings.Contains(vindex.Type, "unique") {
		if len(vindexFromCols) != 1 {
			return nil, nil, nil, fmt.Errorf("unique vindex 'from' should have only one column: %v", vindex)
		}
	} else {
		if len(vindexFromCols) < 2 {
			return nil, nil, nil, fmt.Errorf("non-unique vindex 'from' should have more than one column: %v", vindex)
		}
	}
	vindexToCol = vindex.Params["to"]
	// Make the vindex write_only. If one exists already in the vschema,
	// it will need to match this vindex exactly, including the write_only setting.
	vindex.Params["write_only"] = "true"
	// See if we can create the vindex without errors.
	if _, err := vindexes.CreateVindex(vindex.Type, vindexName, vindex.Params); err != nil {
		return nil, nil, nil, err
	}

	// Validate input table
	if len(specs.Tables) != 1 {
		return nil, nil, nil, fmt.Errorf("exactly one table must be specified in the specs: %v", specs.Tables)
	}
	// Loop executes once.
	for k, ti := range specs.Tables {
		if len(ti.ColumnVindexes) != 1 {
			return nil, nil, nil, fmt.Errorf("exactly one ColumnVindex must be specified for the table: %v", specs.Tables)
		}
		sourceTableName = k
		sourceTable = ti
	}

	// Validate input table and vindex consistency
	if sourceTable.ColumnVindexes[0].Name != vindexName {
		return nil, nil, nil, fmt.Errorf("ColumnVindex name must match vindex name: %s vs %s", sourceTable.ColumnVindexes[0].Name, vindexName)
	}
	if vindex.Owner != "" && vindex.Owner != sourceTableName {
		return nil, nil, nil, fmt.Errorf("vindex owner must match table name: %v vs %v", vindex.Owner, sourceTableName)
	}
	if len(sourceTable.ColumnVindexes[0].Columns) != 0 {
		sourceVindexColumns = sourceTable.ColumnVindexes[0].Columns
	} else {
		if sourceTable.ColumnVindexes[0].Column == "" {
			return nil, nil, nil, fmt.Errorf("at least one column must be specified in ColumnVindexes: %v", sourceTable.ColumnVindexes)
		}
		sourceVindexColumns = []string{sourceTable.ColumnVindexes[0].Column}
	}
	if len(sourceVindexColumns) != len(vindexFromCols) {
		return nil, nil, nil, fmt.Errorf("length of table columns differes from length of vindex columns: %v vs %v", sourceVindexColumns, vindexFromCols)
	}

	// Validate against source vschema
	sourceVSchema, err = wr.ts.GetVSchema(ctx, keyspace)
	if err != nil {
		return nil, nil, nil, err
	}
	if sourceVSchema.Vindexes == nil {
		sourceVSchema.Vindexes = make(map[string]*vschemapb.Vindex)
	}
	// If source and target keyspaces are same, Make vschemas point to the same object.
	if keyspace == targetKeyspace {
		targetVSchema = sourceVSchema
	} else {
		targetVSchema, err = wr.ts.GetVSchema(ctx, targetKeyspace)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if targetVSchema.Vindexes == nil {
		targetVSchema.Vindexes = make(map[string]*vschemapb.Vindex)
	}
	if targetVSchema.Tables == nil {
		targetVSchema.Tables = make(map[string]*vschemapb.Table)
	}
	if existing, ok := sourceVSchema.Vindexes[vindexName]; ok {
		if !proto.Equal(existing, vindex) {
			return nil, nil, nil, fmt.Errorf("a conflicting vindex named %s already exists in the source vschema", vindexName)
		}
	}
	sourceVSchemaTable = sourceVSchema.Tables[sourceTableName]
	if sourceVSchemaTable == nil {
		return nil, nil, nil, fmt.Errorf("source table %s not found in vschema", sourceTableName)
	}
	for _, colVindex := range sourceVSchemaTable.ColumnVindexes {
		// For a conflict, the vindex name and column should match.
		if colVindex.Name != vindexName {
			continue
		}
		colName := colVindex.Column
		if len(colVindex.Columns) != 0 {
			colName = colVindex.Columns[0]
		}
		if colName == sourceVindexColumns[0] {
			return nil, nil, nil, fmt.Errorf("ColumnVindex for table %v already exists: %v, please remove it and try again", sourceTableName, colName)
		}
	}

	// Validate against source schema
	sourceShards, err := wr.ts.GetServingShards(ctx, keyspace)
	if err != nil {
		return nil, nil, nil, err
	}
	onesource := sourceShards[0]
	if onesource.MasterAlias == nil {
		return nil, nil, nil, fmt.Errorf("source shard has no master: %v", onesource.ShardName())
	}
	tableSchema, err := wr.GetSchema(ctx, onesource.MasterAlias, []string{sourceTableName}, nil, false)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(tableSchema.TableDefinitions) != 1 {
		return nil, nil, nil, fmt.Errorf("unexpected number of tables returned from schema: %v", tableSchema.TableDefinitions)
	}

	// Generate "create table" statement
	lines := strings.Split(tableSchema.TableDefinitions[0].Schema, "\n")
	if len(lines) < 3 {
		// Unreachable
		return nil, nil, nil, fmt.Errorf("schema looks incorrect: %s, expecting at least four lines", tableSchema.TableDefinitions[0].Schema)
	}
	var modified []string
	modified = append(modified, strings.Replace(lines[0], sourceTableName, targetTableName, 1))
	for i := range sourceVindexColumns {
		line, err := generateColDef(lines, sourceVindexColumns[i], vindexFromCols[i])
		if err != nil {
			return nil, nil, nil, err
		}
		modified = append(modified, line)
	}
	modified = append(modified, fmt.Sprintf("  `%s` varbinary(128),", vindexToCol))
	buf := sqlparser.NewTrackedBuffer(nil)
	fmt.Fprintf(buf, "  PRIMARY KEY (")
	prefix := ""
	for _, col := range vindexFromCols {
		fmt.Fprintf(buf, "%s`%s`", prefix, col)
		prefix = ", "
	}
	fmt.Fprintf(buf, ")")
	modified = append(modified, buf.String())
	modified = append(modified, ")")
	createDDL = strings.Join(modified, "\n")

	// Generate vreplication query
	buf = sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("select ")
	for i := range vindexFromCols {
		buf.Myprintf("%v as %v, ", sqlparser.NewColIdent(sourceVindexColumns[i]), sqlparser.NewColIdent(vindexFromCols[i]))
	}
	buf.Myprintf("keyspace_id() as %v ", sqlparser.NewColIdent(vindexToCol))
	buf.Myprintf("from %v", sqlparser.NewTableIdent(sourceTableName))
	if vindex.Owner != "" {
		// Only backfill
		buf.Myprintf(" group by ")
		for i := range vindexFromCols {
			buf.Myprintf("%v, ", sqlparser.NewColIdent(vindexFromCols[i]))
		}
		buf.Myprintf("%v", sqlparser.NewColIdent(vindexToCol))
	}
	materializeQuery = buf.String()

	// Update targetVSchema
	var targetTable *vschemapb.Table
	if targetVSchema.Sharded {
		// Choose a primary vindex type for target table based on source specs
		var targetVindexType string
		var targetVindex *vschemapb.Vindex
		for _, field := range tableSchema.TableDefinitions[0].Fields {
			if sourceVindexColumns[0] == field.Name {
				targetVindexType, err = vindexes.ChooseVindexForType(field.Type)
				if err != nil {
					return nil, nil, nil, err
				}
				targetVindex = &vschemapb.Vindex{
					Type: targetVindexType,
				}
				break
			}
		}
		if targetVindex == nil {
			// Unreachable. We validated column names when generating the DDL.
			return nil, nil, nil, fmt.Errorf("column %s not found in schema %v", sourceVindexColumns[0], tableSchema.TableDefinitions[0])
		}
		if existing, ok := targetVSchema.Vindexes[targetVindexType]; ok {
			if !proto.Equal(existing, targetVindex) {
				return nil, nil, nil, fmt.Errorf("a conflicting vindex named %v already exists in the target vschema", targetVindexType)
			}
		} else {
			targetVSchema.Vindexes[targetVindexType] = targetVindex
		}

		targetTable = &vschemapb.Table{
			ColumnVindexes: []*vschemapb.ColumnVindex{{
				Column: vindexFromCols[0],
				Name:   targetVindexType,
			}},
		}
	} else {
		targetTable = &vschemapb.Table{}
	}
	if existing, ok := targetVSchema.Tables[targetTableName]; ok {
		if !proto.Equal(existing, targetTable) {
			return nil, nil, nil, fmt.Errorf("a conflicting table named %v already exists in the target vschema", targetTableName)
		}
	} else {
		targetVSchema.Tables[targetTableName] = targetTable
	}

	ms = &vtctldatapb.MaterializeSettings{
		Workflow:       targetTableName + "_vdx",
		SourceKeyspace: keyspace,
		TargetKeyspace: targetKeyspace,
		StopAfterCopy:  vindex.Owner != "",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      targetTableName,
			SourceExpression: materializeQuery,
			CreateDdl:        createDDL,
		}},
	}

	// Update sourceVSchema
	sourceVSchema.Vindexes[vindexName] = vindex
	sourceVSchemaTable.ColumnVindexes = append(sourceVSchemaTable.ColumnVindexes, sourceTable.ColumnVindexes[0])

	return ms, sourceVSchema, targetVSchema, nil
}

func generateColDef(lines []string, sourceVindexCol, vindexFromCol string) (string, error) {
	source := fmt.Sprintf("`%s`", sourceVindexCol)
	target := fmt.Sprintf("`%s`", vindexFromCol)
	for _, line := range lines[1:] {
		if strings.Contains(line, source) {
			line = strings.Replace(line, source, target, 1)
			line = strings.Replace(line, " AUTO_INCREMENT", "", 1)
			line = strings.Replace(line, " DEFAULT NULL", "", 1)
			return line, nil
		}
	}
	return "", fmt.Errorf("column %s not found in schema %v", sourceVindexCol, lines)
}

// ExternalizeVindex externalizes a lookup vindex that's finished backfilling or has caught up.
func (wr *Wrangler) ExternalizeVindex(ctx context.Context, qualifiedVindexName string) error {
	splits := strings.Split(qualifiedVindexName, ".")
	if len(splits) != 2 {
		return fmt.Errorf("vindex name should be of the form keyspace.vindex: %s", qualifiedVindexName)
	}
	sourceKeyspace, vindexName := splits[0], splits[1]
	sourceVSchema, err := wr.ts.GetVSchema(ctx, sourceKeyspace)
	if err != nil {
		return err
	}
	sourceVindex := sourceVSchema.Vindexes[vindexName]
	if sourceVindex == nil {
		return fmt.Errorf("vindex %s not found in vschema", qualifiedVindexName)
	}
	qualifiedTableName := sourceVindex.Params["table"]
	splits = strings.Split(qualifiedTableName, ".")
	if len(splits) != 2 {
		return fmt.Errorf("table name in vindex should be of the form keyspace.table: %s", qualifiedTableName)
	}
	targetKeyspace, targetTableName := splits[0], splits[1]
	workflow := targetTableName + "_vdx"
	targetShards, err := wr.ts.GetServingShards(ctx, targetKeyspace)
	if err != nil {
		return err
	}

	// Create a parallelizer function.
	forAllTargets := func(f func(*topo.ShardInfo) error) error {
		var wg sync.WaitGroup
		allErrors := &concurrency.AllErrorRecorder{}
		for _, targetShard := range targetShards {
			wg.Add(1)
			go func(targetShard *topo.ShardInfo) {
				defer wg.Done()

				if err := f(targetShard); err != nil {
					allErrors.RecordError(err)
				}
			}(targetShard)
		}
		wg.Wait()
		return allErrors.AggrError(vterrors.Aggregate)
	}

	err = forAllTargets(func(targetShard *topo.ShardInfo) error {
		targetMaster, err := wr.ts.GetTablet(ctx, targetShard.MasterAlias)
		if err != nil {
			return err
		}
		p3qr, err := wr.tmc.VReplicationExec(ctx, targetMaster.Tablet, fmt.Sprintf("select id, state, message from _vt.vreplication where workflow=%s and db_name=%s", encodeString(workflow), encodeString(targetMaster.DbName())))
		if err != nil {
			return err
		}
		qr := sqltypes.Proto3ToResult(p3qr)
		for _, row := range qr.Rows {
			id, err := sqltypes.ToInt64(row[0])
			if err != nil {
				return err
			}
			state := row[1].ToString()
			message := row[2].ToString()
			if sourceVindex.Owner == "" {
				// If there's no owner, all streams need to be running.
				if state != binlogplayer.BlpRunning {
					return fmt.Errorf("stream %d for %v.%v is not in Running state: %v", id, targetShard.Keyspace(), targetShard.ShardName(), state)
				}
			} else {
				// If there is an owner, all streams need to be stopped after copy.
				if state != binlogplayer.BlpStopped || !strings.Contains(message, "Stopped after copy") {
					return fmt.Errorf("stream %d for %v.%v is not in Stopped after copy state: %v, %v", id, targetShard.Keyspace(), targetShard.ShardName(), state, message)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	if sourceVindex.Owner != "" {
		// If there is an owner, we have to delete the streams.
		err := forAllTargets(func(targetShard *topo.ShardInfo) error {
			targetMaster, err := wr.ts.GetTablet(ctx, targetShard.MasterAlias)
			if err != nil {
				return err
			}
			query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow=%s", encodeString(targetMaster.DbName()), encodeString(workflow))
			_, err = wr.tmc.VReplicationExec(ctx, targetMaster.Tablet, query)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// Remove the write_only param and save the source vschema.
	delete(sourceVindex.Params, "write_only")
	return wr.ts.SaveVSchema(ctx, sourceKeyspace, sourceVSchema)
}

// Materialize performs the steps needed to materialize a list of tables based on the materialization specs.
func (wr *Wrangler) Materialize(ctx context.Context, ms *vtctldatapb.MaterializeSettings) error {
	if err := wr.validateNewWorkflow(ctx, ms.TargetKeyspace, ms.Workflow); err != nil {
		return err
	}
	mz, err := wr.buildMaterializer(ctx, ms)
	if err != nil {
		return err
	}
	if err := mz.deploySchema(ctx); err != nil {
		return err
	}
	inserts, err := mz.generateInserts(ctx)
	if err != nil {
		return err
	}
	if err := mz.createStreams(ctx, inserts); err != nil {
		return err
	}
	return mz.startStreams(ctx)
}

func (wr *Wrangler) buildMaterializer(ctx context.Context, ms *vtctldatapb.MaterializeSettings) (*materializer, error) {
	vschema, err := wr.ts.GetVSchema(ctx, ms.TargetKeyspace)
	if err != nil {
		return nil, err
	}
	targetVSchema, err := vindexes.BuildKeyspaceSchema(vschema, ms.TargetKeyspace)
	if err != nil {
		return nil, err
	}
	if targetVSchema.Keyspace.Sharded {
		for _, ts := range ms.TableSettings {
			if targetVSchema.Tables[ts.TargetTable] == nil {
				return nil, fmt.Errorf("table %s not found in vschema for keyspace %s", ts.TargetTable, ms.TargetKeyspace)
			}
		}
	}

	sourceShards, err := wr.ts.GetServingShards(ctx, ms.SourceKeyspace)
	if err != nil {
		return nil, err
	}
	targetShards, err := wr.ts.GetServingShards(ctx, ms.TargetKeyspace)
	if err != nil {
		return nil, err
	}
	return &materializer{
		wr:            wr,
		ms:            ms,
		targetVSchema: targetVSchema,
		sourceShards:  sourceShards,
		targetShards:  targetShards,
	}, nil
}

func (mz *materializer) deploySchema(ctx context.Context) error {
	return mz.forAllTargets(func(target *topo.ShardInfo) error {
		for _, ts := range mz.ms.TableSettings {
			tableSchema, err := mz.wr.GetSchema(ctx, target.MasterAlias, []string{ts.TargetTable}, nil, false)
			if err != nil {
				return err
			}
			if len(tableSchema.TableDefinitions) != 0 {
				// Table already exists.
				continue
			}
			if ts.CreateDdl == "" {
				return fmt.Errorf("target table %v does not exist and there is no create ddl defined", ts.TargetTable)
			}
			createddl := ts.CreateDdl
			if createddl == "copy" {
				sourceTableName, err := sqlparser.TableFromStatement(ts.SourceExpression)
				if err != nil {
					return err
				}
				if sourceTableName.Name.String() != ts.TargetTable {
					return fmt.Errorf("source and target table names must match for copying schema: %v vs %v", sqlparser.String(sourceTableName), ts.TargetTable)
				}
				sourceMaster := mz.sourceShards[0].MasterAlias
				if sourceMaster == nil {
					return fmt.Errorf("source shard must have a master for copying schema: %v", mz.sourceShards[0].ShardName())
				}
				sourceSchema, err := mz.wr.GetSchema(ctx, sourceMaster, []string{ts.TargetTable}, nil, false)
				if err != nil {
					return err
				}
				if len(sourceSchema.TableDefinitions) == 0 {
					return fmt.Errorf("source table %v does not exist", ts.TargetTable)
				}
				createddl = sourceSchema.TableDefinitions[0].Schema
			}
			targetTablet, err := mz.wr.ts.GetTablet(ctx, target.MasterAlias)
			if err != nil {
				return err
			}
			if _, err := mz.wr.tmc.ExecuteFetchAsDba(ctx, targetTablet.Tablet, false, []byte(createddl), 0, false, true); err != nil {
				return err
			}
		}
		return nil
	})
}

func (mz *materializer) generateInserts(ctx context.Context) (string, error) {
	ig := vreplication.NewInsertGenerator(binlogplayer.BlpStopped, "{{.dbname}}")

	for _, source := range mz.sourceShards {
		bls := &binlogdatapb.BinlogSource{
			Keyspace:      mz.ms.SourceKeyspace,
			Shard:         source.ShardName(),
			Filter:        &binlogdatapb.Filter{},
			StopAfterCopy: mz.ms.StopAfterCopy,
		}
		for _, ts := range mz.ms.TableSettings {
			rule := &binlogdatapb.Rule{
				Match: ts.TargetTable,
			}
			// Validate the query.
			stmt, err := sqlparser.Parse(ts.SourceExpression)
			if err != nil {
				return "", err
			}
			sel, ok := stmt.(*sqlparser.Select)
			if !ok {
				return "", fmt.Errorf("unrecognized statement: %s", ts.SourceExpression)
			}
			if mz.targetVSchema.Keyspace.Sharded && mz.targetVSchema.Tables[ts.TargetTable].Type != vindexes.TypeReference {
				cv, err := vindexes.FindBestColVindex(mz.targetVSchema.Tables[ts.TargetTable])
				if err != nil {
					return "", err
				}
				mappedCols := make([]*sqlparser.ColName, 0, len(cv.Columns))
				for _, col := range cv.Columns {
					colName, err := matchColInSelect(col, sel)
					if err != nil {
						return "", err
					}
					mappedCols = append(mappedCols, colName)
				}
				subExprs := make(sqlparser.SelectExprs, 0, len(mappedCols)+2)
				for _, mappedCol := range mappedCols {
					subExprs = append(subExprs, &sqlparser.AliasedExpr{Expr: mappedCol})
				}
				vindexName := fmt.Sprintf("%s.%s", mz.ms.TargetKeyspace, cv.Name)
				subExprs = append(subExprs, &sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte(vindexName))})
				subExprs = append(subExprs, &sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("{{.keyrange}}"))})
				sel.Where = &sqlparser.Where{
					Type: sqlparser.WhereStr,
					Expr: &sqlparser.FuncExpr{
						Name:  sqlparser.NewColIdent("in_keyrange"),
						Exprs: subExprs,
					},
				}
				rule.Filter = sqlparser.String(sel)
			} else {
				rule.Filter = ts.SourceExpression
			}
			bls.Filter.Rules = append(bls.Filter.Rules, rule)
		}
		ig.AddRow(mz.ms.Workflow, bls, "", mz.ms.Cell, mz.ms.TabletTypes)
	}
	return ig.String(), nil
}

func matchColInSelect(col sqlparser.ColIdent, sel *sqlparser.Select) (*sqlparser.ColName, error) {
	for _, selExpr := range sel.SelectExprs {
		switch selExpr := selExpr.(type) {
		case *sqlparser.StarExpr:
			return &sqlparser.ColName{Name: col}, nil
		case *sqlparser.AliasedExpr:
			match := selExpr.As
			if match.IsEmpty() {
				if colExpr, ok := selExpr.Expr.(*sqlparser.ColName); ok {
					match = colExpr.Name
				} else {
					// Cannot match against a complex expression.
					continue
				}
			}
			if match.Equal(col) {
				colExpr, ok := selExpr.Expr.(*sqlparser.ColName)
				if !ok {
					return nil, fmt.Errorf("vindex column cannot be a complex expression: %v", sqlparser.String(selExpr))
				}
				return colExpr, nil
			}
		default:
			return nil, fmt.Errorf("unsupported select expression: %v", sqlparser.String(selExpr))
		}
	}
	return nil, fmt.Errorf("could not find vindex column %v", sqlparser.String(col))
}

func (mz *materializer) createStreams(ctx context.Context, inserts string) error {
	return mz.forAllTargets(func(target *topo.ShardInfo) error {
		targetMaster, err := mz.wr.ts.GetTablet(ctx, target.MasterAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", target.MasterAlias)
		}
		buf := &strings.Builder{}
		t := template.Must(template.New("").Parse(inserts))
		input := map[string]string{
			"keyrange": key.KeyRangeString(target.KeyRange),
			"dbname":   targetMaster.DbName(),
		}
		if err := t.Execute(buf, input); err != nil {
			return err
		}
		if _, err := mz.wr.TabletManagerClient().VReplicationExec(ctx, targetMaster.Tablet, buf.String()); err != nil {
			return err
		}
		return nil
	})
}

func (mz *materializer) startStreams(ctx context.Context) error {
	return mz.forAllTargets(func(target *topo.ShardInfo) error {
		targetMaster, err := mz.wr.ts.GetTablet(ctx, target.MasterAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", target.MasterAlias)
		}
		query := fmt.Sprintf("update _vt.vreplication set state='Running' where db_name=%s and workflow=%s", encodeString(targetMaster.DbName()), encodeString(mz.ms.Workflow))
		if _, err := mz.wr.tmc.VReplicationExec(ctx, targetMaster.Tablet, query); err != nil {
			return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", targetMaster.Tablet, query)
		}
		return nil
	})
}

func (mz *materializer) forAllTargets(f func(*topo.ShardInfo) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, target := range mz.targetShards {
		wg.Add(1)
		go func(target *topo.ShardInfo) {
			defer wg.Done()

			if err := f(target); err != nil {
				allErrors.RecordError(err)
			}
		}(target)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}
