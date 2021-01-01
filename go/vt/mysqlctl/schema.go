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

package mysqlctl

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"context"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

var autoIncr = regexp.MustCompile(` AUTO_INCREMENT=\d+`)

// executeSchemaCommands executes some SQL commands, using the mysql
// command line tool. It uses the dba connection parameters, with credentials.
func (mysqld *Mysqld) executeSchemaCommands(sql string) error {
	params, err := mysqld.dbcfgs.DbaConnector().MysqlParams()
	if err != nil {
		return err
	}

	return mysqld.executeMysqlScript(params, strings.NewReader(sql))
}

func encodeTableName(tableName string) string {
	var buf strings.Builder
	sqltypes.NewVarChar(tableName).EncodeSQL(&buf)
	return buf.String()
}

// tableListSQL returns an IN clause "('t1', 't2'...) for a list of tables."
func tableListSQL(tables []string) (string, error) {
	if len(tables) == 0 {
		return "", vterrors.New(vtrpc.Code_INTERNAL, "no tables for tableListSQL")
	}

	encodedTables := make([]string, len(tables))
	for i, tableName := range tables {
		encodedTables[i] = encodeTableName(tableName)
	}

	return "(" + strings.Join(encodedTables, ", ") + ")", nil
}

// GetSchema returns the schema for database for tables listed in
// tables. If tables is empty, return the schema for all tables.
func (mysqld *Mysqld) GetSchema(ctx context.Context, dbName string, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	sd := &tabletmanagerdatapb.SchemaDefinition{}
	backtickDBName := sqlescape.EscapeID(dbName)

	// get the database creation command
	qr, fetchErr := mysqld.FetchSuperQuery(ctx, fmt.Sprintf("SHOW CREATE DATABASE IF NOT EXISTS %s", backtickDBName))
	if fetchErr != nil {
		return nil, fetchErr
	}
	if len(qr.Rows) == 0 {
		return nil, fmt.Errorf("empty create database statement for %v", dbName)
	}
	sd.DatabaseSchema = strings.Replace(qr.Rows[0][1].ToString(), backtickDBName, "{{.DatabaseName}}", 1)

	tds, err := mysqld.collectBasicTableData(ctx, dbName, tables, excludeTables, includeViews)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}

	// Get per-table schema concurrently.
	tableNames := make([]string, 0, len(tds))
	for _, td := range tds {
		tableNames = append(tableNames, td.Name)

		wg.Add(1)
		go func(td *tabletmanagerdatapb.TableDefinition) {
			defer wg.Done()

			fields, columns, schema, err := mysqld.collectSchema(ctx, dbName, td.Name, td.Type)
			if err != nil {
				allErrors.RecordError(err)
				cancel()
				return
			}

			td.Fields = fields
			td.Columns = columns
			td.Schema = schema
		}(td)
	}

	// Get primary columns concurrently.
	colMap := map[string][]string{}
	if len(tableNames) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var err error
			colMap, err = mysqld.getPrimaryKeyColumns(ctx, dbName, tableNames...)
			if err != nil {
				allErrors.RecordError(err)
				cancel()
				return
			}
		}()
	}

	wg.Wait()
	if err := allErrors.AggrError(vterrors.Aggregate); err != nil {
		return nil, err
	}

	for _, td := range tds {
		td.PrimaryKeyColumns = colMap[td.Name]
	}

	sd.TableDefinitions = tds

	tmutils.GenerateSchemaVersion(sd)
	return sd, nil
}

func (mysqld *Mysqld) collectBasicTableData(ctx context.Context, dbName string, tables, excludeTables []string, includeViews bool) ([]*tabletmanagerdatapb.TableDefinition, error) {
	// get the list of tables we're interested in
	sql := "SELECT table_name, table_type, data_length, table_rows FROM information_schema.tables WHERE table_schema = '" + dbName + "'"
	if !includeViews {
		sql += " AND table_type = '" + tmutils.TableBaseTable + "'"
	}
	qr, err := mysqld.FetchSuperQuery(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		return nil, nil
	}

	filter, err := tmutils.NewTableFilter(tables, excludeTables, includeViews)
	if err != nil {
		return nil, err
	}

	tds := make(tableDefinitions, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		tableName := row[0].ToString()
		tableType := row[1].ToString()

		if !filter.Includes(tableName, tableType) {
			continue
		}

		// compute dataLength
		var dataLength uint64
		if !row[2].IsNull() {
			// dataLength is NULL for views, then we use 0
			dataLength, err = evalengine.ToUint64(row[2])
			if err != nil {
				return nil, err
			}
		}

		// get row count
		var rowCount uint64
		if !row[3].IsNull() {
			rowCount, err = evalengine.ToUint64(row[3])
			if err != nil {
				return nil, err
			}
		}

		tds = append(tds, &tabletmanagerdatapb.TableDefinition{
			Name:       tableName,
			Type:       tableType,
			DataLength: dataLength,
			RowCount:   rowCount,
		})
	}

	sort.Sort(tds)

	return tds, nil
}

func (mysqld *Mysqld) collectSchema(ctx context.Context, dbName, tableName, tableType string) ([]*querypb.Field, []string, string, error) {
	fields, columns, err := mysqld.GetColumns(ctx, dbName, tableName)
	if err != nil {
		return nil, nil, "", err
	}

	schema, err := mysqld.normalizedSchema(ctx, dbName, tableName, tableType)
	if err != nil {
		return nil, nil, "", err
	}

	return fields, columns, schema, nil
}

// normalizedSchema returns a table schema with database names replaced, and auto_increment annotations removed.
func (mysqld *Mysqld) normalizedSchema(ctx context.Context, dbName, tableName, tableType string) (string, error) {
	backtickDBName := sqlescape.EscapeID(dbName)
	qr, fetchErr := mysqld.FetchSuperQuery(ctx, fmt.Sprintf("SHOW CREATE TABLE %s.%s", backtickDBName, sqlescape.EscapeID(tableName)))
	if fetchErr != nil {
		return "", fetchErr
	}
	if len(qr.Rows) == 0 {
		return "", fmt.Errorf("empty create table statement for %v", tableName)
	}

	// Normalize & remove auto_increment because it changes on every insert
	// FIXME(alainjobart) find a way to share this with
	// vt/tabletserver/table_info.go:162
	norm := qr.Rows[0][1].ToString()
	norm = autoIncr.ReplaceAllLiteralString(norm, "")
	if tableType == tmutils.TableView {
		// Views will have the dbname in there, replace it
		// with {{.DatabaseName}}
		norm = strings.Replace(norm, backtickDBName, "{{.DatabaseName}}", -1)
	}

	return norm, nil
}

// ResolveTables returns a list of actual tables+views matching a list
// of regexps
func ResolveTables(ctx context.Context, mysqld MysqlDaemon, dbName string, tables []string) ([]string, error) {
	sd, err := mysqld.GetSchema(ctx, dbName, tables, nil, true)
	if err != nil {
		return nil, err
	}
	result := make([]string, len(sd.TableDefinitions))
	for i, td := range sd.TableDefinitions {
		result[i] = td.Name
	}
	return result, nil
}

// GetColumns returns the columns of table.
func (mysqld *Mysqld) GetColumns(ctx context.Context, dbName, table string) ([]*querypb.Field, []string, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Recycle()

	qr, err := conn.ExecuteFetch(fmt.Sprintf("SELECT * FROM %s.%s WHERE 1=0", sqlescape.EscapeID(dbName), sqlescape.EscapeID(table)), 0, true)
	if err != nil {
		return nil, nil, err
	}

	columns := make([]string, len(qr.Fields))
	for i, field := range qr.Fields {
		columns[i] = field.Name
	}
	return qr.Fields, columns, nil

}

// GetPrimaryKeyColumns returns the primary key columns of table.
func (mysqld *Mysqld) GetPrimaryKeyColumns(ctx context.Context, dbName, table string) ([]string, error) {
	cs, err := mysqld.getPrimaryKeyColumns(ctx, dbName, table)
	if err != nil {
		return nil, err
	}

	return cs[dbName], nil
}

func (mysqld *Mysqld) getPrimaryKeyColumns(ctx context.Context, dbName string, tables ...string) (map[string][]string, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	tableList, err := tableListSQL(tables)
	if err != nil {
		return nil, err
	}
	// sql uses column name aliases to guarantee lower case sensitivity.
	sql := fmt.Sprintf(`
		SELECT
			table_name AS table_name,
			ordinal_position AS ordinal_position,
			column_name AS column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = '%s'
			AND table_name IN %s
			AND constraint_name='PRIMARY'
		ORDER BY table_name, ordinal_position`, dbName, tableList)
	qr, err := conn.ExecuteFetch(sql, len(tables)*100, true)
	if err != nil {
		return nil, err
	}

	named := qr.Named()
	colMap := map[string][]string{}
	for _, row := range named.Rows {
		tableName := row.AsString("table_name", "")
		colMap[tableName] = append(colMap[tableName], row.AsString("column_name", ""))
	}
	return colMap, err
}

// PreflightSchemaChange checks the schema changes in "changes" by applying them
// to an intermediate database that has the same schema as the target database.
func (mysqld *Mysqld) PreflightSchemaChange(ctx context.Context, dbName string, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	results := make([]*tabletmanagerdatapb.SchemaChangeResult, len(changes))

	// Get current schema from the real database.
	originalSchema, err := mysqld.GetSchema(ctx, dbName, nil, nil, true)
	if err != nil {
		return nil, err
	}

	// Populate temporary database with it.
	initialCopySQL := "SET sql_log_bin = 0;\n"
	initialCopySQL += "DROP DATABASE IF EXISTS _vt_preflight;\n"
	initialCopySQL += "CREATE DATABASE _vt_preflight;\n"
	initialCopySQL += "USE _vt_preflight;\n"
	// We're not smart enough to create the tables in a foreign-key-compatible way,
	// so we temporarily disable foreign key checks while adding the existing tables.
	initialCopySQL += "SET foreign_key_checks = 0;\n"
	for _, td := range originalSchema.TableDefinitions {
		if td.Type == tmutils.TableBaseTable {
			initialCopySQL += td.Schema + ";\n"
		}
	}
	for _, td := range originalSchema.TableDefinitions {
		if td.Type == tmutils.TableView {
			// Views will have {{.DatabaseName}} in there, replace
			// it with _vt_preflight
			s := strings.Replace(td.Schema, "{{.DatabaseName}}", "`_vt_preflight`", -1)
			initialCopySQL += s + ";\n"
		}
	}
	if err = mysqld.executeSchemaCommands(initialCopySQL); err != nil {
		return nil, err
	}

	// For each change, record the schema before and after.
	for i, change := range changes {
		beforeSchema, err := mysqld.GetSchema(ctx, "_vt_preflight", nil, nil, true)
		if err != nil {
			return nil, err
		}

		// apply schema change to the temporary database
		sql := "SET sql_log_bin = 0;\n"
		sql += "USE _vt_preflight;\n"
		sql += change
		if err = mysqld.executeSchemaCommands(sql); err != nil {
			return nil, err
		}

		// get the result
		afterSchema, err := mysqld.GetSchema(ctx, "_vt_preflight", nil, nil, true)
		if err != nil {
			return nil, err
		}

		results[i] = &tabletmanagerdatapb.SchemaChangeResult{BeforeSchema: beforeSchema, AfterSchema: afterSchema}
	}

	// and clean up the extra database
	dropSQL := "SET sql_log_bin = 0;\n"
	dropSQL += "DROP DATABASE _vt_preflight;\n"
	if err = mysqld.executeSchemaCommands(dropSQL); err != nil {
		return nil, err
	}

	return results, nil
}

// ApplySchemaChange will apply the schema change to the given database.
func (mysqld *Mysqld) ApplySchemaChange(ctx context.Context, dbName string, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	// check current schema matches
	beforeSchema, err := mysqld.GetSchema(ctx, dbName, nil, nil, true)
	if err != nil {
		return nil, err
	}
	if change.BeforeSchema != nil {
		schemaDiffs := tmutils.DiffSchemaToArray("actual", beforeSchema, "expected", change.BeforeSchema)
		if len(schemaDiffs) > 0 {
			for _, msg := range schemaDiffs {
				log.Warningf("BeforeSchema differs: %v", msg)
			}

			// let's see if the schema was already applied
			if change.AfterSchema != nil {
				schemaDiffs = tmutils.DiffSchemaToArray("actual", beforeSchema, "expected", change.AfterSchema)
				if len(schemaDiffs) == 0 {
					// no diff between the schema we expect
					// after the change and the current
					// schema, we already applied it
					return &tabletmanagerdatapb.SchemaChangeResult{
						BeforeSchema: beforeSchema,
						AfterSchema:  beforeSchema}, nil
				}
			}

			if change.Force {
				log.Warningf("BeforeSchema differs, applying anyway")
			} else {
				return nil, fmt.Errorf("BeforeSchema differs")
			}
		}
	}

	sql := change.SQL
	if !change.AllowReplication {
		sql = "SET sql_log_bin = 0;\n" + sql
	}

	// add a 'use XXX' in front of the SQL
	sql = fmt.Sprintf("USE %s;\n%s", sqlescape.EscapeID(dbName), sql)

	// execute the schema change using an external mysql process
	// (to benefit from the extra commands in mysql cli)
	if err = mysqld.executeSchemaCommands(sql); err != nil {
		return nil, err
	}

	// get AfterSchema
	afterSchema, err := mysqld.GetSchema(ctx, dbName, nil, nil, true)
	if err != nil {
		return nil, err
	}

	// compare to the provided AfterSchema
	if change.AfterSchema != nil {
		schemaDiffs := tmutils.DiffSchemaToArray("actual", afterSchema, "expected", change.AfterSchema)
		if len(schemaDiffs) > 0 {
			for _, msg := range schemaDiffs {
				log.Warningf("AfterSchema differs: %v", msg)
			}
			if change.Force {
				log.Warningf("AfterSchema differs, not reporting error")
			} else {
				return nil, fmt.Errorf("AfterSchema differs")
			}
		}
	}

	return &tabletmanagerdatapb.SchemaChangeResult{BeforeSchema: beforeSchema, AfterSchema: afterSchema}, nil
}

//tableDefinitions is a sortable collection of table definitions
type tableDefinitions []*tabletmanagerdatapb.TableDefinition

func (t tableDefinitions) Len() int {
	return len(t)
}

func (t tableDefinitions) Less(i, j int) bool {
	return t[i].Name < t[j].Name
}

func (t tableDefinitions) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

var _ sort.Interface = (tableDefinitions)(nil)
