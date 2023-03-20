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
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

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
func (mysqld *Mysqld) GetSchema(ctx context.Context, dbName string, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
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

	tds, err := mysqld.collectBasicTableData(ctx, dbName, request.Tables, request.ExcludeTables, request.IncludeViews)
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

			fields, columns, schema, err := mysqld.collectSchema(ctx, dbName, td.Name, td.Type, request.TableSchemaOnly)
			if err != nil {
				// There's a possible race condition: it could happen that a table was dropped in between reading
				// the list of tables (collectBasicTableData(), earlier) and the point above where we investigate
				// the table.
				// This is fine. We identify the situation and keep the table without any fields/columns/key information
				sqlErr, isSQLErr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
				if isSQLErr && sqlErr != nil && sqlErr.Number() == mysql.ERNoSuchTable {
					return
				}

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
	// The below runs a single query on `INFORMATION_SCHEMA` and does not interact with the actual tables.
	// It is therefore safe to run even if some tables are dropped in the interim.
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

func (mysqld *Mysqld) collectSchema(ctx context.Context, dbName, tableName, tableType string, tableSchemaOnly bool) (fields []*querypb.Field, columns []string, schema string, err error) {
	schema, err = mysqld.normalizedSchema(ctx, dbName, tableName, tableType)
	if err != nil {
		return nil, nil, "", err
	}
	if !tableSchemaOnly {
		fields, columns, err = mysqld.GetColumns(ctx, dbName, tableName)
		if err != nil {
			return nil, nil, "", err
		}
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
	req := &tabletmanagerdatapb.GetSchemaRequest{Tables: tables, IncludeViews: true, TableSchemaOnly: true}
	sd, err := mysqld.GetSchema(ctx, dbName, req)
	if err != nil {
		return nil, err
	}
	result := make([]string, len(sd.TableDefinitions))
	for i, td := range sd.TableDefinitions {
		result[i] = td.Name
	}
	return result, nil
}

const (
	GetColumnNamesQuery = `SELECT COLUMN_NAME as column_name
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = %s AND TABLE_NAME = '%s'
		ORDER BY ORDINAL_POSITION`
	GetFieldsQuery = "SELECT %s FROM %s WHERE 1 != 1"
)

func GetColumnsList(dbName, tableName string, exec func(string, int, bool) (*sqltypes.Result, error)) (string, error) {
	var dbName2 string
	if dbName == "" {
		dbName2 = "database()"
	} else {
		dbName2 = fmt.Sprintf("'%s'", dbName)
	}
	query := fmt.Sprintf(GetColumnNamesQuery, dbName2, sqlescape.UnescapeID(tableName))
	qr, err := exec(query, -1, true)
	if err != nil {
		return "", err
	}
	if qr == nil || len(qr.Rows) == 0 {
		err = fmt.Errorf("unable to get columns for table %s.%s using query %s", dbName, tableName, query)
		log.Errorf("%s", fmt.Errorf("unable to get columns for table %s.%s using query %s", dbName, tableName, query))
		return "", err
	}
	selectColumns := ""

	for _, row := range qr.Named().Rows {
		col := row["column_name"].ToString()
		if col == "" {
			continue
		}
		if selectColumns != "" {
			selectColumns += ", "
		}
		selectColumns += sqlescape.EscapeID(col)
	}
	return selectColumns, nil
}

func GetColumns(dbName, table string, exec func(string, int, bool) (*sqltypes.Result, error)) ([]*querypb.Field, []string, error) {
	selectColumns, err := GetColumnsList(dbName, table, exec)
	if err != nil {
		return nil, nil, err
	}
	if selectColumns == "" {
		selectColumns = "*"
	}
	tableSpec := sqlescape.EscapeID(sqlescape.UnescapeID(table))
	if dbName != "" {
		tableSpec = fmt.Sprintf("%s.%s", sqlescape.EscapeID(sqlescape.UnescapeID(dbName)), tableSpec)
	}
	query := fmt.Sprintf(GetFieldsQuery, selectColumns, tableSpec)
	qr, err := exec(query, 0, true)
	if err != nil {
		return nil, nil, err
	}

	columns := make([]string, len(qr.Fields))
	for i, field := range qr.Fields {
		columns[i] = field.Name
	}
	return qr.Fields, columns, nil
}

// GetColumns returns the columns of table.
func (mysqld *Mysqld) GetColumns(ctx context.Context, dbName, table string) ([]*querypb.Field, []string, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Recycle()
	return GetColumns(dbName, table, conn.ExecuteFetch)
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
	sql := `
            SELECT TABLE_NAME as table_name, COLUMN_NAME as column_name
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME IN %s AND LOWER(INDEX_NAME) = 'primary'
            ORDER BY table_name, SEQ_IN_INDEX`
	sql = fmt.Sprintf(sql, dbName, tableList)
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
	req := &tabletmanagerdatapb.GetSchemaRequest{IncludeViews: true, TableSchemaOnly: true}
	originalSchema, err := mysqld.GetSchema(ctx, dbName, req)
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
		req := &tabletmanagerdatapb.GetSchemaRequest{IncludeViews: true}
		beforeSchema, err := mysqld.GetSchema(ctx, "_vt_preflight", req)
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
		afterSchema, err := mysqld.GetSchema(ctx, "_vt_preflight", req)
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
	req := &tabletmanagerdatapb.GetSchemaRequest{IncludeViews: true}
	beforeSchema, err := mysqld.GetSchema(ctx, dbName, req)
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

	// The session used is closed after applying the schema change so we do not need
	// to worry about saving and restoring the session state here
	if change.SQLMode != "" {
		sql = fmt.Sprintf("SET @@session.sql_mode='%s';\n%s", change.SQLMode, sql)
	}

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
	afterSchema, err := mysqld.GetSchema(ctx, dbName, req)
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

// GetPrimaryKeyEquivalentColumns can be used if the table has
// no defined PRIMARY KEY. It will return the columns in a
// viable PRIMARY KEY equivalent (PKE) -- a NON-NULL UNIQUE
// KEY -- in the specified table. When multiple PKE indexes
// are available it will attempt to choose the most efficient
// one based on the column data types and the number of columns
// in the index. See here for the data type storage sizes:
//
//	https://dev.mysql.com/doc/refman/en/storage-requirements.html
//
// If this function is used on a table that DOES have a
// defined PRIMARY KEY then it may return the columns for
// that index if it is likely the most efficient one amongst
// the available PKE indexes on the table.
func (mysqld *Mysqld) GetPrimaryKeyEquivalentColumns(ctx context.Context, dbName, table string) ([]string, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	// We use column name aliases to guarantee lower case for our named results.
	sql := `
            SELECT COLUMN_NAME AS column_name FROM information_schema.STATISTICS AS index_cols INNER JOIN
            (
                SELECT stats.INDEX_NAME, SUM(
                                              CASE LOWER(cols.DATA_TYPE)
                                                WHEN 'enum' THEN 0
                                                WHEN 'tinyint' THEN 1
                                                WHEN 'year' THEN 2
                                                WHEN 'smallint' THEN 3
                                                WHEN 'date' THEN 4
                                                WHEN 'mediumint' THEN 5
                                                WHEN 'time' THEN 6
                                                WHEN 'int' THEN 7
                                                WHEN 'set' THEN 8
                                                WHEN 'timestamp' THEN 9
                                                WHEN 'bigint' THEN 10
                                                WHEN 'float' THEN 11
                                                WHEN 'double' THEN 12
                                                WHEN 'decimal' THEN 13
                                                WHEN 'datetime' THEN 14
                                                WHEN 'binary' THEN 30
                                                WHEN 'char' THEN 31
                                                WHEN 'varbinary' THEN 60
                                                WHEN 'varchar' THEN 61
                                                WHEN 'tinyblob' THEN 80
                                                WHEN 'tinytext' THEN 81
                                                ELSE 1000
                                              END
                                            ) AS type_cost, COUNT(stats.COLUMN_NAME) AS col_count FROM information_schema.STATISTICS AS stats INNER JOIN
                  information_schema.COLUMNS AS cols ON stats.TABLE_SCHEMA = cols.TABLE_SCHEMA AND stats.TABLE_NAME = cols.TABLE_NAME AND stats.COLUMN_NAME = cols.COLUMN_NAME
                WHERE stats.TABLE_SCHEMA = '%s' AND stats.TABLE_NAME = '%s' AND stats.INDEX_NAME NOT IN
                (
                    SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND (NON_UNIQUE = 1 OR NULLABLE = 'YES')
                )
                GROUP BY INDEX_NAME ORDER BY type_cost ASC, col_count ASC LIMIT 1
            ) AS pke ON index_cols.INDEX_NAME = pke.INDEX_NAME
            WHERE index_cols.TABLE_SCHEMA = '%s' AND index_cols.TABLE_NAME = '%s' AND NON_UNIQUE = 0 AND NULLABLE != 'YES'
            ORDER BY SEQ_IN_INDEX ASC`
	sql = fmt.Sprintf(sql, dbName, table, dbName, table, dbName, table)
	qr, err := conn.ExecuteFetch(sql, 1000, true)
	if err != nil {
		return nil, err
	}

	named := qr.Named()
	cols := make([]string, len(qr.Rows))
	for i, row := range named.Rows {
		cols[i] = row.AsString("column_name", "")
	}
	return cols, err
}

// tableDefinitions is a sortable collection of table definitions
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
