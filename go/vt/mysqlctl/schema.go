// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"regexp"
	"strings"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/sqlparser"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

var autoIncr = regexp.MustCompile(" AUTO_INCREMENT=\\d+")

// executeSchemaCommands executes some SQL commands, using the mysql
// command line tool. It uses the dba connection parameters, with credentials.
func (mysqld *Mysqld) executeSchemaCommands(sql string) error {
	params, err := dbconfigs.WithCredentials(&mysqld.dbcfgs.Dba)
	if err != nil {
		return err
	}

	return mysqld.executeMysqlScript(&params, strings.NewReader(sql))
}

// GetSchema returns the schema for database for tables listed in
// tables. If tables is empty, return the schema for all tables.
func (mysqld *Mysqld) GetSchema(dbName string, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	ctx := context.TODO()
	sd := &tabletmanagerdatapb.SchemaDefinition{}
	backtickDBName := sqlparser.Backtick(dbName)

	// get the database creation command
	qr, fetchErr := mysqld.FetchSuperQuery(ctx, fmt.Sprintf("SHOW CREATE DATABASE IF NOT EXISTS %s", backtickDBName))
	if fetchErr != nil {
		return nil, fetchErr
	}
	if len(qr.Rows) == 0 {
		return nil, fmt.Errorf("empty create database statement for %v", dbName)
	}
	sd.DatabaseSchema = strings.Replace(qr.Rows[0][1].String(), backtickDBName, "{{.DatabaseName}}", 1)

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
		return sd, nil
	}

	sd.TableDefinitions = make([]*tabletmanagerdatapb.TableDefinition, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		tableName := row[0].String()
		tableType := row[1].String()

		// compute dataLength
		var dataLength uint64
		if !row[2].IsNull() {
			// dataLength is NULL for views, then we use 0
			dataLength, err = row[2].ParseUint64()
			if err != nil {
				return nil, err
			}
		}

		// get row count
		var rowCount uint64
		if !row[3].IsNull() {
			rowCount, err = row[3].ParseUint64()
			if err != nil {
				return nil, err
			}
		}

		qr, fetchErr := mysqld.FetchSuperQuery(ctx, fmt.Sprintf("SHOW CREATE TABLE %s.%s", backtickDBName, sqlparser.Backtick(tableName)))
		if fetchErr != nil {
			return nil, fetchErr
		}
		if len(qr.Rows) == 0 {
			return nil, fmt.Errorf("empty create table statement for %v", tableName)
		}

		// Normalize & remove auto_increment because it changes on every insert
		// FIXME(alainjobart) find a way to share this with
		// vt/tabletserver/table_info.go:162
		norm := qr.Rows[0][1].String()
		norm = autoIncr.ReplaceAllLiteralString(norm, "")
		if tableType == tmutils.TableView {
			// Views will have the dbname in there, replace it
			// with {{.DatabaseName}}
			norm = strings.Replace(norm, backtickDBName, "{{.DatabaseName}}", -1)
		}

		td := &tabletmanagerdatapb.TableDefinition{}
		td.Name = tableName
		td.Schema = norm

		td.Columns, err = mysqld.GetColumns(dbName, tableName)
		if err != nil {
			return nil, err
		}
		td.PrimaryKeyColumns, err = mysqld.GetPrimaryKeyColumns(dbName, tableName)
		if err != nil {
			return nil, err
		}
		td.Type = tableType
		td.DataLength = dataLength
		td.RowCount = rowCount
		sd.TableDefinitions = append(sd.TableDefinitions, td)
	}

	sd, err = tmutils.FilterTables(sd, tables, excludeTables, includeViews)
	if err != nil {
		return nil, err
	}
	tmutils.GenerateSchemaVersion(sd)
	return sd, nil
}

// ResolveTables returns a list of actual tables+views matching a list
// of regexps
func ResolveTables(mysqld MysqlDaemon, dbName string, tables []string) ([]string, error) {
	sd, err := mysqld.GetSchema(dbName, tables, nil, true)
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
func (mysqld *Mysqld) GetColumns(dbName, table string) ([]string, error) {
	conn, err := getPoolReconnect(context.TODO(), mysqld.dbaPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	qr, err := conn.ExecuteFetch(fmt.Sprintf("SELECT * FROM %s.%s WHERE 1=0", sqlparser.Backtick(dbName), sqlparser.Backtick(table)), 0, true)
	if err != nil {
		return nil, err
	}
	columns := make([]string, len(qr.Fields))
	for i, field := range qr.Fields {
		columns[i] = field.Name
	}
	return columns, nil

}

// GetPrimaryKeyColumns returns the primary key columns of table.
func (mysqld *Mysqld) GetPrimaryKeyColumns(dbName, table string) ([]string, error) {
	conn, err := getPoolReconnect(context.TODO(), mysqld.dbaPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	qr, err := conn.ExecuteFetch(fmt.Sprintf("SHOW INDEX FROM %s.%s", sqlparser.Backtick(dbName), sqlparser.Backtick(table)), 100, true)
	if err != nil {
		return nil, err
	}
	keyNameIndex := -1
	seqInIndexIndex := -1
	columnNameIndex := -1
	for i, field := range qr.Fields {
		switch field.Name {
		case "Key_name":
			keyNameIndex = i
		case "Seq_in_index":
			seqInIndexIndex = i
		case "Column_name":
			columnNameIndex = i
		}
	}
	if keyNameIndex == -1 || seqInIndexIndex == -1 || columnNameIndex == -1 {
		return nil, fmt.Errorf("unknown columns in 'show index' result: %v", qr.Fields)
	}

	columns := make([]string, 0, 5)
	var expectedIndex int64 = 1
	for _, row := range qr.Rows {
		// skip non-primary keys
		if row[keyNameIndex].String() != "PRIMARY" {
			continue
		}

		// check the Seq_in_index is always increasing
		seqInIndex, err := row[seqInIndexIndex].ParseInt64()
		if err != nil {
			return nil, err
		}
		if seqInIndex != expectedIndex {
			return nil, fmt.Errorf("unexpected index: %v != %v", seqInIndex, expectedIndex)
		}
		expectedIndex++

		columns = append(columns, row[columnNameIndex].String())
	}
	return columns, err
}

// PreflightSchemaChange checks the schema changes in "changes" by applying them
// to an intermediate database that has the same schema as the target database.
func (mysqld *Mysqld) PreflightSchemaChange(dbName string, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	results := make([]*tabletmanagerdatapb.SchemaChangeResult, len(changes))

	// Get current schema from the real database.
	originalSchema, err := mysqld.GetSchema(dbName, nil, nil, true)
	if err != nil {
		return nil, err
	}

	// Populate temporary database with it.
	initialCopySQL := "SET sql_log_bin = 0;\n"
	initialCopySQL += "DROP DATABASE IF EXISTS _vt_preflight;\n"
	initialCopySQL += "CREATE DATABASE _vt_preflight;\n"
	initialCopySQL += "USE _vt_preflight;\n"
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
		beforeSchema, err := mysqld.GetSchema("_vt_preflight", nil, nil, true)
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
		afterSchema, err := mysqld.GetSchema("_vt_preflight", nil, nil, true)
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
func (mysqld *Mysqld) ApplySchemaChange(dbName string, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	// check current schema matches
	beforeSchema, err := mysqld.GetSchema(dbName, nil, nil, true)
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
	sql = fmt.Sprintf("USE %s;\n%s", sqlparser.Backtick(dbName), sql)

	// execute the schema change using an external mysql process
	// (to benefit from the extra commands in mysql cli)
	if err = mysqld.executeSchemaCommands(sql); err != nil {
		return nil, err
	}

	// get AfterSchema
	afterSchema, err := mysqld.GetSchema(dbName, nil, nil, true)
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
