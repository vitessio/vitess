// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"regexp"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

var autoIncr = regexp.MustCompile(" AUTO_INCREMENT=\\d+")

// GetSchema returns the schema for database for tables listed in
// tables. If tables is empty, return the schema for all tables.
func (mysqld *Mysqld) GetSchema(dbName string, tables, excludeTables []string, includeViews bool) (*proto.SchemaDefinition, error) {
	sd := &proto.SchemaDefinition{}

	// get the database creation command
	qr, fetchErr := mysqld.fetchSuperQuery("SHOW CREATE DATABASE " + dbName)
	if fetchErr != nil {
		return nil, fetchErr
	}
	if len(qr.Rows) == 0 {
		return nil, fmt.Errorf("empty create database statement for %v", dbName)
	}
	sd.DatabaseSchema = strings.Replace(qr.Rows[0][1].String(), "`"+dbName+"`", "`{{.DatabaseName}}`", 1)

	// build a list of regexp to match table names against
	var tableRegexps []*regexp.Regexp
	if len(tables) > 0 {
		tableRegexps = make([]*regexp.Regexp, len(tables))
		for i, table := range tables {
			var err error
			tableRegexps[i], err = regexp.Compile(table)
			if err != nil {
				return nil, fmt.Errorf("cannot compile regexp %v for table: %v", table, err)
			}
		}
	}
	var excludeTableRegexps []*regexp.Regexp
	if len(excludeTables) > 0 {
		excludeTableRegexps = make([]*regexp.Regexp, len(excludeTables))
		for i, table := range excludeTables {
			var err error
			excludeTableRegexps[i], err = regexp.Compile(table)
			if err != nil {
				return nil, fmt.Errorf("cannot compile regexp %v for excludeTable: %v", table, err)
			}
		}
	}

	// get the list of tables we're interested in
	sql := "SELECT table_name, table_type, data_length, table_rows FROM information_schema.tables WHERE table_schema = '" + dbName + "'"
	if !includeViews {
		sql += " AND table_type = '" + proto.TABLE_BASE_TABLE + "'"
	}
	qr, err := mysqld.fetchSuperQuery(sql)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		return sd, nil
	}

	sd.TableDefinitions = make([]proto.TableDefinition, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		tableName := row[0].String()
		tableType := row[1].String()

		// check it's a table we want
		if tableRegexps != nil {
			foundMatch := false
			for _, tableRegexp := range tableRegexps {
				if tableRegexp.Match(row[0].Raw()) {
					foundMatch = true
					break
				}
			}
			if !foundMatch {
				continue
			}
		}
		foundMatch := false
		for _, tableRegexp := range excludeTableRegexps {
			if tableRegexp.Match(row[0].Raw()) {
				foundMatch = true
				break
			}
		}
		if foundMatch {
			continue
		}

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

		qr, fetchErr := mysqld.fetchSuperQuery("SHOW CREATE TABLE " + dbName + "." + tableName)
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
		if tableType == proto.TABLE_VIEW {
			// Views will have the dbname in there, replace it
			// with {{.DatabaseName}}
			norm = strings.Replace(norm, "`"+dbName+"`", "`{{.DatabaseName}}`", -1)
		}

		td := proto.TableDefinition{}
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

	sd.GenerateSchemaVersion()
	return sd, nil
}

// ResolveTables returns a list of actual tables+views matching a list
// of regexps
func (mysqld *Mysqld) ResolveTables(dbName string, tables []string) ([]string, error) {
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
	conn, err := mysqld.dbaPool.Get()
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	qr, err := conn.ExecuteFetch(fmt.Sprintf("select * from %v.%v where 1=0", dbName, table), 0, true)
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
	conn, err := mysqld.dbaPool.Get()
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	qr, err := conn.ExecuteFetch(fmt.Sprintf("show index from %v.%v", dbName, table), 100, true)
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
		return nil, fmt.Errorf("Unknown columns in 'show index' result: %v", qr.Fields)
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
			return nil, fmt.Errorf("Unexpected index: %v != %v", seqInIndex, expectedIndex)
		}
		expectedIndex++

		columns = append(columns, row[columnNameIndex].String())
	}
	return columns, err
}

// PreflightSchemaChange will apply the schema change to a fake
// database that has the same schema as the target database, see if it
// works.
func (mysqld *Mysqld) PreflightSchemaChange(dbName string, change string) (*proto.SchemaChangeResult, error) {
	// gather current schema on real database
	beforeSchema, err := mysqld.GetSchema(dbName, nil, nil, true)
	if err != nil {
		return nil, err
	}

	// populate temporary database with it
	sql := "SET sql_log_bin = 0;\n"
	sql += "DROP DATABASE IF EXISTS _vt_preflight;\n"
	sql += "CREATE DATABASE _vt_preflight;\n"
	sql += "USE _vt_preflight;\n"
	for _, td := range beforeSchema.TableDefinitions {
		if td.Type == proto.TABLE_BASE_TABLE {
			sql += td.Schema + ";\n"
		}
	}
	if err = mysqld.ExecuteMysqlCommand(sql); err != nil {
		return nil, err
	}

	// apply schema change to the temporary database
	sql = "SET sql_log_bin = 0;\n"
	sql += "USE _vt_preflight;\n"
	sql += change
	if err = mysqld.ExecuteMysqlCommand(sql); err != nil {
		return nil, err
	}

	// get the result
	afterSchema, err := mysqld.GetSchema("_vt_preflight", nil, nil, true)
	if err != nil {
		return nil, err
	}

	// and clean up the extra database
	sql = "SET sql_log_bin = 0;\n"
	sql += "DROP DATABASE _vt_preflight;\n"
	if err = mysqld.ExecuteMysqlCommand(sql); err != nil {
		return nil, err
	}

	return &proto.SchemaChangeResult{BeforeSchema: beforeSchema, AfterSchema: afterSchema}, nil
}

// ApplySchemaChange will apply the schema change to the given database.
func (mysqld *Mysqld) ApplySchemaChange(dbName string, change *proto.SchemaChange) (*proto.SchemaChangeResult, error) {
	// check current schema matches
	beforeSchema, err := mysqld.GetSchema(dbName, nil, nil, false)
	if err != nil {
		return nil, err
	}
	if change.BeforeSchema != nil {
		schemaDiffs := proto.DiffSchemaToArray("actual", beforeSchema, "expected", change.BeforeSchema)
		if len(schemaDiffs) > 0 {
			for _, msg := range schemaDiffs {
				log.Warningf("BeforeSchema differs: %v", msg)
			}

			// let's see if the schema was already applied
			if change.AfterSchema != nil {
				schemaDiffs = proto.DiffSchemaToArray("actual", beforeSchema, "expected", change.AfterSchema)
				if len(schemaDiffs) == 0 {
					// no diff between the schema we expect
					// after the change and the current
					// schema, we already applied it
					return &proto.SchemaChangeResult{BeforeSchema: beforeSchema, AfterSchema: beforeSchema}, nil
				}
			}

			if change.Force {
				log.Warningf("BeforeSchema differs, applying anyway")
			} else {
				return nil, fmt.Errorf("BeforeSchema differs")
			}
		}
	}

	sql := change.Sql
	if !change.AllowReplication {
		sql = "SET sql_log_bin = 0;\n" + sql
	}

	// add a 'use XXX' in front of the SQL
	sql = "USE " + dbName + ";\n" + sql

	// execute the schema change using an external mysql process
	// (to benefit from the extra commands in mysql cli)
	if err = mysqld.ExecuteMysqlCommand(sql); err != nil {
		return nil, err
	}

	// get AfterSchema
	afterSchema, err := mysqld.GetSchema(dbName, nil, nil, false)
	if err != nil {
		return nil, err
	}

	// compare to the provided AfterSchema
	if change.AfterSchema != nil {
		schemaDiffs := proto.DiffSchemaToArray("actual", afterSchema, "expected", change.AfterSchema)
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

	return &proto.SchemaChangeResult{BeforeSchema: beforeSchema, AfterSchema: afterSchema}, nil
}
