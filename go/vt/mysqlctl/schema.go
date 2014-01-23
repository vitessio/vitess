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
func (mysqld *Mysqld) GetSchema(dbName string, tables []string, includeViews bool) (*proto.SchemaDefinition, error) {
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

	// get the list of tables we're interested in
	sql := "SELECT table_name, table_type, data_length FROM information_schema.tables WHERE table_schema = '" + dbName + "'"
	if len(tables) != 0 {
		sql += " AND table_name IN ('" + strings.Join(tables, "','") + "')"
	}
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

	sd.TableDefinitions = make([]proto.TableDefinition, len(qr.Rows))
	for i, row := range qr.Rows {
		tableName := row[0].String()
		tableType := row[1].String()
		var dataLength uint64
		if !row[2].IsNull() {
			// dataLength is NULL for views, then we use 0
			dataLength, err = row[2].ParseUint64()
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

		sd.TableDefinitions[i].Name = tableName
		sd.TableDefinitions[i].Schema = norm

		sd.TableDefinitions[i].Columns, err = mysqld.GetColumns(dbName, tableName)
		if err != nil {
			return nil, err
		}
		sd.TableDefinitions[i].PrimaryKeyColumns, err = mysqld.GetPrimaryKeyColumns(dbName, tableName)
		if err != nil {
			return nil, err
		}
		sd.TableDefinitions[i].Type = tableType
		sd.TableDefinitions[i].DataLength = dataLength
	}

	sd.GenerateSchemaVersion()
	return sd, nil
}

// GetColumns returns the columns of table.
func (mysqld *Mysqld) GetColumns(dbName, table string) ([]string, error) {
	conn, err := mysqld.createConnection()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
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
	conn, err := mysqld.createConnection()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
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

func (mysqld *Mysqld) PreflightSchemaChange(dbName string, change string) (*proto.SchemaChangeResult, error) {
	// gather current schema on real database
	beforeSchema, err := mysqld.GetSchema(dbName, nil, true)
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
	afterSchema, err := mysqld.GetSchema("_vt_preflight", nil, true)
	if err != nil {
		return nil, err
	}

	// and clean up the extra database
	sql = "SET sql_log_bin = 0;\n"
	sql += "DROP DATABASE _vt_preflight;\n"
	if err = mysqld.ExecuteMysqlCommand(sql); err != nil {
		return nil, err
	}

	return &proto.SchemaChangeResult{beforeSchema, afterSchema}, nil
}

func (mysqld *Mysqld) ApplySchemaChange(dbName string, change *proto.SchemaChange) (*proto.SchemaChangeResult, error) {
	// check current schema matches
	beforeSchema, err := mysqld.GetSchema(dbName, nil, false)
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
					return &proto.SchemaChangeResult{beforeSchema, beforeSchema}, nil
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
	afterSchema, err := mysqld.GetSchema(dbName, nil, false)
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

	return &proto.SchemaChangeResult{beforeSchema, afterSchema}, nil
}
