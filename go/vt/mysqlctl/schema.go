// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/concurrency"
)

const (
	TABLE_BASE_TABLE = "BASE TABLE"
	TABLE_VIEW       = "VIEW"
)

type TableDefinition struct {
	Name       string   // the table name
	Schema     string   // the SQL to run to create the table
	Columns    []string // the columns in the order that will be used to dump and load the data
	Type       string   // TABLE_BASE_TABLE or TABLE_VIEW
	DataLength uint64   // how much space the data file takes.
}

// helper methods for sorting
type TableDefinitions []TableDefinition

func (tds TableDefinitions) Len() int {
	return len(tds)
}

func (tds TableDefinitions) Swap(i, j int) {
	tds[i], tds[j] = tds[j], tds[i]
}

// sort by reverse DataLength
type ByReverseDataLength struct {
	TableDefinitions
}

func (bdl ByReverseDataLength) Less(i, j int) bool {
	return bdl.TableDefinitions[j].DataLength < bdl.TableDefinitions[i].DataLength
}

type SchemaDefinition struct {
	// the 'CREATE DATABASE...' statement, with db name as {{.DatabaseName}}
	DatabaseSchema string

	// ordered by TableDefinition.Name by default
	TableDefinitions TableDefinitions

	// the md5 of the concatenation of TableDefinition.Schema
	Version string
}

func (sd *SchemaDefinition) String() string {
	return jscfg.ToJson(sd)
}

func (sd *SchemaDefinition) SortByReverseDataLength() {
	sort.Sort(ByReverseDataLength{sd.TableDefinitions})
}

func (sd *SchemaDefinition) generateSchemaVersion() {
	hasher := md5.New()
	for _, td := range sd.TableDefinitions {
		if _, err := hasher.Write([]byte(td.Schema)); err != nil {
			panic(err) // extremely unlikely
		}
	}
	sd.Version = hex.EncodeToString(hasher.Sum(nil))
}

func (sd *SchemaDefinition) GetTable(table string) (td *TableDefinition, ok bool) {
	for _, td := range sd.TableDefinitions {
		if td.Name == table {
			return &td, true
		}
	}
	return nil, false
}

// generates a report on what's different between two SchemaDefinition
// for now, we skip the VIEW entirely.
func DiffSchema(leftName string, left *SchemaDefinition, rightName string, right *SchemaDefinition, er concurrency.ErrorRecorder) {
	if left.DatabaseSchema != right.DatabaseSchema {
		er.RecordError(fmt.Errorf("%v and %v don't agree on database creation command:\n%v\n differs from:\n%v", leftName, rightName, left.DatabaseSchema, right.DatabaseSchema))
	}

	leftIndex := 0
	rightIndex := 0
	for leftIndex < len(left.TableDefinitions) && rightIndex < len(right.TableDefinitions) {
		// skip views
		if left.TableDefinitions[leftIndex].Type == TABLE_VIEW {
			leftIndex++
			continue
		}
		if right.TableDefinitions[rightIndex].Type == TABLE_VIEW {
			rightIndex++
			continue
		}

		// extra table on the left side
		if left.TableDefinitions[leftIndex].Name < right.TableDefinitions[rightIndex].Name {
			er.RecordError(fmt.Errorf("%v has an extra table named %v", leftName, left.TableDefinitions[leftIndex].Name))
			leftIndex++
			continue
		}

		// extra table on the right side
		if left.TableDefinitions[leftIndex].Name > right.TableDefinitions[rightIndex].Name {
			er.RecordError(fmt.Errorf("%v has an extra table named %v", rightName, right.TableDefinitions[rightIndex].Name))
			rightIndex++
			continue
		}

		// same name, let's see content
		if left.TableDefinitions[leftIndex].Schema != right.TableDefinitions[rightIndex].Schema {
			er.RecordError(fmt.Errorf("%v and %v disagree on schema for table %v:\n%v\n differs from:\n%v", leftName, rightName, left.TableDefinitions[leftIndex].Name, left.TableDefinitions[leftIndex].Schema, right.TableDefinitions[rightIndex].Schema))
		}
		leftIndex++
		rightIndex++
	}

	for leftIndex < len(left.TableDefinitions) {
		if left.TableDefinitions[leftIndex].Type == TABLE_BASE_TABLE {
			er.RecordError(fmt.Errorf("%v has an extra table named %v", leftName, left.TableDefinitions[leftIndex].Name))
		}
		leftIndex++
	}
	for rightIndex < len(right.TableDefinitions) {
		if right.TableDefinitions[rightIndex].Type == TABLE_BASE_TABLE {
			er.RecordError(fmt.Errorf("%v has an extra table named %v", rightName, right.TableDefinitions[rightIndex].Name))
		}
		rightIndex++
	}
}

func DiffSchemaToArray(leftName string, left *SchemaDefinition, rightName string, right *SchemaDefinition) (result []string) {
	er := concurrency.AllErrorRecorder{}
	DiffSchema(leftName, left, rightName, right, &er)
	if er.HasErrors() {
		return er.Errors
	} else {
		return nil
	}
}

var autoIncr = regexp.MustCompile(" AUTO_INCREMENT=\\d+")

// GetSchema returns the schema for database for tables listed in
// tables. If tables is empty, return the schema for all tables.
func (mysqld *Mysqld) GetSchema(dbName string, tables []string, includeViews bool) (*SchemaDefinition, error) {
	sd := &SchemaDefinition{}

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
		sql += " AND table_type = '" + TABLE_BASE_TABLE + "'"
	}
	qr, err := mysqld.fetchSuperQuery(sql)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		return sd, nil
	}

	sd.TableDefinitions = make([]TableDefinition, len(qr.Rows))
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
		if tableType == TABLE_VIEW {
			// Views will have the dbname in there, replace it
			// with {{.DatabaseName}}
			norm = strings.Replace(norm, "`"+dbName+"`", "`{{.DatabaseName}}`", -1)
		}

		sd.TableDefinitions[i].Name = tableName
		sd.TableDefinitions[i].Schema = norm

		columns, err := mysqld.GetColumns(dbName, tableName)
		if err != nil {
			return nil, err
		}
		sd.TableDefinitions[i].Columns = columns
		sd.TableDefinitions[i].Type = tableType
		sd.TableDefinitions[i].DataLength = dataLength
	}

	sd.generateSchemaVersion()
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

type SchemaChange struct {
	Sql              string
	Force            bool
	AllowReplication bool
	BeforeSchema     *SchemaDefinition
	AfterSchema      *SchemaDefinition
}

type SchemaChangeResult struct {
	BeforeSchema *SchemaDefinition
	AfterSchema  *SchemaDefinition
}

func (scr *SchemaChangeResult) String() string {
	return jscfg.ToJson(scr)
}

func (mysqld *Mysqld) PreflightSchemaChange(dbName string, change string) (*SchemaChangeResult, error) {
	// gather current schema on real database
	beforeSchema, err := mysqld.GetSchema(dbName, nil, false)
	if err != nil {
		return nil, err
	}

	// populate temporary database with it
	sql := "SET sql_log_bin = 0;\n"
	sql += "DROP DATABASE IF EXISTS _vt_preflight;\n"
	sql += "CREATE DATABASE _vt_preflight;\n"
	sql += "USE _vt_preflight;\n"
	for _, td := range beforeSchema.TableDefinitions {
		if td.Type == TABLE_BASE_TABLE {
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
	afterSchema, err := mysqld.GetSchema("_vt_preflight", nil, false)
	if err != nil {
		return nil, err
	}

	// and clean up the extra database
	sql = "SET sql_log_bin = 0;\n"
	sql += "DROP DATABASE _vt_preflight;\n"
	if err = mysqld.ExecuteMysqlCommand(sql); err != nil {
		return nil, err
	}

	return &SchemaChangeResult{beforeSchema, afterSchema}, nil
}

func (mysqld *Mysqld) ApplySchemaChange(dbName string, change *SchemaChange) (*SchemaChangeResult, error) {
	// check current schema matches
	beforeSchema, err := mysqld.GetSchema(dbName, nil, false)
	if err != nil {
		return nil, err
	}
	if change.BeforeSchema != nil {
		schemaDiffs := DiffSchemaToArray("actual", beforeSchema, "expected", change.BeforeSchema)
		if len(schemaDiffs) > 0 {
			for _, msg := range schemaDiffs {
				relog.Warning("BeforeSchema differs: %v", msg)
			}

			// let's see if the schema was already applied
			if change.AfterSchema != nil {
				schemaDiffs = DiffSchemaToArray("actual", beforeSchema, "expected", change.AfterSchema)
				if len(schemaDiffs) == 0 {
					// no diff between the schema we expect
					// after the change and the current
					// schema, we already applied it
					return &SchemaChangeResult{beforeSchema, beforeSchema}, nil
				}
			}

			if change.Force {
				relog.Warning("BeforeSchema differs, applying anyway")
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
		schemaDiffs := DiffSchemaToArray("actual", afterSchema, "expected", change.AfterSchema)
		if len(schemaDiffs) > 0 {
			for _, msg := range schemaDiffs {
				relog.Warning("AfterSchema differs: %v", msg)
			}
			if change.Force {
				relog.Warning("AfterSchema differs, not reporting error")
			} else {
				return nil, fmt.Errorf("AfterSchema differs")
			}
		}
	}

	return &SchemaChangeResult{beforeSchema, afterSchema}, nil
}
