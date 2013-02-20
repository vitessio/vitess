// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
)

type TableDefinition struct {
	Name    string   // the table name
	Schema  string   // the SQL to run to create the table
	Columns []string // the columns in the order that will be used to dump and load the data
}

type SchemaDefinition struct {
	// ordered by TableDefinition.Name
	TableDefinitions []TableDefinition

	// the md5 of the concatenation of TableDefinition.Schema
	Version string
}

func (sd *SchemaDefinition) String() string {
	return jscfg.ToJson(sd)
}

func (sd *SchemaDefinition) generateSchemaVersion() {
	hasher := md5.New()
	for _, td := range sd.TableDefinitions {
		if _, err := hasher.Write([]byte(td.Schema)); err != nil {
			// extremely unlikely
			panic(err)
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
func (left *SchemaDefinition) DiffSchema(leftName, rightName string, right *SchemaDefinition, result chan string) {
	leftIndex := 0
	rightIndex := 0
	for leftIndex < len(left.TableDefinitions) && rightIndex < len(right.TableDefinitions) {
		// extra table on the left side
		if left.TableDefinitions[leftIndex].Name < right.TableDefinitions[rightIndex].Name {
			result <- leftName + " has an extra table named " + left.TableDefinitions[leftIndex].Name
			leftIndex++
			continue
		}

		// extra table on the right side
		if left.TableDefinitions[leftIndex].Name > right.TableDefinitions[rightIndex].Name {
			result <- rightName + " has an extra table named " + right.TableDefinitions[rightIndex].Name
			rightIndex++
			continue
		}

		// same name, let's see content
		if left.TableDefinitions[leftIndex].Schema != right.TableDefinitions[rightIndex].Schema {
			result <- leftName + " and " + rightName + " disagree on schema for table " + left.TableDefinitions[leftIndex].Name
		}
		leftIndex++
		rightIndex++
	}

	for leftIndex < len(left.TableDefinitions) {
		result <- leftName + " has an extra table named " + left.TableDefinitions[leftIndex].Name
		leftIndex++
	}
	for rightIndex < len(right.TableDefinitions) {
		result <- rightName + " has an extra table named " + right.TableDefinitions[rightIndex].Name
		rightIndex++
	}
	return
}

func (left *SchemaDefinition) DiffSchemaToArray(leftName, rightName string, right *SchemaDefinition) (result []string) {
	schemaDiffs := make(chan string, 10)
	go func() {
		left.DiffSchema(leftName, rightName, right, schemaDiffs)
		close(schemaDiffs)
	}()
	result = make([]string, 0, 10)
	for msg := range schemaDiffs {
		result = append(result, msg)
	}
	return result
}

var autoIncr = regexp.MustCompile("auto_increment=\\d+")

// GetSchema returns the schema for database for tables listed in
// tables. If tables is empty, return the schema for all tables.
func (mysqld *Mysqld) GetSchema(dbName string, tables []string) (*SchemaDefinition, error) {
	if len(tables) == 0 {
		rows, err := mysqld.fetchSuperQuery("SHOW TABLES IN " + dbName)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return &SchemaDefinition{}, nil
		}
		tables = make([]string, len(rows))
		for i, row := range rows {
			tables[i] = row[0].String()
		}
	}
	sd := &SchemaDefinition{TableDefinitions: make([]TableDefinition, len(tables))}
	for i, tableName := range tables {
		relog.Info("GetSchema(table: %v)", tableName)

		rows, fetchErr := mysqld.fetchSuperQuery("SHOW CREATE TABLE " + dbName + "." + tableName)
		if fetchErr != nil {
			return nil, fetchErr
		}
		if len(rows) == 0 {
			return nil, fmt.Errorf("empty create table statement for %v", tableName)
		}

		// Normalize & remove auto_increment because it changes on every insert
		// FIXME(alainjobart) find a way to share this with
		// vt/tabletserver/table_info.go:162
		norm1 := strings.ToLower(rows[0][1].String())
		norm2 := autoIncr.ReplaceAllLiteralString(norm1, "")

		sd.TableDefinitions[i].Name = tableName
		sd.TableDefinitions[i].Schema = norm2

		columns, err := mysqld.GetColumns(dbName, tableName)
		if err != nil {
			return nil, err
		}
		sd.TableDefinitions[i].Columns = columns
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
	qr, err := conn.ExecuteFetch([]byte(fmt.Sprintf("select * from %v.%v where 1=0", dbName, table)), 0, true)
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
	beforeSchema, err := mysqld.GetSchema(dbName, nil)
	if err != nil {
		return nil, err
	}

	// populate temporary database with it
	sql := "SET sql_log_bin = 0;\n"
	sql += "DROP DATABASE IF EXISTS _vt_preflight;\n"
	sql += "CREATE DATABASE _vt_preflight;\n"
	sql += "USE _vt_preflight;\n"
	for _, td := range beforeSchema.TableDefinitions {
		sql += td.Schema + ";\n"
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
	afterSchema, err := mysqld.GetSchema("_vt_preflight", nil)
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
	beforeSchema, err := mysqld.GetSchema(dbName, nil)
	if err != nil {
		return nil, err
	}
	if change.BeforeSchema != nil {
		schemaDiffs := beforeSchema.DiffSchemaToArray("actual", "expected", change.BeforeSchema)
		if len(schemaDiffs) > 0 {
			for _, msg := range schemaDiffs {
				relog.Warning("BeforeSchema differs: %v", msg)
			}

			// let's see if the schema was already applied
			if change.AfterSchema != nil {
				schemaDiffs = beforeSchema.DiffSchemaToArray("actual", "expected", change.AfterSchema)
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
	afterSchema, err := mysqld.GetSchema(dbName, nil)
	if err != nil {
		return nil, err
	}

	// compare to the provided AfterSchema
	if change.AfterSchema != nil {
		schemaDiffs := afterSchema.DiffSchemaToArray("actual", "expected", change.AfterSchema)
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
