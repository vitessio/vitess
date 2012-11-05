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
	Name   string // the table name
	Schema string // the SQL to run to create the table
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

var autoIncr = regexp.MustCompile("auto_increment=\\d+")

// Return the schema for a database
func (mysqld *Mysqld) GetSchema(dbName string) (*SchemaDefinition, error) {
	rows, err := mysqld.fetchSuperQuery("SHOW TABLES IN " + dbName)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return &SchemaDefinition{}, nil
	}
	sd := &SchemaDefinition{TableDefinitions: make([]TableDefinition, len(rows))}
	for i, row := range rows {
		tableName := row[0].String()
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
	}

	sd.generateSchemaVersion()
	return sd, nil
}

type SchemaChange struct {
	Sql              string
	UseDb            string
	Force            bool
	AllowReplication bool
	BeforeSchema     *SchemaDefinition
	AfterSchema      *SchemaDefinition
}

type SchemaChangeResult struct {
	Error        string
	BeforeSchema *SchemaDefinition
	AfterSchema  *SchemaDefinition
}

func (scr *SchemaChangeResult) String() string {
	return jscfg.ToJson(scr)
}

func (mysqld *Mysqld) ApplySchemaChange(dbName string, change *SchemaChange) (result *SchemaChangeResult) {
	result = &SchemaChangeResult{}

	// check current schema matches
	var err error
	result.BeforeSchema, err = mysqld.GetSchema(dbName)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	if change.BeforeSchema != nil {
		schemaDiffs := make(chan string, 10)
		go func() {
			result.BeforeSchema.DiffSchema("actual", "expected", change.BeforeSchema, schemaDiffs)
			close(schemaDiffs)
		}()
		differs := false
		for msg := range schemaDiffs {
			differs = true
			relog.Warning("BeforeSchema differs: %v", msg)
		}
		if differs {
			if change.Force {
				relog.Warning("BeforeSchema differs, applying anyway")
			} else {
				result.Error = "BeforeSchema differs"
				return result
			}
		}
	}

	sql := change.Sql
	if !change.AllowReplication {
		sql = "SET sql_log_bin = 0;\n" + sql
	}

	// add a 'use XXX' in front of the SQL
	if change.UseDb != "" {
		sql = "USE " + change.UseDb + ";\n" + sql
	} else {
		sql = "USE " + dbName + ";\n" + sql
	}

	// execute the schema change using an external mysql process
	// (to benefit from the extra commands in mysql cli)
	err = mysqld.ExecuteMysqlCommand(sql)
	if err != nil {
		result.Error = err.Error()
		return result
	}

	// populate AfterSchema
	result.AfterSchema, err = mysqld.GetSchema(dbName)
	if err != nil {
		result.Error = err.Error()
		return result
	}

	// compare to the provided AfterSchema
	if change.AfterSchema != nil {
		schemaDiffs := make(chan string, 10)
		go func() {
			result.AfterSchema.DiffSchema("actual", "expected", change.AfterSchema, schemaDiffs)
			close(schemaDiffs)
		}()
		differs := false
		for msg := range schemaDiffs {
			differs = true
			relog.Warning("AfterSchema differs: %v", msg)
		}
		if differs {
			if change.Force {
				relog.Warning("AfterSchema differs, not reporting error")
			} else {
				result.Error = "AfterSchema differs"
				return result
			}
		}
	}

	return result
}
