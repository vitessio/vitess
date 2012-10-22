// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"

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
func (left *SchemaDefinition) diffSchema(leftName, rightName string, right *SchemaDefinition) (result []string) {
	leftIndex := 0
	rightIndex := 0
	for leftIndex < len(left.TableDefinitions) && rightIndex < len(right.TableDefinitions) {
		// extra table on the left side
		if left.TableDefinitions[leftIndex].Name < right.TableDefinitions[rightIndex].Name {
			result = append(result, leftName+" has an extra table named "+left.TableDefinitions[leftIndex].Name)
			leftIndex++
			continue
		}

		// extra table on the right side
		if left.TableDefinitions[leftIndex].Name > right.TableDefinitions[rightIndex].Name {
			result = append(result, rightName+" has an extra table named "+right.TableDefinitions[rightIndex].Name)
			rightIndex++
			continue
		}

		// same name, let's see content
		if left.TableDefinitions[leftIndex].Schema != right.TableDefinitions[rightIndex].Schema {
			result = append(result, leftName+" and "+rightName+" disagree on schema for table "+left.TableDefinitions[leftIndex].Name)
		}
		leftIndex++
		rightIndex++
	}

	for leftIndex < len(left.TableDefinitions) {
		result = append(result, leftName+" has an extra table named "+left.TableDefinitions[leftIndex].Name)
		leftIndex++
	}
	for rightIndex < len(right.TableDefinitions) {
		result = append(result, rightName+" has an extra table named "+right.TableDefinitions[rightIndex].Name)
		rightIndex++
	}
	return
}

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
		tableName := row[0].(string)
		relog.Info("GetSchema(table: %v)", tableName)

		rows, fetchErr := mysqld.fetchSuperQuery("SHOW CREATE TABLE " + dbName + "." + tableName)
		if fetchErr != nil {
			return nil, fetchErr
		}
		if len(rows) == 0 {
			return nil, fmt.Errorf("empty create table statement for %v", tableName)
		}

		sd.TableDefinitions[i].Name = tableName
		sd.TableDefinitions[i].Schema = rows[0][1].(string)
	}

	sd.generateSchemaVersion()
	return sd, nil
}
