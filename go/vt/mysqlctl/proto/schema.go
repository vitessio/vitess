// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/concurrency"
)

const (
	TABLE_BASE_TABLE = "BASE TABLE"
	TABLE_VIEW       = "VIEW"
)

type TableDefinition struct {
	Name              string   // the table name
	Schema            string   // the SQL to run to create the table
	Columns           []string // the columns in the order that will be used to dump and load the data
	PrimaryKeyColumns []string // the columns used by the primary key, in order
	Type              string   // TABLE_BASE_TABLE or TABLE_VIEW
	DataLength        uint64   // how much space the data file takes.
	RowCount          uint64   // how many rows in the table (may
	// be approximate count)
}

// helper methods for sorting
type TableDefinitions []*TableDefinition

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

func (sd *SchemaDefinition) GenerateSchemaVersion() {
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
			return td, true
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
		return er.ErrorStrings()
	} else {
		return nil
	}
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
