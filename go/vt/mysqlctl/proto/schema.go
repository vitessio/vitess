// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/concurrency"
)

const (
	// TableBaseTable indicates the table type is a base table.
	TableBaseTable = "BASE TABLE"
	// TableView indicates the table type is a view.
	TableView = "VIEW"
)

// TableDefinition contains all schema information about a table.
type TableDefinition struct {
	Name              string   // the table name
	Schema            string   // the SQL to run to create the table
	Columns           []string // the columns in the order that will be used to dump and load the data
	PrimaryKeyColumns []string // the columns used by the primary key, in order
	Type              string   // TableBaseTable or TableView
	DataLength        uint64   // how much space the data file takes.
	RowCount          uint64   // how many rows in the table (may
	// be approximate count)
}

// TableDefinitions is a list of TableDefinition.
type TableDefinitions []*TableDefinition

// Len returns TableDefinitions length.
func (tds TableDefinitions) Len() int {
	return len(tds)
}

// Swap used for sorting TableDefinitions.
func (tds TableDefinitions) Swap(i, j int) {
	tds[i], tds[j] = tds[j], tds[i]
}

// SchemaDefinition defines schema for a certain database.
type SchemaDefinition struct {
	// the 'CREATE DATABASE...' statement, with db name as {{.DatabaseName}}
	DatabaseSchema string

	// ordered by TableDefinition.Name by default
	TableDefinitions TableDefinitions

	// the md5 of the concatenation of TableDefinition.Schema
	Version string
}

func (sd *SchemaDefinition) String() string {
	return jscfg.ToJSON(sd)
}

// FilterTables returns a copy which includes only
// whitelisted tables (tables), no blacklisted tables (excludeTables) and optionally views (includeViews).
func (sd *SchemaDefinition) FilterTables(tables, excludeTables []string, includeViews bool) (*SchemaDefinition, error) {
	copy := *sd
	copy.TableDefinitions = make([]*TableDefinition, 0, len(sd.TableDefinitions))

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

	for _, table := range sd.TableDefinitions {
		// check it's a table we want
		if tableRegexps != nil {
			foundMatch := false
			for _, tableRegexp := range tableRegexps {
				if tableRegexp.MatchString(table.Name) {
					foundMatch = true
					break
				}
			}
			if !foundMatch {
				continue
			}
		}
		excluded := false
		for _, tableRegexp := range excludeTableRegexps {
			if tableRegexp.MatchString(table.Name) {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}

		if !includeViews && table.Type == TableView {
			continue
		}

		copy.TableDefinitions = append(copy.TableDefinitions, table)
	}

	// Regenerate hash over tables because it may have changed.
	if copy.Version != "" {
		copy.GenerateSchemaVersion()
	}

	return &copy, nil
}

// GenerateSchemaVersion return a unique schema version string based on
// its TableDefinitions.
func (sd *SchemaDefinition) GenerateSchemaVersion() {
	hasher := md5.New()
	for _, td := range sd.TableDefinitions {
		if _, err := hasher.Write([]byte(td.Schema)); err != nil {
			panic(err) // extremely unlikely
		}
	}
	sd.Version = hex.EncodeToString(hasher.Sum(nil))
}

// GetTable returns TableDefinition for a given table name.
func (sd *SchemaDefinition) GetTable(table string) (td *TableDefinition, ok bool) {
	for _, td := range sd.TableDefinitions {
		if td.Name == table {
			return td, true
		}
	}
	return nil, false
}

// ToSQLStrings converts a SchemaDefinition to an array of SQL strings. The array contains all
// the SQL statements needed for creating the database, tables, and views - in that order.
// All SQL statements will have {{.DatabaseName}} in place of the actual db name.
func (sd *SchemaDefinition) ToSQLStrings() []string {
	sqlStrings := make([]string, 0, len(sd.TableDefinitions)+1)
	createViewSql := make([]string, 0, len(sd.TableDefinitions))

	sqlStrings = append(sqlStrings, sd.DatabaseSchema)

	for _, td := range sd.TableDefinitions {
		if td.Type == TableView {
			createViewSql = append(createViewSql, td.Schema)
		} else {
			lines := strings.Split(td.Schema, "\n")
			for i, line := range lines {
				if strings.HasPrefix(line, "CREATE TABLE `") {
					lines[i] = strings.Replace(line, "CREATE TABLE `", "CREATE TABLE `{{.DatabaseName}}`.`", 1)
				}
			}
			sqlStrings = append(sqlStrings, strings.Join(lines, "\n"))
		}
	}

	return append(sqlStrings, createViewSql...)
}

// DiffSchema generates a report on what's different between two SchemaDefinitions
// including views.
func DiffSchema(leftName string, left *SchemaDefinition, rightName string, right *SchemaDefinition, er concurrency.ErrorRecorder) {
	if left == nil && right == nil {
		return
	}
	if left == nil || right == nil {
		er.RecordError(fmt.Errorf("%v and %v are different, %s: %v, %s: %v", leftName, rightName, leftName, left, rightName, right))
		return
	}
	if left.DatabaseSchema != right.DatabaseSchema {
		er.RecordError(fmt.Errorf("%v and %v don't agree on database creation command:\n%v\n differs from:\n%v", leftName, rightName, left.DatabaseSchema, right.DatabaseSchema))
	}

	leftIndex := 0
	rightIndex := 0
	for leftIndex < len(left.TableDefinitions) && rightIndex < len(right.TableDefinitions) {
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

		if left.TableDefinitions[leftIndex].Type != right.TableDefinitions[rightIndex].Type {
			er.RecordError(fmt.Errorf("%v and %v disagree on table type for table %v:\n%v\n differs from:\n%v", leftName, rightName, left.TableDefinitions[leftIndex].Name, left.TableDefinitions[leftIndex].Type, right.TableDefinitions[rightIndex].Type))
		}

		leftIndex++
		rightIndex++
	}

	for leftIndex < len(left.TableDefinitions) {
		if left.TableDefinitions[leftIndex].Type == TableBaseTable {
			er.RecordError(fmt.Errorf("%v has an extra table named %v", leftName, left.TableDefinitions[leftIndex].Name))
		}
		if left.TableDefinitions[leftIndex].Type == TableView {
			er.RecordError(fmt.Errorf("%v has an extra view named %v", leftName, left.TableDefinitions[leftIndex].Name))
		}
		leftIndex++
	}
	for rightIndex < len(right.TableDefinitions) {
		if right.TableDefinitions[rightIndex].Type == TableBaseTable {
			er.RecordError(fmt.Errorf("%v has an extra table named %v", rightName, right.TableDefinitions[rightIndex].Name))
		}
		if right.TableDefinitions[rightIndex].Type == TableView {
			er.RecordError(fmt.Errorf("%v has an extra view named %v", rightName, right.TableDefinitions[rightIndex].Name))
		}
		rightIndex++
	}
}

// DiffSchemaToArray diffs two schemas and return the schema diffs if there is any.
func DiffSchemaToArray(leftName string, left *SchemaDefinition, rightName string, right *SchemaDefinition) (result []string) {
	er := concurrency.AllErrorRecorder{}
	DiffSchema(leftName, left, rightName, right, &er)
	if er.HasErrors() {
		return er.ErrorStrings()
	}
	return nil
}

// SchemaChange contains all necessary information to apply a schema change.
type SchemaChange struct {
	Sql              string
	Force            bool
	AllowReplication bool
	BeforeSchema     *SchemaDefinition
	AfterSchema      *SchemaDefinition
}

// SchemaChangeResult contains before and after table schemas for
// a schema change sql.
type SchemaChangeResult struct {
	BeforeSchema *SchemaDefinition
	AfterSchema  *SchemaDefinition
}

func (scr *SchemaChangeResult) String() string {
	return jscfg.ToJSON(scr)
}
