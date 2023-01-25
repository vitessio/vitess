/*
Copyright 2023 The Vitess Authors.

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

package sidecardb

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"vitess.io/vitess/go/mysql"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/stats"

	"vitess.io/vitess/go/mysql/fakesqldb"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schemadiff"
)

const (
	SidecarDBName              = "_vt"
	CreateVTDatabaseQuery      = "create database if not exists _vt"
	UseVTDatabaseQuery         = "use _vt"
	ShowVTDatabasesQuery       = "SHOW DATABASES LIKE '_vt'"
	SelectCurrentDatabaseQuery = "select database()"
	ShowCreateTableQuery       = "show create table _vt.%s"
	GetCurrentTablesQuery      = "show tables from _vt"

	CreateTableRegexp = "CREATE TABLE .* _vt.*"
	AlterTableRegexp  = "ALTER TABLE _vt.*"
)

// all tables needed in _vt sidecar have their schema in the vtschema subdirectory
//
//go:embed vtschema/*
var schemaLocation embed.FS

type vtTable struct {
	module string // which module uses this table
	path   string // path of the schema relative to this module
	name   string // table name
	schema string // create table dml
}

func (t *vtTable) String() string {
	return fmt.Sprintf("_vt.%s (%s)", t.name, t.module)
}

var vtTables []*vtTable
var ddlCount *stats.Counter

func init() {
	initSchemaFiles()
	ddlCount = stats.NewCounter("SidecarDbDDLQueryCount", "Number of create/upgrade queries executed")
}

func validateSchemaDefinition(name, schema string) error {
	stmt, err := sqlparser.ParseStrictDDL(schema)
	if err != nil {
		return err
	}
	createTable, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected CREATE TABLE. Got %v", sqlparser.CanonicalString(stmt))
	}
	tableName := createTable.Table.Name.String()
	qualifier := createTable.Table.Qualifier.String()
	if qualifier != SidecarDBName {
		return fmt.Errorf("database qualifier for %s is %s, not %s", name, qualifier, SidecarDBName)
	}
	if tableName != name {
		return fmt.Errorf("table in file name for %s does not match the table specified:%s in it", name, tableName)
	}
	if !createTable.IfNotExists {
		return fmt.Errorf("if not exists is not defined on table %s", name)
	}
	return nil
}

func initSchemaFiles() {
	err := fs.WalkDir(schemaLocation, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			module, name := filepath.Split(path)
			arr := strings.Split(module, "/")
			if len(arr) >= 1 {
				module = arr[1]
			}
			name = strings.Split(name, ".")[0]
			schema, err := schemaLocation.ReadFile(path)
			if err != nil {
				panic(err)
			}
			if err := validateSchemaDefinition(name, string(schema)); err != nil {
				return err
			}
			vtTables = append(vtTables, &vtTable{name: name, module: module, path: path, schema: string(schema)})
		}
		return nil
	})
	if err != nil {
		log.Errorf("error loading schema files: %+v", err)
	}
}

// printCallerDetails is a helper for dev debugging
func printCallerDetails() {
	pc, _, line, ok := runtime.Caller(2)
	details := runtime.FuncForPC(pc)
	if ok && details != nil {
		log.Infof("_vt schema init called from %s:%d\n", details.Name(), line)
	}
}

type vtSchemaInit struct {
	ctx            context.Context
	exec           Exec
	existingTables map[string]bool
	dbCreated      bool // the first upgrade/create query will be prefixed by a _vt creation script
}

// Exec is the prototype of a callback that has to be passed to Init() to execute the specified query in the database
type Exec func(ctx context.Context, query string, maxRows int, wantFields bool, useVT bool) (*sqltypes.Result, error)

// GetDDLCount metric returns the count of sidecardb ddls that have been run as part of this vttablet's init process.
func GetDDLCount() int64 {
	return ddlCount.Get()
}

// Init creates or upgrades the _vt sidecar database based on declarative schema for all _vt tables
func Init(ctx context.Context, exec Exec) error {
	printCallerDetails() // for debug purposes only, remove in v18
	log.Infof("Starting sidecardb Init()")
	si := &vtSchemaInit{
		ctx:  ctx,
		exec: exec,
	}
	dbExists, err := si.doesVTDatabaseExist()
	if err != nil {
		return err
	}
	if !dbExists {
		if err := si.createVTDatabase(); err != nil {
			return err
		}
		si.dbCreated = true
	}

	if _, err := si.setCurrentDatabase(SidecarDBName); err != nil {
		return err
	}

	if err := si.loadExistingTables(); err != nil {
		return err
	}

	for _, table := range vtTables {
		if err := si.createOrUpgradeTable(table); err != nil {
			return err
		}
	}
	return nil
}

func (si *vtSchemaInit) doesVTDatabaseExist() (bool, error) {
	rs, err := si.exec(si.ctx, ShowVTDatabasesQuery, 2, false, false)
	if err != nil {
		log.Error(err)
		return false, err
	}

	switch len(rs.Rows) {
	case 0:
		log.Infof("doesVTDatabaseExist: not found")
		return false, nil
	case 1:
		log.Infof("doesVTDatabaseExist: found")
		return true, nil
	default:
		log.Errorf("found too many rows for _vt: %d", len(rs.Rows))
		return false, fmt.Errorf("found too many rows for _vt: %d", len(rs.Rows))
	}
}

func (si *vtSchemaInit) createVTDatabase() error {
	_, err := si.exec(si.ctx, CreateVTDatabaseQuery, 1, false, false)
	if err != nil {
		log.Error(err)
		return err
	}
	log.Infof("createVTDatabase: %s", CreateVTDatabaseQuery)
	return nil
}

// sets db of current connection, returning the currently selected database
func (si *vtSchemaInit) setCurrentDatabase(dbName string) (string, error) {
	rs, err := si.exec(si.ctx, SelectCurrentDatabaseQuery, 1, false, false)
	if err != nil {
		return "", err
	}
	if rs == nil || rs.Rows == nil { // we get this in tests
		return "", nil
	}
	currentDB := rs.Rows[0][0].ToString()
	if currentDB != "" { // while running tests we can get currentDB as empty
		_, err = si.exec(si.ctx, fmt.Sprintf("use %s", dbName), 1000, false, false)
		if err != nil {
			return "", err
		}
	}
	return currentDB, nil
}

// gets existing schema of table
func (si *vtSchemaInit) getCurrentSchema(tableName string) (string, error) {
	var currentTableSchema string

	rs, err := si.exec(si.ctx, fmt.Sprintf(ShowCreateTableQuery, tableName), 1, false, false)
	if err != nil {
		log.Errorf("Error getting _vt table schema for %s: %+v", tableName, err)
		return "", err
	}
	if len(rs.Rows) > 0 {
		currentTableSchema = rs.Rows[0][1].ToString()
	}
	return currentTableSchema, nil
}

// remove the extra DEFAULT CHARSET which we get from "show create table"
func stripCharset(schema string) string {
	ind := strings.Index(schema, "DEFAULT CHARSET")
	if ind <= 0 {
		return schema
	}
	return schema[:ind]
}

// finds diff that needs to be applied to current table schema to get the desired one. Will be an empty string if they match.
// could be a create statement if the table does not exist or an alter if table exists but has a different schema
func (si *vtSchemaInit) findTableSchemaDiff(current, desired string) (string, error) {
	// todo: remove once this is fixed in schemadiff
	current = stripCharset(current)

	hints := &schemadiff.DiffHints{}
	diff, err := schemadiff.DiffCreateTablesQueries(current, desired, hints)
	if err != nil {
		return "", err
	}

	var tableAlterSQL string
	if diff != nil {
		tableAlterSQL = diff.CanonicalStatementString()

		// temp logging, should be removed in v17
		log.Infof("alter sql %s", tableAlterSQL)
		log.Infof("current schema %s", current)
	}

	return tableAlterSQL, nil
}

// createOrUpgradeTable expects that we are already in the _vt database
func (si *vtSchemaInit) createOrUpgradeTable(table *vtTable) error {
	var desiredTableSchema string
	ctx := si.ctx
	bytes, err := schemaLocation.ReadFile(table.path)
	if err != nil {
		return err
	}
	desiredTableSchema = string(bytes)

	var tableAlterSQL string
	tableExists := si.tableExists(table.name)
	if tableExists {
		currentTableSchema, err := si.getCurrentSchema(table.name)
		if err != nil {
			return err
		}
		tableAlterSQL, err = si.findTableSchemaDiff(currentTableSchema, desiredTableSchema)
		if err != nil {
			return err
		}
	} else {
		tableAlterSQL = desiredTableSchema
	}

	if tableAlterSQL != "" {
		if !si.dbCreated {
			// We use CreateVTDatabaseQuery to also create the first binlog entry when a primary comes up.
			// That statement doesn't make it to the replicas, so we run the query again so that it is replicated
			// to the replicas so that the replicas can create the _vt database.
			if err := si.createVTDatabase(); err != nil {
				return err
			}
			si.dbCreated = true
		}
		_, err = si.exec(ctx, tableAlterSQL, 1, false, true)
		if err != nil {
			sqlErr, isSQLErr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
			if isSQLErr && sqlErr != nil && sqlErr.Number() == mysql.ERTableExists {
				// should never come here
				log.Infof("table %s already exists", table)
				return nil
			}
			log.Errorf("Error altering _vt table %s: %+v", table, err)
			return err
		}
		ddlCount.Add(1)

		if tableExists {
			log.Infof("Updated _vt table %s: %s", table, tableAlterSQL)
		} else {
			log.Infof("Created %s", table)
		}
		return nil
	}
	log.Infof("Table %s was already correct", table.name)
	return nil
}

// is table already in _vt?
func (si *vtSchemaInit) tableExists(tableName string) bool {
	_, ok := si.existingTables[tableName]
	return ok
}

// cache existing tables from _vt
func (si *vtSchemaInit) loadExistingTables() error {
	si.existingTables = make(map[string]bool)
	rs, err := si.exec(si.ctx, GetCurrentTablesQuery, 1000, false, false)
	if err != nil {
		return err
	}
	for _, row := range rs.Rows {
		si.existingTables[row[0].ToString()] = true
	}
	return nil
}

// region unit-test-only
// query patterns to handle in mocks
var vtSchemaInitQueryPatterns = []string{
	ShowVTDatabasesQuery,
	SelectCurrentDatabaseQuery,
	CreateVTDatabaseQuery,
	UseVTDatabaseQuery,
	GetCurrentTablesQuery,
	CreateTableRegexp,
	AlterTableRegexp,
}

// AddVTSchemaInitQueries adds _vt schema related queries to a mock db
func AddVTSchemaInitQueries(db *fakesqldb.DB) {
	result := &sqltypes.Result{}
	for _, q := range vtSchemaInitQueryPatterns {
		db.AddQueryPattern(q, result)
	}
	for _, table := range vtTables {
		result = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"Table|Create Table",
			"varchar|varchar"),
			fmt.Sprintf("%s|%s", table.name, table.schema),
		)
		db.AddQuery(fmt.Sprintf(ShowCreateTableQuery, table.name), result)
	}
}

// MatchesVTInitQuery returns true if query has one of the test patterns as a substring, or it matches a provided regexp
func MatchesVTInitQuery(query string) bool {
	for _, q := range vtSchemaInitQueryPatterns {
		if strings.Contains(query, q) {
			return true
		}
		if match, _ := regexp.MatchString(strings.ToLower(q), strings.ToLower(query)); match {
			return true
		}
	}
	return false
}

// endregion
