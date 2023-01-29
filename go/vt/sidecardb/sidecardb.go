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
	CreateSidecarDatabaseQuery = "create database if not exists _vt"
	UseSidecarDatabaseQuery    = "use _vt"
	ShowSidecarDatabasesQuery  = "SHOW DATABASES LIKE '_vt'"
	SelectCurrentDatabaseQuery = "select database()"
	ShowCreateTableQuery       = "show create table _vt.%s"
	GetCurrentTablesQuery      = "show tables from _vt"

	CreateTableRegexp = "CREATE TABLE .* `_vt`.*"
	AlterTableRegexp  = "ALTER TABLE `_vt`.*"
)

// all tables needed in the sidecar database have their schema in the schema subdirectory
//
//go:embed schema/*
var schemaLocation embed.FS

type sidecarTable struct {
	module string // which module uses this table
	path   string // path of the schema relative to this module
	name   string // table name
	schema string // create table dml
}

func (t *sidecarTable) String() string {
	return fmt.Sprintf("_vt.%s (%s)", t.name, t.module)
}

var sidecarTables []*sidecarTable
var ddlCount *stats.Counter

func init() {
	initSchemaFiles()
	ddlCount = stats.NewCounter("SidecarDbDDLQueryCount", "Number of create/upgrade queries executed")
}

func validateSchemaDefinition(name, schema string) (string, error) {
	stmt, err := sqlparser.ParseStrictDDL(schema)

	if err != nil {
		return "", err
	}
	createTable, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected CREATE TABLE. Got %v", sqlparser.CanonicalString(stmt))
	}
	tableName := createTable.Table.Name.String()
	qualifier := createTable.Table.Qualifier.String()
	if qualifier != SidecarDBName {
		return "", fmt.Errorf("database qualifier for %s is %s, not %s", name, qualifier, SidecarDBName)
	}
	if tableName != name {
		return "", fmt.Errorf("table in file name for %s does not match the table specified:%s in it", name, tableName)
	}
	if !createTable.IfNotExists {
		return "", fmt.Errorf("if not exists is not defined on table %s", name)
	}
	normalizedSchema := sqlparser.CanonicalString(createTable)
	return normalizedSchema, nil
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
			var normalizedSchema string
			if normalizedSchema, err = validateSchemaDefinition(name, string(schema)); err != nil {
				return err
			}
			sidecarTables = append(sidecarTables, &sidecarTable{name: name, module: module, path: path, schema: normalizedSchema})
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

type schemaInit struct {
	ctx            context.Context
	exec           Exec
	existingTables map[string]bool
	dbCreated      bool // the first upgrade/create query will also create the sidecar database if required
}

// Exec is the prototype of a callback that has to be passed to Init() to execute the specified query in the database
type Exec func(ctx context.Context, query string, maxRows int, useDB bool) (*sqltypes.Result, error)

// GetDDLCount metric returns the count of sidecardb ddls that have been run as part of this vttablet's init process.
func GetDDLCount() int64 {
	return ddlCount.Get()
}

// Init creates or upgrades the sidecar database based on declarative schema for all tables in the schema
func Init(ctx context.Context, exec Exec) error {
	printCallerDetails() // for debug purposes only, remove in v17
	log.Infof("Starting sidecardb.Init()")
	si := &schemaInit{
		ctx:  ctx,
		exec: exec,
	}

	// there are paths in the tablet initialization where we are in read-only mode but the schema is already updated
	// hence we should not always try to create the database, since it will then error out as the db is read-only
	dbExists, err := si.doesSidecarDBExist()
	if err != nil {
		return err
	}
	if !dbExists {
		if err := si.createSidecarDB(); err != nil {
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

	for _, table := range sidecarTables {
		if err := si.createOrUpgradeTable(table); err != nil {
			return err
		}
	}
	return nil
}

func (si *schemaInit) doesSidecarDBExist() (bool, error) {
	rs, err := si.exec(si.ctx, ShowSidecarDatabasesQuery, 2, false)
	if err != nil {
		log.Error(err)
		return false, err
	}

	switch len(rs.Rows) {
	case 0:
		log.Infof("doesSidecarDBExist: not found")
		return false, nil
	case 1:
		log.Infof("doesSidecarDBExist: found")
		return true, nil
	default:
		log.Errorf("found too many rows for sidecarDB %s: %d", SidecarDBName, len(rs.Rows))
		return false, fmt.Errorf("found too many rows for sidecarDB %s: %d", SidecarDBName, len(rs.Rows))
	}
}

func (si *schemaInit) createSidecarDB() error {
	_, err := si.exec(si.ctx, CreateSidecarDatabaseQuery, 1, false)
	if err != nil {
		log.Error(err)
		return err
	}
	log.Infof("createSidecarDB: %s", CreateSidecarDatabaseQuery)
	return nil
}

// sets db of current connection, returning the currently selected database
func (si *schemaInit) setCurrentDatabase(dbName string) (string, error) {
	rs, err := si.exec(si.ctx, SelectCurrentDatabaseQuery, 1, false)
	if err != nil {
		return "", err
	}
	if rs == nil || rs.Rows == nil { // we get this in tests
		return "", nil
	}
	currentDB := rs.Rows[0][0].ToString()
	if currentDB != "" { // while running tests we can get currentDB as empty
		_, err = si.exec(si.ctx, fmt.Sprintf("use %s", dbName), 1, false)
		if err != nil {
			return "", err
		}
	}
	return currentDB, nil
}

// gets existing schema of table
func (si *schemaInit) getCurrentSchema(tableName string) (string, error) {
	var currentTableSchema string

	rs, err := si.exec(si.ctx, fmt.Sprintf(ShowCreateTableQuery, tableName), 1, false)
	if err != nil {
		log.Errorf("Error getting table schema for %s: %+v", tableName, err)
		return "", err
	}
	if len(rs.Rows) > 0 {
		currentTableSchema = rs.Rows[0][1].ToString()
	}
	return currentTableSchema, nil
}

// finds diff that needs to be applied to current table schema to get the desired one. Will be an empty string if they match.
// could be a create statement if the table does not exist or an alter if table exists but has a different schema
func (si *schemaInit) findTableSchemaDiff(current, desired string) (string, error) {
	hints := &schemadiff.DiffHints{
		TableCharsetCollateStrategy: schemadiff.TableCharsetCollateIgnoreAlways,
	}
	diff, err := schemadiff.DiffCreateTablesQueries(current, desired, hints)
	if err != nil {
		return "", err
	}

	var tableAlterSQL string
	if diff != nil {
		tableAlterSQL = diff.CanonicalStatementString()

		// temp logging to debug any eventual issues around the new schema init, should be removed in v17
		log.Infof("alter sql %s", tableAlterSQL)
		log.Infof("current schema %s", current)
	}

	return tableAlterSQL, nil
}

func (si *schemaInit) createOrUpgradeTable(table *sidecarTable) error {
	ctx := si.ctx
	desiredTableSchema := table.schema

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
			// We use CreateSidecarDatabaseQuery to also create the first binlog entry when a primary comes up.
			// That statement doesn't make it to the replicas, so we run the query again so that it is replicated
			// to the replicas so that the replicas can create the sidecar database.
			if err := si.createSidecarDB(); err != nil {
				return err
			}
			si.dbCreated = true
		}
		_, err := si.exec(ctx, tableAlterSQL, 1, true)
		if err != nil {
			sqlErr, isSQLErr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
			if isSQLErr && sqlErr != nil && sqlErr.Number() == mysql.ERTableExists {
				// should never come here
				log.Infof("table %s already exists", table)
				return nil
			}
			log.Errorf("Error altering table %s: %+v", table, err)
			return err
		}
		ddlCount.Add(1)

		if tableExists {
			log.Infof("Updated table %s: %s", table, tableAlterSQL)
		} else {
			log.Infof("Created %s", table)
		}
		return nil
	}
	log.Infof("Table %s was already correct", table.name)
	return nil
}

// is table already present?
func (si *schemaInit) tableExists(tableName string) bool {
	_, ok := si.existingTables[tableName]
	return ok
}

// cache existing tables in the sidecar database
func (si *schemaInit) loadExistingTables() error {
	si.existingTables = make(map[string]bool)
	rs, err := si.exec(si.ctx, GetCurrentTablesQuery, -1, false)
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
var sidecarDBInitQueryPatterns = []string{
	ShowSidecarDatabasesQuery,
	SelectCurrentDatabaseQuery,
	CreateSidecarDatabaseQuery,
	UseSidecarDatabaseQuery,
	GetCurrentTablesQuery,
	CreateTableRegexp,
	AlterTableRegexp,
}

// AddSidecarDBSchemaInitQueries adds sidecar database schema related queries to a mock db
func AddSidecarDBSchemaInitQueries(db *fakesqldb.DB) {
	result := &sqltypes.Result{}
	for _, q := range sidecarDBInitQueryPatterns {
		db.AddQueryPattern(q, result)
	}
	for _, table := range sidecarTables {
		result = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"Table|Create Table",
			"varchar|varchar"),
			fmt.Sprintf("%s|%s", table.name, table.schema),
		)
		db.AddQuery(fmt.Sprintf(ShowCreateTableQuery, table.name), result)
	}
}

// MatchesSidecarDBInitQuery returns true if query has one of the test patterns as a substring, or it matches a provided regexp
func MatchesSidecarDBInitQuery(query string) bool {
	query = strings.ToLower(query)
	for _, q := range sidecarDBInitQueryPatterns {
		q = strings.ToLower(q)
		if strings.Contains(query, q) {
			return true
		}
		if match, _ := regexp.MatchString(q, query); match {
			return true
		}
	}
	return false
}

// endregion
