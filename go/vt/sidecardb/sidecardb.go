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

	"vitess.io/vitess/go/mysql/fakesqldb"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/stats"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schemadiff"
)

const (
	SidecarDBName              = "_vt"
	CreateSidecarDatabaseQuery = "create database if not exists _vt"
	UseSidecarDatabaseQuery    = "use _vt"
	ShowSidecarDatabasesQuery  = "SHOW DATABASES LIKE '\\_vt'"
	SelectCurrentDatabaseQuery = "select database()"
	ShowCreateTableQuery       = "show create table _vt.%s"

	CreateTableRegexp = "CREATE TABLE .* `\\_vt`\\..*"
	AlterTableRegexp  = "ALTER TABLE `\\_vt`\\..*"
)

// All tables needed in the sidecar database have their schema in the schema subdirectory.
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
	return fmt.Sprintf("%s.%s (%s)", SidecarDBName, t.name, t.module)
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
		return "", fmt.Errorf("database qualifier specified for the %s table is %s rather than the expected value of %s",
			name, qualifier, SidecarDBName)
	}
	if !strings.EqualFold(tableName, name) {
		return "", fmt.Errorf("table name of %s does not match the table name specified within the file: %s", name, tableName)
	}
	if !createTable.IfNotExists {
		return "", fmt.Errorf("%s file did not include the required IF NOT EXISTS clause in the CREATE TABLE statement for the %s table", name, tableName)
	}
	normalizedSchema := sqlparser.CanonicalString(createTable)
	return normalizedSchema, nil
}

func initSchemaFiles() {
	sqlFileExtension := ".sql"
	err := fs.WalkDir(schemaLocation, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			var module string
			dir, fname := filepath.Split(path)
			if !strings.HasSuffix(strings.ToLower(fname), sqlFileExtension) {
				log.Infof("Ignoring non-SQL file: %s, found during sidecar database initialization", path)
				return nil
			}
			dirparts := strings.Split(strings.Trim(dir, "/"), "/")
			switch len(dirparts) {
			case 1:
				module = dir
			case 2:
				module = fmt.Sprintf("%s/%s", dirparts[0], dirparts[1])
			default:
				return fmt.Errorf("unexpected path value of %s specified for sidecar schema table; expected structure is <module>[/<submodule>]/<tablename>.sql", dir)
			}

			name := strings.Split(fname, ".")[0]
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

// printCallerDetails is a helper for dev debugging.
func printCallerDetails() {
	pc, _, line, ok := runtime.Caller(2)
	details := runtime.FuncForPC(pc)
	if ok && details != nil {
		log.Infof("%s schema init called from %s:%d\n", SidecarDBName, details.Name(), line)
	}
}

type schemaInit struct {
	ctx            context.Context
	exec           Exec
	existingTables map[string]bool
	dbCreated      bool // The first upgrade/create query will also create the sidecar database if required.
}

// Exec is a callback that has to be passed to Init() to execute the specified query in the database.
type Exec func(ctx context.Context, query string, maxRows int, useDB bool) (*sqltypes.Result, error)

// GetDDLCount metric returns the count of sidecardb ddls that have been run as part of this vttablet's init process.
func GetDDLCount() int64 {
	return ddlCount.Get()
}

// Init creates or upgrades the sidecar database based on declarative schema for all tables in the schema.
func Init(ctx context.Context, exec Exec) error {
	printCallerDetails() // for debug purposes only, remove in v17
	log.Infof("Starting sidecardb.Init()")
	si := &schemaInit{
		ctx:  ctx,
		exec: exec,
	}

	// There are paths in the tablet initialization where we are in read-only mode but the schema is already updated.
	// Hence, we should not always try to create the database, since it will then error out as the db is read-only.
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

	resetSQLMode, err := si.setPermissiveSQLMode()
	if err != nil {
		return err
	}
	defer resetSQLMode()

	for _, table := range sidecarTables {
		if err := si.ensureSchema(table); err != nil {
			return err
		}
	}
	return nil
}

// setPermissiveSQLMode gets the current sql_mode for the session, removes any
// restrictions, and returns a function to restore it back to the original session value.
// We need to allow for the recreation of any data that currently exists in the table, such
// as e.g. allowing any zero dates that may already exist in a preexisting sidecar table.
func (si *schemaInit) setPermissiveSQLMode() (func(), error) {
	rs, err := si.exec(si.ctx, `select @@session.sql_mode as sql_mode`, 1, false)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not read sql_mode: %v", err)
	}
	sqlMode, err := rs.Named().Row().ToString("sql_mode")
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not read sql_mode: %v", err)
	}

	resetSQLModeFunc := func() {
		restoreSQLModeQuery := fmt.Sprintf("set @@session.sql_mode='%s'", sqlMode)
		_, _ = si.exec(si.ctx, restoreSQLModeQuery, 0, false)
	}

	if _, err := si.exec(si.ctx, "set @@session.sql_mode=''", 0, false); err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "could not change sql_mode: %v", err)
	}
	return resetSQLModeFunc, nil
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

// Sets db of current connection, returning the currently selected database.
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

// Gets existing schema of a table in the sidecar database.
func (si *schemaInit) getCurrentSchema(tableName string) (string, error) {
	var currentTableSchema string

	rs, err := si.exec(si.ctx, fmt.Sprintf(ShowCreateTableQuery, tableName), 1, false)
	if err != nil {
		if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Number() == mysql.ERNoSuchTable {
			// table does not exist in the sidecar database
			return "", nil
		}
		log.Errorf("Error getting table schema for %s: %+v", tableName, err)
		return "", err
	}
	if len(rs.Rows) > 0 {
		currentTableSchema = rs.Rows[0][1].ToString()
	}
	return currentTableSchema, nil
}

// findTableSchemaDiff gets the diff that needs to be applied to current table schema to get the desired one. Will be an empty string if they match.
// This could be a CREATE statement if the table does not exist or an ALTER if table exists but has a different schema.
func (si *schemaInit) findTableSchemaDiff(tableName, current, desired string) (string, error) {
	hints := &schemadiff.DiffHints{
		TableCharsetCollateStrategy: schemadiff.TableCharsetCollateIgnoreAlways,
	}
	diff, err := schemadiff.DiffCreateTablesQueries(current, desired, hints)
	if err != nil {
		return "", err
	}

	var ddl string
	if diff != nil {
		ddl = diff.CanonicalStatementString()

		// Temporary logging to debug any eventual issues around the new schema init, should be removed in v17.
		log.Infof("Current schema for table %s:\n%s", tableName, current)
		if ddl == "" {
			log.Infof("No changes needed for table %s", tableName)
		} else {
			log.Infof("Applying ddl for table %s:\n%s", tableName, ddl)
		}
	}

	return ddl, nil
}

// ensureSchema first checks if the table exist, in which case it runs the create script provided in
// the schema directory. If the table exists, schemadiff is used to compare the existing schema with the desired one.
// If it needs to be altered then we run the alter script.
func (si *schemaInit) ensureSchema(table *sidecarTable) error {
	ctx := si.ctx
	desiredTableSchema := table.schema

	var ddl string
	currentTableSchema, err := si.getCurrentSchema(table.name)
	if err != nil {
		return err
	}
	ddl, err = si.findTableSchemaDiff(table.name, currentTableSchema, desiredTableSchema)
	if err != nil {
		return err
	}

	if ddl != "" {
		if !si.dbCreated {
			// We use CreateSidecarDatabaseQuery to also create the first binlog entry when a primary comes up.
			// That statement doesn't make it to the replicas, so we run the query again so that it is replicated
			// to the replicas so that the replicas can create the sidecar database.
			if err := si.createSidecarDB(); err != nil {
				return err
			}
			si.dbCreated = true
		}
		_, err := si.exec(ctx, ddl, 1, true)
		if err != nil {
			log.Errorf("Error running ddl %s for table %s during sidecar database initialization %s: %+v", ddl, table, err)
			return err
		}
		log.Infof("Applied ddl %s for table %s during sidecar database initialization %s", ddl, table)
		ddlCount.Add(1)
		return nil
	}
	log.Infof("Table schema was already up to date for the %s table in the %s sidecar database", table.name, SidecarDBName)
	return nil
}

// region unit-test-only
// This section uses helpers used in tests, but also in the go/vt/vtexplain/vtexplain_vttablet.go.
// Hence, it is here and not in the _test.go file.

// Query patterns to handle in mocks.
var sidecarDBInitQueries = []string{
	ShowSidecarDatabasesQuery,
	SelectCurrentDatabaseQuery,
	CreateSidecarDatabaseQuery,
	UseSidecarDatabaseQuery,
}

var sidecarDBInitQueryPatterns = []string{
	CreateTableRegexp,
	AlterTableRegexp,
}

// AddSchemaInitQueries adds sidecar database schema related queries to a mock db.
func AddSchemaInitQueries(db *fakesqldb.DB, populateTables bool) {
	result := &sqltypes.Result{}
	for _, q := range sidecarDBInitQueryPatterns {
		db.AddQueryPattern(q, result)
	}
	for _, q := range sidecarDBInitQueries {
		db.AddQuery(q, result)
	}
	for _, table := range sidecarTables {
		result = &sqltypes.Result{}
		if populateTables {
			result = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"Table|Create Table",
				"varchar|varchar"),
				fmt.Sprintf("%s|%s", table.name, table.schema),
			)
		}
		db.AddQuery(fmt.Sprintf(ShowCreateTableQuery, table.name), result)
	}

	sqlModeResult := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"sql_mode",
		"varchar"),
		"ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
	)
	db.AddQuery("select @@session.sql_mode as sql_mode", sqlModeResult)

	db.AddQuery("set @@session.sql_mode=''", &sqltypes.Result{})
}

// MatchesInitQuery returns true if query has one of the test patterns as a substring, or it matches a provided regexp.
func MatchesInitQuery(query string) bool {
	query = strings.ToLower(query)
	for _, q := range sidecarDBInitQueries {
		if strings.EqualFold(q, query) {
			return true
		}
	}
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
