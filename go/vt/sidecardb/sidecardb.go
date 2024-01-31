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
	"sync"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/history"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/vtenv"

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
	sidecarDBExistsQuery  = "select 'true' as 'dbexists' from information_schema.SCHEMATA where SCHEMA_NAME = %a"
	showCreateTableQuery  = "show create table %s.%s"
	sidecarCollationQuery = "select @@global.collation_server"

	maxDDLErrorHistoryLength = 100

	// failOnSchemaInitError decides whether we fail the schema init process when we encounter an error while
	// applying a table schema upgrade DDL or continue with the next table.
	// If true, tablets will not launch. The cluster will not come up until the issue is resolved.
	// If false, the init process will continue trying to upgrade other tables. So some functionality might be broken
	// due to an incorrect schema, but the cluster should come up and serve queries.
	// This is an operational trade-off: if we always fail it could cause a major incident since the entire cluster will be down.
	// If we are more permissive, it could cause hard-to-detect errors, because a module
	// doesn't load or behaves incorrectly due to an incomplete upgrade. Errors however will be reported and if the
	// related stats endpoints are monitored we should be able to diagnose/get alerted in a timely fashion.
	failOnSchemaInitError = false

	StatsKeyPrefix     = "SidecarDBDDL"
	StatsKeyQueryCount = StatsKeyPrefix + "QueryCount"
	StatsKeyErrorCount = StatsKeyPrefix + "ErrorCount"
	StatsKeyErrors     = StatsKeyPrefix + "Errors"
)

var (
	sidecarTables []*sidecarTable

	// All tables needed in the sidecar database have
	// their schema in the schema subdirectory.
	//go:embed schema/*
	schemaLocation embed.FS
	// Load the schema definitions one time.
	once sync.Once

	ddlCount        *stats.Counter
	ddlErrorCount   *stats.Counter
	ddlErrorHistory *history.History
	mu              sync.Mutex
)

type sidecarTable struct {
	module string // which module uses this table
	path   string // path of the schema relative to this module
	name   string // table name
	schema string // create table dml
}

type ddlError struct {
	tableName string
	err       error
}

func init() {
	ddlCount = stats.NewCounter(StatsKeyQueryCount, "Number of queries executed")
	ddlErrorCount = stats.NewCounter(StatsKeyErrorCount, "Number of errors during sidecar schema upgrade")
	ddlErrorHistory = history.New(maxDDLErrorHistoryLength)
	stats.Publish(StatsKeyErrors, stats.StringMapFunc(func() map[string]string {
		mu.Lock()
		defer mu.Unlock()
		result := make(map[string]string, len(ddlErrorHistory.Records()))
		for _, e := range ddlErrorHistory.Records() {
			d, ok := e.(*ddlError)
			if ok {
				result[d.tableName] = d.err.Error()
			}
		}
		return result
	}))
}

func validateSchemaDefinition(name, schema string, parser *sqlparser.Parser) (string, error) {
	stmt, err := parser.ParseStrictDDL(schema)

	if err != nil {
		return "", err
	}
	createTable, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected CREATE TABLE. Got %v", sqlparser.CanonicalString(stmt))
	}
	tableName := createTable.Table.Name.String()
	// The database qualifier should be configured externally.
	qualifier := createTable.Table.Qualifier.String()
	if qualifier != "" {
		return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "database qualifier of %s specified for the %s table when there should not be one", qualifier, name)
	}
	createTable.Table.Qualifier = sqlparser.NewIdentifierCS(sidecar.GetName())
	if !strings.EqualFold(tableName, name) {
		return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "table name of %s does not match the table name specified within the file: %s", name, tableName)
	}
	if !createTable.IfNotExists {
		return "", vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "%s file did not include the required IF NOT EXISTS clause in the CREATE TABLE statement for the %s table", name, tableName)
	}
	normalizedSchema := sqlparser.CanonicalString(createTable)
	return normalizedSchema, nil
}

// loadSchemaDefinitions loads the embedded schema definitions
// into a slice of sidecarTables for processing.
func loadSchemaDefinitions(parser *sqlparser.Parser) {
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
				module = dirparts[0]
			case 2:
				module = dirparts[1]
			default:
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected path value of %s specified for sidecar schema table; expected structure is <module>[/<submodule>]/<tablename>.sql", dir)
			}

			name := strings.Split(fname, ".")[0]
			schema, err := schemaLocation.ReadFile(path)
			if err != nil {
				panic(err)
			}
			var normalizedSchema string
			if normalizedSchema, err = validateSchemaDefinition(name, string(schema), parser); err != nil {
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
		log.Infof("%s schema init called from %s:%d\n", sidecar.GetName(), details.Name(), line)
	}
}

type schemaInit struct {
	ctx       context.Context
	env       *vtenv.Environment
	exec      Exec
	dbCreated bool // The first upgrade/create query will also create the sidecar database if required.
	coll      collations.ID
}

// Exec is a callback that has to be passed to Init() to
// execute the specified query within the database.
type Exec func(ctx context.Context, query string, maxRows int, useDB bool) (*sqltypes.Result, error)

// GetDDLCount metric returns the count of sidecardb DDLs that
// have been run as part of this vttablet's init process.
func getDDLCount() int64 {
	return ddlCount.Get()
}

// GetDDLErrorCount returns the count of sidecardb DDLs that have been errored out as part of this vttablet's init process.
func getDDLErrorCount() int64 {
	return ddlErrorCount.Get()
}

// GetDDLErrorHistory returns the errors encountered as part of this vttablet's init process..
func getDDLErrorHistory() []*ddlError {
	var errors []*ddlError
	for _, e := range ddlErrorHistory.Records() {
		ddle, ok := e.(*ddlError)
		if ok {
			errors = append(errors, ddle)
		}
	}
	return errors
}

// Init creates or upgrades the sidecar database based on
// the declarative schema defined for all tables.
func Init(ctx context.Context, env *vtenv.Environment, exec Exec) error {
	printCallerDetails() // for debug purposes only, remove in v17
	log.Infof("Starting sidecardb.Init()")

	once.Do(func() {
		loadSchemaDefinitions(env.Parser())
	})

	si := &schemaInit{
		ctx:  ctx,
		exec: exec,
		env:  env,
	}

	// There are paths in the tablet initialization where we
	// are in read-only mode but the schema is already updated.
	// Hence, we should not always try to CREATE the
	// database, since it will then error out as the instance
	// is read-only.
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

	if err := si.setCurrentDatabase(sidecar.GetIdentifier()); err != nil {
		return err
	}

	resetSQLMode, err := si.setPermissiveSQLMode()
	if err != nil {
		return err
	}
	defer resetSQLMode()

	if si.coll, err = si.collation(); err != nil {
		return err
	}

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
	query, err := sqlparser.ParseAndBind(sidecarDBExistsQuery, sqltypes.StringBindVariable(sidecar.GetName()))
	if err != nil {
		return false, err
	}
	rs, err := si.exec(si.ctx, query, 2, false)
	if err != nil {
		log.Error(err)
		return false, err
	}

	switch len(rs.Rows) {
	case 0:
		log.Infof("doesSidecarDBExist: %s not found", sidecar.GetName())
		return false, nil
	case 1:
		log.Infof("doesSidecarDBExist: found %s", sidecar.GetName())
		return true, nil
	default:
		// This should never happen.
		return false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid results for SidecarDB query %q as it produced %d rows", query, len(rs.Rows))
	}
}

func (si *schemaInit) createSidecarDB() error {
	_, err := si.exec(si.ctx, sidecar.GetCreateQuery(), 1, false)
	if err != nil {
		log.Error(err)
		return err
	}
	log.Infof("createSidecarDB: %s", sidecar.GetName())
	return nil
}

// Sets the current db in the used connection.
func (si *schemaInit) setCurrentDatabase(dbName string) error {
	_, err := si.exec(si.ctx, fmt.Sprintf("use %s", dbName), 1, false)
	return err
}

func (si *schemaInit) collation() (collations.ID, error) {
	rs, err := si.exec(si.ctx, sidecarCollationQuery, 2, false)
	if err != nil {
		log.Error(err)
		return collations.Unknown, err
	}

	switch len(rs.Rows) {
	case 1:
		return si.env.CollationEnv().LookupByName(rs.Rows[0][0].ToString()), nil
	default:
		// This should never happen.
		return collations.Unknown, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid results for SidecarDB query %q as it produced %d rows", sidecarCollationQuery, len(rs.Rows))
	}
}

// Gets existing schema of a table in the sidecar database.
func (si *schemaInit) getCurrentSchema(tableName string) (string, error) {
	var currentTableSchema string

	// We escape the tableName because it can be a keyword.
	// Converting the tableName to a case-sensitive identifier and converting back to a string using the
	// sqlparser package, ensures that the table name is escaped with backticks if required.
	escapedTableName := sqlparser.String(sqlparser.NewIdentifierCS(tableName))
	rs, err := si.exec(si.ctx, sqlparser.BuildParsedQuery(showCreateTableQuery, sidecar.GetIdentifier(), escapedTableName).Query, 1, false)
	if err != nil {
		if sqlErr, ok := err.(*sqlerror.SQLError); ok && sqlErr.Number() == sqlerror.ERNoSuchTable {
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

// findTableSchemaDiff gets the diff which needs to be applied
// to the current table schema in order to reach the desired one.
// The result will be an empty string if they match.
// This will be a CREATE statement if the table does not exist
// or an ALTER if the table exists but has a different schema.
func (si *schemaInit) findTableSchemaDiff(tableName, current, desired string) (string, error) {
	hints := &schemadiff.DiffHints{
		TableCharsetCollateStrategy: schemadiff.TableCharsetCollateIgnoreAlways,
		AlterTableAlgorithmStrategy: schemadiff.AlterTableAlgorithmStrategyCopy,
	}
	env := schemadiff.NewEnv(si.env, si.coll)
	diff, err := schemadiff.DiffCreateTablesQueries(env, current, desired, hints)
	if err != nil {
		return "", err
	}

	var ddl string
	if diff != nil {
		ddl = diff.CanonicalStatementString()

		if ddl == "" {
			log.Infof("No changes needed for table %s", tableName)
		} else {
			log.Infof("Applying DDL for table %s:\n%s", tableName, ddl)
		}
	}

	return ddl, nil
}

// ensureSchema uses schemadiff to compare the live schema
// with the desired one and applies any DDL statements
// necessary to converge on the desired schema.
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
			// We use createSidecarDB to also create the
			// first binlog entry when a primary comes up.
			// That statement doesn't make it to the
			// replicas, so we run the query again so that
			// it is replicated to the replicas so that the
			// replicas can create the sidecar database.
			if err := si.createSidecarDB(); err != nil {
				return err
			}
			si.dbCreated = true
		}
		_, err := si.exec(ctx, ddl, 1, true)
		if err != nil {
			ddlErr := vterrors.Wrapf(err,
				"Error running DDL %s for table %s during sidecar database initialization", ddl, table)
			recordDDLError(table.name, ddlErr)
			if failOnSchemaInitError {
				return ddlErr
			}
			return nil
		}
		log.Infof("Applied DDL %s for table %s during sidecar database initialization", ddl, table)
		ddlCount.Add(1)
		return nil
	}
	log.Infof("Table schema was already up to date for the %s table in the %s sidecar database", table.name, sidecar.GetName())
	return nil
}

func recordDDLError(tableName string, err error) {
	log.Error(err)
	ddlErrorCount.Add(1)
	ddlErrorHistory.Add(&ddlError{
		tableName: tableName,
		err:       err,
	})
}

func (t *sidecarTable) String() string {
	return fmt.Sprintf("%s.%s (%s)", sidecar.GetIdentifier(), sqlparser.String(sqlparser.NewIdentifierCS(t.name)), t.module)
}

// region unit-test-only
// This section uses helpers used in tests, but also in
// go/vt/vtexplain/vtexplain_vttablet.go.
// Hence, it is here and not in the _test.go file.

// AddSchemaInitQueries adds sidecar database schema related
// queries to a mock db.
// This is for unit tests only!
func AddSchemaInitQueries(db *fakesqldb.DB, populateTables bool, parser *sqlparser.Parser) {
	once.Do(func() {
		loadSchemaDefinitions(parser)
	})
	result := &sqltypes.Result{}
	for _, q := range sidecar.DBInitQueryPatterns {
		db.AddQueryPattern(q, result)
	}
	for _, q := range sidecar.DBInitQueries {
		db.AddQuery(sqlparser.BuildParsedQuery(q, sidecar.GetIdentifier()).Query, result)
	}
	sdbe, _ := sqlparser.ParseAndBind(sidecarDBExistsQuery, sqltypes.StringBindVariable(sidecar.GetName()))
	db.AddQuery(sdbe, result)
	for _, table := range sidecarTables {
		result = &sqltypes.Result{}
		if populateTables {
			result = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"Table|Create Table",
				"varchar|varchar"),
				fmt.Sprintf("%s|%s", table.name, table.schema),
			)
		}
		db.AddQuery(sqlparser.BuildParsedQuery(showCreateTableQuery, sidecar.GetIdentifier(),
			sqlparser.String(sqlparser.NewIdentifierCS(table.name))).Query, result)
	}

	sqlModeResult := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"sql_mode",
		"varchar"),
		config.DefaultSQLMode,
	)
	db.AddQuery("select @@session.sql_mode as sql_mode", sqlModeResult)
	collationResult := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"@@global.collation_server ",
		"varchar"),
		"utf8mb4_0900_ai_ci",
	)
	db.AddQuery("select @@global.collation_server", collationResult)
	db.AddQuery("set @@session.sql_mode=''", &sqltypes.Result{})
}

// MatchesInitQuery returns true if the query has one of the
// test patterns as a substring or it matches a provided regexp.
// This is for unit tests only!
func MatchesInitQuery(query string) bool {
	query = strings.ToLower(query)
	for _, q := range sidecar.DBInitQueries {
		if strings.EqualFold(sqlparser.BuildParsedQuery(q, sidecar.GetIdentifier()).Query, query) {
			return true
		}
	}
	sdbe, _ := sqlparser.ParseAndBind(sidecarDBExistsQuery, sqltypes.StringBindVariable(sidecar.GetName()))
	if strings.EqualFold(sdbe, query) {
		return true
	}
	for _, q := range sidecar.DBInitQueryPatterns {
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
