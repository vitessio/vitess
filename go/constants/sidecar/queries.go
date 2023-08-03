package sidecar

import "vitess.io/vitess/go/vt/sqlparser"

// region unit-test-only
// This section uses helpers used in tests, but also in
// go/vt/vtexplain/vtexplain_vttablet.go.
// Hence, it is here and not in the _test.go file.
const (
	createDBQuery     = "create database if not exists %s"
	createTableRegexp = "(?i)CREATE TABLE .* `?\\_vt\\`?..*"
	alterTableRegexp  = "(?i)ALTER TABLE `?\\_vt\\`?..*"
)

var (
	DBInitQueries = []string{
		"use %s",
		createDBQuery,
	}
	// Query patterns to handle in mocks.
	DBInitQueryPatterns = []string{
		createTableRegexp,
		alterTableRegexp,
	}
)

// GetCreateQuery returns the CREATE DATABASE SQL statement
// used to create the sidecar database.
func GetCreateQuery() string {
	return sqlparser.BuildParsedQuery(createDBQuery, GetIdentifier()).Query
}

// GetIdentifier returns the sidecar database name as an SQL
// identifier string, most importantly this means that it will
// be properly escaped if/as needed.
func GetIdentifier() string {
	ident := sqlparser.NewIdentifierCS(GetName())
	return sqlparser.String(ident)
}
