/*
Copyright 2025 The Vitess Authors.

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

package mysqltopo

import (
	"database/sql"
	"fmt"
	"net"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// newRawSchema creates an empty database with no topo tables and returns a base
// connection (no default schema), a DSN targeting the new schema, the schema
// name, and a cleanup func.
func newRawSchema(t *testing.T) (baseDB *sql.DB, schemaDSN, schemaName string, cleanup func()) {
	t.Helper()
	cfg, err := mysql.ParseDSN(mySQLTopoTestAddr)
	require.NoError(t, err)

	schemaName = generateRandomSchemaName()
	cfg.DBName = ""
	baseDB, err = sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	_, err = baseDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", schemaName))
	require.NoError(t, err)

	cfg.DBName = schemaName
	schemaDSN = cfg.FormatDSN()
	cleanup = func() {
		_, _ = baseDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", schemaName))
		_ = baseDB.Close()
	}
	return baseDB, schemaDSN, schemaName, cleanup
}

// writeTopoConfig creates a minimal topo_config table (matching the strata
// bootstrap convention) and records the given topo server address.
func writeTopoConfig(t *testing.T, baseDB *sql.DB, schema, topoServer string) {
	t.Helper()
	_, err := baseDB.Exec(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`.topo_config (`key` VARCHAR(64) NOT NULL PRIMARY KEY, `value` VARCHAR(255) NOT NULL)", schema))
	require.NoError(t, err)
	_, err = baseDB.Exec(fmt.Sprintf(
		"INSERT INTO `%s`.topo_config (`key`, `value`) VALUES ('topo_server', ?)", schema), topoServer)
	require.NoError(t, err)
}

// TestNewServerDoesNotCreateSchema is the core regression test: opening a server
// against an uninitialized node must fail AND must not create any topo tables.
// Auto-creating them is what previously turned a mistargeted cluster member into
// an empty phantom topo.
func TestNewServerDoesNotCreateSchema(t *testing.T) {
	baseDB, schemaDSN, schemaName, cleanup := newRawSchema(t)
	t.Cleanup(cleanup)

	_, err := NewServer(schemaDSN, "/test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not been initialized")

	var count int
	err = baseDB.QueryRow(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name IN ('topo_data', 'topo_locks', 'topo_elections')",
		schemaName,
	).Scan(&count)
	require.NoError(t, err)
	require.Zero(t, count, "NewServer must not create topo tables outside of CreateSchema")
}

// TestCreateSchemaEnablesOpen verifies CreateSchema initializes the topo (and is
// idempotent), after which NewServer opens successfully.
func TestCreateSchemaEnablesOpen(t *testing.T) {
	_, schemaDSN, _, cleanup := newRawSchema(t)
	t.Cleanup(cleanup)

	require.NoError(t, CreateSchema(schemaDSN))
	require.NoError(t, CreateSchema(schemaDSN), "CreateSchema must be idempotent")

	srv, err := NewServer(schemaDSN, "/test")
	require.NoError(t, err)
	t.Cleanup(srv.Close)
}

// TestNewServerResolvesTopoServerFromConfig exercises the etcd-style redirect:
// when the connected node's topo_config names a different topo server, NewServer
// transparently reconnects there. A single MySQL instance is reached via two
// equivalent address spellings (127.0.0.1 <-> localhost) to simulate two nodes.
func TestNewServerResolvesTopoServerFromConfig(t *testing.T) {
	baseDB, schemaDSN, schemaName, cleanup := newRawSchema(t)
	t.Cleanup(cleanup)
	require.NoError(t, CreateSchema(schemaDSN))

	cfg, err := mysql.ParseDSN(schemaDSN)
	require.NoError(t, err)
	host, port, err := net.SplitHostPort(cfg.Addr)
	require.NoError(t, err)

	var topoServerAddr string
	switch host {
	case "127.0.0.1":
		topoServerAddr = net.JoinHostPort("localhost", port)
	case "localhost":
		topoServerAddr = net.JoinHostPort("127.0.0.1", port)
	default:
		t.Skipf("test MySQL host %q has no equivalent alias to exercise redirection", host)
	}

	writeTopoConfig(t, baseDB, schemaName, topoServerAddr)

	srv, err := NewServer(schemaDSN, "/test")
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	require.Contains(t, srv.serverAddr, topoServerAddr,
		"server should have redirected to the resolved topo server address")
}

// TestNewServerNoRedirectWhenSelf verifies that a topo_config pointing at the
// connected node itself does not trigger a redirect.
func TestNewServerNoRedirectWhenSelf(t *testing.T) {
	baseDB, schemaDSN, schemaName, cleanup := newRawSchema(t)
	t.Cleanup(cleanup)
	require.NoError(t, CreateSchema(schemaDSN))

	cfg, err := mysql.ParseDSN(schemaDSN)
	require.NoError(t, err)
	writeTopoConfig(t, baseDB, schemaName, cfg.Addr)

	srv, err := NewServer(schemaDSN, "/test")
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	require.Contains(t, srv.serverAddr, cfg.Addr)
}
