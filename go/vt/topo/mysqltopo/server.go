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
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// Server is the implementation of topo.Server for MySQL.
type Server struct {
	// conn is the MySQL connection.
	conn *mysql.Conn

	// mu protects the following fields.
	mu sync.Mutex

	// cells is a map of cell name to cell Server.
	cells map[string]*Server

	// root is the root path for this server.
	root string

	// params is the MySQL connection parameters
	params *mysql.ConnParams

	// replicationWatcher watches MySQL replication for changes
	replicationWatcher *ReplicationWatcher
}

// MySQLVersion implements topo.Version for MySQL.
type MySQLVersion int64

// String implements topo.Version.String.
func (v MySQLVersion) String() string {
	return fmt.Sprintf("%d", v)
}

// NewServer returns a new MySQL topo.Server.
func NewServer(ctx context.Context, serverAddr, root string) (*Server, error) {
	// Parse the server address to get connection parameters
	params := parseServerAddr(serverAddr)

	// Ensure the schema exists
	if err := createSchemaIfNotExists(*params); err != nil {
		return nil, err
	}

	// Connect to MySQL using Vitess internal connection handling
	conn, err := mysql.Connect(ctx, params)
	if err != nil {
		return nil, err
	}

	// Create the server
	server := &Server{
		conn:   conn,
		params: params,
		cells:  make(map[string]*Server),
		root:   root,
	}

	// Create the required tables
	if err := server.createTablesIfNotExist(); err != nil {
		conn.Close()
		return nil, err
	}

	// Clean up any expired locks and elections on startup
	server.cleanupExpiredData()

	// Create and start the replication watcher (skip in test mode)
	if !isTestMode() {
		server.replicationWatcher = NewReplicationWatcher(server)
		if err := server.replicationWatcher.Start(); err != nil {
			log.Warningf("Failed to start replication watcher: %v", err)
			// Continue without replication watcher - we'll fall back to polling
		}
	}

	return server, nil
}

// parseServerAddr parses the server address into MySQL connection parameters.
func parseServerAddr(serverAddr string) *mysql.ConnParams {
	// Default configuration
	params := &mysql.ConnParams{
		Host:   "localhost",
		Port:   3306,
		Uname:  "root",
		DbName: DefaultTopoSchema,
	}

	// Parse the server address
	if serverAddr != "" {
		parts := strings.Split(serverAddr, "@")
		if len(parts) > 1 {
			// Format is user:pass@host:port/dbname
			userParts := strings.Split(parts[0], ":")
			params.Uname = userParts[0]
			if len(userParts) > 1 {
				params.Pass = userParts[1]
			}

			hostParts := strings.Split(parts[1], "/")
			// Parse host:port
			if strings.Contains(hostParts[0], ":") {
				hostPortParts := strings.Split(hostParts[0], ":")
				params.Host = hostPortParts[0]
				if port, err := strconv.Atoi(hostPortParts[1]); err == nil {
					params.Port = port
				}
			} else {
				params.Host = hostParts[0]
			}

			if len(hostParts) > 1 {
				params.DbName = hostParts[1]
			}
		} else {
			// Format is host:port/dbname
			hostParts := strings.Split(serverAddr, "/")
			// Parse host:port
			if strings.Contains(hostParts[0], ":") {
				hostPortParts := strings.Split(hostParts[0], ":")
				params.Host = hostPortParts[0]
				if port, err := strconv.Atoi(hostPortParts[1]); err == nil {
					params.Port = port
				}
			} else {
				params.Host = hostParts[0]
			}

			if len(hostParts) > 1 {
				params.DbName = hostParts[1]
			}
		}
	}

	// Use default schema if no database name was specified
	if params.DbName == "" {
		params.DbName = DefaultTopoSchema
	}

	return params
}

// createSchemaIfNotExists creates the schema if it doesn't exist.
func createSchemaIfNotExists(params mysql.ConnParams) error {
	// Save the original database name
	schemaName := params.DbName

	// Connect without specifying a database
	tempParams := params
	tempParams.DbName = ""

	// Connect using Vitess internal connection handling
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &tempParams)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Create the schema if it doesn't exist
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", schemaName)
	_, err = conn.ExecuteFetch(query, 0, false)
	return err
}

// createTablesIfNotExist creates the required tables if they don't exist.
func (s *Server) createTablesIfNotExist() error {
	// Create the tables
	queries := []string{
		// topo_files table stores the file data
		`CREATE TABLE IF NOT EXISTS topo_files (
			path VARCHAR(512) NOT NULL PRIMARY KEY,
			data MEDIUMBLOB,
			version BIGINT NOT NULL AUTO_INCREMENT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			KEY (version)
		) ENGINE=InnoDB`,

		// topo_locks table stores lock information
		`CREATE TABLE IF NOT EXISTS topo_locks (
			path VARCHAR(512) NOT NULL PRIMARY KEY,
			owner VARCHAR(255) NOT NULL,
			contents TEXT,
			expiration TIMESTAMP NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB`,

		// topo_directories table stores directory information
		`CREATE TABLE IF NOT EXISTS topo_directories (
			path VARCHAR(512) NOT NULL PRIMARY KEY,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB`,

		// topo_watch table stores watch information for change notifications
		`CREATE TABLE IF NOT EXISTS topo_watch (
			path VARCHAR(512) NOT NULL,
			watcher_id VARCHAR(64) NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (path, watcher_id)
		) ENGINE=InnoDB`,

		// topo_elections table stores election information
		`CREATE TABLE IF NOT EXISTS topo_elections (
			name VARCHAR(512) NOT NULL PRIMARY KEY,
			leader_id VARCHAR(255) NOT NULL,
			contents TEXT,
			expiration TIMESTAMP NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		) ENGINE=InnoDB`,
	}

	// Execute each query
	for _, query := range queries {
		if _, err := s.conn.ExecuteFetch(query, 0, false); err != nil {
			return err
		}
	}

	return nil
}

// Close is part of the topo.Server interface.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Stop the replication watcher if it's running
	if s.replicationWatcher != nil {
		s.replicationWatcher.Stop()
		s.replicationWatcher = nil
	}

	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}

	for _, cell := range s.cells {
		cell.Close()
	}
	s.cells = nil
}

// Factory is a factory for Server objects.
type Factory struct{}

// HasGlobalReadOnlyCell implements topo.Factory.
func (f Factory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	// MySQL topo doesn't support read-only cells
	return false
}

// Create implements topo.Factory.
func (f Factory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	server, err := NewServer(context.Background(), serverAddr, root)
	if err != nil {
		return nil, err
	}
	return server, nil
}

func init() {
	topo.RegisterFactory("mysql", &Factory{})
}

// queryRow executes a query expected to return at most one row
// it applies Sprintf on the arguments, and escapes them all.
func (s *Server) queryRow(query string, args ...string) (*sqltypes.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.queryRowUnsafe(query, args...)
}

// exec executes a query that modifies data (INSERT, UPDATE, DELETE)
// it applies Sprintf on the arguments, and escapes them all.
func (s *Server) exec(query string, args ...string) (*sqltypes.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.execUnsafe(query, args...)
}

// query executes a query that returns multiple rows
// it applies Sprintf on the arguments, and escapes them all.
func (s *Server) query(query string, args ...string) (*sqltypes.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.queryUnsafe(query, args...)
}

// queryRowUnsafe executes a query without acquiring the mutex (internal use only)
func (s *Server) queryRowUnsafe(query string, args ...string) (*sqltypes.Result, error) {
	if s.conn == nil {
		return nil, fmt.Errorf("connection closed")
	}
	return s.conn.ExecuteFetch(expandQuery(query, args...), 1, false)
}

// execUnsafe executes a query without acquiring the mutex (internal use only)
func (s *Server) execUnsafe(query string, args ...string) (*sqltypes.Result, error) {
	if s.conn == nil {
		return nil, fmt.Errorf("connection closed")
	}
	return s.conn.ExecuteFetch(expandQuery(query, args...), 0, false)
}

// queryUnsafe executes a query without acquiring the mutex (internal use only)
func (s *Server) queryUnsafe(query string, args ...string) (*sqltypes.Result, error) {
	if s.conn == nil {
		return nil, fmt.Errorf("connection closed")
	}
	return s.conn.ExecuteFetch(expandQuery(query, args...), -1, false)
}

// convertError converts a MySQL error to a topo error.
func convertError(err error, path string) error {
	if err == nil {
		return nil
	}

	// Check for connection closed errors
	errStr := err.Error()
	if strings.Contains(errStr, "connection closed") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset") {
		return topo.NewError(topo.Interrupted, path)
	}

	// Check for MySQL-specific errors
	if strings.Contains(errStr, "doesn't exist") ||
		strings.Contains(errStr, "Table") && strings.Contains(errStr, "doesn't exist") {
		return topo.NewError(topo.NoNode, path)
	}

	if strings.Contains(errStr, "Duplicate entry") {
		return topo.NewError(topo.NodeExists, path)
	}

	// For context cancellation
	if err == context.Canceled {
		return topo.NewError(topo.Interrupted, path)
	}
	if err == context.DeadlineExceeded {
		return topo.NewError(topo.Timeout, path)
	}

	// Default: return the original error wrapped in a generic topo error
	return err
}

// isTestMode returns true if we're running in test mode.
func isTestMode() bool {
	// Check if we're running under go test
	for _, arg := range os.Args {
		if strings.Contains(arg, "test") || strings.HasSuffix(arg, ".test") {
			return true
		}
	}

	// Check for test-specific database names
	if len(os.Args) > 0 && strings.Contains(os.Args[0], "topo_test_") {
		return true
	}

	return false
}

// fullPath returns the full path by combining the server's root with the given path.
func (s *Server) fullPath(path string) string {
	// Normalize the path to ensure it starts with /
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	// If root is "/" or empty, just return the path
	if s.root == "/" || s.root == "" {
		return path
	}

	// Combine root and path
	if path == "/" {
		return s.root
	}

	return s.root + path
}

// relativePath returns the relative path by removing the server's root prefix.
func (s *Server) relativePath(fullPath string) string {
	// If root is "/" or empty, just return the path
	if s.root == "/" || s.root == "" {
		return fullPath
	}

	// Remove the root prefix
	if strings.HasPrefix(fullPath, s.root) {
		relative := fullPath[len(s.root):]
		if relative == "" {
			return "/"
		}
		return relative
	}

	// If the path doesn't start with root, return as-is
	return fullPath
}

// cleanupExpiredData removes expired locks and elections on startup.
// This helps ensure that stale data doesn't interfere with cluster operations.
func (s *Server) cleanupExpiredData() {
	// Clean up expired locks
	if _, err := s.conn.ExecuteFetch("DELETE FROM topo_locks WHERE expiration < NOW()", 0, false); err != nil {
		log.Warningf("Failed to cleanup expired locks: %v", err)
	}

	// Clean up expired elections
	if _, err := s.conn.ExecuteFetch("DELETE FROM topo_elections WHERE expiration < NOW()", 0, false); err != nil {
		log.Warningf("Failed to cleanup expired elections: %v", err)
	}

	// Clean up tablets in inconsistent states that can interfere with cluster startup
	// These states typically indicate tablets that weren't cleanly shut down
	inconsistentStates := []string{"RESTORE", "DRAINED", "BACKUP"}
	for _, state := range inconsistentStates {
		query := fmt.Sprintf("DELETE FROM topo_files WHERE path LIKE '%%/tablets/%%' AND data LIKE '%%\"type\":\"%s\"%%'", state)
		if _, err := s.conn.ExecuteFetch(query, 0, false); err != nil {
			log.Warningf("Failed to cleanup tablets in %s state: %v", state, err)
		}
	}

	log.Infof("MySQL topology server startup cleanup completed")
}
