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

/*
Package mysqltopo implements topo.Server with MySQL as the backend.

We expect the following behavior from the MySQL database:

  - The topo schema is created explicitly via CreateSchema (during cluster
    bootstrap); opening a server with NewServer never creates tables.
  - Transactions are used to ensure consistency.
  - MySQL replication is used for change notifications (no polling).
  - Clients connect as MySQL replicas to receive real-time changes.

We follow these conventions within this package:

  - Call convertError(err) on any errors returned from the MySQL driver.
    Functions defined in this package can be assumed to have already converted
    errors as necessary.
  - Use MySQL AUTO_INCREMENT for versioning.
  - Store topology data in JSON format in MEDIUMBLOB columns.
*/
package mysqltopo

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/utils"
)

const (
	// DefaultSchema is the default database schema name for MySQL topo
	DefaultSchema = "topo"

	// DefaultLockTTL is the default TTL for locks in seconds
	DefaultLockTTL = 30

	// DefaultElectionTTL is the default TTL for elections in seconds
	DefaultElectionTTL = 30
)

var (
	lockTTL     = DefaultLockTTL
	electionTTL = DefaultElectionTTL

	// rdsAddr matches Amazon RDS hostnames
	rdsAddr = regexp.MustCompile(`\.rds\.amazonaws\.com(:\d+)?$`)

	// rdsTLSOnce ensures we only register the RDS TLS config once
	rdsTLSOnce sync.Once

	// https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
	//go:embed rdsGlobalBundle.pem
	rdsGlobalBundle []byte
)

// Factory is the mysql topo.Factory implementation.
type Factory struct{}

// HasGlobalReadOnlyCell is part of the topo.Factory interface.
// For MySQL topo, all cells share the same database connection, so we return true.
// This prevents Vitess from trying to create separate connections per cell using
// the ServerAddress from CellInfo (which doesn't contain credentials).
func (f Factory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	return true
}

// Create is part of the topo.Factory interface.
func (f Factory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	return NewServer(serverAddr, root)
}

// Server is the implementation of topo.Server for MySQL.
type Server struct {
	// db is the MySQL database connection
	db *sql.DB

	// root is the root path for this client
	root string

	// serverAddr is the MySQL server address
	serverAddr string

	// schemaName is the database schema name
	schemaName string

	// mu protects the server state
	mu sync.RWMutex

	// closed indicates if the server has been closed
	closed bool

	// hasNotificationSystem indicates if this server has acquired a reference
	// to the shared notification system. This ensures we only release what we acquired.
	hasNotificationSystem bool

	// ctx is the server context for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

// MySQLVersion implements topo.Version for MySQL.
type MySQLVersion int64

// String implements topo.Version.String.
func (v MySQLVersion) String() string {
	return strconv.FormatInt(int64(v), 10)
}

func init() {
	for _, cmd := range topo.FlagBinaries {
		servenv.OnParseFor(cmd, registerMySQLTopoFlags)
	}
	topo.RegisterFactory("mysql", Factory{})
}

func registerMySQLTopoFlags(fs *pflag.FlagSet) {
	utils.SetFlagIntVar(fs, &lockTTL, "topo-mysql-lock-ttl", lockTTL, "lock TTL in seconds for MySQL topo")
	utils.SetFlagIntVar(fs, &electionTTL, "topo-mysql-election-ttl", electionTTL, "election TTL in seconds for MySQL topo")
}

// isRDSHost returns true if the host is an Amazon RDS hostname
func isRDSHost(host string) bool {
	return rdsAddr.MatchString(host)
}

// initRDSTLS registers the RDS TLS configuration with the MySQL driver
func initRDSTLS() error {
	var err error
	rdsTLSOnce.Do(func() {
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(rdsGlobalBundle) {
			err = errors.New("failed to append RDS CA certificates")
			return
		}
		tlsConfig := &tls.Config{
			RootCAs: caCertPool,
		}
		err = mysql.RegisterTLSConfig("rds-topo", tlsConfig)
	})
	return err
}

// NewServer returns a new MySQL topo.Server for an already-initialized topology.
//
// If the DSN points at a cluster member that is not itself the node hosting the
// topology, NewServer resolves the real topo server from the member's
// topo_config table and transparently reconnects there (see connectResolved).
// NewServer never creates the topo schema: a node whose schema is missing is
// reported as an error rather than silently initialized, since creating tables
// on a mistargeted member would turn it into an empty phantom topo. Use
// CreateSchema to initialize a topology.
func NewServer(serverAddr, root string) (*Server, error) {
	// Parse the server address to get MySQL DSN
	cfg, err := mysql.ParseDSN(serverAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse MySQL DSN: %v", err)
	}
	if cfg.DBName == "" {
		cfg.DBName = DefaultSchema // Use default schema if not specified
	}

	// If this DSN has no credentials (empty user), it's likely a placeholder from CellInfo.
	// Since HasGlobalReadOnlyCell returns true, this connection should never actually be used.
	// Return a minimal server that will fail if actually used, but allows the topology to be set up.
	if cfg.User == "" {
		log.Info("MySQL topo: skipping connection for DSN without credentials (will use global connection)")
		return &Server{
			root:       root,
			serverAddr: serverAddr,
			schemaName: cfg.DBName,
		}, nil
	}

	// Connect, transparently redirecting to the real topo server when this DSN
	// points at a non-topo cluster member. cfg is rewritten to the node we
	// actually connected to.
	db, cfg, err := connectResolved(cfg)
	if err != nil {
		return nil, err
	}

	// Create server context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	server := &Server{
		db:         db,
		root:       root,
		serverAddr: cfg.FormatDSN(),
		schemaName: cfg.DBName,
		ctx:        ctx,
		cancel:     cancel,
	}

	// Require the topo schema to already exist. Unlike a bootstrap, opening a
	// server never creates tables: doing so against a node that is not actually
	// a topo server (for example a mistargeted member) would silently turn that
	// node into an empty phantom topo. Initialization is the explicit job of
	// CreateSchema.
	exists, err := topoDataTableExists(db, cfg.DBName)
	if err != nil {
		cancel()
		db.Close()
		return nil, fmt.Errorf("failed to check for existing tables: %v", err)
	}
	if !exists {
		cancel()
		db.Close()
		return nil, fmt.Errorf("MySQL topo schema not found in database %q on %s: the topology has not been initialized (tables are not auto-created; run a cluster bootstrap)", cfg.DBName, cfg.Addr)
	}

	log.Info("MySQL topo opened", slog.String("addr", cfg.Addr), slog.String("schema", cfg.DBName))
	return server, nil
}

// connect opens and verifies a MySQL connection for the given config, applying
// RDS TLS when the address is an RDS endpoint.
func connect(cfg *mysql.Config) (*sql.DB, error) {
	if isRDSHost(cfg.Addr) {
		if err := initRDSTLS(); err != nil {
			return nil, fmt.Errorf("failed to initialize RDS TLS: %v", err)
		}
		cfg.TLSConfig = "rds-topo"
	}

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL topo at %s (schema %q, user %q): %v", cfg.Addr, cfg.DBName, cfg.User, err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping MySQL topo at %s (schema %q, user %q): %v", cfg.Addr, cfg.DBName, cfg.User, err)
	}
	return db, nil
}

// connectResolved opens a connection to the MySQL topo described by cfg. If the
// node it connects to records a different topo server in its topo_config table,
// connectResolved closes that connection and reopens against the real topo
// server, returning the connection and the (possibly rewritten) config. This
// mirrors the "connect to any member, get routed to the authority" behavior of
// clustered topologies like etcd, so callers never need to know which node
// currently hosts the topology.
//
// Resolution is best-effort: when topo_config is absent (for example a non-strata
// MySQL topo), the original connection is returned unchanged.
func connectResolved(cfg *mysql.Config) (*sql.DB, *mysql.Config, error) {
	db, err := connect(cfg)
	if err != nil {
		return nil, nil, err
	}

	topoAddr, ok := lookupTopoServer(db)
	if !ok || topoAddr == "" || topoAddr == cfg.Addr {
		// Already the topo server, or no topo_config to resolve from.
		return db, cfg, nil
	}

	// Redirect to the real topo server, keeping the same credentials and schema.
	log.Info("MySQL topo: resolved topo server from topo_config",
		slog.String("from", cfg.Addr), slog.String("to", topoAddr))
	if err := db.Close(); err != nil {
		log.Warn("MySQL topo: error closing pre-resolution connection", slog.Any("error", err))
	}
	resolved := cfg.Clone()
	resolved.Addr = topoAddr
	db, err = connect(resolved)
	if err != nil {
		return nil, nil, err
	}
	return db, resolved, nil
}

// lookupTopoServer reads the topo server address recorded in the topo_config
// table (the strata bootstrap convention) of the connection's current schema.
// The bool result is false when topo_config does not exist or has no
// topo_server row, in which case the caller treats the connected node as the
// topo server itself. The query is unqualified: topo_config lives in the same
// schema as the topo tables, which is already the connection's default database,
// so the schema name need not be interpolated into the SQL.
func lookupTopoServer(db *sql.DB) (string, bool) {
	var addr string
	if err := db.QueryRow("SELECT `value` FROM topo_config WHERE `key` = 'topo_server'").Scan(&addr); err != nil {
		return "", false
	}
	return addr, true
}

// topoDataTableExists reports whether the topo_data table exists in the schema.
func topoDataTableExists(db *sql.DB, schema string) (bool, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = 'topo_data'", schema).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// CreateSchema initializes the MySQL topo schema (topo_data, topo_locks,
// topo_elections) in the database named by serverAddr's DSN. It connects
// directly to that node without topo_config resolution and is the only entry
// point that creates topo tables — NewServer never does — so it must be invoked
// explicitly against the node chosen to host the topology (i.e. from a cluster
// bootstrap). It first verifies the GTID/binlog configuration required for
// change notifications. Table creation is idempotent.
func CreateSchema(serverAddr string) error {
	cfg, err := mysql.ParseDSN(serverAddr)
	if err != nil {
		return fmt.Errorf("failed to parse MySQL DSN: %v", err)
	}
	if cfg.DBName == "" {
		cfg.DBName = DefaultSchema
	}

	db, err := connect(cfg)
	if err != nil {
		return err
	}
	defer db.Close()

	// Binlog replication is required for change notifications and is only
	// meaningful on the node that hosts the topology, so it is checked here at
	// creation time rather than on every open.
	if err := checkMySQLConfiguration(db); err != nil {
		return fmt.Errorf("MySQL configuration check failed: %v", err)
	}
	if err := createTables(db); err != nil {
		return fmt.Errorf("failed to create tables: %v", err)
	}
	cleanupExpiredData(db)
	return nil
}

// createTables creates the required topo tables if they don't already exist.
func createTables(db *sql.DB) error {
	queries := []string{
		// topo_data table stores the topology data
		`CREATE TABLE IF NOT EXISTS topo_data (
			path VARCHAR(512) NOT NULL PRIMARY KEY,
			data MEDIUMBLOB,
			version BIGINT NOT NULL DEFAULT 1,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		) ENGINE=InnoDB`,

		// topo_locks table stores lock information
		`CREATE TABLE IF NOT EXISTS topo_locks (
			path VARCHAR(512) NOT NULL PRIMARY KEY,
			contents TEXT,
			expires_at TIMESTAMP NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			INDEX expires_idx (expires_at)
		) ENGINE=InnoDB`,

		// topo_elections table stores leader election information
		`CREATE TABLE IF NOT EXISTS topo_elections (
			name VARCHAR(512) NOT NULL PRIMARY KEY,
			leader_id VARCHAR(255) NOT NULL,
			contents TEXT,
			expires_at TIMESTAMP NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			INDEX expires_idx (expires_at)
		) ENGINE=InnoDB`,
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("failed to create table: %v", err)
		}
	}

	return nil
}

// checkClosed returns an error if the server has been closed.
func (s *Server) checkClosed() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return context.Canceled
	}
	return nil
}

// Close implements topo.Server.Close.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.closed = true

	log.Info("MySQL topo: closing server", slog.String("root", s.root), slog.String("schema", s.schemaName))

	// Cancel the server context
	if s.cancel != nil {
		s.cancel()
	}

	// Release the notification system only if we acquired a reference
	if s.hasNotificationSystem {
		releaseNotificationSystem(s.schemaName)
	}

	// Close the database connection
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			log.Warn("MySQL topo: error closing database connection", slog.String("root", s.root), slog.String("schema", s.schemaName), slog.Any("error", err))
		}
	}
}

// getNotificationSystemForServer gets the notification system for this server.
// It acquires a reference to the shared notification system on the first call,
// and returns the cached reference on subsequent calls. The reference is released
// when the server is closed.
func (s *Server) getNotificationSystemForServer() (*notificationSystem, error) {
	s.mu.Lock()
	alreadyAcquired := s.hasNotificationSystem
	if !alreadyAcquired {
		s.hasNotificationSystem = true
	}
	s.mu.Unlock()

	if alreadyAcquired {
		// We already hold a reference, just look up the existing notification system
		// without incrementing the refcount again.
		notificationSystemsMu.Lock()
		defer notificationSystemsMu.Unlock()
		ns, exists := notificationSystems[s.schemaName]
		if !exists {
			return nil, fmt.Errorf("notification system for schema %s not found", s.schemaName)
		}
		return ns, nil
	}

	return getNotificationSystem(s.schemaName, s.serverAddr)
}

// resolvePath returns the full path by combining the server's root with the given path
// For example:
// keyspaces/commerce => '/vitess/global/keyspaces/commerce'
// keyspaces/commerce/shards/0 => '/vitess/global/keyspaces/commerce/shards/0'
func (s *Server) resolvePath(filePath string) string {
	if s.root == "" || s.root == "/" {
		return filePath
	}
	return path.Join(s.root, filePath)
}

// relativePath converts a fullDirPath back to a relativePath
func (s *Server) relativePath(filePath, fullDirPath string) string {
	return strings.TrimPrefix(strings.TrimPrefix(filePath, fullDirPath), "/")
}

// convertError converts a MySQL error to a topo error.
func convertError(err error, path string) error {
	if err == nil {
		return nil
	}

	// Handle context errors
	if err == context.Canceled {
		return topo.NewError(topo.Interrupted, path)
	}
	if err == context.DeadlineExceeded {
		return topo.NewError(topo.Timeout, path)
	}

	// Handle SQL errors
	if err == sql.ErrNoRows {
		return topo.NewError(topo.NoNode, path)
	}

	// Handle MySQL-specific errors
	sqlErr, isSQLErr := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
	if isSQLErr && sqlErr != nil && sqlErr.Number() == sqlerror.ERDupEntry {
		return topo.NewError(topo.NodeExists, path)
	}
	// Default: return the original error
	return err
}

// cleanupExpiredData removes expired locks and elections.
func cleanupExpiredData(db *sql.DB) {
	now := time.Now()

	// Clean up expired locks - ignore errors if table doesn't exist yet
	if _, err := db.Exec("DELETE FROM topo_locks WHERE expires_at < ?", now); err != nil {
		log.Info("Skipping lock cleanup (table may not exist yet)", slog.Any("error", err))
	}

	// Clean up expired elections - ignore errors if table doesn't exist yet
	if _, err := db.Exec("DELETE FROM topo_elections WHERE expires_at < ?", now); err != nil {
		log.Info("Skipping election cleanup (table may not exist yet)", slog.Any("error", err))
	}
}

// matchDirectory creates a LIKE pattern for prefix matching a directory.
// It escapes special LIKE characters (_ and %) and appends % for prefix matching
func matchDirectory(prefix string) string {
	// Escape special LIKE characters
	pattern := strings.ReplaceAll(prefix, "_", "\\_")
	pattern = strings.ReplaceAll(pattern, "%", "\\%")
	pattern += "%"
	return pattern
}

// checkMySQLConfiguration verifies that MySQL is configured correctly for binlog replication.
func checkMySQLConfiguration(db *sql.DB) error {
	// Check GTID mode
	var gtidMode string
	err := db.QueryRow("SELECT @@GLOBAL.gtid_mode").Scan(&gtidMode)
	if err != nil {
		return fmt.Errorf("failed to check GTID mode: %v", err)
	}

	if gtidMode != "ON" {
		return fmt.Errorf("GTID mode is '%s' but must be 'ON' for MySQL topo server to work with binlog replication. Please set gtid_mode=ON in your MySQL configuration", gtidMode)
	}

	// Check that binary logging is enabled
	var logBin string
	err = db.QueryRow("SELECT @@GLOBAL.log_bin").Scan(&logBin)
	if err != nil {
		return fmt.Errorf("failed to check binary logging status: %v", err)
	}

	if logBin != "1" && logBin != "ON" {
		return errors.New("binary logging is disabled but is required for MySQL topo server. Please set log_bin=ON in your MySQL configuration")
	}

	return nil
}
