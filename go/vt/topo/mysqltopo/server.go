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

  - Tables are created automatically if they don't exist.
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
	"fmt"
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
	DefaultSchema = "vitess_topo"

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
			err = fmt.Errorf("failed to append RDS CA certificates")
			return
		}
		tlsConfig := &tls.Config{
			RootCAs: caCertPool,
		}
		err = mysql.RegisterTLSConfig("rds-topo", tlsConfig)
	})
	return err
}

// NewServer returns a new MySQL topo.Server.
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
		log.Infof("MySQL topo: skipping connection for DSN without credentials (will use global connection)")
		return &Server{
			root:       root,
			serverAddr: serverAddr,
			schemaName: cfg.DBName,
		}, nil
	}

	// If connecting to RDS, configure TLS
	if isRDSHost(cfg.Addr) {
		if err := initRDSTLS(); err != nil {
			return nil, fmt.Errorf("failed to initialize RDS TLS: %v", err)
		}
		cfg.TLSConfig = "rds-topo"
	}

	// Connect to MySQL
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %v", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping MySQL: %v", err)
	}

	// Create server context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	server := &Server{
		db:         db,
		root:       root,
		serverAddr: serverAddr,
		schemaName: cfg.DBName,
		ctx:        ctx,
		cancel:     cancel,
	}

	// Check if tables already exist (read-only mode check)
	var tableExists int
	err = db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = 'topo_data'", cfg.DBName).Scan(&tableExists)
	if err != nil {
		cancel()
		db.Close()
		return nil, fmt.Errorf("failed to check for existing tables: %v", err)
	}

	// If tables don't exist, we need write permissions to create them
	if tableExists == 0 {
		// Check MySQL configuration requirements for binlog replication (only needed for write mode)
		if err := checkMySQLConfiguration(db); err != nil {
			cancel()
			db.Close()
			return nil, fmt.Errorf("MySQL configuration check failed: %v", err)
		}

		// Create the required tables
		if err := server.createTablesIfNotExist(); err != nil {
			cancel()
			db.Close()
			return nil, fmt.Errorf("failed to create tables: %v", err)
		}

		// Clean up expired data on startup (after tables are created)
		server.cleanupExpiredData()
	} else {
		log.Infof("MySQL topo tables already exist, operating in read-only mode (skipping table creation and binlog checks)")
	}

	return server, nil
}

// createTablesIfNotExist creates the required tables if they don't exist.
func (s *Server) createTablesIfNotExist() error {
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
		if _, err := s.db.Exec(query); err != nil {
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

	log.Infof("MySQL topo: closing server (root=%s, schema=%s)", s.root, s.schemaName)

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
			log.Warningf("MySQL topo: error closing database connection (root=%s, schema=%s): %v", s.root, s.schemaName, err)
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
func (s *Server) cleanupExpiredData() {
	now := time.Now()

	// Clean up expired locks - ignore errors if table doesn't exist yet
	if _, err := s.db.Exec("DELETE FROM topo_locks WHERE expires_at < ?", now); err != nil {
		log.Infof("Skipping lock cleanup (table may not exist yet): %v", err)
	}

	// Clean up expired elections - ignore errors if table doesn't exist yet
	if _, err := s.db.Exec("DELETE FROM topo_elections WHERE expires_at < ?", now); err != nil {
		log.Infof("Skipping election cleanup (table may not exist yet): %v", err)
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
		return fmt.Errorf("binary logging is disabled but is required for MySQL topo server. Please set log_bin=ON in your MySQL configuration")
	}

	return nil
}
