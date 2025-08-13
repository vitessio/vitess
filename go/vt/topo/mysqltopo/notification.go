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
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// Global notification system that shares across all server instances with the same schema
var (
	notificationSystemsMu sync.RWMutex
	notificationSystems   = make(map[string]*notificationSystem)
)

// notificationSystem handles MySQL replication and distributes notifications to all watchers
// across all server instances that use the same schema.
type notificationSystem struct {
	schemaName string
	serverAddr string

	// MySQL database connection for queries
	db        *sql.DB
	isMySQL84 bool // Flag to determine if server is MySQL 8.4+

	// MySQL replication using Vitess binlog
	binlogConn *binlog.BinlogConnection
	connector  dbconfigs.Connector

	// Watchers from all server instances
	watchersMu        sync.RWMutex
	watchers          map[string]map[*watcher]bool          // path -> watchers
	recursiveWatchers map[string]map[*recursiveWatcher]bool // pathPrefix -> watchers

	// Local cache for deletion detection
	knownKeysMu sync.RWMutex
	knownKeys   map[string]int64 // path -> version, used to detect deletions

	// Control
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	refCount atomic.Int32
}

// watcher represents a single file watch.
type watcher struct {
	path    string
	changes chan *topo.WatchData
	ctx     context.Context
	cancel  context.CancelFunc
	deleted atomic.Bool
}

// recursiveWatcher represents a recursive directory watch.
type recursiveWatcher struct {
	pathPrefix string
	changes    chan *topo.WatchDataRecursive
	ctx        context.Context
	cancel     context.CancelFunc
}

// getNotificationSystem gets or creates a notification system for the given schema.
func getNotificationSystem(schemaName, serverAddr string) (*notificationSystem, error) {
	key := schemaName // Use schema name as key since notifications should be shared across same schema

	notificationSystemsMu.Lock()
	defer notificationSystemsMu.Unlock()

	if ns, exists := notificationSystems[key]; exists {
		ns.refCount.Add(1)
		return ns, nil
	}

	// Create new notification system
	ns, err := newNotificationSystem(schemaName, serverAddr)
	if err != nil {
		return nil, err
	}

	ns.refCount.Store(1)
	notificationSystems[key] = ns
	return ns, nil
}

// releaseNotificationSystem decrements the reference count and cleans up if needed.
func releaseNotificationSystem(schemaName string) {
	key := schemaName

	notificationSystemsMu.Lock()
	defer notificationSystemsMu.Unlock()

	if ns, exists := notificationSystems[key]; exists {
		ns.refCount.Add(-1)
		if ns.refCount.Load() <= 0 {
			ns.close()
			delete(notificationSystems, key)
		}
	}
}

// newNotificationSystem creates a new notification system.
func newNotificationSystem(schemaName, serverAddr string) (*notificationSystem, error) {
	// Create database connection for queries
	cfg, err := mysqldriver.ParseDSN(serverAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse MySQL DSN: %v", err)
	}
	if cfg.DBName == "" {
		cfg.DBName = schemaName
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %v", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping MySQL: %v", err)
	}

	// Detect MySQL version
	isMySQL84, err := detectMySQL84(db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to detect MySQL version: %v", err)
	}

	// Check that GTID mode is enabled - required for binlog replication
	if err := checkGTIDMode(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("MySQL GTID mode check failed: %v", err)
	}

	// Create connection parameters for binlog streaming
	cfg.DBName = schemaName

	// Parse host and port from cfg.Addr
	host, portStr, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to parse host and port from %s: %v", cfg.Addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("invalid port number %s: %v", portStr, err)
	}

	connParams := mysql.ConnParams{
		Host:   host,
		Port:   port,
		Uname:  cfg.User,
		Pass:   cfg.Passwd,
		DbName: schemaName,
	}
	connector := dbconfigs.New(&connParams)

	ctx, cancel := context.WithCancel(context.Background())

	ns := &notificationSystem{
		schemaName:        schemaName,
		serverAddr:        serverAddr,
		db:                db,
		isMySQL84:         isMySQL84,
		connector:         connector,
		watchers:          make(map[string]map[*watcher]bool),
		recursiveWatchers: make(map[string]map[*recursiveWatcher]bool),
		knownKeys:         make(map[string]int64),
		ctx:               ctx,
		cancel:            cancel,
	}

	// Initialize the binlog connection
	if err := ns.init(); err != nil {
		cancel()
		db.Close()
		return nil, fmt.Errorf("failed to initialize notification system: %v", err)
	}

	return ns, nil
}

// init initializes the notification system.
func (ns *notificationSystem) init() error {
	// Create the binlog connection using Vitess binlog library
	var err error
	ns.binlogConn, err = binlog.NewBinlogConnection(ns.connector)
	if err != nil {
		return fmt.Errorf("failed to create binlog connection: %v", err)
	}

	// Initialize the known keys cache with current data to avoid sending
	// notifications for existing data when watchers are first created
	if err := ns.initializeKnownKeys(); err != nil {
		return fmt.Errorf("failed to initialize known keys: %v", err)
	}

	// Start the replication goroutine
	ns.wg.Add(1)
	go ns.run()

	return nil
}

// initializeKnownKeys populates the known keys cache with current data
// to avoid sending notifications for existing data when watchers are first created.
func (ns *notificationSystem) initializeKnownKeys() error {
	rows, err := ns.db.QueryContext(ns.ctx, "SELECT path, version FROM topo_data")
	if err != nil {
		// Check if the error is due to missing database or table
		errStr := err.Error()
		if strings.Contains(errStr, "Unknown database") || strings.Contains(errStr, "doesn't exist") {
			// Database or table doesn't exist yet, which is fine
			return nil
		}
		return fmt.Errorf("failed to query topo_data for initialization: %v", err)
	}
	defer rows.Close()

	ns.knownKeysMu.Lock()
	defer ns.knownKeysMu.Unlock()

	for rows.Next() {
		var path string
		var version int64

		if err := rows.Scan(&path, &version); err != nil {
			log.Warningf("Failed to scan topo_data row during initialization: %v", err)
			continue
		}

		ns.knownKeys[path] = version
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating topo_data rows during initialization: %v", err)
	}

	return nil
}

// run is the main replication loop.
func (ns *notificationSystem) run() {
	defer ns.wg.Done()
	defer ns.binlogConn.Close()

	log.Infof("Starting MySQL notification system for schema %s", ns.schemaName)

	// Start binlog streaming from current position
	_, eventChan, errChan, err := ns.binlogConn.StartBinlogDumpFromCurrent(ns.ctx)
	if err != nil {
		log.Errorf("Failed to start binlog dump: %v", err)
		return
	}

	for {
		select {
		case <-ns.ctx.Done():
			log.Infof("Stopping MySQL notification system for schema %s", ns.schemaName)
			return
		case err := <-errChan:
			if err != nil {
				if ns.ctx.Err() != nil {
					return // Context cancelled
				}
				log.Warningf("Binlog stream error: %v", err)
				time.Sleep(time.Second)
				continue
			}
		case ev := <-eventChan:
			if ev == nil {
				log.Infof("Binlog event channel closed")
				return
			}
			// Process the event
			ns.processEvent(ev)
		}
	}
}

// processEvent processes a binlog event and notifies watchers if needed.
func (ns *notificationSystem) processEvent(ev mysql.BinlogEvent) {
	// Only process events that are valid
	if !ev.IsValid() {
		return
	}

	// We're not interested in table map events
	// It requires logic to parse them.
	if ev.IsTableMap() {
		return
	}

	// For other events (INSERT, UPDATE, DELETE, QUERY etc)
	// We treat them all the same which is to say that we
	// check for changes. We don't try and understand the row image,
	// because it requires tablemap parsing, and more complex logic.
	ns.checkForTopoDataChanges()
}

// checkForTopoDataChanges polls the topo_data table for recent changes.
// based on a notification from processEvent that there is a likely change.
// We don't know of what the change is, it might be unrelated.
// We have to call notifyChange() or notify Deletion() if we see
// any modifications though. We prefer to scan the table rather
// than read the stream because:
//  1. It's simpler and saves parsing the table map.
//  2. There are no staleness issues, particularly if
//     there is a path updated twice in quick succession.
func (ns *notificationSystem) checkForTopoDataChanges() {
	// Query all current data from topo_data table
	rows, err := ns.db.QueryContext(ns.ctx, "SELECT path, data, version FROM topo_data")
	if err != nil {
		// Check if the error is due to missing database or table
		errStr := err.Error()
		if strings.Contains(errStr, "Unknown database") || strings.Contains(errStr, "doesn't exist") {
			// Database or table was dropped, stop the notification system
			log.Infof("Database %s no longer exists, stopping notification system", ns.schemaName)
			ns.cancel()
			return
		}
		log.Warningf("Failed to query topo_data for changes: %v", err)
		return
	}
	defer rows.Close()

	// Build a map of current data
	currentData := make(map[string]struct {
		data    []byte
		version int64
	})

	for rows.Next() {
		var path string
		var data []byte
		var version int64

		if err := rows.Scan(&path, &data, &version); err != nil {
			log.Warningf("Failed to scan topo_data row: %v", err)
			continue
		}

		currentData[path] = struct {
			data    []byte
			version int64
		}{data: data, version: version}
	}

	if err := rows.Err(); err != nil {
		log.Warningf("Error iterating topo_data rows: %v", err)
		return
	}

	ns.knownKeysMu.Lock()
	defer ns.knownKeysMu.Unlock()

	// Check for new or updated entries
	for path, entry := range currentData {
		if knownVersion, exists := ns.knownKeys[path]; !exists || knownVersion != entry.version {
			// This is a new or updated entry
			ns.knownKeys[path] = entry.version
			// Notify watchers of the change
			ns.notifyChange(path, entry.data, MySQLVersion(entry.version))
		}
	}

	// Check for deleted entries
	for path := range ns.knownKeys {
		if _, exists := currentData[path]; !exists {
			// This entry was deleted
			delete(ns.knownKeys, path)
			// Notify watchers of the deletion
			ns.notifyDeletion(path)
		}
	}
}

// addWatcher adds a new file watcher.
func (ns *notificationSystem) addWatcher(w *watcher) {
	ns.watchersMu.Lock()
	defer ns.watchersMu.Unlock()

	if ns.watchers[w.path] == nil {
		ns.watchers[w.path] = make(map[*watcher]bool)
	}
	ns.watchers[w.path][w] = true
}

// removeWatcher removes a file watcher.
func (ns *notificationSystem) removeWatcher(w *watcher) {
	ns.watchersMu.Lock()
	defer ns.watchersMu.Unlock()

	if watchers := ns.watchers[w.path]; watchers != nil {
		delete(watchers, w)
		if len(watchers) == 0 {
			delete(ns.watchers, w.path)
		}
	}
}

// addRecursiveWatcher adds a new recursive watcher.
func (ns *notificationSystem) addRecursiveWatcher(w *recursiveWatcher) {
	ns.watchersMu.Lock()
	defer ns.watchersMu.Unlock()

	if ns.recursiveWatchers[w.pathPrefix] == nil {
		ns.recursiveWatchers[w.pathPrefix] = make(map[*recursiveWatcher]bool)
	}
	ns.recursiveWatchers[w.pathPrefix][w] = true
}

// removeRecursiveWatcher removes a recursive watcher.
func (ns *notificationSystem) removeRecursiveWatcher(w *recursiveWatcher) {
	ns.watchersMu.Lock()
	defer ns.watchersMu.Unlock()

	if watchers := ns.recursiveWatchers[w.pathPrefix]; watchers != nil {
		delete(watchers, w)
		if len(watchers) == 0 {
			delete(ns.recursiveWatchers, w.pathPrefix)
		}
	}
}

// notifyChange notifies watchers of a file change.
func (ns *notificationSystem) notifyChange(path string, data []byte, version topo.Version) {
	ns.watchersMu.RLock()
	defer ns.watchersMu.RUnlock()

	// Notify exact path watchers
	if watchers := ns.watchers[path]; watchers != nil {
		watchData := &topo.WatchData{
			Contents: data,
			Version:  version,
		}

		for w := range watchers {
			select {
			case w.changes <- watchData:
			case <-w.ctx.Done():
				// Watcher was cancelled, will be cleaned up later
			}
		}
	}

	// Notify recursive watchers
	for prefix, watchers := range ns.recursiveWatchers {
		if strings.HasPrefix(path, prefix) {
			watchData := &topo.WatchDataRecursive{
				Path: path,
				WatchData: topo.WatchData{
					Contents: data,
					Version:  version,
				},
			}

			for w := range watchers {
				select {
				case w.changes <- watchData:
				case <-w.ctx.Done():
					// Watcher was cancelled, will be cleaned up later
				}
			}
		}
	}
}

// notifyDeletion notifies watchers of a file deletion.
func (ns *notificationSystem) notifyDeletion(path string) {
	ns.watchersMu.RLock()
	defer ns.watchersMu.RUnlock()

	// Notify exact path watchers
	if watchers := ns.watchers[path]; watchers != nil {
		watchData := &topo.WatchData{
			Err: topo.NewError(topo.NoNode, path),
		}

		for w := range watchers {
			select {
			case w.changes <- watchData:
				// Mark this watcher as deleted and cancel it
				w.deleted.Store(true)
				w.cancel()
			case <-w.ctx.Done():
				// Watcher was cancelled, will be cleaned up later
			}
		}
	}

	// Notify recursive watchers
	for prefix, watchers := range ns.recursiveWatchers {
		if strings.HasPrefix(path, prefix) {
			watchData := &topo.WatchDataRecursive{
				Path: path,
				WatchData: topo.WatchData{
					Err: topo.NewError(topo.NoNode, path),
				},
			}

			for w := range watchers {
				select {
				case w.changes <- watchData:
					// For recursive watchers, we don't automatically cancel on single file deletion
					// since they may be watching for other files under the same prefix
				case <-w.ctx.Done():
					// Watcher was cancelled, will be cleaned up later
				}
			}
		}
	}
}

// close shuts down the notification system.
func (ns *notificationSystem) close() {
	ns.cancel()

	ns.watchersMu.Lock()
	// Cancel all watchers
	for _, watchers := range ns.watchers {
		for w := range watchers {
			w.cancel()
		}
	}

	for _, watchers := range ns.recursiveWatchers {
		for w := range watchers {
			w.cancel()
		}
	}

	ns.watchers = nil
	ns.recursiveWatchers = nil
	ns.watchersMu.Unlock()

	ns.wg.Wait()

	// Close the binlog connection
	if ns.binlogConn != nil {
		ns.binlogConn.Close()
	}

	// Close the database connection
	if ns.db != nil {
		ns.db.Close()
	}
}

// detectMySQL84 detects if the MySQL server is version 8.4 or higher.
func detectMySQL84(db *sql.DB) (bool, error) {
	var version string
	err := db.QueryRow("SELECT VERSION()").Scan(&version)
	if err != nil {
		return false, err
	}

	// Parse version string (e.g., "8.4.0-mysql" or "8.0.35-mysql")
	// We look for versions >= 8.4
	if strings.HasPrefix(version, "8.4") || strings.HasPrefix(version, "8.5") ||
		strings.HasPrefix(version, "8.6") || strings.HasPrefix(version, "8.7") ||
		strings.HasPrefix(version, "8.8") || strings.HasPrefix(version, "8.9") ||
		strings.HasPrefix(version, "9.") {
		return true, nil
	}

	return false, nil
}

// checkGTIDMode verifies that GTID mode is enabled on the MySQL server.
// This is required for the binlog replication functionality to work properly.
func checkGTIDMode(db *sql.DB) error {
	var gtidMode string
	err := db.QueryRow("SELECT @@GLOBAL.gtid_mode").Scan(&gtidMode)
	if err != nil {
		return fmt.Errorf("failed to check GTID mode: %v", err)
	}

	if gtidMode != "ON" {
		return fmt.Errorf("GTID mode is '%s' but must be 'ON' for MySQL topo server to work with binlog replication. Please set gtid_mode=ON in your MySQL configuration", gtidMode)
	}

	// Also check that log_bin is enabled
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
