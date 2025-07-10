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
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysqldriver "github.com/go-sql-driver/mysql"

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

	// MySQL replication configuration
	cfg    *replication.BinlogSyncerConfig
	syncer *replication.BinlogSyncer

	// Watchers from all server instances
	watchersMu        sync.RWMutex
	watchers          map[string]map[*watcher]bool          // path -> watchers
	recursiveWatchers map[string]map[*recursiveWatcher]bool // pathPrefix -> watchers

	// Control
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	refCount   int
	refCountMu sync.Mutex
}

// watcher represents a single file watch.
type watcher struct {
	path      string
	changes   chan *topo.WatchData
	ctx       context.Context
	cancel    context.CancelFunc
	deletedMu sync.Mutex
	deleted   bool // Flag to track if this watcher was cancelled due to file deletion
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
		ns.refCountMu.Lock()
		ns.refCount++
		ns.refCountMu.Unlock()
		return ns, nil
	}

	// Create new notification system
	ns, err := newNotificationSystem(schemaName, serverAddr)
	if err != nil {
		return nil, err
	}

	ns.refCount = 1
	notificationSystems[key] = ns
	return ns, nil
}

// releaseNotificationSystem decrements the reference count and cleans up if needed.
func releaseNotificationSystem(schemaName string) {
	key := schemaName

	notificationSystemsMu.Lock()
	defer notificationSystemsMu.Unlock()

	if ns, exists := notificationSystems[key]; exists {
		ns.refCountMu.Lock()
		ns.refCount--
		shouldClose := ns.refCount <= 0
		ns.refCountMu.Unlock()

		if shouldClose {
			ns.close()
			delete(notificationSystems, key)
		}
	}
}

// newNotificationSystem creates a new notification system.
func newNotificationSystem(schemaName, serverAddr string) (*notificationSystem, error) {
	// Parse the server address to get connection details
	host, port, user, password, err := parseReplicationAddr(serverAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse replication address: %v", err)
	}

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

	// Create replication configuration
	replCfg := replication.BinlogSyncerConfig{
		ServerID: generateServerID(),
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     user,
		Password: password,
	}

	ctx, cancel := context.WithCancel(context.Background())

	ns := &notificationSystem{
		schemaName:        schemaName,
		serverAddr:        serverAddr,
		db:                db,
		isMySQL84:         isMySQL84,
		cfg:               &replCfg,
		watchers:          make(map[string]map[*watcher]bool),
		recursiveWatchers: make(map[string]map[*recursiveWatcher]bool),
		ctx:               ctx,
		cancel:            cancel,
	}

	// Initialize the syncer
	if err := ns.init(); err != nil {
		cancel()
		db.Close()
		return nil, fmt.Errorf("failed to initialize notification system: %v", err)
	}

	return ns, nil
}

// init initializes the notification system.
func (ns *notificationSystem) init() error {
	// Create the binlog syncer
	ns.syncer = replication.NewBinlogSyncer(*ns.cfg)

	// Get the current master position
	position, err := ns.getCurrentPosition()
	if err != nil {
		return fmt.Errorf("failed to get current position: %v", err)
	}

	// Start the replication goroutine
	ns.wg.Add(1)
	go ns.run(position)

	return nil
}

// getCurrentPosition gets the current master binlog position by querying MySQL.
func (ns *notificationSystem) getCurrentPosition() (gomysql.Position, error) {
	var binlogFile, fake string
	var binlogPos uint32
	var binlogPosStmt = "SHOW MASTER STATUS"
	if ns.isMySQL84 {
		binlogPosStmt = "SHOW BINARY LOG STATUS"
	}
	err := ns.db.QueryRow(binlogPosStmt).Scan(&binlogFile, &binlogPos, &fake, &fake, &fake)
	if err != nil {
		return gomysql.Position{}, err
	}
	return gomysql.Position{
		Name: binlogFile,
		Pos:  binlogPos,
	}, nil
}

// run is the main replication loop.
func (ns *notificationSystem) run(position gomysql.Position) {
	defer ns.wg.Done()
	defer ns.syncer.Close()

	log.Infof("Starting MySQL notification system for schema %s", ns.schemaName)
	log.Infof("Starting binlog sync from position: %s:%d", position.Name, position.Pos)

	streamer, err := ns.syncer.StartSync(position)
	if err != nil {
		log.Errorf("Failed to start binlog sync: %v", err)
		return
	}

	for {
		select {
		case <-ns.ctx.Done():
			log.Infof("Stopping MySQL notification system for schema %s", ns.schemaName)
			return
		default:
		}

		// Read the next binlog event
		ctx, cancel := context.WithTimeout(ns.ctx, 5*time.Second)
		ev, err := streamer.GetEvent(ctx)
		cancel()

		if err != nil {
			if ns.ctx.Err() != nil {
				return // Context cancelled
			}
			log.Warningf("Failed to get binlog event: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// Process the event
		ns.processEvent(ev)
	}
}

// processEvent processes a binlog event and notifies watchers if needed.
func (ns *notificationSystem) processEvent(ev *replication.BinlogEvent) {
	switch e := ev.Event.(type) {
	case *replication.RowsEvent:
		// Handle row changes in our topo tables
		if string(e.Table.Schema) == ns.schemaName {
			ns.handleRowsEvent(e)
		}
	case *replication.QueryEvent:
		// Handle DDL or other queries that might affect our tables
		if strings.Contains(string(e.Query), "topo_data") ||
			strings.Contains(string(e.Query), "topo_locks") ||
			strings.Contains(string(e.Query), "topo_elections") {
			log.Infof("DDL event on topo table: %s", string(e.Query))
		}
	}
}

// handleRowsEvent handles row change events for topo tables.
func (ns *notificationSystem) handleRowsEvent(e *replication.RowsEvent) {
	tableName := string(e.Table.Table)

	switch tableName {
	case "topo_data":
		ns.handleTopoDataEvent(e)
	case "topo_locks":
		// Lock changes don't need to trigger watches
		log.Infof("Lock event on table %s", tableName)
	case "topo_elections":
		// Election changes might need special handling in the future
		log.Infof("Election event on table %s", tableName)
	}
}

// handleTopoDataEvent handles changes to the topo_data table.
// When we receive a replication event, we read the latest version from the table
// to ensure we send the most up-to-date data, preventing issues with delayed replication.
func (ns *notificationSystem) handleTopoDataEvent(e *replication.RowsEvent) {
	for _, row := range e.Rows {
		if len(row) < 1 {
			continue // Invalid row
		}

		// Extract path from the row
		path, ok := row[0].(string)
		if !ok {
			continue
		}

		// Instead of using potentially stale data from the binlog event,
		// read the latest version from the table to ensure we send current data
		ns.readAndNotifyLatestData(path)
	}
}

// readAndNotifyLatestData reads the latest data for a path from the database and notifies watchers.
func (ns *notificationSystem) readAndNotifyLatestData(path string) {
	// Query the latest data for this path using the existing database connection
	var data []byte
	var version int64
	err := ns.db.QueryRow("SELECT data, version FROM topo_data WHERE path = ?", path).Scan(&data, &version)
	if err != nil {
		if err == sql.ErrNoRows {
			// Path was deleted, notify deletion
			ns.notifyDeletion(path)
			return
		}
		log.Errorf("Failed to query latest data for path %s: %v", path, err)
		return
	}

	// Notify watchers with the latest data
	ns.notifyChange(path, data, MySQLVersion(version))
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
			default:
				// Channel is full, skip this notification
				log.Warningf("Watch channel full for path %s", path)
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
				default:
					// Channel is full, skip this notification
					log.Warningf("Recursive watch channel full for prefix %s", prefix)
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
				w.deletedMu.Lock()
				w.deleted = true
				w.deletedMu.Unlock()
				w.cancel()
			case <-w.ctx.Done():
				// Watcher was cancelled, will be cleaned up later
			default:
				// Channel is full, skip this notification
				log.Warningf("Watch channel full for path %s", path)
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
				default:
					// Channel is full, skip this notification
					log.Warningf("Recursive watch channel full for prefix %s", prefix)
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

// parseReplicationAddr parses the server address for replication connection.
func parseReplicationAddr(addr string) (host string, port int, user, password string, err error) {
	cfg, err := mysqldriver.ParseDSN(addr) // Ensure the DSN is valid
	if err != nil {
		return "", 0, "", "", fmt.Errorf("invalid MySQL DSN: %v", err)
	}
	// Use the net package to split cfg.Addr into host and port
	host, portStr, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return "", 0, "", "", fmt.Errorf("failed to split host and port: %v", err)
	}
	// Cast portStr to integer
	port, err = strconv.Atoi(portStr)
	if err != nil {
		return "", 0, "", "", fmt.Errorf("invalid port number: %v", err)
	}
	return host, port, cfg.User, cfg.Passwd, nil

}

// generateServerID generates a unique server ID for replication.
func generateServerID() uint32 {
	// Use current timestamp to generate a unique server ID
	// In production, this should be more sophisticated
	return uint32(time.Now().Unix() % 1000000)
}
