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
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
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

const TopoDataTableName = "topo_data"

// notificationSystem handles MySQL replication and distributes notifications to all watchers
// across all server instances that use the same schema.
type notificationSystem struct {
	schemaName string
	serverAddr string

	// MySQL database connection for queries
	db        *sql.DB
	isMySQL84 bool // Flag to determine if server is MySQL 8.4+

	// MySQL replication using Vitess binlog
	binlogConn      *binlog.BinlogConnection
	connector       dbconfigs.Connector
	topoDataTableID uint64 // tableID for the topo_data table

	// Binlog format information
	format mysql.BinlogFormat

	// Position tracking for binlog streaming
	lastPositionMu sync.Mutex
	lastPosition   replication.Position

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

	// Check that GTID mode is enabled and format = ROW
	if err := checkMySQLSettings(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("MySQL configuration check failed: %v", err)
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

// run is the main replication loop with retry logic.
func (ns *notificationSystem) run() {
	defer ns.wg.Done()
	defer func() {
		if ns.binlogConn != nil {
			ns.binlogConn.Close()
		}
	}()

	log.Infof("Starting MySQL notification system for schema %s", ns.schemaName)

	const (
		maxRetries     = 5
		baseRetryDelay = 1 * time.Second
		maxRetryDelay  = 30 * time.Second
	)

	retryCount := 0

	for {
		if ns.ctx.Err() != nil {
			log.Infof("Context cancelled, stopping MySQL notification system for schema %s", ns.schemaName)
			return
		}

		// Create a new binlog connection for each retry attempt
		// This ensures we don't reuse a broken connection object
		if ns.binlogConn != nil {
			ns.binlogConn.Close()
		}
		var err error
		ns.binlogConn, err = binlog.NewBinlogConnection(ns.connector)
		if err != nil {
			if ns.ctx.Err() != nil {
				return // Context cancelled
			}

			retryCount++
			if retryCount > maxRetries {
				log.Errorf("Failed to create new binlog connection after %d retries: %v", maxRetries, err)
				return
			}

			// Calculate exponential backoff delay
			delay := time.Duration(1<<uint(retryCount-1)) * baseRetryDelay
			if delay > maxRetryDelay {
				delay = maxRetryDelay
			}

			log.Warningf("Failed to create binlog connection (attempt %d/%d): %v, retrying in %v", retryCount, maxRetries, err, delay)

			select {
			case <-ns.ctx.Done():
				return
			case <-time.After(delay):
				continue
			}
		}

		// Get the current last position
		ns.lastPositionMu.Lock()
		currentPosition := ns.lastPosition
		ns.lastPositionMu.Unlock()

		// If we have a previous position, try to restart from there
		var eventChan <-chan mysql.BinlogEvent
		var errChan <-chan error

		if !currentPosition.IsZero() {
			log.Infof("Restarting binlog dump from position %v (retry %d/%d)", currentPosition, retryCount, maxRetries)
			eventChan, errChan, err = ns.binlogConn.StartBinlogDumpFromPosition(ns.ctx, "", currentPosition)
		} else {
			log.Infof("Starting binlog dump from current position")
			var startPosition replication.Position
			startPosition, eventChan, errChan, err = ns.binlogConn.StartBinlogDumpFromCurrent(ns.ctx)
			if err == nil {
				// Save the starting position
				ns.lastPositionMu.Lock()
				ns.lastPosition = startPosition
				ns.lastPositionMu.Unlock()
			}
		}

		if err != nil {
			if ns.ctx.Err() != nil {
				return // Context cancelled
			}

			retryCount++
			if retryCount > maxRetries {
				log.Errorf("Failed to start binlog dump after %d retries: %v", maxRetries, err)
				return
			}

			// Calculate exponential backoff delay
			delay := time.Duration(1<<uint(retryCount-1)) * baseRetryDelay
			if delay > maxRetryDelay {
				delay = maxRetryDelay
			}

			log.Warningf("Failed to start binlog dump (attempt %d/%d): %v, retrying in %v", retryCount, maxRetries, err, delay)

			select {
			case <-ns.ctx.Done():
				return
			case <-time.After(delay):
				continue
			}
		}

		// Reset retry count on successful connection
		retryCount = 0

		// Process events from the binlog stream
		// This function is blocking, and continues to loop
		// through replicaiton events until it receives an error.
		if err := ns.processEventStream(eventChan, errChan); err != nil {
			if ns.ctx.Err() != nil {
				return // Context cancelled
			}

			// We received an error, it's not context related so presumably it
			// is MySQL connection related (server has gone away etc.)
			// We don't have to check what kind of error it is,
			// we can just continue which will restart the for loop and connect to the
			// last saved position.
			log.Warningf("Error processing binlog event stream: %v", err)
			continue
		}

		// If we reach here, the event stream ended normally
		// This is triggered by closing the channel eventChan
		log.Infof("Binlog event stream ended normally")
		return
	}
}

// processEventStream processes events from the binlog stream and updates the last position.
func (ns *notificationSystem) processEventStream(eventChan <-chan mysql.BinlogEvent, errChan <-chan error) error {
	for {
		select {
		case <-ns.ctx.Done():
			return ns.ctx.Err()
		case err := <-errChan:
			if err != nil {
				return err
			}
		case ev := <-eventChan:
			if ev == nil {
				return nil // Channel closed normally
			}

			// Process the event.
			if err := ns.processEvent(ev); err != nil {
				return fmt.Errorf("failed to process binlog event: %v", err)
			}

			// Update position tracking with GTID from the event if it's a GTID event
			// Only extract GTID from actual GTID events and only if we have a valid format
			if !ns.format.IsZero() && ev.IsGTID() {
				if gtidEvent, _, err := ev.GTID(ns.format); err == nil {
					ns.lastPositionMu.Lock()
					if ns.lastPosition.GTIDSet != nil {
						ns.lastPosition.GTIDSet = ns.lastPosition.GTIDSet.AddGTID(gtidEvent)
					}
					ns.lastPositionMu.Unlock()
				}
			}
		}
	}
}

// processEvent processes a binlog event and notifies watchers if needed.
// It is called in a go-routine but for simplicity we allow it to return
// errors that the caller will handle.
func (ns *notificationSystem) processEvent(ev mysql.BinlogEvent) error {
	if !ev.IsValid() {
		// Only process events that are valid
		return errors.New("invalid binlog event")
	}

	// We need to keep checking for FORMAT_DESCRIPTION_EVENT even after we've
	// seen one, because another one might come along (e.g. on log rotate due to
	// binlog settings change) that changes the format.
	if ev.IsFormatDescription() {
		format, err := ev.Format()
		if err != nil {
			return err
		}
		ns.format = format
		return nil
	}

	// We can't parse anything until we get a FORMAT_DESCRIPTION_EVENT that
	// tells us the size of the event header.
	if ns.format.IsZero() {
		// The only thing that should come before the FORMAT_DESCRIPTION_EVENT
		// is a fake ROTATE_EVENT, which the primary sends to tell us the name
		// of the current log file.
		if ev.IsRotate() {
			return nil
		}
		return errors.New("received an event before receiving a binlog format event, this is unexpected")
	}

	// Strip the checksum, if any. We don't actually verify the checksum, so discard it.
	// This is important to do before parsing TableMap events to avoid a panic.
	ev, _, err := ev.StripChecksum(ns.format)
	if err != nil {
		return fmt.Errorf("can't strip checksum from binlog event: %v, event data: %#v", err, ev)
	}

	// We only care about the table "topo_data". So if we receive a TableMap event,
	// we just need to determine the tableID for that table and save it for later.
	if ev.IsTableMap() {
		tableID := ev.TableID(ns.format)
		tm, err := ev.TableMap(ns.format)
		if err != nil {
			return fmt.Errorf("failed to parse TableMap event: %v", err)
		}
		if tm.Name == TopoDataTableName && tm.Database == ns.schemaName {
			ns.topoDataTableID = tableID
		}
		return nil
	}

	if ev.IsWriteRows() || ev.IsUpdateRows() || ev.IsDeleteRows() || ev.IsPartialUpdateRows() {
		tableID := ev.TableID(ns.format)
		if tableID == ns.topoDataTableID {
			// This is a data modification event on the topo_data table
			// This is enough for us to check for changes. We do not
			// rely on the contents of this row, we only use it
			// as a signal.
			if err := ns.checkForTopoDataChanges(); err != nil {
				return err // could not check for changes.
			}
		}
	}
	return nil
}

// checkForTopoDataChanges polls the topo_data table for recent changes.
// based on a notification from processEvent that there is a likely change.
// We don't know of what the change is, it might be unrelated.
// We have to call notifyChange() or notify Deletion() if we see
// any modifications though. We prefer to scan the table rather
// than read the stream because there are no staleness issues,
// particularly if there is a path updated twice in quick succession.
func (ns *notificationSystem) checkForTopoDataChanges() error {
	// Query all current data from topo_data table
	rows, err := ns.db.QueryContext(ns.ctx, "SELECT path, data, version FROM topo_data")
	if err != nil {
		// Check if the error is due to missing database or table
		errStr := err.Error()
		if strings.Contains(errStr, "Unknown database") || strings.Contains(errStr, "doesn't exist") {
			// Database or table was dropped, stop the notification system
			ns.cancel()
			return fmt.Errorf("topo database appears to be incorrectly formatted: %v", errStr)
		}
		return fmt.Errorf("failed to query topo_data for changes: %v", err)
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
			return fmt.Errorf("failed to scan topo_data row: %v", err)
		}

		currentData[path] = struct {
			data    []byte
			version int64
		}{data: data, version: version}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating topo_data rows: %v", err)
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
	return nil
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
		_ = ns.db.Close()
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

// checkMySQLSettings verifies that GTID mode is enabled on the MySQL server.
// This is required for the binlog replication functionality to work properly.
func checkMySQLSettings(db *sql.DB) error {
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

	// Check that the binlog format is row.
	var binlogFormat string
	err = db.QueryRow("SELECT @@GLOBAL.binlog_format").Scan(&binlogFormat)
	if err != nil {
		return fmt.Errorf("failed to check binlog format: %v", err)
	}
	if binlogFormat != "ROW" {
		return fmt.Errorf("binlog format is '%s' but must be 'ROW' for MySQL topo server to work with binlog replication. Please set binlog_format=ROW in your MySQL configuration", binlogFormat)
	}

	return nil
}
