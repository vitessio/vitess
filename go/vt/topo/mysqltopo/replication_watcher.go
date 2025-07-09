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
	"math/rand/v2"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// ReplicationWatcher watches MySQL replication events for topo changes.
type ReplicationWatcher struct {
	s              *Server
	ctx            context.Context
	cancel         context.CancelFunc
	watchCallbacks map[string][]watchCallback
	mu             sync.Mutex
	conn           *mysql.Conn
	running        bool
}

// watchCallback is a callback function for watch events.
type watchCallback struct {
	path    string
	channel chan<- interface{}
}

// NewReplicationWatcher creates a new ReplicationWatcher.
func NewReplicationWatcher(s *Server) *ReplicationWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &ReplicationWatcher{
		s:              s,
		ctx:            ctx,
		cancel:         cancel,
		watchCallbacks: make(map[string][]watchCallback),
		running:        false,
	}
}

// Start starts the replication watcher.
func (w *ReplicationWatcher) Start() error {
	if w.running {
		return nil
	}

	// Get connection parameters from the server
	connParams := w.s.params

	// Create a connection for replication
	var err error
	w.conn, err = mysql.Connect(w.ctx, connParams)
	if err != nil {
		log.Warningf("Failed to connect to MySQL for replication: %v", err)
		return err
	}

	// Configure the connection for replication
	err = w.configureReplicationConnection()
	if err != nil {
		log.Warningf("Failed to configure replication connection: %v", err)
		w.conn.Close()
		return err
	}

	// Get the current binlog position
	position, err := w.getCurrentPosition()
	if err != nil {
		log.Warningf("Failed to get current binlog position: %v", err)
		w.conn.Close()
		return err
	}

	// Create a unique server ID for this replication client
	serverID := uint32(rand.IntN(100000) + 1000000)

	// Start binlog dump from current position
	err = w.conn.SendBinlogDumpCommand(serverID, "", position)
	if err != nil {
		log.Warningf("Failed to start binlog dump: %v", err)
		w.conn.Close()
		return err
	}

	// Start a goroutine to process binlog events
	go w.processBinlogEvents()

	w.running = true
	return nil
}

// Stop stops the replication watcher.
func (w *ReplicationWatcher) Stop() {
	if !w.running {
		return
	}

	w.cancel()
	if w.conn != nil {
		w.conn.Close()
	}
	w.running = false
}

// RegisterWatch registers a watch for a path.
func (w *ReplicationWatcher) RegisterWatch(path string, ch chan<- interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	callbacks, ok := w.watchCallbacks[path]
	if !ok {
		callbacks = make([]watchCallback, 0)
	}

	callbacks = append(callbacks, watchCallback{
		path:    path,
		channel: ch,
	})

	w.watchCallbacks[path] = callbacks
}

// UnregisterWatch unregisters a watch for a path.
func (w *ReplicationWatcher) UnregisterWatch(path string, ch chan<- interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	callbacks, ok := w.watchCallbacks[path]
	if !ok {
		return
	}

	newCallbacks := make([]watchCallback, 0, len(callbacks))
	for _, callback := range callbacks {
		if callback.channel != ch {
			newCallbacks = append(newCallbacks, callback)
		}
	}

	if len(newCallbacks) == 0 {
		delete(w.watchCallbacks, path)
	} else {
		w.watchCallbacks[path] = newCallbacks
	}
}

// getCurrentPosition gets the current binlog position using the existing MySQL flavor infrastructure.
func (w *ReplicationWatcher) getCurrentPosition() (replication.Position, error) {
	// Create a MySQL connection using the existing Vitess infrastructure
	// which will automatically handle version detection and flavor selection
	conn, err := mysql.Connect(w.ctx, w.s.params)
	if err != nil {
		return replication.Position{}, err
	}
	defer conn.Close()

	// Use the existing ShowPrimaryStatus method which automatically handles
	// SHOW MASTER STATUS vs SHOW BINARY LOG STATUS based on MySQL version
	primaryStatus, err := conn.ShowPrimaryStatus()
	if err != nil {
		return replication.Position{}, err
	}

	// Prefer GTID position when available and compatible
	if !primaryStatus.Position.IsZero() {
		// Check if this is a GTID-compatible position
		if primaryStatus.Position.MatchesFlavor(replication.Mysql56FlavorID) {
			return primaryStatus.Position, nil
		}
	}
	// Fall back to file position for file-based replication or when GTIDs are not available
	return primaryStatus.FilePosition, nil
}

// configureReplicationConnection configures the MySQL connection for replication.
func (w *ReplicationWatcher) configureReplicationConnection() error {
	// Set the master_heartbeat_period to prevent connection timeouts
	// This is important for long-running replication connections (30 seconds)
	_, err := w.conn.ExecuteFetch("SET @master_heartbeat_period = 30000000000", 0, false)
	if err != nil {
		log.Warningf("Failed to set master_heartbeat_period: %v", err)
		// Don't fail on this, as it's not critical
	}

	// Tell the server that we understand the format of events that will be used
	// if binlog_checksum is enabled on the server. This is the correct way to
	// handle checksums - we tell the server we can handle them, rather than
	// disabling them entirely.
	_, err = w.conn.ExecuteFetch("SET @source_binlog_checksum = @@global.binlog_checksum, @master_binlog_checksum = @@global.binlog_checksum", 0, false)
	if err != nil {
		log.Warningf("Failed to set binlog_checksum variables: %v", err)
		// Try the older syntax for compatibility
		_, err = w.conn.ExecuteFetch("SET @master_binlog_checksum = @@global.binlog_checksum", 0, false)
		if err != nil {
			log.Warningf("Failed to set master_binlog_checksum: %v", err)
			// This might still work, so don't fail here
		}
	}

	// Set slave_uuid to avoid conflicts
	// Generate a unique UUID for this replication client
	_, err = w.conn.ExecuteFetch("SET @slave_uuid = UUID()", 0, false)
	if err != nil {
		log.Warningf("Failed to set slave_uuid: %v", err)
		// Not critical, continue
	}

	return nil
}

// processBinlogEvents processes binlog events from MySQL.
func (w *ReplicationWatcher) processBinlogEvents() {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Recovered from panic in processBinlogEvents: %v", r)
			// Try to restart the replication watcher after a short delay
			time.Sleep(5 * time.Second)
			if err := w.Start(); err != nil {
				log.Errorf("Failed to restart replication watcher: %v", err)
			}
		}
	}()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			// Get the next binlog event
			ev, err := w.conn.ReadBinlogEvent()
			if err != nil {
				if err == context.Canceled {
					return
				}
				log.Errorf("Error reading binlog event: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Process the event
			w.processEvent(ev)
		}
	}
}

// processEvent processes a single binlog event.
func (w *ReplicationWatcher) processEvent(ev mysql.BinlogEvent) {
	// We're only interested in row events (INSERT, UPDATE, DELETE)
	if ev.IsWriteRows() || ev.IsUpdateRows() || ev.IsDeleteRows() {
		// We need to get the table map first to know which table this affects
		// For now, we'll process all row events and check the table name later
		w.processRowEvent(ev)
	}
}

// processRowEvent processes row-based replication events.
func (w *ReplicationWatcher) processRowEvent(ev mysql.BinlogEvent) {
	// To process row events, we need the table map information
	// This is a simplified implementation - in a real scenario, you'd need to
	// maintain a cache of table maps from TABLE_MAP_EVENT events

	// For now, we'll trigger notifications for any row changes
	// and let the watchers filter based on their specific paths

	// Since we can't easily extract the table name without the table map,
	// we'll notify all watchers about potential changes
	// This is less efficient but ensures we don't miss any changes

	w.notifyAllWatchers()
}

// notifyAllWatchers notifies all registered watchers about potential changes.
func (w *ReplicationWatcher) notifyAllWatchers() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for path, callbacks := range w.watchCallbacks {
		// Get the current value of the path
		data, version, err := w.s.Get(context.Background(), path)

		// Prepare the watch data
		var watchData *topo.WatchData
		if err != nil {
			if topo.IsErrType(err, topo.NoNode) {
				// Path was deleted
				watchData = &topo.WatchData{
					Err: topo.NewError(topo.NoNode, path),
				}
			} else {
				// Other error
				watchData = &topo.WatchData{
					Err: err,
				}
			}
		} else {
			// Path exists
			watchData = &topo.WatchData{
				Contents: data,
				Version:  version,
			}
		}

		// Notify all callbacks for this path
		for _, callback := range callbacks {
			select {
			case callback.channel <- watchData:
				// Successfully sent the notification
			default:
				// Channel is full, log a warning
				log.Warningf("Failed to send watch notification for path %s: channel is full", path)
			}
		}
	}
}
