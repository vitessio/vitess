package heartbeat

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	log "github.com/golang/glog"
)

// Writer runs on master tablets and writes heartbeats to the _vt.heartbeat
// table at a regular interval, defined by heartbeat_interval.
type Writer struct {
	mu      sync.Mutex
	isOpen  bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	inError bool

	topoServer  topo.Server
	tabletAlias topodata.TabletAlias
	now         func() time.Time
	conn        *dbconnpool.DBConnection
	errorLog    *logutil.ThrottledLogger
}

// NewWriter creates a new Writer.
func NewWriter(topoServer topo.Server, alias topodata.TabletAlias) *Writer {
	return &Writer{
		topoServer:  topoServer,
		tabletAlias: alias,
		now:         time.Now,
		errorLog:    logutil.NewThrottledLogger("HeartbeatWriter", 60*time.Second),
	}
}

// Open sets up the Writer's connections and launches the goroutine
// responsible for periodically writing to the heartbeat table.
func (w *Writer) Open(dbc dbconfigs.DBConfigs) error {
	if !*enableHeartbeat {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isOpen {
		return nil
	}

	allPrivs, err := dbconfigs.WithCredentials(&dbc.AllPrivs)
	if err != nil {
		return fmt.Errorf("Failed to get credentials for heartbeat: %v", err)
	}
	conn, err := dbconnpool.NewDBConnection(&allPrivs, tabletenv.MySQLStats)
	if err != nil {
		return fmt.Errorf("Failed to create connection for heartbeat: %v", err)
	}
	w.conn = conn
	ctx, cancel := context.WithCancel(tabletenv.LocalContext())
	w.cancel = cancel
	w.wg.Add(1)
	go w.run(ctx)

	w.isOpen = true
	return nil
}

// Close closes the Writer's connections, cancels the goroutine, and
// waits for the goroutine to finish.
func (w *Writer) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.isOpen {
		return
	}
	w.cancel()
	w.wg.Wait()
	w.conn.Close()
	w.isOpen = false
}

// run is the main goroutine of the Writer. It initializes
// the heartbeat table then writes heartbeat at the heartbeat_interval
// until cancelled
func (w *Writer) run(ctx context.Context) {
	defer w.wg.Done()
	defer tabletenv.LogError()

	log.Info("Initializing heartbeat table")
	w.waitForHeartbeatTable(ctx)

	log.Info("Beginning heartbeat writes")
	for {
		w.writeHeartbeat()
		if waitOrExit(ctx, *interval) {
			log.Info("Stopped heartbeat writes.")
			return
		}
	}
}

// waitForHeartbeatTable continually retries initializing of
// _vt.heartbeat table, until success or cancellation by the Context
func (w *Writer) waitForHeartbeatTable(ctx context.Context) {
	for {
		err := w.initHeartbeatTable()
		if err != nil {
			w.recordError(fmt.Errorf("Failed to initialize heartbeat table: %v", err))
			if waitOrExit(ctx, 10*time.Second) {
				return
			}
		}
		return
	}
}

// initHeartbeatTable attempts to create the _vt.heartbeat table exactly once.
func (w *Writer) initHeartbeatTable() error {
	_, err := w.conn.ExecuteFetch("CREATE DATABASE IF NOT EXISTS _vt", 0, false)
	if err != nil {
		return err
	}
	_, err = w.conn.ExecuteFetch("CREATE TABLE IF NOT EXISTS _vt.heartbeat (ts bigint NOT NULL, master_uid int unsigned NOT NULL PRIMARY KEY)", 0, false)
	if err != nil {
		return err
	}
	_, err = w.conn.ExecuteFetch(fmt.Sprintf("INSERT INTO _vt.heartbeat (ts, master_uid) VALUES (%d, %d) ON DUPLICATE KEY UPDATE ts=VALUES(ts)", w.now().UnixNano(), w.tabletAlias.Uid), 0, false)
	return err
}

// writeHeartbeat writes exactly one heartbeat record to _vt.heartbeat
func (w *Writer) writeHeartbeat() {
	updateQuery := "UPDATE _vt.heartbeat SET ts=%d WHERE master_uid=%d"
	_, err := w.conn.ExecuteFetch(fmt.Sprintf(updateQuery, w.now().UnixNano(), w.tabletAlias.Uid), 0, false)
	if err != nil {
		w.recordError(fmt.Errorf("Failed to update heartbeat: %v", err))
		return
	}
	w.inError = false
	counters.Add("Writes", 1)
}

func (w *Writer) recordError(err error) {
	w.errorLog.Errorf("%v", err)
	counters.Add("Errors", 1)
}
