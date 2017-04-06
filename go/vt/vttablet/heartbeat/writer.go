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
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"
)

const (
	sqlCreateSidecarDB      = "create database if not exists %s"
	sqlCreateHeartbeatTable = `CREATE TABLE IF NOT EXISTS %s.heartbeat (
  master_uid INT UNSIGNED NOT NULL PRIMARY KEY,
  ts BIGINT UNSIGNED NOT NULL
        ) engine=InnoDB`
	sqlInsertInitialRow = "INSERT INTO %s.heartbeat (master_uid, ts) VALUES (%d, %d) ON DUPLICATE KEY UPDATE ts=VALUES(ts)"
	sqlUpdateHeartbeat  = "UPDATE %v.heartbeat SET ts=%d WHERE master_uid=%d"
)

// Writer runs on master tablets and writes heartbeats to the _vt.heartbeat
// table at a regular interval, defined by heartbeat_interval.
type Writer struct {
	tabletAlias topodata.TabletAlias
	dbName      string
	now         func() time.Time
	errorLog    *logutil.ThrottledLogger

	mu     sync.Mutex
	isOpen bool
	pool   *connpool.Pool
	ticks  *timer.Timer
}

// NewWriter creates a new Writer.
func NewWriter(checker connpool.MySQLChecker, alias topodata.TabletAlias, config tabletenv.TabletConfig) *Writer {
	return &Writer{
		tabletAlias: alias,
		now:         time.Now,
		ticks:       timer.NewTimer(*interval),
		errorLog:    logutil.NewThrottledLogger("HeartbeatWriter", 60*time.Second),
		pool:        connpool.New(config.PoolNamePrefix+"HeartbeatWritePool", 1, time.Duration(config.IdleTimeout*1e9), checker),
	}
}

// Init runs at tablet startup and creates the necessary tables for heartbeat.
func (w *Writer) Init(dbc dbconfigs.DBConfigs) error {
	if !*enableHeartbeat {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	log.Info("Initializing heartbeat table.")
	w.dbName = sqlparser.Backtick(dbc.SidecarDBName)
	err := w.initializeTables(&dbc.Dba)
	if err != nil {
		w.recordError(err)
		return err
	}

	return nil
}

// Open sets up the Writer's db connection and launches the ticker
// responsible for periodically writing to the heartbeat table.
// Open may be called multiple times, as long as it was closed since
// last invocation.
func (w *Writer) Open(dbc dbconfigs.DBConfigs) {
	if !*enableHeartbeat {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isOpen {
		log.Fatalf("BUG: Writer object cannot be initialized twice without closing in between: %v", w)
		return
	}
	log.Info("Beginning heartbeat writes")
	w.pool.Open(&dbc.App, &dbc.Dba)
	w.ticks.Start(func() { w.writeHeartbeat() })
	w.isOpen = true
}

// Close closes the Writer's db connection and stops the periodic ticker. A writer
// object can be re-opened after closing.
func (w *Writer) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.isOpen {
		return
	}
	w.ticks.Stop()
	w.pool.Close()
	log.Info("Stopped heartbeat writes.")
	w.isOpen = false
}

// initializeTables attempts to create the heartbeat tables and record an
// initial row.
func (w *Writer) initializeTables(cp *sqldb.ConnParams) error {
	conn, err := dbconnpool.NewDBConnection(cp, stats.NewTimings(""))
	if err != nil {
		return fmt.Errorf("Failed to create connection for heartbeat: %v", err)
	}
	defer conn.Close()
	statements := []string{
		fmt.Sprintf(sqlCreateSidecarDB, w.dbName),
		fmt.Sprintf(sqlCreateHeartbeatTable, w.dbName),
		fmt.Sprintf(sqlInsertInitialRow, w.dbName, w.tabletAlias.Uid, w.now().UnixNano()),
	}
	for _, s := range statements {
		if _, err := conn.ExecuteFetch(s, 0, false); err != nil {
			return fmt.Errorf("Failed to execute heartbeat init query: %v", err)
		}
	}
	writes.Add(1)
	return nil
}

// writeHeartbeat updates the heartbeat row for this tablet with the current time in nanoseconds.
func (w *Writer) writeHeartbeat() {
	defer tabletenv.LogError()
	ctx, cancel := context.WithDeadline(context.Background(), w.now().Add(*interval))
	defer cancel()
	err := w.exec(ctx, fmt.Sprintf(sqlUpdateHeartbeat, w.dbName, w.now().UnixNano(), w.tabletAlias.Uid))
	if err != nil {
		w.recordError(err)
		return
	}
	writes.Add(1)
}

func (w *Writer) exec(ctx context.Context, query string) error {
	conn, err := w.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()
	_, err = conn.Exec(ctx, query, 0, false)
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) recordError(err error) {
	w.errorLog.Errorf("%v", err)
	writeErrors.Add(1)
}
