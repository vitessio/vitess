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

	"strconv"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/hack"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"
)

const (
	sqlTurnoffBinlog        = "set @@session.sql_log_bin = 0"
	sqlCreateSidecarDB      = "create database if not exists %s"
	sqlCreateHeartbeatTable = `CREATE TABLE IF NOT EXISTS %s.heartbeat (
  keyspaceShard VARBINARY(256) NOT NULL PRIMARY KEY,
  tabletUid INT UNSIGNED NOT NULL,
  ts BIGINT UNSIGNED NOT NULL
        ) engine=InnoDB`
	sqlInsertInitialRow = "INSERT INTO %s.heartbeat (ts, tabletUid, keyspaceShard) VALUES (%a, %a, %a) ON DUPLICATE KEY UPDATE ts=VALUES(ts)"
	sqlUpdateHeartbeat  = "UPDATE %s.heartbeat SET ts=%a, tabletUid=%a WHERE keyspaceShard=%a"
)

// Writer runs on master tablets and writes heartbeats to the _vt.heartbeat
// table at a regular interval, defined by heartbeat_interval.
type Writer struct {
	enabled       bool
	interval      time.Duration
	tabletAlias   topodata.TabletAlias
	keyspaceShard string
	dbName        string
	now           func() time.Time
	errorLog      *logutil.ThrottledLogger

	mu     sync.Mutex
	isOpen bool
	pool   *connpool.Pool
	ticks  *timer.Timer
}

// NewWriter creates a new Writer.
func NewWriter(checker connpool.MySQLChecker, alias topodata.TabletAlias, config tabletenv.TabletConfig) *Writer {
	if !config.HeartbeatEnable {
		return &Writer{}
	}
	return &Writer{
		enabled:     true,
		tabletAlias: alias,
		now:         time.Now,
		interval:    config.HeartbeatInterval,
		ticks:       timer.NewTimer(config.HeartbeatInterval),
		errorLog:    logutil.NewThrottledLogger("HeartbeatWriter", 60*time.Second),
		pool:        connpool.New(config.PoolNamePrefix+"HeartbeatWritePool", 1, time.Duration(config.IdleTimeout*1e9), checker),
	}
}

// Init runs at tablet startup and last minute initialization of db settings, and
// creates the necessary tables for heartbeat.
func (w *Writer) Init(dbc dbconfigs.DBConfigs, target query.Target) error {
	if !w.enabled {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	log.Info("Initializing heartbeat table.")
	w.dbName = sqlparser.Backtick(dbc.SidecarDBName)
	w.keyspaceShard = fmt.Sprintf("%s:%s", target.Keyspace, target.Shard)
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
	if !w.enabled {
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
	if !w.enabled {
		return
	}
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
// initial row. This happens on every tablet individually, regardless of slave
// or master. For that reason, we use values that are common between them, such as keyspace:shard,
// and we also execute them with an isolated connection that turns off the binlog and
// is closed at the end.
func (w *Writer) initializeTables(cp *sqldb.ConnParams) error {
	conn, err := dbconnpool.NewDBConnection(cp, stats.NewTimings(""))
	if err != nil {
		return fmt.Errorf("Failed to create connection for heartbeat: %v", err)
	}
	defer conn.Close()
	statements := []string{
		sqlTurnoffBinlog,
		fmt.Sprintf(sqlCreateSidecarDB, w.dbName),
		fmt.Sprintf(sqlCreateHeartbeatTable, w.dbName),
	}
	for _, s := range statements {
		if _, err := conn.ExecuteFetch(s, 0, false); err != nil {
			return fmt.Errorf("Failed to execute heartbeat init query: %v", err)
		}
	}
	insert, err := w.bindHeartbeatVars(sqlInsertInitialRow)
	if err != nil {
		return fmt.Errorf("Failed to bindHeartbeatVars initial heartbeat insert: %v", err)
	}
	_, err = conn.ExecuteFetch(insert, 0, false)
	if err != nil {
		return fmt.Errorf("Failed to execute initial heartbeat insert: %v", err)
	}
	writes.Add(1)
	return nil
}

// bindHeartbeatVars takes a heartbeat write (insert or update) and
// adds the necessary fields to the query as bind vars. This is done
// to protect ourselves against a badly formed keyspace or shard name.
func (w *Writer) bindHeartbeatVars(query string) (string, error) {
	bindVars := map[string]interface{}{
		"ks":  sqltypes.MakeString([]byte(w.keyspaceShard)),
		"ts":  sqltypes.MakeTrusted(sqltypes.Uint64, strconv.AppendUint(nil, uint64(w.now().UnixNano()), 10)),
		"uid": sqltypes.MakeTrusted(sqltypes.Uint32, strconv.AppendUint(nil, uint64(w.tabletAlias.Uid), 10)),
	}
	parsed := sqlparser.BuildParsedQuery(query, w.dbName, ":ts", ":uid", ":ks")
	bound, err := parsed.GenerateQuery(bindVars)
	if err != nil {
		return "", err
	}
	return hack.String(bound), nil
}

// writeHeartbeat updates the heartbeat row for this tablet with the current time in nanoseconds.
func (w *Writer) writeHeartbeat() {
	defer tabletenv.LogError()
	ctx, cancel := context.WithDeadline(context.Background(), w.now().Add(w.interval))
	defer cancel()
	update, err := w.bindHeartbeatVars(sqlUpdateHeartbeat)
	if err != nil {
		w.recordError(err)
		return
	}
	err = w.exec(ctx, update)
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
