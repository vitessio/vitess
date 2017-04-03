package heartbeat

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// Writer is the engine for reading and writing heartbeats
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
}

// NewWriter creates a new Engine
func NewWriter(topoServer topo.Server, alias topodata.TabletAlias) *Writer {
	return &Writer{
		topoServer:  topoServer,
		tabletAlias: alias,
		now:         time.Now,
	}
}

// Open starts the Engine service
func (me *Writer) Open(dbc dbconfigs.DBConfigs) error {
	if !*enableHeartbeat {
		return nil
	}

	me.mu.Lock()
	defer me.mu.Unlock()
	if me.isOpen {
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
	me.conn = conn
	ctx, cancel := context.WithCancel(tabletenv.LocalContext())
	me.cancel = cancel
	me.wg.Add(1)
	go me.run(ctx)

	me.isOpen = true
	return nil
}

// Close closes the Engine service
func (me *Writer) Close() {
	me.mu.Lock()
	defer me.mu.Unlock()
	if !me.isOpen {
		return
	}
	me.conn.Close()
	me.cancel()
	me.wg.Wait()
	me.isOpen = false
}

func (me *Writer) run(ctx context.Context) {
	defer func() {
		tabletenv.LogError()
		me.wg.Done()
	}()

	me.waitForHeartbeatTable(ctx)

	for {
		me.writeHeartbeat()
		if waitOrExit(ctx, *interval) {
			return
		}
	}
}

func (me *Writer) waitForHeartbeatTable(ctx context.Context) {
	glog.Info("Initializing heartbeat table")
	for {
		err := me.initHeartbeatTable()
		if err != nil {
			me.recordError("Failed to initialize heartbeat table: %v", err)
			if waitOrExit(ctx, 10*time.Second) {
				return
			}
		}
	}
}

func (me *Writer) initHeartbeatTable() error {
	_, err := me.conn.ExecuteFetch("CREATE DATABASE IF NOT EXISTS _vt", 0, false)
	if err != nil {
		return err
	}
	_, err = me.conn.ExecuteFetch("CREATE TABLE IF NOT EXISTS _vt.heartbeat (ts bigint NOT NULL, master_uid int unsigned NOT NULL PRIMARY KEY)", 0, false)
	if err != nil {
		return err
	}
	_, err = me.conn.ExecuteFetch(fmt.Sprintf("INSERT INTO _vt.heartbeat (ts, master_uid) VALUES (%d, %d) ON DUPLICATE KEY UPDATE ts=VALUES(ts)", time.Now().UnixNano(), me.tabletAlias.Uid), 0, false)
	return err
}

func (me *Writer) writeHeartbeat() {
	updateQuery := "UPDATE _vt.heartbeat SET ts=%d WHERE master_uid=%d"
	_, err := me.conn.ExecuteFetch(fmt.Sprintf(updateQuery, me.now().Nanosecond(), me.tabletAlias.Uid), 0, false)
	if err != nil {
		me.recordError("Failed to update heartbeat: %v", err)
		return
	}
	me.inError = false
	counters.Add("Writes", 1)
}

func (me *Writer) recordError(formatString string, err error) {
	if !me.inError {
		glog.Errorf(formatString, err)
	}
	counters.Add("Errors", 1)
	me.inError = true
}
