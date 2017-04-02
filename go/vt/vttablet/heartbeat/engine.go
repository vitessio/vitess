package heartbeat

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var (
	enableHeartbeat = flag.Bool("enable_heartbeat", false, "Should vttablet record (if master) or check (if replica) replication heartbeat.")
	interval        = flag.Duration("heartbeat_interval", 1*time.Second, "How frequently to read and write replication heartbeat. Default 1s")

	counters = stats.NewCounters("HeartbeatCounters")
	_        = stats.NewRates("HeartbeatRates", counters, 15, 60*time.Second)
)

// Engine is the engine for reading and writing heartbeats
type Engine struct {
	mu      sync.Mutex
	isOpen  bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	inError bool

	topoServer  topo.Server
	target      query.Target
	tabletType  topodata.TabletType
	tabletAlias topodata.TabletAlias
	conn        *dbconnpool.DBConnection
}

// NewEngine creates a new Engine
func NewEngine(topoServer topo.Server, alias topodata.TabletAlias) *Engine {
	return &Engine{
		topoServer:  topoServer,
		tabletAlias: alias,
	}
}

// Open starts the Engine service
func (me *Engine) Open(dbc dbconfigs.DBConfigs, target query.Target) error {
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
	me.target = target
	me.tabletType = target.TabletType

	ctx, cancel := context.WithCancel(tabletenv.LocalContext())
	me.cancel = cancel
	me.wg.Add(1)
	go me.run(ctx)

	me.isOpen = true
	return nil
}

func (me *Engine) waitForHeartbeatTable(ctx context.Context, currentType topodata.TabletType) error {
	glog.Info("Initializing heartbeat table")
	for {
		err := me.initHeartbeatTable()
		if err == nil {
			return nil
		}
		me.recordError("Failed to initialize heartbeat table: %v", err)
		if exit := waitOrExit(ctx, 10*time.Second); exit {
			return err
		}
	}
}

func (me *Engine) initHeartbeatTable() error {
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

// Close closes the Engine service
func (me *Engine) Close() {
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

// ChangeTabletType informs the heartbeat engine of a tablet type change,
// which may result in a change from reading heartbeats to writing heartbeats
// or vice versa
func (me *Engine) ChangeTabletType(tabletType topodata.TabletType) {
	if !*enableHeartbeat {
		return
	}
	me.mu.Lock()
	me.tabletType = tabletType
	me.mu.Unlock()
}

func (me *Engine) run(ctx context.Context) {
	defer func() {
		tabletenv.LogError()
		me.wg.Done()
	}()

	currentType := topodata.TabletType_UNKNOWN
	currentID := me.tabletAlias.Uid

	for {
		if newType, newID, ok := me.checkStateChange(ctx, currentType, currentID); ok {
			currentType = newType
			currentID = newID

			switch currentType {
			case topodata.TabletType_MASTER:
				me.writeHeartbeat()
			default:
				me.readHeartbeat(currentID)
			}
		}

		if exit := waitOrExit(ctx, *interval); exit {
			return
		}

	}
}

func waitOrExit(ctx context.Context, interval time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(interval):
		return false
	}
}

func (me *Engine) checkStateChange(ctx context.Context, currentType topodata.TabletType, currentID uint32) (topodata.TabletType, uint32, bool) {
	me.mu.Lock()
	newType := me.tabletType
	me.mu.Unlock()

	if newType == currentType {
		return currentType, currentID, true
	}

	glog.Infof("Switching heartbeat tabletType to %v", newType)
	if newType == topodata.TabletType_MASTER {
		id := me.tabletAlias.Uid
		if err := me.waitForHeartbeatTable(ctx, currentType); err != nil {
			me.recordError("Error waiting for heartbeat table: %v", err)
			return topodata.TabletType_UNKNOWN, 0, false
		}
		return newType, id, true
	}

	info, err := me.topoServer.GetShard(ctx, me.target.Keyspace, me.target.Shard)
	if err != nil {
		me.recordError("Could not get current master: %v", err)
		return topodata.TabletType_UNKNOWN, 0, false
	}
	return newType, info.MasterAlias.GetUid(), true
}

func (me *Engine) writeHeartbeat() {
	updateQuery := "UPDATE _vt.heartbeat SET ts=%d WHERE master_uid=%d"
	_, err := me.conn.ExecuteFetch(fmt.Sprintf(updateQuery, currentTime(), me.tabletAlias.Uid), 0, false)
	if err != nil {
		me.recordError("Failed to update heartbeat: %v", err)
		return
	}
	me.recordSuccess("Writes")
}

func (me *Engine) readHeartbeat(currentID uint32) {
	res, err := me.conn.ExecuteFetch(fmt.Sprintf("SELECT ts FROM _vt.heartbeat WHERE master_uid=%d", currentID), 1, false)
	if err != nil {
		me.recordError("Failed to read heartbeat: %v", err)
		return
	}
	if len(res.Rows) != 1 {
		me.recordError("Failed to read heartbeat: %v", fmt.Errorf("Heartbeat query for master_uid %v did not result in 1 row. Got %v", currentID, len(res.Rows)))
		return
	}
	ts, err := res.Rows[0][0].ParseInt64()
	if err != nil {
		me.recordError("Failed to parse heartbeat timestamp: %v", err)
		return
	}

	lag := currentTime() - ts
	counters.Add("LagNs", lag)
	me.recordSuccess("Reads")
}

func (me *Engine) recordSuccess(name string) {
	me.inError = false
	counters.Add(name, 1)
}

func (me *Engine) recordError(formatString string, err error) {
	if !me.inError {
		glog.Errorf(formatString, err)
	}
	counters.Add("Errors", 1)
	me.inError = true
}

func currentTime() int64 {
	return time.Now().UnixNano()
}
