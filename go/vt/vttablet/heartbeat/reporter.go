package heartbeat

import (
	"context"
	"fmt"
	"html/template"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/tabletmanager/events"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// reporter implements health.Reporter
type reporter struct {
	mu         sync.Mutex
	topoServer topo.Server
	mysqld     mysqlctl.MysqlDaemon
	tablet     *topodata.Tablet
	now        func() time.Time
	wg         *sync.WaitGroup

	isMaster        bool
	lastKnownLag    time.Duration
	lastKnownMaster uint32
	lastKnownError  error
}

// HTMLName is part of the health.Reporter interface
func (r *reporter) HTMLName() template.HTML {
	return template.HTML("MySQLHeartbeat")
}

// Report is part of the health.Reporter interface. It simply polls
// the last reported values from the watchHeartbeat routine
func (r *reporter) Report(isSlaveType, shouldQueryServiceBeRunning bool) (time.Duration, error) {
	if !isSlaveType {
		return 0, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastKnownError != nil {
		return 0, r.lastKnownError
	}
	return r.lastKnownLag, nil
}

// watchHeartbeat is meant to be called as a goroutine, and calls
// readHeartbeat repeatedly until told to exit by cancelling the Context
func (r *reporter) watchHeartbeat(ctx context.Context) {
	defer func() {
		tabletenv.LogError()
		r.wg.Done()
	}()

	event.AddListener(func(change *events.StateChange) {
		r.mu.Lock()
		r.isMaster = change.NewTablet.Type == topodata.TabletType_MASTER
		r.mu.Unlock()
	})

	for {
		r.mu.Lock()
		isMaster := r.isMaster
		r.mu.Unlock()

		if !isMaster {
			r.readHeartbeat(ctx)
		}

		if waitOrExit(ctx, *interval) {
			return
		}
	}
}

// readHeartbeat reads from the heartbeat table exactly once.
func (r *reporter) readHeartbeat(ctx context.Context) {
	conn, err := r.mysqld.GetAppConnection(ctx)
	defer conn.Recycle()

	if err != nil {
		r.recordError("Failed to get mysql connection: %v", err)
		return
	}
	res, err := conn.ExecuteFetch("SELECT ts, master_uid FROM _vt.heartbeat ORDER BY ts DESC LIMIT 1", 1, false)
	if err != nil {
		r.recordError("Failed to read heartbeat: %v", err)
		return
	}

	if len(res.Rows) != 1 {
		r.recordError("Failed to read heartbeat: %v", fmt.Errorf("writer query did not result in 1 row. Got %v", len(res.Rows)))
		return
	}

	ts, err := res.Rows[0][0].ParseInt64()
	if err != nil {
		r.recordError("Failed to parse heartbeat timestamp: %v", err)
		return
	}

	rawUID, err := res.Rows[0][1].ParseUint64()
	if err != nil {
		r.recordError("Failed to parse heartbeat master uid: %v", err)
		return
	}

	// Validate that we're reading from the right master. This is not synchronized
	// because it only happens here.
	masterUID := uint32(rawUID)
	if masterUID != r.lastKnownMaster {
		info, err := r.topoServer.GetShard(ctx, r.tablet.Keyspace, r.tablet.Shard)
		if err != nil {
			r.recordError("Could not get current master: %v", err)
			return
		}
		if info.MasterAlias.Uid != masterUID {
			r.recordError("", fmt.Errorf("Latest heartbeat is not from known master %v, with ts=%v, master_uid=%v", info.MasterAlias, ts, masterUID))
			return
		}
		r.lastKnownMaster = masterUID
	}

	lag := r.now().Sub(time.Unix(0, ts))
	counters.Add("LagNs", lag.Nanoseconds())

	r.mu.Lock()
	r.lastKnownLag = lag
	r.lastKnownError = nil
	r.mu.Unlock()
}

// recordError ensures we only log once when in an error state. It keeps
// track of the last error, which will be reported to the health check
// next time it checks in.
func (r *reporter) recordError(formatString string, err error) {
	r.mu.Lock()
	lastKnownError := r.lastKnownError
	r.lastKnownError = err
	r.mu.Unlock()

	if lastKnownError == nil {
		glog.Errorf(formatString, err)
	}
	counters.Add("Errors", 1)
}

// RegisterReporter registers the heartbeat reporter so that its
// measurements will be picked up in healthchecks
func RegisterReporter(topoServer topo.Server, mysqld mysqlctl.MysqlDaemon, tablet *topodata.Tablet) (*sync.WaitGroup, context.CancelFunc) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	if *enableHeartbeat {
		reporter := &reporter{
			topoServer: topoServer,
			mysqld:     mysqld,
			tablet:     tablet,
			now:        time.Now,
			wg:         &wg,
		}
		wg.Add(1)
		go reporter.watchHeartbeat(ctx)
		health.DefaultAggregator.Register("heartbeat_reporter", reporter)
	}
	return &wg, cancel
}
