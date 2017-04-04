package heartbeat

import (
	"context"
	"fmt"
	"html/template"
	"sync"
	"time"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/tabletmanager/events"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// Reporter implements health.Reporter. It includes
// a goroutine which reads from the _vt.hearbeat table at
// heartbeat_interval, comparing the time their against the current
// time to create a lag value. This lag is reported in metrics and
// also to the healthchecks.
type Reporter struct {
	topoServer      topo.Server
	mysqld          mysqlctl.MysqlDaemon
	tablet          *topodata.Tablet
	now             func() time.Time
	wg              *sync.WaitGroup
	cancel          context.CancelFunc
	errorLog        *logutil.ThrottledLogger
	dbName          string
	lastKnownMaster uint32

	mu             sync.Mutex
	isMaster       bool
	lastKnownLag   time.Duration
	lastKnownError error
}

// RegisterReporter registers the heartbeat reporter so that its
// measurements will be picked up in healthchecks.
func RegisterReporter(topoServer topo.Server, mysqld mysqlctl.MysqlDaemon, tablet *topodata.Tablet, dbc dbconfigs.DBConfigs) *Reporter {
	if !*enableHeartbeat {
		return nil
	}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	reader := &Reporter{
		topoServer: topoServer,
		mysqld:     mysqld,
		tablet:     tablet,
		now:        time.Now,
		wg:         wg,
		cancel:     cancel,
		errorLog:   logutil.NewThrottledLogger("HeartbeatReporter", 60*time.Second),
		dbName:     sqlparser.Backtick(dbc.SidecarDBName),
	}
	wg.Add(1)
	go reader.watchHeartbeat(ctx)
	health.DefaultAggregator.Register("heartbeat_reporter", reader)
	return reader
}

// HTMLName is part of the health.Reporter interface.
func (r *Reporter) HTMLName() template.HTML {
	return template.HTML("MySQLHeartbeat")
}

// Report is part of the health.Reporter interface. It returns the last reported value
// written by the watchHeartbeat goroutine. If we're the master, it just returns 0.
func (r *Reporter) Report(isSlaveType, shouldQueryServiceBeRunning bool) (time.Duration, error) {
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

// Close cancels the watchHeartbeat goroutine and waits for it to finish.
func (r *Reporter) Close() {
	r.cancel()
	r.wg.Wait()
}

// watchHeartbeat is meant to be called as a goroutine, and calls
// readHeartbeat repeatedly until told to exit by Close.
func (r *Reporter) watchHeartbeat(ctx context.Context) {
	defer r.wg.Done()
	defer tabletenv.LogError()

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
func (r *Reporter) readHeartbeat(ctx context.Context) {
	conn, err := r.mysqld.GetAppConnection(ctx)
	if err != nil {
		r.recordError(fmt.Errorf("Failed to get mysql connection: %v", err))
		return
	}
	defer conn.Recycle()
	res, err := conn.ExecuteFetch(fmt.Sprintf("SELECT ts, master_uid FROM %s.heartbeat ORDER BY ts DESC LIMIT 1", r.dbName), 1, false)
	if err != nil {
		r.recordError(fmt.Errorf("Failed to read heartbeat: %v", err))
		return
	}

	ts, rawUID, err := parseHeartbeatResult(res)
	if err != nil {
		r.recordError(fmt.Errorf("Failed to parse heartbeat result: %v", err))
		return
	}

	// Validate that we're reading from the right master. This is not synchronized
	// because it only happens here.
	masterUID := uint32(rawUID)
	if masterUID != r.lastKnownMaster {
		info, err := r.topoServer.GetShard(ctx, r.tablet.Keyspace, r.tablet.Shard)
		if err != nil {
			r.recordError(fmt.Errorf("Could not get current master: %v", err))
			return
		}
		if info.MasterAlias.Uid != masterUID {
			r.recordError(fmt.Errorf("Latest heartbeat is not from known master %v, with ts=%v, master_uid=%v", info.MasterAlias, ts, masterUID))
			return
		}
		r.lastKnownMaster = masterUID
	}

	lag := r.now().Sub(time.Unix(0, ts))
	counters.Add("LagNs", lag.Nanoseconds())
	counters.Add("Reads", 1)

	r.mu.Lock()
	r.lastKnownLag = lag
	r.lastKnownError = nil
	r.mu.Unlock()
}

func parseHeartbeatResult(res *sqltypes.Result) (int64, uint64, error) {
	if len(res.Rows) != 1 {
		return 0, 0, fmt.Errorf("Failed to read heartbeat: writer query did not result in 1 row. Got %v", len(res.Rows))
	}
	ts, err := res.Rows[0][0].ParseInt64()
	if err != nil {
		return 0, 0, err
	}
	rawUID, err := res.Rows[0][1].ParseUint64()
	if err != nil {
		return 0, 0, err
	}
	return ts, rawUID, nil
}

// recordError keeps track of the lastKnown error for reporting to the healthcheck.
// Errors tracked here are logged with throttling to cut down on log spam since
// operations can happen very frequently in this package.
func (r *Reporter) recordError(err error) {
	r.mu.Lock()
	r.lastKnownError = err
	r.mu.Unlock()
	r.errorLog.Errorf("%v", err)
	counters.Add("Errors", 1)
}
