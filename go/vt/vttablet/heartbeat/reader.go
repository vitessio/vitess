package heartbeat

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const (
	sqlFetchMostRecentHeartbeat = "SELECT ts FROM %s.heartbeat ORDER BY ts DESC LIMIT 1"
)

// Reader reads the heartbeat table at a configured interval in order
// to calculate replication lag. It is meant to be run on a slave, and paired
// with a Writer on a master. It's primarily created and launched from Reporter.
// Lag is calculated by comparing the most recent timestamp in the heartbeat
// table against the current time at read time. This value is reported in metrics and
// also to the healthchecks.
type Reader struct {
	mysqld   mysqlctl.MysqlDaemon
	dbName   string
	now      func() time.Time
	errorLog *logutil.ThrottledLogger

	mu             sync.Mutex
	isOpen         bool
	ticks          *timer.Timer
	lastKnownLag   time.Duration
	lastKnownError error
}

// NewReader returns a new heartbeat reader.
func NewReader(mysqld mysqlctl.MysqlDaemon, dbName string) *Reader {
	return &Reader{
		mysqld:   mysqld,
		dbName:   sqlparser.Backtick(dbName),
		now:      time.Now,
		ticks:    timer.NewTimer(*interval),
		errorLog: logutil.NewThrottledLogger("HeartbeatReporter", 60*time.Second),
	}
}

// Open starts the heartbeat ticker. It may be called multiple
// times, as long as it was closed since last invocation.
func (r *Reader) Open(dbc dbconfigs.DBConfigs) {
	if !*enableHeartbeat {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.isOpen {
		log.Fatalf("BUG: Reader object cannot be initialized twice without closing in between: %v", r)
		return
	}

	log.Info("Beginning heartbeat reads")
	r.ticks.Start(func() { r.readHeartbeat() })
	r.isOpen = true
}

// Close cancels the watchHeartbeat periodic ticker.
// A reader object can be re-opened after closing.
func (r *Reader) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.isOpen {
		return
	}
	r.ticks.Stop()
	log.Info("Stopped heartbeat reads")
	r.isOpen = false
}

// GetLatest returns the most recent lag measurement or error encountered.
func (r *Reader) GetLatest() (time.Duration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastKnownError != nil {
		return 0, r.lastKnownError
	}
	return r.lastKnownLag, nil
}

// readHeartbeat reads from the heartbeat table exactly once, updating
// the last known lag and/or error, and incrementing counters.
func (r *Reader) readHeartbeat() {
	defer tabletenv.LogError()

	ctx, cancel := context.WithDeadline(context.Background(), r.now().Add(*interval))
	defer cancel()

	res, err := r.fetchMostRecentHeartbeat(ctx)
	if err != nil {
		r.recordError(fmt.Errorf("Failed to read most recent heartbeat: %v", err))
		return
	}
	ts, err := parseHeartbeatResult(res)
	if err != nil {
		r.recordError(fmt.Errorf("Failed to parse heartbeat result: %v", err))
		return
	}

	lag := r.now().Sub(time.Unix(0, ts))
	lagNs.Add(lag.Nanoseconds())
	reads.Add(1)

	r.mu.Lock()
	r.lastKnownLag = lag
	r.lastKnownError = nil
	r.mu.Unlock()
}

// fetchMostRecentHeartbeat fetches the most recently recorded heartbeat from the heartbeat table,
// returning a result with the timestamp of the heartbeat.
func (r *Reader) fetchMostRecentHeartbeat(ctx context.Context) (*sqltypes.Result, error) {
	conn, err := r.mysqld.GetAppConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return conn.ExecuteFetch(fmt.Sprintf(sqlFetchMostRecentHeartbeat, r.dbName), 1, false)
}

// parseHeartbeatResult turns a raw result into the timestamp for processing.
func parseHeartbeatResult(res *sqltypes.Result) (int64, error) {
	if len(res.Rows) != 1 {
		return 0, fmt.Errorf("Failed to read heartbeat: writer query did not result in 1 row. Got %v", len(res.Rows))
	}
	ts, err := res.Rows[0][0].ParseInt64()
	if err != nil {
		return 0, err
	}
	return ts, nil
}

// recordError keeps track of the lastKnown error for reporting to the healthcheck.
// Errors tracked here are logged with throttling to cut down on log spam since
// operations can happen very frequently in this package.
func (r *Reader) recordError(err error) {
	r.mu.Lock()
	r.lastKnownError = err
	r.mu.Unlock()
	r.errorLog.Errorf("%v", err)
	readErrors.Add(1)
}
