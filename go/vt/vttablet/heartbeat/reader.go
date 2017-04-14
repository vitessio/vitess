package heartbeat

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/hack"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const (
	sqlFetchMostRecentHeartbeat = "SELECT ts FROM %s.heartbeat WHERE keyspaceShard=%a"
)

// Reader reads the heartbeat table at a configured interval in order
// to calculate replication lag. It is meant to be run on a slave, and paired
// with a Writer on a master. It's primarily created and launched from Reporter.
// Lag is calculated by comparing the most recent timestamp in the heartbeat
// table against the current time at read time. This value is reported in metrics and
// also to the healthchecks.
type Reader struct {
	enabled       bool
	interval      time.Duration
	keyspaceShard string
	dbName        string
	now           func() time.Time
	errorLog      *logutil.ThrottledLogger

	mu             sync.Mutex
	isOpen         bool
	pool           *connpool.Pool
	ticks          *timer.Timer
	lastKnownLag   time.Duration
	lastKnownError error
}

// NewReader returns a new heartbeat reader.
func NewReader(checker connpool.MySQLChecker, config tabletenv.TabletConfig) *Reader {
	if !config.HeartbeatEnable {
		return &Reader{}
	}

	return &Reader{
		enabled:  true,
		now:      time.Now,
		interval: config.HeartbeatInterval,
		ticks:    timer.NewTimer(config.HeartbeatInterval),
		errorLog: logutil.NewThrottledLogger("HeartbeatReporter", 60*time.Second),
		pool:     connpool.New(config.PoolNamePrefix+"HeartbeatReadPool", 1, time.Duration(config.IdleTimeout*1e9), checker),
	}
}

// Init does last minute initialization of db settings, such as dbName
// and keyspaceShard
func (r *Reader) Init(dbc dbconfigs.DBConfigs, target query.Target) {
	if !r.enabled {
		return
	}
	r.dbName = sqlparser.Backtick(dbc.SidecarDBName)
	r.keyspaceShard = fmt.Sprintf("%s:%s", target.Keyspace, target.Shard)
}

// Open starts the heartbeat ticker and opens the db pool. It may be called multiple
// times, as long as it was closed since last invocation.
func (r *Reader) Open(dbc dbconfigs.DBConfigs) {
	if !r.enabled {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.isOpen {
		log.Fatalf("BUG: Reader object cannot be initialized twice without closing in between: %v", r)
		return
	}

	log.Info("Beginning heartbeat reads")
	r.pool.Open(&dbc.App, &dbc.Dba)
	r.ticks.Start(func() { r.readHeartbeat() })
	r.isOpen = true
}

// Close cancels the watchHeartbeat periodic ticker and closes the db pool.
// A reader object can be re-opened after closing.
func (r *Reader) Close() {
	if !r.enabled {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.isOpen {
		return
	}
	r.ticks.Stop()
	r.pool.Close()
	log.Info("Stopped heartbeat reads")
	r.isOpen = false
}

// GetLatest returns the most recently recorded lag measurement or error encountered.
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

	ctx, cancel := context.WithDeadline(context.Background(), r.now().Add(r.interval))
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
	conn, err := r.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	sel, err := r.bindHeartbeatFetch()
	if err != nil {
		return nil, err
	}
	return conn.Exec(ctx, sel, 1, false)
}

// bindHeartbeatFetch takes a heartbeat read and adds the necessary
// fields to the query as bind vars. This is done to protect ourselves
// against a badly formed keyspace or shard name.
func (r *Reader) bindHeartbeatFetch() (string, error) {
	bindVars := map[string]interface{}{
		"ks": sqltypes.MakeString([]byte(r.keyspaceShard)),
	}
	parsed := sqlparser.BuildParsedQuery(sqlFetchMostRecentHeartbeat, r.dbName, ":ks")
	bound, err := parsed.GenerateQuery(bindVars)
	if err != nil {
		return "", err
	}
	return hack.String(bound), nil
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
