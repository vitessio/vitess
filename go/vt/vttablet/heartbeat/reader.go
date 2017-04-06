package heartbeat

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/tabletmanager/events"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const (
	sqlFetchMostRecentHeartbeat = "SELECT ts, master_uid FROM %s.heartbeat ORDER BY ts DESC LIMIT 1"
)

// Reader reads the heartbeat table at a configured interval in order
// to calculate replication lag. It is meant to be run on a slave, and paired
// with a Writer on a master. It's primarily created and launched from Reporter.
// Lag is calculated by comparing the most recent timestamp in the heartbeat
// table against the current time at read time. This value is reported in metrics and
// also to the healthchecks.
type Reader struct {
	topoServer topo.Server
	mysqld     mysqlctl.MysqlDaemon
	tablet     *topodata.Tablet
	now        func() time.Time
	errorLog   *logutil.ThrottledLogger

	wg     sync.WaitGroup
	cancel context.CancelFunc
	dbName string

	mu             sync.Mutex
	tabletType     topodata.TabletType
	lastKnownLag   time.Duration
	lastKnownError error
	isOpen         bool
}

// NewReader returns a new heartbeat reader.
func NewReader(topoServer topo.Server, mysqld mysqlctl.MysqlDaemon, tablet *topodata.Tablet) *Reader {
	return &Reader{
		topoServer: topoServer,
		mysqld:     mysqld,
		tablet:     tablet,
		now:        time.Now,
		errorLog:   logutil.NewThrottledLogger("HeartbeatReporter", 60*time.Second),
	}
}

// Open starts the heartbeat goroutine. It can only
// be called once and further invocations will raise a
// fatal error.
func (r *Reader) Open(dbc dbconfigs.DBConfigs) {
	if !*enableHeartbeat {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.isOpen {
		log.Fatalf("BUG: Reader object cannot be initialized twice: %v", r)
	}

	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.dbName = sqlparser.Backtick(dbc.SidecarDBName)

	r.wg.Add(1)
	go r.watchHeartbeat(ctx)
	r.isOpen = true
}

// Close cancels the watchHeartbeat goroutine and waits for it to finish.
// A Reader may not be re-opened once closed.
func (r *Reader) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancel()
	r.wg.Wait()
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

// watchHeartbeat is meant to be called as a goroutine, and calls
// fetchMostRecentHeartbeat repeatedly until told to exit by Close.
func (r *Reader) watchHeartbeat(ctx context.Context) {
	defer r.wg.Done()
	defer tabletenv.LogError()

	r.registerTabletStateListener()

	for {
		if err := r.readHeartbeat(ctx); err != nil {
			r.recordError(err)
		}

		if waitOrExit(ctx, *interval) {
			return
		}
	}
}

// registerTabletStateListener registers an event listener which
// can alert us to when the tablet becomes a master. In that case
// we want to halt reads, which we handle in readHeartbeat.
func (r *Reader) registerTabletStateListener() {
	event.AddListener(func(change *events.StateChange) {
		r.mu.Lock()
		r.tabletType = change.NewTablet.Type
		r.mu.Unlock()
	})
}

// readHeartbeat reads from the heartbeat table exactly once, if we're a replica.
// If we're a master, it exits without doing work.
func (r *Reader) readHeartbeat(ctx context.Context) error {
	if r.isMaster() {
		return nil
	}

	res, err := r.fetchMostRecentHeartbeat(ctx)
	if err != nil {
		return fmt.Errorf("Failed to read most recent heartbeat: %v", err)
	}
	ts, err := parseHeartbeatResult(res)
	if err != nil {
		return fmt.Errorf("Failed to parse heartbeat result: %v", err)
	}

	lag := r.now().Sub(time.Unix(0, ts))
	lagNs.Add(lag.Nanoseconds())
	reads.Add(1)

	r.mu.Lock()
	r.lastKnownLag = lag
	r.lastKnownError = nil
	r.mu.Unlock()
	return nil
}

func (r *Reader) isMaster() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tabletType == topodata.TabletType_MASTER
}

// fetchMostRecentHeartbeat fetches the most recently recorded heartbeat from the heartbeat table,
// returning a result with the timestamp and master_uid that the heartbeat came from.
func (r *Reader) fetchMostRecentHeartbeat(ctx context.Context) (*sqltypes.Result, error) {
	conn, err := r.mysqld.GetAppConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return conn.ExecuteFetch(fmt.Sprintf(sqlFetchMostRecentHeartbeat, r.dbName), 1, false)
}

// parseHeartbeatResult turns a raw result into the timestamp and master uid values
// for processing.
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
