package heartbeat

import (
	"context"
	"fmt"
	"sync"
	"time"

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
// table against the current time at read time. This value isreported in metrics and
// also to the healthchecks.
type Reader struct {
	topoServer topo.Server
	mysqld     mysqlctl.MysqlDaemon
	tablet     *topodata.Tablet
	now        func() time.Time
	errorLog   *logutil.ThrottledLogger

	wg              *sync.WaitGroup
	cancel          context.CancelFunc
	dbName          string
	lastKnownMaster uint32

	mu             sync.Mutex
	isMaster       bool
	lastKnownLag   time.Duration
	lastKnownError error
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

// Open starts the heartbeat goroutine
func (r *Reader) Open(dbc dbconfigs.DBConfigs) {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	r.dbName = sqlparser.Backtick(dbc.SidecarDBName)
	r.wg = wg
	r.cancel = cancel

	wg.Add(1)
	go r.watchHeartbeat(ctx)
}

// Close cancels the watchHeartbeat goroutine and waits for it to finish.
func (r *Reader) Close() {
	r.cancel()
	r.wg.Wait()
}

// GetLatest returns the most recent lag measurement or error encountered
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
			if err := r.readHeartbeat(ctx); err != nil {
				r.recordError(err)
			}
		}

		if waitOrExit(ctx, *interval) {
			return
		}
	}
}

// readHeartbeat reads from the heartbeat table exactly once.
func (r *Reader) readHeartbeat(ctx context.Context) error {
	res, err := r.fetchMostRecentHeartbeat(ctx)
	if err != nil {
		return fmt.Errorf("Failed to read most recent heartbeat: %v", err)
	}
	ts, masterUID, err := parseHeartbeatResult(res)
	if err != nil {
		return fmt.Errorf("Failed to parse heartbeat result: %v", err)
	}
	// Validate that we're reading from the right master. This is not synchronized
	// because it only happens here.
	if masterUID != r.lastKnownMaster {
		info, err := r.topoServer.GetShard(ctx, r.tablet.Keyspace, r.tablet.Shard)
		if err != nil {
			return fmt.Errorf("Could not get current master: %v", err)
		}
		if info.MasterAlias.Uid != masterUID {
			return fmt.Errorf("Latest heartbeat is not from known master %v, with ts=%v, master_uid=%v", info.MasterAlias, ts, masterUID)
		}
		r.lastKnownMaster = masterUID
	}

	lag := r.now().Sub(time.Unix(0, ts))
	lagNs.Add(lag.Nanoseconds())
	readCount.Add(1)

	r.mu.Lock()
	r.lastKnownLag = lag
	r.lastKnownError = nil
	r.mu.Unlock()
	return nil
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
func parseHeartbeatResult(res *sqltypes.Result) (int64, uint32, error) {
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
	return ts, uint32(rawUID), nil
}

// recordError keeps track of the lastKnown error for reporting to the healthcheck.
// Errors tracked here are logged with throttling to cut down on log spam since
// operations can happen very frequently in this package.
func (r *Reader) recordError(err error) {
	r.mu.Lock()
	r.lastKnownError = err
	r.mu.Unlock()
	r.errorLog.Errorf("%v", err)
	readErrorCount.Add(1)
}
