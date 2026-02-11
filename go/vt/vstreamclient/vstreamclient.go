package vstreamclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// VStreamClient is the primary struct
type VStreamClient struct {
	name string
	conn *vtgateconn.VTGateConn

	// this session is used to obtain the shards for the keyspace, and to manage the state table
	session *vtgateconn.VTGateSession

	// the reader is the vstream reader, which is used to read the binlog events
	reader vtgateconn.VStreamReader
	filter *binlogdatapb.Filter
	flags  *vtgatepb.VStreamFlags

	// vgtidStateKeyspace and vgtidStateTable are the keyspace and table where the last VGtid is stored
	vgtidStateKeyspace string
	vgtidStateTable    string

	// may not be necessary...
	shardsByKeyspace map[string][]string

	// keep per table state and config, which is used to generate the vgtid filter.
	// this is a map of keyspace.table to TableConfig, since that's how the binlog table is stored
	tables map[string]*TableConfig

	// to avoid flushing too often, we will only flush if it has been at least minFlushDuration since the last flush.
	// we're relying on heartbeat events to handle max duration between flushes, in case there are no other events.
	minFlushDuration time.Duration

	// this is the duration between heartbeat events. This is not a duration because the server side
	// parameter only has a granularity of seconds.
	heartbeatSeconds int

	// lastEventReceivedAtUnix is the time the last event was received, which is used in the heartbeat monitor
	// to let us know if we've been disconnected from the stream.
	lastEventReceivedAtUnix atomic.Int64
	// lastFlushedVgtid is the last vgtid that was flushed, which is compared to the latestVgtid to determine
	// if we need to flush again.
	lastFlushedVgtid, latestVgtid *binlogdatapb.VGtid

	stats VStreamStats

	// these are the optional functions that are called for each event type
	eventFuncs map[binlogdatapb.VEventType]EventFunc
}

// VStreamStats keeps track of the number of rows processed and flushed for the whole stream
type VStreamStats struct {
	// how many rows have been processed for the whole stream, across all tables. These are incremented as each row
	// is processed, regardless of whether it is flushed.
	RowInsertCount int
	RowUpdateCount int
	RowDeleteCount int

	FlushedRowCount int // sum of rows flushed, regardless of table and including all insert/update/delete events

	// how many times the flush function was executed for the whole stream. Not incremented for no-ops
	FlushCount int
	// sum of successful, individual, table flush functions. Only increments if the table flush func is called
	TableFlushCount int

	LastFlushedAt time.Time // only set after a flush successfully completes
}

// New initializes a new VStreamClient, which is used to stream binlog events from Vitess.
func New(ctx context.Context, name string, conn *vtgateconn.VTGateConn, tables []TableConfig, opts ...Option) (*VStreamClient, error) {
	// validate required parameters
	if len(name) > 64 {
		return nil, fmt.Errorf("vstreamclient: name must be 64 characters or less, got %d", len(name))
	}

	if conn == nil {
		return nil, errors.New("vstreamclient: conn is required")
	}

	// initialize the VStreamClient, with options and settings to be set later
	v := &VStreamClient{
		name:             name,
		conn:             conn,
		session:          conn.Session("", nil),
		tables:           make(map[string]*TableConfig),
		minFlushDuration: DefaultMinFlushDuration,
	}

	var err error

	// load all shards, so we can validate settings before starting. It's not technically necessary to do this here,
	// but it's more user-friendly to fail early if there is misconfiguration. This needs to be done before running
	// the options, so that the shards are available for validation.
	v.shardsByKeyspace, err = getShardsByKeyspace(ctx, v.session)
	if err != nil {
		return nil, err
	}

	err = v.initTables(tables)
	if err != nil {
		return nil, err
	}

	// set options from the variadic list
	for _, opt := range opts {
		if err = opt(v); err != nil {
			return nil, err
		}
	}

	// validate required options and set defaults where possible

	if len(v.tables) == 0 {
		return nil, errors.New("vstreamclient: no tables configured")
	}

	// convert the tables into filter + rules

	rules := make([]*binlogdatapb.Rule, 0, len(v.tables))

	for _, table := range v.tables {
		rules = append(rules, &binlogdatapb.Rule{
			Match:  table.Keyspace,
			Filter: table.Query,
		})
	}

	v.filter = &binlogdatapb.Filter{
		Rules: rules,
	}

	if v.flags == nil {
		v.flags = DefaultFlags()
		if v.heartbeatSeconds > 0 {
			v.flags.HeartbeatInterval = uint32(v.heartbeatSeconds)
		}
	}

	// handle state lookup

	err = initStateTable(ctx, v.session, v.vgtidStateKeyspace, v.vgtidStateTable)
	if err != nil {
		return nil, err
	}

	var storedTableConfig map[string]*TableConfig
	var copyCompleted bool
	v.latestVgtid, storedTableConfig, copyCompleted, err = getLatestVGtid(ctx, v.session, v.name, v.vgtidStateKeyspace, v.vgtidStateTable)
	if err != nil {
		return nil, err
	}

	if v.latestVgtid == nil {
		// we need to bootstrap the stream, which means we need to create a new vgtid and store the table config
		v.latestVgtid, err = initVGtid(ctx, v.session, v.name, v.vgtidStateKeyspace, v.vgtidStateTable, v.tables, v.shardsByKeyspace)
		if err != nil {
			return nil, err
		}
	} else {
		// we need to check if the tables have changed since the last stream, to make
		// sure users aren't expecting to catch up on a new table that was added after the last stream.
		err = validateTableConfig(v.tables, storedTableConfig)
		if err != nil {
			return nil, err
		}

		// since we have a vgtid, but the copy never completed, vstream docs say we need to restart from the beginning
		if !copyCompleted {
			// TODO: we could probably handle the recovery, by resetting the vgtid to nil, and calling some user
			//       defined function to have them truncate/recreate the intended destination tables.
			return nil, errors.New("vstreamclient: copy phase not completed, need to restart stream")
		}
	}

	// initialize the streamer

	v.reader, err = conn.VStream(ctx, topodatapb.TabletType_REPLICA, v.latestVgtid, v.filter, v.flags)
	if err != nil {
		return nil, fmt.Errorf("vstreamclient: failed to create vstream: %w", err)
	}

	return v, nil
}

// Close flushes any pending buffered data and cleans up resources. Note that the VStream
// itself is stopped by cancelling the context passed to Run(), not by calling Close().
// Close should be called after Run() returns to ensure all buffered data is flushed.
func (v *VStreamClient) Close(ctx context.Context) error {
	// Flush any remaining buffered data that hasn't been flushed yet
	hasData := false
	for _, table := range v.tables {
		if len(table.currentBatch) > 0 {
			hasData = true
			break
		}
	}

	if !hasData {
		return nil
	}

	// Temporarily set lastFlushedAt to zero to force flush regardless of minFlushDuration
	v.stats.LastFlushedAt = time.Time{}

	return v.flush(ctx)
}

func getShardsByKeyspace(ctx context.Context, session *vtgateconn.VTGateSession) (map[string][]string, error) {
	query := "SHOW VITESS_SHARDS"
	result, err := session.Execute(ctx, query, nil, false)
	if err != nil {
		return nil, fmt.Errorf("vstreamclient: failed to get shards by keyspace: %w", err)
	}

	shardsByKeyspace := make(map[string][]string)

	for _, row := range result.Rows {
		keyspace, shard, found := strings.Cut(row[0].ToString(), "/")
		if !found {
			return nil, fmt.Errorf("vstreamclient: failed to parse keyspace_id: %s", row[0].ToString())
		}

		shardsByKeyspace[keyspace] = append(shardsByKeyspace[keyspace], shard)
	}

	return shardsByKeyspace, nil
}
