package vstreamer

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/sqlescape"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// VStreamer is the primary struct
type VStreamer struct {
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

	// stats about the stream
	// how many times the flush function was executed for the whole stream. Not incremented for no-ops
	flushCount int
	// how many times each individual table flush function was called. Only increments if the table flush func is called.
	tableFlushCount int
	// sum of rows flushed, regardless of table
	flushedRowCount int
	lastFlushedAt   time.Time

	// these are the optional functions that are called for each event type
	eventFuncs map[binlogdatapb.VEventType]EventFunc
}

// TableConfig is the configuration for a table, which is used to configure filtering and scanning of the results
type TableConfig struct {
	Keyspace string
	Table    string

	// if no configured, this is set to "select * from keyspace.table", streaming all the fields. This can be
	// overridden to select only the fields that are needed, which can reduce memory usage and improve performance,
	// and also alias the fields to match the struct fields.
	Query string

	// MaxRowsPerFlush serves two purposes:
	//  1. it limits the number of rows that are flushed at once, to avoid large transactions. If more than
	//     this number of rows are processed, they will be flushed in chunks of this size.
	//  2. if this number is exceeded before reaching the minFlushDuration, it will trigger a flush to avoid
	//     holding too much memory in the rows slice.
	MaxRowsPerFlush int

	// if true, will reuse the same slice for each batch, which can reduce memory allocations. It does mean
	// that the caller must copy the data if they want to keep it, because the slice will be reused.
	ReuseBatchSlice bool

	// if true, will error and block the stream if there are fields in the result that don't match the struct
	ErrorOnUnknownFields bool

	// this is the function that will be called to flush the rows to the handler. This is called when the
	// minFlushDuration has passed, or the maxRowsPerFlush has been exceeded.
	FlushFn FlushFunc

	// TODO: translate this to *sql.Tx so we can pass it to the flush function
	FlushInTx bool

	// stats about flushes for this table
	flushCount      int
	flushedRowCount int
	// the time this table had rows flushed, which might be different from the stream flush time
	lastFlushedAt time.Time

	// this is the data type for this table, which is used to scan the results. Regardless whether a pointer
	// or value is supplied, the return value will always be []*DataType.
	DataType       any
	underlyingType reflect.Type

	// this stores the current batch of rows for this table, which caches the results until the next flush.
	// This is always a []*DataType. A nil value represents a row delete...
	// TODO(not right...)
	currentBatch reflect.Value

	// this is the mapping of fields to the query results, which is used to scan the results. This is done
	// on a per-shard basis, because while unlikely, it's possible that the same table in different shards
	// could have different schemas.
	shards map[string]shardConfig
}

// shardConfig is the per-shard configuration for a table, which is used to scan the results
type shardConfig struct {
	fieldMap map[string]fieldMapping
	fields   []*querypb.Field
}

// fieldMapping caches the mapping of table fields to struct fields, to reduce reflection overhead. This is
// configured once per shard, and used for all rows in that shard, unless a DDL event changes the schema,
// in which case the mapping is updated.
type fieldMapping struct {
	rowIndex    int
	structIndex []int
	structType  any
	kind        reflect.Kind
	isPointer   bool
}

// New initializes a new VStreamer, which is used to stream binlog events from Vitess.
func New(ctx context.Context, conn *vtgateconn.VTGateConn, tables []TableConfig, opts ...Option) (*VStreamer, error) {
	// validate required parameters
	if conn == nil {
		return nil, fmt.Errorf("vstreamer: conn is required")
	}

	// initialize the VStreamer, with options and settings to be set later
	v := &VStreamer{
		conn:             conn,
		session:          conn.Session("", nil),
		tables:           make(map[string]*TableConfig),
		minFlushDuration: 500 * time.Millisecond,
	}

	// load all shards, so we can validate settings before starting. It's not technically necessary to do this here,
	// but it's more user-friendly to fail early if there is misconfiguration. This needs to be done before running
	// the options, so that the shards are available for validation.
	var err error
	// v.shardsByKeyspace, err = getShardsByKeyspace(ctx, v.session)
	// if err != nil {
	// 	return nil, err
	// }

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
		return nil, fmt.Errorf("vstreamer: no tables configured")
	}

	// convert the tables into filter + rules

	rules := make([]*binlogdatapb.Rule, 0, len(v.tables))

	for _, table := range v.tables {
		if table.Query == "" {
			table.Query = fmt.Sprintf("select * from %s.%s", sqlescape.EscapeID(table.Keyspace), sqlescape.EscapeID(table.Table))
		}

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

	// initialize the streamer

	v.reader, err = conn.VStream(ctx, topodatapb.TabletType_REPLICA, v.latestVgtid, v.filter, v.flags)
	if err != nil {
		return nil, fmt.Errorf("vstreamer: failed to create vstream: %w", err)
	}

	return v, nil
}

func getShardsByKeyspace(ctx context.Context, session *vtgateconn.VTGateSession) (map[string][]string, error) {
	query := "select keyspace_id, shard_name from _vt.shard"
	result, err := session.Execute(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("vstreamer: failed to get shards by keyspace: %w", err)
	}

	shardsByKeyspace := make(map[string][]string)

	for _, row := range result.Rows {
		keyspace, shard, found := strings.Cut(row[0].ToString(), "/")
		if !found {
			return nil, fmt.Errorf("vstreamer: failed to parse keyspace_id: %s", row[0].ToString())
		}

		shardsByKeyspace[keyspace] = append(shardsByKeyspace[keyspace], shard)
	}

	return shardsByKeyspace, nil
}

func (v *VStreamer) initTables(tables []TableConfig) error {
	if len(v.tables) > 0 {
		return fmt.Errorf("vstreamer: %d tables already configured", len(v.tables))
	}

	if len(tables) == 0 {
		return fmt.Errorf("vstreamer: no tables provided")
	}

	for _, table := range tables {
		// basic validation
		if table.DataType == nil {
			return fmt.Errorf("vstreamer: table %s.%s has no data type", table.Keyspace, table.Table)
		}

		if table.Keyspace == "" {
			return fmt.Errorf("vstreamer: table %v has no keyspace", table)
		}

		if table.Table == "" {
			return fmt.Errorf("vstreamer: table %v has no table name", table)
		}

		// make sure the keyspace and table exist in the cluster
		// keyspace, ok := v.shardsByKeyspace[table.Keyspace]
		// if !ok {
		// 	return fmt.Errorf("vstreamer: keyspace %s not found in the cluster", table.Keyspace)
		// }
		//
		// if !slices.Contains(keyspace, table.Table) {
		// 	return fmt.Errorf("vstreamer: table %s not found in keyspace %s in the cluster", table.Table, table.Keyspace)
		// }

		// the key is the keyspace and table name, separated by a period. We use this because the vstream
		// API uses this as the table name, and it's unique.
		k := fmt.Sprintf("%s.%s", table.Keyspace, table.Table)

		// if the same table is referenced multiple times in the same stream, only one table will actually
		// receive events. This prevents users from unknowingly missing events for the second table reference.
		if _, ok := v.tables[k]; ok {
			return fmt.Errorf("duplicate table %s in keyspace %s", table.Table, table.Keyspace)
		}

		// set defaults if not provided
		if table.Query == "" {
			table.Query = "select * from " + sqlescape.EscapeID(table.Table)
		}

		if table.MaxRowsPerFlush == 0 {
			table.MaxRowsPerFlush = DefaultMaxRowsPerFlush
		}

		// regardless whether the user provided a pointer to a struct or a struct, we want to store the
		// underlying type of the struct, so we can create new instances of it later
		table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

		if table.underlyingType.Kind() != reflect.Struct {
			return fmt.Errorf("vstreamer: data type for table %s.%s must be a struct", table.Keyspace, table.Table)
		}

		table.shards = make(map[string]shardConfig)

		// initialize the slice containing the batch of rows for this table
		table.resetBatch()

		// store the table in the map
		v.tables[k] = &table
	}

	return nil
}

func initStateTable(ctx context.Context, session *vtgateconn.VTGateSession, stateKeyspace, stateTable string) error {
	query := fmt.Sprintf(`create table if not exists %s.%s (
  keyspace varbinary(512) not null,
  table varbinary(512) not null,
  vgtid json,
  PRIMARY KEY (keyspace, table),
)`, stateKeyspace, stateTable)
	_, err := session.Execute(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("vstreamer: failed to create state table: %w", err)
	}

	return nil
}

func getLastVGtid(ctx context.Context, session *vtgateconn.VTGateSession, keyspace, table string) (*binlogdatapb.VGtid, error) {
	query := fmt.Sprintf(`select last_vgtid from %s.%s`, keyspace, table)
	result, err := session.Execute(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("vstreamer: failed to get last vgtid for %s.%s: %w", keyspace, table, err)
	}

	// if there are no rows, or the value is null, return nil, which will start the stream from the beginning
	if len(result.Rows) == 0 {
		return nil, nil
	}

	if result.Rows[0][0].IsNull() {
		return nil, nil
	}

	// otherwise, unmarshal the JSON value which should be a valid VGtid
	lastVGtidJSON, err := result.Rows[0][0].ToBytes()
	if err != nil {
		return nil, fmt.Errorf("vstreamer: failed to convert last vgtid to bytes: %w", err)
	}

	var lastVGtid binlogdatapb.VGtid
	err = json.Unmarshal(lastVGtidJSON, &lastVGtid)
	if err != nil {
		return nil, fmt.Errorf("vstreamer: failed to unmarshal last vgtid: %w", err)
	}

	return &lastVGtid, nil
}

func storeLastVGtid(ctx context.Context, session *vtgateconn.VTGateSession, keyspace, table string, vgtid *binlogdatapb.VGtid) error {
	query := fmt.Sprintf(`insert into last_vgtid from %s.%s`, keyspace, table)
	result, err := session.Execute(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("vstreamer: failed to get last vgtid for %s.%s: %w", keyspace, table, err)
	}

	// if there are no rows, or the value is null, return nil which will start the stream from the beginning
	if len(result.Rows) == 0 {
		return nil
	}

	if result.Rows[0][0].IsNull() {
		return nil
	}

	// otherwise, unmarshal the JSON value which should be a valid VGtid
	lastVGtidJSON, err := result.Rows[0][0].ToBytes()
	if err != nil {
		return fmt.Errorf("vstreamer: failed to convert last vgtid to bytes: %w", err)
	}

	var lastVGtid binlogdatapb.VGtid
	err = json.Unmarshal(lastVGtidJSON, &lastVGtid)
	if err != nil {
		return fmt.Errorf("vstreamer: failed to unmarshal last vgtid: %w", err)
	}

	return nil
}
