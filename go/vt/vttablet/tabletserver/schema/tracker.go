/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package schema

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// VStreamer defines the functions of VStreamer
// that the schema tracker needs.
type VStreamer interface {
	Stream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter,
		throttlerApp throttlerapp.Name, send func([]*binlogdatapb.VEvent) error, options *binlogdatapb.VStreamOptions) error
}

// Tracker watches the replication stream and saves the latest schema into the schema_version table when a DDL is encountered.
type Tracker struct {
	enabled bool

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup

	env    tabletenv.Env
	vs     VStreamer
	engine *Engine
	wait   func(context.Context, time.Duration) bool
}

// NewTracker creates a Tracker, needs an Open SchemaEngine (which implements the trackerEngine interface)
func NewTracker(env tabletenv.Env, vs VStreamer, engine *Engine) *Tracker {
	return &Tracker{
		enabled: env.Config().TrackSchemaVersions,
		env:     env,
		vs:      vs,
		engine:  engine,
		wait:    waitWithContext,
	}
}

// Open enables the tracker functionality
func (tr *Tracker) Open() {
	if !tr.enabled {
		return
	}
	log.Info("Schema Tracker: opening")

	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.cancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(tabletenv.LocalContext())
	tr.cancel = cancel
	tr.wg.Add(1)

	go tr.process(ctx)
}

// Close disables the tracker functionality
func (tr *Tracker) Close() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.cancel == nil {
		return
	}

	tr.cancel()
	tr.cancel = nil
	tr.wg.Wait()
	log.Info("Schema Tracker: closed")
}

// Enable forces tracking to be on or off.
// Only used for testing.
func (tr *Tracker) Enable(enabled bool) {
	tr.mu.Lock()
	tr.enabled = enabled
	tr.mu.Unlock()
	if enabled {
		tr.Open()
	} else {
		tr.Close()
	}
}

func (tr *Tracker) process(ctx context.Context) {
	defer tr.env.LogError()
	defer tr.wg.Done()
	startupGTID, err := tr.startupPosition(ctx)
	if err != nil {
		log.Error(fmt.Sprintf("error getting the schema tracker's startup position: %v", err))
		return
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}

	gtid := startupGTID
	prevGtid := startupGTID
	options := &binlogdatapb.VStreamOptions{
		// We only want GTID and DDL events streamed to us.
		EventTypes: []binlogdatapb.VEventType{
			binlogdatapb.VEventType_GTID,
			binlogdatapb.VEventType_DDL,
		},
	}
	for {
		err := tr.vs.Stream(ctx, gtid, nil, filter, throttlerapp.SchemaTrackerName, func(events []*binlogdatapb.VEvent) error {
			for _, event := range events {
				if event.Type == binlogdatapb.VEventType_GTID {
					prevGtid = gtid
					gtid = event.Gtid
					continue
				}
				if event.Type == binlogdatapb.VEventType_DDL &&
					MustReloadSchemaOnDDL(event.Statement, tr.engine.cp.DBName(), tr.env.Environment().Parser()) {
					if err := tr.schemaUpdated(ctx, gtid, event.Statement, event.Timestamp); err != nil {
						tr.env.Stats().ErrorCounters.Add(vtrpcpb.Code_INTERNAL.String(), 1)
						log.Error(fmt.Sprintf("Error updating schema: %s for ddl %q at pos %s",
							tr.env.Environment().Parser().TruncateForLog(err.Error()), event.Statement, gtid))
						gtid = prevGtid
						return err
					}
					prevGtid = gtid
				}
			}
			return nil
		}, options)
		select {
		case <-ctx.Done():
			return
		default:
			if err != nil {
				gtid = prevGtid
				if isResumePositionUnavailable(err) {
					log.Warn("schema tracker's resume position is no longer available in the binlog; "+
						"saving a fresh schema snapshot and resuming from the current position",
						slog.String("position", gtid),
						slog.Any("error", err))
					if pos, serr := tr.insertCurrentSchemaSnapshot(ctx); serr != nil {
						log.Error(fmt.Sprintf("error saving a fresh schema snapshot: %v", serr))
					} else {
						gtid = pos
						prevGtid = pos
					}
				}
			}
			log.Warn(fmt.Sprintf("Schema Version Tracker's vstream ended (error: %v), retrying in 5 seconds...", err))
			if !tr.wait(ctx, 5*time.Second) {
				return
			}
		}
	}
}

func waitWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (tr *Tracker) currentPosition(ctx context.Context) (replication.Position, error) {
	conn, err := tr.engine.cp.Connect(ctx)
	if err != nil {
		return replication.Position{}, err
	}
	defer conn.Close()
	return conn.PrimaryPosition()
}

// lastSavedPosition returns the position of the most recently saved schema
// version, or "" when the schema_version table is empty.
func (tr *Tracker) lastSavedPosition(ctx context.Context) (string, error) {
	conn, err := tr.engine.GetConnection(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Recycle()
	result, err := conn.Conn.Exec(ctx, sqlparser.BuildParsedQuery("select pos from %s.schema_version order by id desc limit 1",
		sidecar.GetIdentifier()).Query, 1, false)
	if err != nil {
		return "", err
	}
	if len(result.Rows) == 0 {
		return "", nil
	}
	return result.Rows[0][0].ToString(), nil
}

// insertCurrentSchemaSnapshot saves the current schema at the current position
// and returns that position. It gives the historian a correct baseline when
// there is no usable saved position to resume from.
func (tr *Tracker) insertCurrentSchemaSnapshot(ctx context.Context) (string, error) {
	if err := tr.engine.Reload(ctx); err != nil {
		return "", err
	}
	timestamp := time.Now().UnixNano() / 1e9
	pos, err := tr.currentPosition(ctx)
	if err != nil {
		return "", err
	}
	gtid := replication.EncodePosition(pos)
	log.Info("Saving schema snapshot for gtid " + gtid)
	if err := tr.saveCurrentSchemaToDb(ctx, gtid, "", timestamp); err != nil {
		return "", err
	}
	return gtid, nil
}

// startupPosition returns the position the tracker starts streaming from: the
// last position saved in the schema_version table, so that any DDL whose save
// failed (e.g. it was cancelled, timed out, or interrupted by a restart or
// reparent) is replayed and saved. When there is no usable saved position it
// saves a snapshot of the current schema and starts from there.
func (tr *Tracker) startupPosition(ctx context.Context) (string, error) {
	lastPos, err := tr.lastSavedPosition(ctx)
	if err != nil {
		return "", err
	}
	if lastPos != "" {
		if _, err := replication.DecodePosition(lastPos); err == nil {
			log.Info("Schema tracker resuming from the last saved schema version position", slog.String("position", lastPos))
			return lastPos, nil
		}
		log.Warn("Schema tracker cannot parse the last saved schema version position; saving a fresh schema snapshot",
			slog.String("position", lastPos))
	}
	return tr.insertCurrentSchemaSnapshot(ctx)
}

// isResumePositionUnavailable reports whether a vstream failed because its
// start position cannot be served from the binlog: it was purged, or it
// contains GTIDs unknown to the server (MySQL error 1236).
//
// The error reaches us flattened to text — the source vstreamer wraps it with
// %v and it then crosses the gRPC boundary — so the errno is recovered from the
// message rather than via errors.As. This relies on the original SQLError's
// "(errno <n>) (sqlstate <s>)" suffix (emitted by SQLError.Error() and matched
// by sqlerror.NewSQLErrorFromError) surviving every wrapping layer.
// TestIsResumePositionUnavailable pins that contract against the real shapes.
func isResumePositionUnavailable(err error) bool {
	sqlErr, ok := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
	return ok && (sqlErr.Number() == sqlerror.ERMasterFatalReadingBinlog || sqlErr.Number() == sqlerror.ERSourceHasPurgedRequiredGtids)
}

func (tr *Tracker) schemaUpdated(ctx context.Context, gtid string, ddl string, timestamp int64) error {
	log.Info(fmt.Sprintf("Processing schemaUpdated event for gtid %s, ddl %s", gtid, ddl))
	if gtid == "" || ddl == "" {
		return errors.New("got invalid gtid or ddl in schemaUpdated")
	}
	// Engine will have reloaded the schema because vstream will reload it on a DDL
	return tr.saveCurrentSchemaToDb(ctx, gtid, ddl, timestamp)
}

// schemaVersionSaveTimeout bounds a schema version save (an information_schema
// read plus an insert into the sidecar schema_version table) so that a stuck
// MySQL cannot wedge the tracker's event loop or Tracker.Close, which waits on
// it. A timed-out save is retried from the previous GTID like any other failed
// save.
const schemaVersionSaveTimeout = 1 * time.Minute

func (tr *Tracker) saveCurrentSchemaToDb(ctx context.Context, gtid, ddl string, timestamp int64) error {
	ctx, cancel := context.WithTimeout(ctx, schemaVersionSaveTimeout)
	defer cancel()

	blob, err := tr.engine.MarshalMinimalSchema()
	if err != nil {
		return err
	}

	conn, err := tr.engine.GetConnection(ctx)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// We serialize a blob here, encodeString is for strings only
	// and should not be used for binary data.
	blobVal := sqltypes.MakeTrusted(sqltypes.VarBinary, blob)
	buf := bytes2.Buffer{}
	blobVal.EncodeSQLBytes2(&buf)
	query := sqlparser.BuildParsedQuery("insert into %s.schema_version "+
		"(pos, ddl, schemax, time_updated) "+
		"values (%s, %s, %s, %d)", sidecar.GetIdentifier(), encodeString(gtid),
		encodeString(ddl), buf.String(), timestamp).Query
	_, err = conn.Conn.Exec(ctx, query, 1, false)
	if err != nil {
		return err
	}
	return nil
}

func encodeString(in string) string {
	return sqltypes.EncodeStringSQL(in)
}

// MustReloadSchemaOnDDL returns true when the tracker should reload schema for a DDL.
// It fail-closes on parse errors and otherwise reloads only for statements that affect
// the tracked database and are not online DDL artifacts.
func MustReloadSchemaOnDDL(sql string, dbname string, parser *sqlparser.Parser) bool {
	ast, err := parser.Parse(sql)
	if err != nil {
		return true
	}
	switch stmt := ast.(type) {
	case sqlparser.DBDDLStatement:
		return false
	case sqlparser.DDLStatement:
		for _, table := range stmt.AffectedTables() {
			if table.IsEmpty() {
				continue
			}
			if table.Qualifier.NotEmpty() && table.Qualifier.String() != dbname {
				continue
			}
			tableName := table.Name.String()
			if schema.IsOnlineDDLTableName(tableName) {
				continue
			}
			return true
		}
	}
	return false
}
