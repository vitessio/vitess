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
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

func TestTracker(t *testing.T) {
	initialSchemaInserted := false
	se, db, cancel := getTestSchemaEngine(t, 0)
	defer cancel()
	gtid1 := "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10"
	ddl1 := "create table tracker_test (id int)"
	query := "CREATE TABLE IF NOT EXISTS _vt.schema_version.*"
	db.AddQueryPattern(query, &sqltypes.Result{})

	db.AddQueryPattern("insert into _vt.schema_version.*1-10.*", &sqltypes.Result{})
	db.AddQueryPatternWithCallback("insert into _vt.schema_version.*1-3.*", &sqltypes.Result{}, func(query string) {
		initialSchemaInserted = true
	})
	// simulates empty schema_version table, so initial schema should be inserted
	db.AddQuery("select pos from _vt.schema_version order by id desc limit 1", &sqltypes.Result{Rows: [][]sqltypes.Value{}})
	// called to get current position
	db.AddQuery("SELECT @@GLOBAL.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"",
		"varchar"),
		"7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-3",
	))
	vs := &fakeVstreamer{
		done:             make(chan struct{}),
		closeDoneOnError: true,
		events: [][]*binlogdatapb.VEvent{{
			{
				Type: binlogdatapb.VEventType_GTID,
				Gtid: gtid1,
			},
			{
				Type:      binlogdatapb.VEventType_DDL,
				Statement: ddl1,
			},
			{
				Type:      binlogdatapb.VEventType_GTID,
				Statement: "", // This event should cause an error updating schema since gtid is bad
			},
			{
				Type:      binlogdatapb.VEventType_DDL,
				Statement: ddl1,
			},
			{
				Type: binlogdatapb.VEventType_GTID,
				Gtid: gtid1,
			},
			{
				Type:      binlogdatapb.VEventType_DDL,
				Statement: "",
			},
		}},
	}
	cfg := se.env.Config()
	cfg.TrackSchemaVersions = true
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TrackerTest")
	initial := env.Stats().ErrorCounters.Counts()["INTERNAL"]
	tracker := NewTracker(env, vs, se)
	tracker.Open()
	<-vs.done
	cancel()
	tracker.Close()
	final := env.Stats().ErrorCounters.Counts()["INTERNAL"]
	require.GreaterOrEqual(t, final, initial+1)
	require.True(t, initialSchemaInserted)
}

func TestTrackerRetriesAfterFailedSchemaSave(t *testing.T) {
	se, db, cancel := getTestSchemaEngine(t, 0)
	defer cancel()

	const (
		startupGTID = "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-3"
		gtid1       = "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10"
		gtid2       = "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-11"
		ddl1        = "create table tracker_retry (id int)"
		ddl2        = "create table tracker_retry_later (id int)"
	)

	db.AddQueryPattern("CREATE TABLE IF NOT EXISTS _vt.schema_version.*", &sqltypes.Result{})
	db.AddQueryPatternWithCallback("insert into _vt.schema_version.*1-3.*", &sqltypes.Result{}, func(query string) {})
	db.AddQuery("select pos from _vt.schema_version order by id desc limit 1", &sqltypes.Result{Rows: [][]sqltypes.Value{}})
	db.AddQuery("SELECT @@GLOBAL.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"",
		"varchar"),
		"7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-3",
	))

	const rejectedInsertPattern = "insert into _vt.schema_version.*1-10.*"
	db.RejectQueryPattern(rejectedInsertPattern, "save failed")

	var gtid1Saves, gtid2Saves int
	vs := &fakeVstreamer{
		done: make(chan struct{}),
		streamCalls: []fakeVstreamCall{
			{
				events: [][]*binlogdatapb.VEvent{
					{
						{Type: binlogdatapb.VEventType_GTID, Gtid: gtid1},
						{Type: binlogdatapb.VEventType_DDL, Statement: ddl1},
					},
					{
						{Type: binlogdatapb.VEventType_GTID, Gtid: gtid2},
						{Type: binlogdatapb.VEventType_DDL, Statement: ddl2},
					},
				},
			},
			{
				before: func() {
					db.RemoveQueryPattern(rejectedInsertPattern)
					db.AddQueryPatternWithCallback(rejectedInsertPattern, &sqltypes.Result{}, func(query string) {
						gtid1Saves++
					})
				},
				events: [][]*binlogdatapb.VEvent{{
					{Type: binlogdatapb.VEventType_GTID, Gtid: gtid1},
					{Type: binlogdatapb.VEventType_DDL, Statement: ddl1},
				}},
			},
		},
	}
	db.AddQueryPatternWithCallback("insert into _vt.schema_version.*1-11.*", &sqltypes.Result{}, func(query string) {
		gtid2Saves++
	})

	cfg := se.env.Config()
	cfg.TrackSchemaVersions = true
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TrackerRetryTest")
	tracker := NewTracker(env, vs, se)
	tracker.wait = func(ctx context.Context, d time.Duration) bool { return waitWithContext(ctx, 0) }

	tracker.Open()
	<-vs.done
	tracker.Close()

	startPositions := vs.getStartPositions()
	require.Greater(t, len(startPositions), 1)
	require.Equal(t, startupGTID, startPositions[0])
	require.Equal(t, startupGTID, startPositions[1])
	assert.Equal(t, 1, gtid1Saves)
	assert.Zero(t, gtid2Saves)
}

func TestTrackerRetriesFromStartupGTIDWhenFirstStreamFailsBeforeGTID(t *testing.T) {
	se, db, cancel := getTestSchemaEngine(t, 0)
	defer cancel()

	const startupGTID = "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-3"

	db.AddQueryPattern("CREATE TABLE IF NOT EXISTS _vt.schema_version.*", &sqltypes.Result{})
	db.AddQueryPattern("insert into _vt.schema_version.*1-3.*", &sqltypes.Result{})
	db.AddQuery("select pos from _vt.schema_version order by id desc limit 1", &sqltypes.Result{Rows: [][]sqltypes.Value{}})
	db.AddQuery("SELECT @@GLOBAL.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"",
		"varchar"),
		"7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-3",
	))

	vs := &fakeVstreamer{
		done: make(chan struct{}),
		streamCalls: []fakeVstreamCall{
			{err: errors.New("stream failed before gtid")},
			{},
		},
	}

	cfg := se.env.Config()
	cfg.TrackSchemaVersions = true
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TrackerRetryStartupGTIDTest")
	tracker := NewTracker(env, vs, se)
	tracker.wait = func(ctx context.Context, d time.Duration) bool { return waitWithContext(ctx, 0) }

	tracker.Open()
	<-vs.done
	tracker.Close()

	startPositions := vs.getStartPositions()
	require.Greater(t, len(startPositions), 1)
	require.Equal(t, startupGTID, startPositions[0])
	require.Equal(t, startupGTID, startPositions[1])
}

func TestTrackerRetriesFromLastSavedGTIDAfterSuccessfulFirstDDL(t *testing.T) {
	se, db, cancel := getTestSchemaEngine(t, 0)
	defer cancel()

	const (
		startupGTID = "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-3"
		gtid1       = "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10"
		ddl1        = "create table tracker_retry_after_saved_first_ddl (id int)"
	)

	var startupSchemaInserted bool
	var gtid1Saves int

	db.AddQueryPattern("CREATE TABLE IF NOT EXISTS _vt.schema_version.*", &sqltypes.Result{})
	db.AddQueryPatternWithCallback("insert into _vt.schema_version.*1-3.*", &sqltypes.Result{}, func(query string) {
		startupSchemaInserted = true
	})
	db.AddQueryPatternWithCallback("insert into _vt.schema_version.*1-10.*", &sqltypes.Result{}, func(query string) {
		gtid1Saves++
	})
	db.AddQuery("select pos from _vt.schema_version order by id desc limit 1", &sqltypes.Result{Rows: [][]sqltypes.Value{}})
	db.AddQuery("SELECT @@GLOBAL.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"",
		"varchar"),
		"7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-3",
	))

	vs := &fakeVstreamer{
		done: make(chan struct{}),
		streamCalls: []fakeVstreamCall{
			{
				events: [][]*binlogdatapb.VEvent{{
					{Type: binlogdatapb.VEventType_GTID, Gtid: gtid1},
					{Type: binlogdatapb.VEventType_DDL, Statement: ddl1},
				}},
				errAfterEvents: errors.New("stream failed after saving first ddl"),
			},
			{},
		},
	}

	cfg := se.env.Config()
	cfg.TrackSchemaVersions = true
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TrackerRetrySavedFirstDDLTest")
	tracker := NewTracker(env, vs, se)
	tracker.wait = func(ctx context.Context, d time.Duration) bool { return waitWithContext(ctx, 0) }

	tracker.Open()
	<-vs.done
	tracker.Close()

	startPositions := vs.getStartPositions()
	require.Greater(t, len(startPositions), 1)
	require.True(t, startupSchemaInserted)
	assert.Equal(t, 1, gtid1Saves)
	require.Equal(t, startupGTID, startPositions[0])
	require.Equal(t, gtid1, startPositions[1])
}

// TestTrackerResumesFromLastSavedPosition proves that the tracker starts
// streaming from the last position saved in the schema_version table instead
// of the current position, so that any DDL whose save failed (cancelled, timed
// out, or interrupted by a restart or reparent) is replayed and saved.
func TestTrackerResumesFromLastSavedPosition(t *testing.T) {
	se, db, cancel := getTestSchemaEngine(t, 0)
	defer cancel()

	const storedPos = "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-42"

	db.AddQueryPattern("CREATE TABLE IF NOT EXISTS _vt.schema_version.*", &sqltypes.Result{})
	db.AddQuery("select pos from _vt.schema_version order by id desc limit 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos",
		"varchar"),
		storedPos,
	))

	var schemaSaves int
	db.AddQueryPatternWithCallback("insert into _vt.schema_version.*", &sqltypes.Result{}, func(query string) {
		schemaSaves++
	})

	vs := &fakeVstreamer{
		done:   make(chan struct{}),
		events: [][]*binlogdatapb.VEvent{{}},
	}
	cfg := se.env.Config()
	cfg.TrackSchemaVersions = true
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TrackerResumeTest")
	tracker := NewTracker(env, vs, se)

	tracker.Open()
	<-vs.done
	tracker.Close()

	startPositions := vs.getStartPositions()
	require.NotEmpty(t, startPositions)
	assert.Equal(t, storedPos, startPositions[0])
	assert.Zero(t, schemaSaves)
}

// TestTrackerSavesSnapshotWhenStoredPositionIsInvalid proves that a stored
// position that cannot be parsed does not wedge the tracker: it saves a fresh
// snapshot of the current schema and resumes from the current position.
func TestTrackerSavesSnapshotWhenStoredPositionIsInvalid(t *testing.T) {
	se, db, cancel := getTestSchemaEngine(t, 0)
	defer cancel()

	const currentGTID = "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-3"

	db.AddQueryPattern("CREATE TABLE IF NOT EXISTS _vt.schema_version.*", &sqltypes.Result{})
	db.AddQuery("select pos from _vt.schema_version order by id desc limit 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos",
		"varchar"),
		"this-is-not-a-valid-position",
	))
	db.AddQuery("SELECT @@GLOBAL.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"",
		"varchar"),
		"7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-3",
	))

	var snapshotSaves int
	db.AddQueryPatternWithCallback("insert into _vt.schema_version.*1-3.*", &sqltypes.Result{}, func(query string) {
		snapshotSaves++
	})

	vs := &fakeVstreamer{
		done:   make(chan struct{}),
		events: [][]*binlogdatapb.VEvent{{}},
	}
	cfg := se.env.Config()
	cfg.TrackSchemaVersions = true
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TrackerInvalidPosTest")
	tracker := NewTracker(env, vs, se)

	tracker.Open()
	<-vs.done
	tracker.Close()

	startPositions := vs.getStartPositions()
	require.NotEmpty(t, startPositions)
	assert.Equal(t, currentGTID, startPositions[0])
	assert.Equal(t, 1, snapshotSaves)
}

// TestTrackerSavesSnapshotWhenResumePositionIsPurged proves that when the
// binlog can no longer serve the stored resume position (MySQL error 1236),
// the tracker saves a fresh schema snapshot and resumes from the current
// position instead of retrying the unservable position forever.
func TestTrackerSavesSnapshotWhenResumePositionIsPurged(t *testing.T) {
	se, db, cancel := getTestSchemaEngine(t, 0)
	defer cancel()

	const (
		storedPos   = "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-42"
		currentGTID = "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-45"
	)

	db.AddQueryPattern("CREATE TABLE IF NOT EXISTS _vt.schema_version.*", &sqltypes.Result{})
	db.AddQuery("select pos from _vt.schema_version order by id desc limit 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos",
		"varchar"),
		storedPos,
	))
	db.AddQuery("SELECT @@GLOBAL.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"",
		"varchar"),
		"7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-45",
	))

	var snapshotSaves int
	db.AddQueryPatternWithCallback("insert into _vt.schema_version.*1-45.*", &sqltypes.Result{}, func(query string) {
		snapshotSaves++
	})

	// The vstreamer flattens errors to text (its wrapError uses %v), so the
	// tracker must recover the errno from the message rather than via errors.As.
	purgedErr := fmt.Errorf("stream (at source tablet) error @ %s: %v", storedPos,
		sqlerror.NewSQLError(sqlerror.ERMasterFatalReadingBinlog, sqlerror.SSUnknownSQLState,
			"Cannot replicate because the master purged required binary logs"))

	vs := &fakeVstreamer{
		done: make(chan struct{}),
		streamCalls: []fakeVstreamCall{
			{err: purgedErr},
			{},
		},
	}
	cfg := se.env.Config()
	cfg.TrackSchemaVersions = true
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TrackerPurgedPosTest")
	tracker := NewTracker(env, vs, se)
	tracker.wait = func(ctx context.Context, d time.Duration) bool { return waitWithContext(ctx, 0) }

	tracker.Open()
	<-vs.done
	tracker.Close()

	startPositions := vs.getStartPositions()
	require.Greater(t, len(startPositions), 1)
	assert.Equal(t, storedPos, startPositions[0])
	assert.Equal(t, currentGTID, startPositions[1])
	assert.Equal(t, 1, snapshotSaves)
}

func TestTrackerShouldNotInsertInitialSchema(t *testing.T) {
	initialSchemaInserted := false
	se, db, cancel := getTestSchemaEngine(t, 0)
	gtid1 := "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10"

	defer cancel()
	// simulates existing rows in schema_version, so initial schema should not be inserted
	db.AddQuery("select pos from _vt.schema_version order by id desc limit 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos",
		"varchar"),
		"MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-3",
	))
	db.AddQueryPatternWithCallback("insert into _vt.schema_version.*1-3.*", &sqltypes.Result{}, func(query string) {
		initialSchemaInserted = true
	})
	vs := &fakeVstreamer{
		done: make(chan struct{}),
		events: [][]*binlogdatapb.VEvent{{
			{
				Type: binlogdatapb.VEventType_GTID,
				Gtid: gtid1,
			},
		}},
	}
	cfg := se.env.Config()
	cfg.TrackSchemaVersions = true
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TrackerTest")
	tracker := NewTracker(env, vs, se)
	tracker.Open()
	<-vs.done
	cancel()
	tracker.Close()
	require.False(t, initialSchemaInserted)
}

var _ VStreamer = (*fakeVstreamer)(nil)

type fakeVstreamer struct {
	done             chan struct{}
	events           [][]*binlogdatapb.VEvent
	streamCalls      []fakeVstreamCall
	closeDoneOnError bool

	mu             sync.Mutex
	doneOnce       sync.Once
	startPositions []string
	lastOptions    *binlogdatapb.VStreamOptions
}

type fakeVstreamCall struct {
	before         func()
	events         [][]*binlogdatapb.VEvent
	err            error
	errAfterEvents error
}

func (f *fakeVstreamer) Stream(ctx context.Context, startPos string, tablePKs []*binlogdatapb.TableLastPK,
	filter *binlogdatapb.Filter, throttlerApp throttlerapp.Name, send func([]*binlogdatapb.VEvent) error, options *binlogdatapb.VStreamOptions,
) error {
	f.mu.Lock()
	callIndex := len(f.startPositions)
	f.startPositions = append(f.startPositions, startPos)
	f.lastOptions = options
	call := fakeVstreamCall{events: f.events}
	if callIndex < len(f.streamCalls) {
		call = f.streamCalls[callIndex]
	}
	f.mu.Unlock()

	if call.before != nil {
		call.before()
	}
	if call.err != nil {
		if f.closeDoneOnError {
			f.doneOnce.Do(func() {
				close(f.done)
			})
		}
		return call.err
	}
	for _, events := range call.events {
		err := send(events)
		if err != nil {
			if f.closeDoneOnError {
				f.doneOnce.Do(func() {
					close(f.done)
				})
			}
			return err
		}
	}
	if call.errAfterEvents != nil {
		if f.closeDoneOnError {
			f.doneOnce.Do(func() {
				close(f.done)
			})
		}
		return call.errAfterEvents
	}
	f.doneOnce.Do(func() {
		close(f.done)
	})
	<-ctx.Done()
	return nil
}

func (f *fakeVstreamer) getStartPositions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.startPositions...)
}

func TestMustReloadSchemaOnDDL(t *testing.T) {
	type testcase struct {
		name   string
		query  string
		dbname string
		want   bool
	}
	db1, db2 := "db1", "db2"
	testcases := []*testcase{
		{name: "unqualified create table in target db", query: "create table x(i int);", dbname: db1, want: true},
		{name: "parse failure fails closed", query: "bad", dbname: db2, want: true},
		{name: "qualified create table in target db", query: "create table db2.x(i int);", dbname: db2, want: true},
		{name: "drop table in target db", query: "drop table db2.x", dbname: db2, want: true},
		{name: "drop view in target db", query: "drop view db2.x", dbname: db2, want: true},
		{name: "rename table within target db", query: "rename table db2.x to db2.y;", dbname: db2, want: true},
		{name: "multi-table rename with target db table on from side", query: "rename table db2.x to db1.y, db1.a to db1.b;", dbname: db2, want: true},
		{name: "qualified create table in other db", query: "create table db1.x(i int);", dbname: db2, want: false},
		{name: "sidecar table is ignored", query: "create table _vt.x(i int);", dbname: db1, want: false},
		{name: "online ddl artifact in other db is ignored", query: "DROP VIEW IF EXISTS `pseudo_gtid`.`_pseudo_gtid_hint__asc:55B364E3:0000000000056EE2:6DD57B85`", dbname: db2, want: false},
		{name: "database ddl is ignored", query: "create database db1;", dbname: db1, want: false},
		{name: "online ddl artifact in target db is ignored", query: "create table db1._4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014_gho(i int);", dbname: db1, want: false},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, MustReloadSchemaOnDDL(tc.query, tc.dbname, sqlparser.NewTestParser()))
		})
	}
}

func TestTrackerRequestsOnlyGTIDAndDDL(t *testing.T) {
	se, db, cancel := getTestSchemaEngine(t, 0)
	defer cancel()

	db.AddQuery("select pos from _vt.schema_version order by id desc limit 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos",
		"varchar"),
		"MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-3",
	))

	vs := &fakeVstreamer{
		done:   make(chan struct{}),
		events: [][]*binlogdatapb.VEvent{{}},
	}

	cfg := se.env.Config()
	cfg.TrackSchemaVersions = true
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TrackerTest")
	tracker := NewTracker(env, vs, se)

	tracker.Open()
	<-vs.done
	cancel()
	tracker.Close()

	require.NotNil(t, vs.lastOptions)
	require.Equal(t, []binlogdatapb.VEventType{
		binlogdatapb.VEventType_GTID,
		binlogdatapb.VEventType_DDL,
	}, vs.lastOptions.EventTypes)
}

func TestWaitWithContextStopsOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan bool, 1)

		go func() {
			done <- waitWithContext(ctx, time.Minute)
		}()

		synctest.Wait()
		cancel()
		synctest.Wait()

		select {
		case waited := <-done:
			require.False(t, waited)
		default:
			require.FailNow(t, "waitWithContext did not stop after context cancellation")
		}
	})
}

// TestIsResumePositionUnavailable pins the contract that isResumePositionUnavailable
// depends on: the MySQL 1236 errno must stay recoverable from the error text after
// the source vstreamer wraps it (with %v) and after it is flattened across the gRPC
// boundary. If any layer stops preserving the "(errno 1236) (sqlstate ...)" suffix
// that SQLError.Error() emits, detection silently breaks and the tracker would retry
// an unservable position forever, so this guards the real error shapes rather than
// only a hand-built string.
func TestIsResumePositionUnavailable(t *testing.T) {
	const storedPos = "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-42"
	purged := sqlerror.NewSQLError(sqlerror.ERMasterFatalReadingBinlog, sqlerror.SSUnknownSQLState,
		"Cannot replicate because the master purged required binary logs")
	// wrapError in the source vstreamer wraps the error with exactly this format.
	wrapped := fmt.Errorf("stream (at source tablet) error @ (including the GTID we failed to process) %s: %v", storedPos, purged)

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain error", errors.New("some other failure"), false},
		{"direct SQLError 1236", purged, true},
		{"vstreamer-wrapped (%v)", wrapped, true},
		{"gRPC-flattened message", fmt.Errorf("rpc error: code = Unknown desc = %s", wrapped.Error()), true},
		{"different errno", sqlerror.NewSQLError(sqlerror.ERNoSuchTable, sqlerror.SSUnknownSQLState, "no such table"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isResumePositionUnavailable(tc.err))
		})
	}
}
