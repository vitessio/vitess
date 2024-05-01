/*
Copyright 2019 The Vitess Authors.

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

package vstreamer

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	_flag "vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	engine *Engine
	env    *testenv.Env

	ignoreKeyspaceShardInFieldAndRowEvents bool
	testRowEventFlags                      bool
)

func TestMain(m *testing.M) {
	_flag.ParseFlagsForTest()
	ignoreKeyspaceShardInFieldAndRowEvents = true

	exitCode := func() int {
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		env, err = testenv.Init(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer env.Close()

		// engine cannot be initialized in testenv because it introduces
		// circular dependencies
		engine = NewEngine(env.TabletEnv, env.SrvTopo, env.SchemaEngine, nil, env.Cells[0])
		engine.InitDBConfig(env.KeyspaceName, env.ShardName)
		engine.Open()
		defer engine.Close()

		return m.Run()
	}()
	os.Exit(exitCode)
}

func newEngine(t *testing.T, ctx context.Context, binlogRowImage string) {
	if engine != nil {
		engine.Close()
	}
	if env != nil {
		env.Close()
	}
	var err error
	env, err = testenv.Init(ctx)
	require.NoError(t, err)

	setBinlogRowImage(t, binlogRowImage)

	// engine cannot be initialized in testenv because it introduces
	// circular dependencies
	engine = NewEngine(env.TabletEnv, env.SrvTopo, env.SchemaEngine, nil, env.Cells[0])
	engine.InitDBConfig(env.KeyspaceName, env.ShardName)
	engine.Open()
}

func customEngine(t *testing.T, modifier func(mysql.ConnParams) mysql.ConnParams) *Engine {
	original, err := env.Dbcfgs.AppWithDB().MysqlParams()
	require.NoError(t, err)
	modified := modifier(*original)
	cfg := env.TabletEnv.Config().Clone()
	cfg.DB = dbconfigs.NewTestDBConfigs(modified, modified, modified.DbName)

	engine := NewEngine(tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "VStreamerTest"), env.SrvTopo, env.SchemaEngine, nil, env.Cells[0])
	engine.InitDBConfig(env.KeyspaceName, env.ShardName)
	engine.Open()
	return engine
}

func setBinlogRowImage(t *testing.T, mode string) {
	execStatements(t, []string{
		fmt.Sprintf("set @@binlog_row_image='%s'", mode),
		fmt.Sprintf("set @@session.binlog_row_image='%s'", mode),
		fmt.Sprintf("set @@global.binlog_row_image='%s'", mode),
	})
}

func runCases(t *testing.T, filter *binlogdatapb.Filter, testcases []testcase, position string, tablePK []*binlogdatapb.TableLastPK) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg, ch := startStream(ctx, t, filter, position, tablePK)
	defer wg.Wait()
	// If position is 'current', we wait for a heartbeat to be
	// sure the vstreamer has started.
	if position == "current" {
		log.Infof("Starting stream with current position")
		expectLog(ctx, t, "current pos", ch, [][]string{{`gtid`, `type:OTHER`}})
	}
	log.Infof("Starting to run test cases")
	for _, tcase := range testcases {
		switch input := tcase.input.(type) {
		case []string:
			execStatements(t, input)
		case string:
			execStatement(t, input)
		default:
			t.Fatalf("unexpected input: %#v", input)
		}
		engine.se.Reload(ctx)
		expectLog(ctx, t, tcase.input, ch, tcase.output)
	}
	cancel()
	if evs, ok := <-ch; ok {
		t.Fatalf("unexpected evs: %v", evs)
	}
	log.Infof("Last line of runCases")
}

func expectLog(ctx context.Context, t *testing.T, input any, ch <-chan []*binlogdatapb.VEvent, output [][]string) {
	timer := time.NewTimer(1 * time.Minute)
	defer timer.Stop()
	for _, wantset := range output {
		var evs []*binlogdatapb.VEvent
		inCopyPhase := false
		haveEnumOrSetField := func(fields []*querypb.Field) bool {
			return slices.ContainsFunc(fields, func(f *querypb.Field) bool {
				// We can't simply use querypb.Type_ENUM or querypb.Type_SET here
				// because if a binary collation is used then the field Type will
				// be BINARY. And we don't have the binlog event metadata from the
				// original event any longer that we could use to get the MySQL type
				// (which would still be ENUM or SET). So we instead look at the column
				// type string value which will be e.g enum('s','m','l').
				colTypeStr := strings.ToLower(f.GetColumnType())
				if strings.HasPrefix(colTypeStr, "enum(") || strings.HasPrefix(colTypeStr, "set(") {
					return true
				}
				return false
			})
		}
		for {
			select {
			case allevs, ok := <-ch:
				if !ok {
					require.FailNow(t, "expectLog: not ok, stream ended early")
				}
				for _, ev := range allevs {
					// Ignore spurious heartbeats that can happen on slow machines.
					if ev.Throttled || ev.Type == binlogdatapb.VEventType_HEARTBEAT {
						continue
					}
					switch ev.Type {
					case binlogdatapb.VEventType_OTHER:
						if strings.Contains(ev.Gtid, copyPhaseStart) {
							inCopyPhase = true
						}
					case binlogdatapb.VEventType_COPY_COMPLETED:
						inCopyPhase = false
					case binlogdatapb.VEventType_FIELD:
						// This is always set in the copy phase. It's also set in the
						// running phase when the table has an ENUM or SET field.
						ev.FieldEvent.EnumSetStringValues = inCopyPhase || haveEnumOrSetField(ev.FieldEvent.Fields)
					}
					evs = append(evs, ev)
				}
			case <-ctx.Done():
				require.Fail(t, "expectLog: Done(), stream ended early")
			case <-timer.C:
				require.Fail(t, "expectLog: timed out waiting for events: %v", wantset)
			}
			if len(evs) != 0 {
				break
			}
		}

		numEventsToMatch := len(evs)
		if len(wantset) != len(evs) {
			log.Warningf("%v: evs\n%v, want\n%v, >> got length %d, wanted length %d", input, evs, wantset, len(evs), len(wantset))
			if len(wantset) < len(evs) {
				numEventsToMatch = len(wantset)
			}
		}
		for i := 0; i < numEventsToMatch; i++ {
			want := wantset[i]
			// CurrentTime is not testable.
			evs[i].CurrentTime = 0
			evs[i].Keyspace = ""
			evs[i].Shard = ""
			switch want {
			case "begin":
				if evs[i].Type != binlogdatapb.VEventType_BEGIN {
					t.Fatalf("%v (%d): event: %v, want begin", input, i, evs[i])
				}
			case "gtid":
				if evs[i].Type != binlogdatapb.VEventType_GTID {
					t.Fatalf("%v (%d): event: %v, want gtid", input, i, evs[i])
				}
			case "lastpk":
				if evs[i].Type != binlogdatapb.VEventType_LASTPK {
					t.Fatalf("%v (%d): event: %v, want lastpk", input, i, evs[i])
				}
			case "commit":
				if evs[i].Type != binlogdatapb.VEventType_COMMIT {
					t.Fatalf("%v (%d): event: %v, want commit", input, i, evs[i])
				}
			case "other":
				if evs[i].Type != binlogdatapb.VEventType_OTHER {
					t.Fatalf("%v (%d): event: %v, want other", input, i, evs[i])
				}
			case "ddl":
				if evs[i].Type != binlogdatapb.VEventType_DDL {
					t.Fatalf("%v (%d): event: %v, want ddl", input, i, evs[i])
				}
			case "copy_completed":
				if evs[i].Type != binlogdatapb.VEventType_COPY_COMPLETED {
					t.Fatalf("%v (%d): event: %v, want copy_completed", input, i, evs[i])
				}
			default:
				evs[i].Timestamp = 0
				if evs[i].Type == binlogdatapb.VEventType_FIELD {
					for j := range evs[i].FieldEvent.Fields {
						evs[i].FieldEvent.Fields[j].Flags = 0
						if ignoreKeyspaceShardInFieldAndRowEvents {
							evs[i].FieldEvent.Keyspace = ""
							evs[i].FieldEvent.Shard = ""
						}
					}
				}
				if ignoreKeyspaceShardInFieldAndRowEvents && evs[i].Type == binlogdatapb.VEventType_ROW {
					evs[i].RowEvent.Keyspace = ""
					evs[i].RowEvent.Shard = ""
				}
				if !testRowEventFlags && evs[i].Type == binlogdatapb.VEventType_ROW {
					evs[i].RowEvent.Flags = 0
				}
				want = env.RemoveAnyDeprecatedDisplayWidths(want)
				if got := fmt.Sprintf("%v", evs[i]); got != want {
					log.Errorf("%v (%d): event:\n%q, want\n%q", input, i, got, want)
					t.Fatalf("%v (%d): event:\n%q, want\n%q", input, i, got, want)
				}
			}
		}
		if len(wantset) != len(evs) {
			t.Fatalf("%v: evs\n%v, want\n%v, got length %d, wanted length %d", input, evs, wantset, len(evs), len(wantset))
		}
	}
}

func startStream(ctx context.Context, t *testing.T, filter *binlogdatapb.Filter, position string, tablePKs []*binlogdatapb.TableLastPK) (*sync.WaitGroup, <-chan []*binlogdatapb.VEvent) {
	switch position {
	case "":
		position = primaryPosition(t)
	case "vscopy":
		position = ""
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	ch := make(chan []*binlogdatapb.VEvent)

	go func() {
		defer close(ch)
		defer wg.Done()
		if vstream(ctx, t, position, tablePKs, filter, ch) != nil {
			t.Log("vstream returned error")
		}
	}()
	return &wg, ch
}

func vstream(ctx context.Context, t *testing.T, pos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, ch chan []*binlogdatapb.VEvent) error {
	if filter == nil {
		filter = &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "/.*/",
			}},
		}
	}
	return engine.Stream(ctx, pos, tablePKs, filter, throttlerapp.VStreamerName, func(evs []*binlogdatapb.VEvent) error {
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()

		log.Infof("Received events: %v", evs)
		select {
		case ch <- evs:
		case <-ctx.Done():
			return fmt.Errorf("engine.Stream Done() stream ended early")
		case <-timer.C:
			t.Log("VStream timed out waiting for events")
			return io.EOF
		}
		return nil
	})
}

func execStatement(t *testing.T, query string) {
	t.Helper()
	if err := env.Mysqld.ExecuteSuperQuery(context.Background(), query); err != nil {
		t.Fatal(err)
	}
}

func execStatements(t *testing.T, queries []string) {
	if err := env.Mysqld.ExecuteSuperQueryList(context.Background(), queries); err != nil {
		t.Fatal(err)
	}
}

func primaryPosition(t *testing.T) string {
	t.Helper()
	// We use the engine's cp because there is one test that overrides
	// the flavor to FilePos. If so, we have to obtain the position
	// in that flavor format.
	connParam, err := engine.env.Config().DB.DbaWithDB().MysqlParams()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := mysql.Connect(context.Background(), connParam)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	pos, err := conn.PrimaryPosition()
	if err != nil {
		t.Fatal(err)
	}
	return replication.EncodePosition(pos)
}

func setVSchema(t *testing.T, vschema string) {
	t.Helper()

	curCount := engine.vschemaUpdates.Get()
	if err := env.SetVSchema(vschema); err != nil {
		t.Fatal(err)
	}
	// Wait for curCount to go up.
	updated := false
	for i := 0; i < 10; i++ {
		if engine.vschemaUpdates.Get() != curCount {
			updated = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !updated {
		log.Infof("vschema did not get updated")
		t.Error("vschema did not get updated")
	}
}

func insertSomeRows(t *testing.T, numRows int) {
	var queries []string
	for idx, query := range []string{
		"insert into t1 (id11, id12) values",
		"insert into t2 (id21, id22) values",
	} {
		for i := 1; i <= numRows; i++ {
			queries = append(queries, fmt.Sprintf("%s (%d, %d)", query, i, i*(idx+1)*10))
		}
	}
	execStatements(t, queries)
}
