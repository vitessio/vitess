/*
Copyright 2025 The Vitess Authors.

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

package vstreamclient

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/test/endtoend/cluster"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vstreamclient"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// Customer is the concrete type that will be built from the stream.
type Customer struct {
	ID        int64     `vstream:"id"`
	Email     string    `vstream:"email"`
	DeletedAt time.Time `vstream:"-"`
}

type Order struct {
	ID         int64  `vstream:"id"`
	CustomerID int64  `vstream:"customer_id"`
	Note       string `vstream:"note"`
}

type testEnv struct {
	ctx     context.Context
	conn    *vtgateconn.VTGateConn
	session *vtgateconn.VTGateSession
	qCtx    context.Context
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	ctx := context.Background()
	conn, err := cluster.DialVTGate(ctx, t.Name(), vtgateGrpcAddress, "test_user", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
	})

	return &testEnv{
		ctx:     ctx,
		conn:    conn,
		session: conn.Session("", nil),
		qCtx:    t.Context(),
	}
}

func (te *testEnv) newDefaultClient(t *testing.T, name string, tables []vstreamclient.TableConfig, opts ...vstreamclient.Option) *vstreamclient.VStreamClient {
	t.Helper()

	defaultOpts := []vstreamclient.Option{
		vstreamclient.WithMinFlushDuration(500 * time.Millisecond),
		vstreamclient.WithHeartbeatSeconds(1),
		vstreamclient.WithStateTable("commerce", "vstreams"),
	}

	client, err := vstreamclient.New(te.ctx, name, te.conn, tables, append(defaultOpts, opts...)...)
	require.NoError(t, err)
	return client
}

func (te *testEnv) exec(t *testing.T, query string, bindVariables map[string]*querypb.BindVariable) {
	t.Helper()

	_, err := te.session.Execute(te.qCtx, query, bindVariables, false)
	require.NoError(t, err)
}

func (te *testEnv) execBackground(t *testing.T, query string, bindVariables map[string]*querypb.BindVariable) {
	t.Helper()

	_, err := te.session.Execute(context.Background(), query, bindVariables, false)
	require.NoError(t, err)
}

func (te *testEnv) execBackgroundAllowMissingColumn(t *testing.T, query string, bindVariables map[string]*querypb.BindVariable) {
	t.Helper()

	_, err := te.session.Execute(context.Background(), query, bindVariables, false)
	if err == nil {
		return
	}

	if strings.Contains(err.Error(), "errno 1091") || strings.Contains(err.Error(), "check that column/key exists") {
		return
	}

	require.NoError(t, err)
}

// runUntil runs the client until condition reports true, then stops it with GracefulShutdown and
// waits for Run to exit. GracefulShutdown flushes and checkpoints buffered work at the next safe
// boundary before Run returns, so anything the condition observed (e.g. rows seen by FlushFn) is
// durably checkpointed by the time runUntil returns — later clients with the same stream name
// resume instead of replaying. The deadlines are generous so slow CI runners don't flake; fast
// runs return as soon as the condition is met instead of burning a fixed wall-clock window.
func (te *testEnv) runUntil(t *testing.T, client *vstreamclient.VStreamClient, condition func() bool) {
	t.Helper()

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- client.Run(runCtx)
	}()

	deadline := time.After(30 * time.Second)
	for !condition() {
		select {
		case err := <-runErrCh:
			require.NoError(t, err, "vstreamclient exited before the run condition was met")
			require.FailNow(t, "vstreamclient exited cleanly before the run condition was met")
		case <-deadline:
			require.FailNow(t, "timed out waiting for the run condition")
		case <-time.After(50 * time.Millisecond):
		}
	}

	client.GracefulShutdown(15 * time.Second)
	err := recvOrFail(t, runErrCh, "vstreamclient Run to exit")
	if err != nil && !errors.Is(err, context.Canceled) && !isExpectedRunStop(err, runCtx) {
		require.NoError(t, err, "failed to run vstreamclient")
	}
}

// runUntilCopyCompleted runs the client until the stream's copy phase has been flushed and
// checkpointed. The copy-completed flush writes buffered rows before persisting copy_completed,
// so once the column reads true, all copy-phase rows have been delivered to FlushFn and the next
// client with the same stream name will resume from the stored vgtid instead of re-copying.
func (te *testEnv) runUntilCopyCompleted(t *testing.T, client *vstreamclient.VStreamClient, streamName string) {
	t.Helper()

	te.runUntil(t, client, te.copyCompleted(streamName))
}

// copyCompleted returns a condition that reports whether the stream's copy phase has been
// checkpointed in the state table.
func (te *testEnv) copyCompleted(streamName string) func() bool {
	return func() bool {
		result, err := te.session.Execute(te.qCtx, "select copy_completed from commerce.vstreams where name = :name", map[string]*querypb.BindVariable{
			"name": {Type: querypb.Type_VARBINARY, Value: []byte(streamName)},
		}, false)
		if err != nil || len(result.Rows) == 0 {
			return false
		}

		completed, err := result.Rows[0][0].ToBool()
		return err == nil && completed
	}
}

func (te *testEnv) runAsync(client *vstreamclient.VStreamClient, timeout time.Duration) (context.Context, context.CancelFunc, chan error) {
	runCtx, cancelRun := context.WithTimeout(context.Background(), timeout)
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- client.Run(runCtx)
	}()
	return runCtx, cancelRun, runErrCh
}

func isExpectedRunStop(err error, runCtx context.Context) bool {
	if err == nil {
		return true
	}
	if runCtx != nil && runCtx.Err() != nil {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "code = DeadlineExceeded") ||
		strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "code = Canceled") ||
		strings.Contains(msg, "error code: CANCEL")
}

// recvOrFail receives from ch or fails the test after a generous timeout. A bare channel receive
// would block forever if the expected flush never happens, hanging the whole package until the go
// test binary timeout kills it.
func recvOrFail[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(30 * time.Second):
		require.FailNow(t, "timed out waiting for "+what)
		var zero T
		return zero
	}
}

// rowCollector accumulates flushed rows behind a mutex. FlushFn runs on the goroutine that calls
// Run, while tests poll and assert on the collected rows from the test goroutine, so appending to
// and reading an unsynchronized slice is a data race.
type rowCollector[T any] struct {
	mu   sync.Mutex
	rows []T
}

func (c *rowCollector[T]) collect(rows []vstreamclient.Row) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, row := range rows {
		c.rows = append(c.rows, row.Data.(T))
	}
}

func (c *rowCollector[T]) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.rows)
}

func (c *rowCollector[T]) snapshot() []T {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.rows)
}

func customerBindVars(id int64, email string) map[string]*querypb.BindVariable {
	return map[string]*querypb.BindVariable{
		"id":    {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(id, 10))},
		"email": {Type: querypb.Type_VARCHAR, Value: []byte(email)},
	}
}

func idBindVar(id int64) map[string]*querypb.BindVariable {
	return map[string]*querypb.BindVariable{
		"id": {Type: querypb.Type_UINT64, Value: []byte(strconv.FormatInt(id, 10))},
	}
}

func queryLatestVGtid(t *testing.T, ctx context.Context, session *vtgateconn.VTGateSession, name string) *binlogdatapb.VGtid {
	t.Helper()

	result, err := session.Execute(ctx, "select latest_vgtid from commerce.vstreams where name = :name", map[string]*querypb.BindVariable{
		"name": {Type: querypb.Type_VARBINARY, Value: []byte(name)},
	}, false)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	bytes, err := result.Rows[0][0].ToBytes()
	require.NoError(t, err)

	vgtid := &binlogdatapb.VGtid{}
	err = protojson.Unmarshal(bytes, vgtid)
	require.NoError(t, err)
	return vgtid
}
