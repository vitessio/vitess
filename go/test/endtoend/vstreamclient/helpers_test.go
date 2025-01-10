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
	"strconv"
	"strings"
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

func (te *testEnv) runUntilTimeout(t *testing.T, client *vstreamclient.VStreamClient, timeout time.Duration) {
	t.Helper()

	runCtx, cancelRun := context.WithTimeout(context.Background(), timeout)
	defer cancelRun()

	err := client.Run(runCtx)
	if err != nil && !isExpectedRunStop(err, runCtx) {
		t.Fatalf("failed to run vstreamclient: %v", err)
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
