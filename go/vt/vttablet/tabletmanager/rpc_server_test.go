/*
Copyright 2026 The Vitess Authors.

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

package tabletmanager

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/callinfo/fakecallinfo"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/vterrors"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestHandleRPCPanicLogs checks that the records logged for an RPC keep the
// request and the error out of the message.
func TestHandleRPCPanicLogs(t *testing.T) {
	tm := &TabletManager{tabletAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}}
	ctx := callinfo.NewContext(t.Context(), &fakecallinfo.FakeCallInfo{Remote: "client-host", Method: "ExecuteFetchAsDba"})
	req := &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{Query: []byte("insert into t values ('row-value-1')")}
	wantRequest := fmt.Sprintf("%v", req)

	assertNoValuesInMessages := func(t *testing.T, logs *logtest.Logs) {
		for _, record := range logs.Records(t) {
			assert.NotContains(t, record["msg"], "row-value")
			assert.NotContains(t, record["msg"], "zone1")
			assert.NotContains(t, record["msg"], "client-host")
		}
	}

	t.Run("success", func(t *testing.T) {
		logs := logtest.Capture(t, slog.LevelInfo)
		var err error
		func() {
			defer tm.HandleRPCPanic(ctx, "ExecuteFetchAsDba", req, nil, true /* verbose */, &err)
		}()
		require.NoError(t, err)

		record := logs.Record(t, "TabletManager RPC succeeded")
		assert.Equal(t, "INFO", record["level"])
		assert.Equal(t, "ExecuteFetchAsDba", record["rpc"])
		assert.Equal(t, "zone1-0000000100", record["tablet"])
		assert.Equal(t, "client-host:ExecuteFetchAsDba(fakeRPC)", record["caller"])
		assert.Equal(t, wantRequest, record["request"])
		assert.NotContains(t, record, "response")
		assertNoValuesInMessages(t, logs)
	})

	t.Run("non-verbose success", func(t *testing.T) {
		logs := logtest.Capture(t, slog.LevelInfo)
		var err error
		func() {
			defer tm.HandleRPCPanic(ctx, "ExecuteFetchAsDba", req, nil, false /* verbose */, &err)
		}()
		require.NoError(t, err)
		assert.Empty(t, logs.Records(t))
	})

	t.Run("error", func(t *testing.T) {
		logs := logtest.Capture(t, slog.LevelInfo)
		err := vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "bad query 'row-value-3'")
		func() {
			defer tm.HandleRPCPanic(ctx, "ExecuteFetchAsDba", req, nil, false /* verbose */, &err)
		}()
		require.ErrorContains(t, err, "TabletManager.ExecuteFetchAsDba on zone1-0000000100: bad query 'row-value-3'")
		assert.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(vterrors.FromGRPC(err)))

		record := logs.Record(t, "TabletManager RPC error")
		assert.Equal(t, "WARN", record["level"])
		assert.Equal(t, "ExecuteFetchAsDba", record["rpc"])
		assert.Equal(t, "zone1-0000000100", record["tablet"])
		assert.Equal(t, "client-host:ExecuteFetchAsDba(fakeRPC)", record["caller"])
		assert.Equal(t, wantRequest, record["request"])
		assert.Equal(t, "bad query 'row-value-3'", record["error"])
		assert.Equal(t, "INVALID_ARGUMENT", record["error_code"])
		assertNoValuesInMessages(t, logs)
	})

	t.Run("panic", func(t *testing.T) {
		logs := logtest.Capture(t, slog.LevelInfo)
		err := func() (err error) {
			defer tm.HandleRPCPanic(ctx, "ExecuteFetchAsDba", req, nil, false /* verbose */, &err)
			panic("boom 'row-value-4'")
		}()
		require.EqualError(t, err, "caught panic during ExecuteFetchAsDba: boom 'row-value-4'")

		record := logs.Record(t, "TabletManager RPC panic")
		assert.Equal(t, "ExecuteFetchAsDba", record["rpc"])
		assert.Equal(t, "zone1-0000000100", record["tablet"])
		assert.Equal(t, wantRequest, record["request"])
		assert.Equal(t, "boom 'row-value-4'", record["panic"])
		assert.NotEmpty(t, record["stack"])
		assertNoValuesInMessages(t, logs)
	})
}
