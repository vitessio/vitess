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

package vtbench

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type fakeClientConn struct {
	execFn func(ctx context.Context) (*sqltypes.Result, error)
}

func (f *fakeClientConn) connect(ctx context.Context, cp ConnParams) error {
	return nil
}

func (f *fakeClientConn) execute(ctx context.Context, query string, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return f.execFn(ctx)
}

func newBenchForTest(threads, count int) *Bench {
	return &Bench{
		Threads: threads,
		Count:   count,
		Rows:    stats.NewCounter("", ""),
		Errors:  stats.NewCountersWithSingleLabel("", "", "code"),
		Timings: stats.NewTimings("", "", ""),
	}
}

func runClientLoop(ctx context.Context, b *Bench, conn clientConn) {
	bt := benchThread{b: b, i: 0, conn: conn}
	b.lock.Lock()
	b.wg.Add(2)
	done := make(chan struct{})
	go func() {
		bt.clientLoop(ctx)
		close(done)
	}()
	b.lock.Unlock()
	<-done
}

func TestProtocolString(t *testing.T) {
	tests := []struct {
		name     string
		protocol ClientProtocol
		expected string
	}{
		{
			name:     "mysql protocol",
			protocol: MySQL,
			expected: "mysql",
		},
		{
			name:     "grpc vtgate protocol",
			protocol: GRPCVtgate,
			expected: "grpc-vtgate",
		},
		{
			name:     "grpc vttablet protocol",
			protocol: GRPCVttablet,
			expected: "grpc-vttablet",
		},
		{
			name:     "unknown protocol",
			protocol: ClientProtocol(99),
			expected: "unknown-protocol-99",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.protocol.String()
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBenchGetQuery(t *testing.T) {
	tests := []struct {
		name             string
		inputQuery       string
		threadIndex      int
		expectedQuery    string
		expectedBindVars int
	}{
		{
			name:             "no thread placeholder",
			inputQuery:       "SELECT * FROM users",
			threadIndex:      5,
			expectedQuery:    "SELECT * FROM users",
			expectedBindVars: 0,
		},
		{
			name:             "single thread placeholder",
			inputQuery:       "SELECT * FROM users WHERE id = :thread",
			threadIndex:      0,
			expectedQuery:    "SELECT * FROM users WHERE id = 0",
			expectedBindVars: 0,
		},
		{
			name:             "single thread placeholder with value",
			inputQuery:       "SELECT * FROM users WHERE id = :thread",
			threadIndex:      42,
			expectedQuery:    "SELECT * FROM users WHERE id = 42",
			expectedBindVars: 0,
		},
		{
			name:             "multiple thread placeholders",
			inputQuery:       "INSERT INTO logs (thread_id, user_id) VALUES (:thread, :thread)",
			threadIndex:      7,
			expectedQuery:    "INSERT INTO logs (thread_id, user_id) VALUES (7, 7)",
			expectedBindVars: 0,
		},
		{
			name:             "thread placeholder in different contexts",
			inputQuery:       "UPDATE stats SET thread=:thread WHERE thread_id=:thread",
			threadIndex:      15,
			expectedQuery:    "UPDATE stats SET thread=15 WHERE thread_id=15",
			expectedBindVars: 0,
		},
		{
			name:             "thread placeholder at boundaries",
			inputQuery:       ":thread:thread",
			threadIndex:      3,
			expectedQuery:    "33",
			expectedBindVars: 0,
		},
		{
			name:             "empty query",
			inputQuery:       "",
			threadIndex:      5,
			expectedQuery:    "",
			expectedBindVars: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bench := &Bench{Query: tc.inputQuery}

			query, bindVars := bench.getQuery(tc.threadIndex)

			assert.Equal(t, tc.expectedQuery, query)
			assert.Len(t, bindVars, tc.expectedBindVars)
		})
	}
}

func TestNewBench(t *testing.T) {
	cp := ConnParams{
		Hosts:    []string{"localhost"},
		Port:     3306,
		Protocol: MySQL,
	}

	bench := NewBench(10, 100, cp, "SELECT 1")

	assert.Equal(t, 10, bench.Threads)
	assert.Equal(t, 100, bench.Count)
	assert.Equal(t, "SELECT 1", bench.Query)
	assert.Equal(t, MySQL, bench.ConnParams.Protocol)
	assert.NotNil(t, bench.Rows)
	assert.NotNil(t, bench.Timings)
}

func TestBenchCreateConnsUnknownProtocol(t *testing.T) {
	bench := &Bench{
		Threads: 1,
		ConnParams: ConnParams{
			Hosts:    []string{"localhost"},
			Port:     3306,
			Protocol: ClientProtocol(999),
		},
		Rows:    stats.NewCounter("test_unknown_protocol_rows", ""),
		Timings: stats.NewTimings("test_unknown_protocol", "", "timings"),
	}

	err := bench.createConns(t.Context())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unimplemented connection protocol")
	assert.Contains(t, err.Error(), "unknown-protocol-999")
}

func TestErrorCode(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected vtrpcpb.Code
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: vtrpcpb.Code_OK,
		},
		{
			name:     "plain error falls through to UNKNOWN",
			err:      errors.New("boom"),
			expected: vtrpcpb.Code_UNKNOWN,
		},
		{
			name:     "vterrors carries its code",
			err:      vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "bad input"),
			expected: vtrpcpb.Code_INVALID_ARGUMENT,
		},
		{
			name:     "SQLError duplicate-key maps via VtRpcErrorCode",
			err:      sqlerror.NewSQLError(sqlerror.ERDupEntry, "23000", "duplicate entry"),
			expected: vtrpcpb.Code_ALREADY_EXISTS,
		},
		{
			name:     "SQLError syntax error maps to INVALID_ARGUMENT",
			err:      sqlerror.NewSQLError(sqlerror.ERSyntaxError, "42000", "syntax error"),
			expected: vtrpcpb.Code_INVALID_ARGUMENT,
		},
		{
			name:     "context.DeadlineExceeded recognized",
			err:      context.DeadlineExceeded,
			expected: vtrpcpb.Code_DEADLINE_EXCEEDED,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, errorCode(tc.err))
		})
	}
}

func TestBenchClientLoopContinueOnError(t *testing.T) {
	b := newBenchForTest(1, 5)
	b.ContinueOnError = true

	var calls atomic.Int64
	conn := &fakeClientConn{
		execFn: func(ctx context.Context) (*sqltypes.Result, error) {
			calls.Add(1)
			return nil, sqlerror.NewSQLError(sqlerror.ERDupEntry, "23000", "duplicate entry")
		},
	}

	runClientLoop(t.Context(), b, conn)

	assert.Equal(t, int64(5), calls.Load(), "all iterations should run with ContinueOnError")
	assert.Equal(t, int64(5), b.Errors.Counts()[vtrpcpb.Code_ALREADY_EXISTS.String()])
	assert.Empty(t, b.Errors.Counts()[vtrpcpb.Code_UNKNOWN.String()], "SQLError must not be bucketed as UNKNOWN")
}

func TestBenchClientLoopContinueOnErrorStopsOnContextCancel(t *testing.T) {
	b := newBenchForTest(1, 1_000_000)
	b.ContinueOnError = true

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	var calls atomic.Int64
	conn := &fakeClientConn{
		execFn: func(ctx context.Context) (*sqltypes.Result, error) {
			if calls.Add(1) == 3 {
				cancel()
			}
			return nil, context.Canceled
		},
	}

	done := make(chan struct{})
	go func() {
		runClientLoop(ctx, b, conn)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("clientLoop did not return after context cancel under ContinueOnError")
	}

	assert.Less(t, calls.Load(), int64(1_000_000), "loop must stop after context cancel even with ContinueOnError")
}

func TestMysqlClientConnExecutePanicsWithBindVars(t *testing.T) {
	c := &mysqlClientConn{}

	bindVars := map[string]*querypb.BindVariable{
		"v1": {Type: querypb.Type_INT64, Value: []byte("1")},
	}

	assert.Panics(t, func() {
		_, _ = c.execute(t.Context(), "SELECT 1", bindVars)
	}, "execute should panic when bind vars are provided for mysql protocol")
}
