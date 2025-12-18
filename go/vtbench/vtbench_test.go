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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

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

	err := bench.createConns(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unimplemented connection protocol")
	assert.Contains(t, err.Error(), "unknown-protocol-999")
}

func TestMysqlClientConnExecutePanicsWithBindVars(t *testing.T) {
	c := &mysqlClientConn{}

	bindVars := map[string]*querypb.BindVariable{
		"v1": {Type: querypb.Type_INT64, Value: []byte("1")},
	}

	assert.Panics(t, func() {
		_, _ = c.execute(context.Background(), "SELECT 1", bindVars)
	}, "execute should panic when bind vars are provided for mysql protocol")
}
