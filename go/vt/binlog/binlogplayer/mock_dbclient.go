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

package binlogplayer

import (
	"regexp"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
)

// MockDBClient mocks a DBClient.
// It must be configured to expect requests in a specific order.
type MockDBClient struct {
	t             *testing.T
	expect        []*mockExpect
	currentResult int
	done          chan struct{}
}

type mockExpect struct {
	query  string
	re     *regexp.Regexp
	result *sqltypes.Result
	err    error
}

// NewMockDBClient returns a new DBClientMock.
func NewMockDBClient(t *testing.T) *MockDBClient {
	return &MockDBClient{
		t:    t,
		done: make(chan struct{}),
	}
}

// ExpectRequest adds an expected result to the mock.
// This function should not be called conncurrently with other commands.
func (dc *MockDBClient) ExpectRequest(query string, result *sqltypes.Result, err error) {
	select {
	case <-dc.done:
		dc.done = make(chan struct{})
	default:
	}
	dc.expect = append(dc.expect, &mockExpect{
		query:  query,
		result: result,
		err:    err,
	})
}

// ExpectRequestRE adds an expected result to the mock.
// queryRE is a regular expression.
// This function should not be called conncurrently with other commands.
func (dc *MockDBClient) ExpectRequestRE(queryRE string, result *sqltypes.Result, err error) {
	select {
	case <-dc.done:
		dc.done = make(chan struct{})
	default:
	}
	dc.expect = append(dc.expect, &mockExpect{
		query:  queryRE,
		re:     regexp.MustCompile(queryRE),
		result: result,
		err:    err,
	})
}

// Wait waits for all expected requests to be executed.
// dc.t.Fatalf is executed on 1 second timeout. Wait should
// not be called concurrently with ExpectRequest.
func (dc *MockDBClient) Wait() {
	dc.t.Helper()
	select {
	case <-dc.done:
		return
	case <-time.After(5 * time.Second):
		dc.t.Fatalf("timeout waiting for requests, want: %v", dc.expect[dc.currentResult].query)
	}
}

// DBName is part of the DBClient interface
func (dc *MockDBClient) DBName() string {
	return "db"
}

// Connect is part of the DBClient interface
func (dc *MockDBClient) Connect() error {
	return nil
}

// Begin is part of the DBClient interface
func (dc *MockDBClient) Begin() error {
	_, err := dc.ExecuteFetch("begin", 1)
	return err
}

// Commit is part of the DBClient interface
func (dc *MockDBClient) Commit() error {
	_, err := dc.ExecuteFetch("commit", 1)
	return err
}

// Rollback is part of the DBClient interface
func (dc *MockDBClient) Rollback() error {
	_, err := dc.ExecuteFetch("rollback", 1)
	return err
}

// Close is part of the DBClient interface
func (dc *MockDBClient) Close() {
}

// ExecuteFetch is part of the DBClient interface
func (dc *MockDBClient) ExecuteFetch(query string, maxrows int) (qr *sqltypes.Result, err error) {
	dc.t.Helper()
	dc.t.Logf("DBClient query: %v", query)
	if dc.currentResult >= len(dc.expect) {
		dc.t.Fatalf("DBClientMock: query: %s, no more requests are expected", query)
	}
	result := dc.expect[dc.currentResult]
	if result.re == nil {
		if query != result.query {
			dc.t.Fatalf("DBClientMock: query: %s, want %s", query, result.query)
		}
	} else {
		if !result.re.MatchString(query) {
			dc.t.Fatalf("DBClientMock: query: %s, must match %s", query, result.query)
		}
	}
	dc.currentResult++
	if dc.currentResult >= len(dc.expect) {
		close(dc.done)
	}
	return result.result, result.err
}
