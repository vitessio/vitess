/*
Copyright 2017 Google Inc.

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

// DBClientMock mocks a DBClient.
type DBClientMock struct {
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

// NewDBClientMock returns a new DBClientMock
func NewDBClientMock(t *testing.T) *DBClientMock {
	return &DBClientMock{
		t:    t,
		done: make(chan struct{}),
	}
}

// ExpectRequest adds an expected result to the mock.
// This function should not be called conncurrently with other commands.
func (dc *DBClientMock) ExpectRequest(query string, result *sqltypes.Result, err error) {
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
func (dc *DBClientMock) ExpectRequestRE(queryRE string, result *sqltypes.Result, err error) {
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
func (dc *DBClientMock) Wait() {
	dc.t.Helper()
	select {
	case <-dc.done:
		return
	case <-time.After(1 * time.Second):
		dc.t.Fatalf("timeout waiting for requests, want: %v", dc.expect[dc.currentResult].query)
	}
}

// DBName is part of the DBClient interface
func (dc *DBClientMock) DBName() string {
	return "db"
}

// Connect is part of the DBClient interface
func (dc *DBClientMock) Connect() error {
	return nil
}

// Begin is part of the DBClient interface
func (dc *DBClientMock) Begin() error {
	_, err := dc.ExecuteFetch("BEGIN", 1)
	return err
}

// Commit is part of the DBClient interface
func (dc *DBClientMock) Commit() error {
	_, err := dc.ExecuteFetch("COMMIT", 1)
	return err
}

// Rollback is part of the DBClient interface
func (dc *DBClientMock) Rollback() error {
	_, err := dc.ExecuteFetch("ROLLBACK", 1)
	return err
}

// Close is part of the DBClient interface
func (dc *DBClientMock) Close() {
	return
}

// ExecuteFetch is part of the DBClient interface
func (dc *DBClientMock) ExecuteFetch(query string, maxrows int) (qr *sqltypes.Result, err error) {
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
