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
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

const mockClientUNameFiltered = "Filtered"
const mockClientUNameDba = "Dba"

// MockDBClient mocks a DBClient.
// It must be configured to expect requests in a specific order.
type MockDBClient struct {
	t             *testing.T
	UName         string
	expect        []*mockExpect
	expectMu      sync.Mutex
	currentResult int
	done          chan struct{}
	invariants    map[string]*sqltypes.Result
	Tag           string
	parser        *sqlparser.Parser
}

type mockExpect struct {
	query  string
	re     *regexp.Regexp
	result *sqltypes.Result
	err    error
}

// NewMockDBClient returns a new DBClientMock with the default "Filtered" UName.
func NewMockDBClient(t *testing.T) *MockDBClient {
	return &MockDBClient{
		t:     t,
		UName: mockClientUNameFiltered,
		done:  make(chan struct{}),
		invariants: map[string]*sqltypes.Result{
			"CREATE TABLE IF NOT EXISTS _vt.vreplication_log":           {},
			"select id, type, state, message from _vt.vreplication_log": {},
			"insert into _vt.vreplication_log":                          {},
			// The following statements don't have a deterministic order as they are
			// executed in the normal program flow, but ALSO done in a defer as a protective
			// measure as they are resetting the values back to the original one. This also
			// means that the values they set are based on the session defaults, which can
			// change. So we make these invariants for unit test stability.
			"select @@foreign_key_checks": sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"@@foreign_key_checks",
					"int64",
				),
				"1",
			),
			"set @@session.foreign_key_checks": {},
			"set foreign_key_checks":           {},
			"select @@session.sql_mode": sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"sql_mode", "varchar",
				),
				"ONLY_FULL_GROUP_BY,NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			),
			"set @@session.sql_mode": {},
			"set sql_mode":           {},
		},
		parser: sqlparser.NewTestParser(),
	}
}

// NewMockDbaClient returns a new DBClientMock with the default "Dba" UName.
func NewMockDbaClient(t *testing.T) *MockDBClient {
	return &MockDBClient{
		t:      t,
		UName:  mockClientUNameDba,
		done:   make(chan struct{}),
		parser: sqlparser.NewTestParser(),
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
	dc.expectMu.Lock()
	defer dc.expectMu.Unlock()
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
	dc.expectMu.Lock()
	defer dc.expectMu.Unlock()
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
	// Serialize ExecuteFetch to enforce a strict order on shared dbClients.
	dc.expectMu.Lock()
	defer dc.expectMu.Unlock()

	dc.t.Helper()
	msg := "DBClient query: %v"
	if dc.Tag != "" {
		msg = fmt.Sprintf("[%s] %s", dc.Tag, msg)
	}
	dc.t.Logf(msg, query)

	for q, result := range dc.invariants {
		if strings.Contains(strings.ToLower(query), strings.ToLower(q)) {
			return result, nil
		}
	}

	if dc.currentResult >= len(dc.expect) {
		msg := "DBClientMock: query: %s, no more requests are expected"
		if dc.Tag != "" {
			msg = fmt.Sprintf("[%s] %s", dc.Tag, msg)
		}
		dc.t.Fatalf(msg, query)
	}
	result := dc.expect[dc.currentResult]
	if result.re == nil {
		if query != result.query {
			msg := "DBClientMock: query: %s, want %s"
			if dc.Tag != "" {
				msg = fmt.Sprintf("[%s] %s", dc.Tag, msg)
			}
			dc.t.Fatalf(msg, query, result.query)
		}
	} else {
		if !result.re.MatchString(query) {
			msg := "DBClientMock: query: %s, must match %s"
			if dc.Tag != "" {
				msg = fmt.Sprintf("[%s] %s", dc.Tag, msg)
			}
			dc.t.Fatalf(msg, query, result.query)
		}
	}
	dc.currentResult++
	if dc.currentResult >= len(dc.expect) {
		close(dc.done)
	}
	return result.result, result.err
}

func (dc *MockDBClient) ExecuteFetchMulti(query string, maxrows int) ([]*sqltypes.Result, error) {
	queries, err := dc.parser.SplitStatementToPieces(query)
	if err != nil {
		return nil, err
	}
	results := make([]*sqltypes.Result, 0, len(queries))
	for _, query := range queries {
		qr, err := dc.ExecuteFetch(query, maxrows)
		if err != nil {
			return nil, err
		}
		results = append(results, qr)
	}
	return results, nil
}

// AddInvariant can be used to customize the behavior of the mock client.
func (dc *MockDBClient) AddInvariant(query string, result *sqltypes.Result) {
	dc.expectMu.Lock()
	defer dc.expectMu.Unlock()
	dc.invariants[query] = result
}

// RemoveInvariant can be used to customize the behavior of the mock client.
func (dc *MockDBClient) RemoveInvariant(query string) {
	dc.expectMu.Lock()
	defer dc.expectMu.Unlock()
	delete(dc.invariants, query)
}
