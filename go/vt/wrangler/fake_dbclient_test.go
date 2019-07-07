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

package wrangler

import (
	"fmt"
	"regexp"
	"testing"

	"vitess.io/vitess/go/sqltypes"
)

func verifyQueries(t *testing.T, dcs []*fakeDBClient) {
	for _, dc := range dcs {
		dc.verifyQueries(t)
	}
}

type dbResult struct {
	result *sqltypes.Result
	err    error
	called bool
}

// fakeDBClient fakes a binlog_player.DBClient.
type fakeDBClient struct {
	queries   map[string]*dbResult
	queriesRE map[string]*dbResult
}

// NewfakeDBClient returns a new DBClientMock.
func newFakeDBClient() *fakeDBClient {
	return &fakeDBClient{
		queries: map[string]*dbResult{
			"use _vt": {result: &sqltypes.Result{}, called: true},
			"select * from _vt.vreplication where db_name='db'": {result: &sqltypes.Result{}},
		},
		queriesRE: make(map[string]*dbResult),
	}
}

func (dc *fakeDBClient) addQuery(query string, result *sqltypes.Result, err error) {
	dc.queries[query] = &dbResult{result: result, err: err}
}

func (dc *fakeDBClient) addQueryRE(query string, result *sqltypes.Result, err error) {
	dc.queriesRE[query] = &dbResult{result: result, err: err}
}

// DBName is part of the DBClient interface
func (dc *fakeDBClient) DBName() string {
	return "db"
}

// Connect is part of the DBClient interface
func (dc *fakeDBClient) Connect() error {
	return nil
}

// Begin is part of the DBClient interface
func (dc *fakeDBClient) Begin() error {
	return nil
}

// Commit is part of the DBClient interface
func (dc *fakeDBClient) Commit() error {
	return nil
}

// Rollback is part of the DBClient interface
func (dc *fakeDBClient) Rollback() error {
	return nil
}

// Close is part of the DBClient interface
func (dc *fakeDBClient) Close() {
}

// ExecuteFetch is part of the DBClient interface
func (dc *fakeDBClient) ExecuteFetch(query string, maxrows int) (qr *sqltypes.Result, err error) {
	if dbr := dc.queries[query]; dbr != nil {
		dbr.called = true
		return dbr.result, dbr.err
	}
	for re, dbr := range dc.queriesRE {
		if regexp.MustCompile(re).MatchString(query) {
			dbr.called = true
			return dbr.result, dbr.err
		}
	}
	return nil, fmt.Errorf("unexpected query: %s", query)
}

func (dc *fakeDBClient) verifyQueries(t *testing.T) {
	t.Helper()
	for query, dbr := range dc.queries {
		if !dbr.called {
			t.Errorf("query: %v was not called", query)
		}
	}
	for query, dbr := range dc.queriesRE {
		if !dbr.called {
			t.Errorf("query: %v was not called", query)
		}
	}
}
