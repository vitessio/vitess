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
	t.Helper()
	for _, dc := range dcs {
		dc.verifyQueries(t)
	}
}

type dbResults struct {
	index   int
	results []*dbResult
	err     error
}

type dbResult struct {
	result *sqltypes.Result
	err    error
}

func (dbrs *dbResults) next(query string) (*sqltypes.Result, error) {
	if dbrs.called() {
		return nil, fmt.Errorf("query results exhausted: %s", query)
	}
	i := dbrs.index
	dbrs.index++
	return dbrs.results[i].result, dbrs.results[i].err
}

func (dbrs *dbResults) called() bool {
	return dbrs.index == len(dbrs.results)
}

// fakeDBClient fakes a binlog_player.DBClient.
type fakeDBClient struct {
	queries    map[string]*dbResults
	queriesRE  map[string]*dbResults
	invariants map[string]*sqltypes.Result
}

// NewfakeDBClient returns a new DBClientMock.
func newFakeDBClient() *fakeDBClient {
	return &fakeDBClient{
		queries:   make(map[string]*dbResults),
		queriesRE: make(map[string]*dbResults),
		invariants: map[string]*sqltypes.Result{
			"use _vt": {},
			"select * from _vt.vreplication where db_name='db'": {},
		},
	}
}

func (dc *fakeDBClient) addQuery(query string, result *sqltypes.Result, err error) {
	dbr := &dbResult{result: result, err: err}
	if dbrs, ok := dc.queries[query]; ok {
		dbrs.results = append(dbrs.results, dbr)
		return
	}
	dc.queries[query] = &dbResults{results: []*dbResult{dbr}, err: err}
}

func (dc *fakeDBClient) addQueryRE(query string, result *sqltypes.Result, err error) {
	dbr := &dbResult{result: result, err: err}
	if dbrs, ok := dc.queriesRE[query]; ok {
		dbrs.results = append(dbrs.results, dbr)
		return
	}
	dc.queriesRE[query] = &dbResults{results: []*dbResult{dbr}, err: err}
}

func (dc *fakeDBClient) addInvariant(query string, result *sqltypes.Result) {
	dc.invariants[query] = result
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
	if dbrs := dc.queries[query]; dbrs != nil {
		return dbrs.next(query)
	}
	for re, dbrs := range dc.queriesRE {
		if regexp.MustCompile(re).MatchString(query) {
			return dbrs.next(query)
		}
	}
	if result := dc.invariants[query]; result != nil {
		return result, nil
	}
	return nil, fmt.Errorf("unexpected query: %s", query)
}

func (dc *fakeDBClient) verifyQueries(t *testing.T) {
	t.Helper()
	for query, dbrs := range dc.queries {
		if !dbrs.called() {
			t.Errorf("query: %v has unreturned results", query)
		}
	}
	for query, dbrs := range dc.queriesRE {
		if !dbrs.called() {
			t.Errorf("query: %v has unreturned results", query)
		}
	}
}
