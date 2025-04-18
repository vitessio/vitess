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
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
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
	if dbrs.exhausted() {
		log.Infof(fmt.Sprintf("Unexpected query >%s<", query))
		return nil, fmt.Errorf("code executed this query, but the test did not expect it: %s", query)
	}
	i := dbrs.index
	dbrs.index++
	return dbrs.results[i].result, dbrs.results[i].err
}

func (dbrs *dbResults) exhausted() bool {
	return dbrs.index == len(dbrs.results)
}

// fakeDBClient fakes a binlog_player.DBClient.
type fakeDBClient struct {
	mu         sync.Mutex
	name       string
	queries    map[string]*dbResults
	queriesRE  map[string]*dbResults
	invariants map[string]*sqltypes.Result
}

// NewfakeDBClient returns a new DBClientMock.
func newFakeDBClient(name string) *fakeDBClient {
	return &fakeDBClient{
		name:      name,
		queries:   make(map[string]*dbResults),
		queriesRE: make(map[string]*dbResults),
		invariants: map[string]*sqltypes.Result{
			"use _vt": {},
			"select * from _vt.vreplication where db_name='db'":         {},
			"select id, type, state, message from _vt.vreplication_log": {},
			"insert into _vt.vreplication_log":                          {},
			"SELECT db_name FROM _vt.vreplication LIMIT 0":              {},
			"select @@session.auto_increment_increment":                 {},
		},
	}
}

func (dc *fakeDBClient) addQuery(query string, result *sqltypes.Result, err error) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dbr := &dbResult{result: result, err: err}
	if dbrs, ok := dc.queries[query]; ok {
		dbrs.results = append(dbrs.results, dbr)
		return
	}
	dc.queries[query] = &dbResults{results: []*dbResult{dbr}, err: err}
}

func (dc *fakeDBClient) addQueryRE(query string, result *sqltypes.Result, err error) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dbr := &dbResult{result: result, err: err}
	if dbrs, ok := dc.queriesRE[query]; ok {
		dbrs.results = append(dbrs.results, dbr)
		return
	}
	dc.queriesRE[query] = &dbResults{results: []*dbResult{dbr}, err: err}
}

func (dc *fakeDBClient) getInvariant(query string) *sqltypes.Result {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.invariants[query]
}

// note: addInvariant will replace a previous result for a query with the provided one: this is used in the tests
func (dc *fakeDBClient) addInvariant(query string, result *sqltypes.Result) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
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

func (dc *fakeDBClient) IsClosed() bool {
	return false
}

// ExecuteFetch is part of the DBClient interface
func (dc *fakeDBClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	qr, err := dc.executeFetch(query, maxrows)
	return qr, err
}

func (dc *fakeDBClient) ExecuteFetchMulti(query string, maxrows int) ([]*sqltypes.Result, error) {
	queries, err := sqlparser.NewTestParser().SplitStatementToPieces(query)
	if err != nil {
		return nil, err
	}
	results := make([]*sqltypes.Result, 0, len(queries))
	for _, query := range queries {
		qr, err := dc.executeFetch(query, maxrows)
		if err != nil {
			return nil, err
		}
		results = append(results, qr)
	}
	return results, nil
}

func (dc *fakeDBClient) SupportsCapability(capability capabilities.FlavorCapability) (bool, error) {
	return false, nil
}

// ExecuteFetch is part of the DBClient interface
func (dc *fakeDBClient) executeFetch(query string, maxrows int) (*sqltypes.Result, error) {
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
	for q, result := range dc.invariants { //supports allowing just a prefix of an expected query
		if strings.Contains(query, q) {
			return result, nil
		}
	}

	log.Infof("Missing query: >>>>>>>>>>>>>>>>>>%s<<<<<<<<<<<<<<<", query)
	return nil, fmt.Errorf("unexpected query: %s", query)
}

func (dc *fakeDBClient) verifyQueries(t *testing.T) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	t.Helper()
	for query, dbrs := range dc.queries {
		if !dbrs.exhausted() {
			assert.FailNowf(t, "expected query did not get executed during the test", query)
		}
	}
	for query, dbrs := range dc.queriesRE {
		if !dbrs.exhausted() {
			assert.FailNowf(t, "expected regex query did not get executed during the test", query)
		}
	}
}
