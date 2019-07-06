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

	"vitess.io/vitess/go/sqltypes"
)

type dbResult struct {
	result *sqltypes.Result
	err    error
}

// fakeDBClient fakes a binlog_player.DBClient.
type fakeDBClient struct {
	queries   map[string]*dbResult
	queriesRE map[*regexp.Regexp]*dbResult
}

// NewfakeDBClient returns a new DBClientMock.
func newFakeDBClient() *fakeDBClient {
	return &fakeDBClient{
		queries:   make(map[string]*dbResult),
		queriesRE: make(map[*regexp.Regexp]*dbResult),
	}
}

func (dc *fakeDBClient) setResult(query string, result *sqltypes.Result, err error) {
	dc.queries[query] = &dbResult{result: result, err: err}
}

func (dc *fakeDBClient) setResultRE(query string, result *sqltypes.Result, err error) {
	dc.queriesRE[regexp.MustCompile(query)] = &dbResult{result: result, err: err}
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
		return dbr.result, dbr.err
	}
	for re, dbr := range dc.queriesRE {
		if re.MatchString(query) {
			return dbr.result, dbr.err
		}
	}
	return nil, fmt.Errorf("unexpected query: %s", query)
}
