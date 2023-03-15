//go:build gofuzz
// +build gofuzz

/*
Copyright 2021 The Vitess Authors.
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

package vreplication

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	fuzz "github.com/AdaLogics/go-fuzz-headers"
)

var initter sync.Once

func initTesting() {
	testing.Init()
}

func createQueries(f *fuzz.ConsumeFuzzer) ([]string, error) {
	query1, err := f.GetSQLString()
	if err != nil {
		return []string{}, err
	}
	query2, err := f.GetSQLString()
	if err != nil {
		return []string{}, err
	}
	query3, err := f.GetSQLString()
	if err != nil {
		return []string{}, err
	}
	query4, err := f.GetSQLString()
	if err != nil {
		return []string{}, err
	}
	query5, err := f.GetSQLString()
	if err != nil {
		return []string{}, err
	}
	return []string{query1, query2, query3, query4, query5}, nil
}

func makeExpectations(dbClient *binlogplayer.MockDBClient, f *fuzz.ConsumeFuzzer) error {
	noOfExpects, err := f.GetInt()
	if err != nil {
		return err
	}
	if noOfExpects%15 == 0 {
		return fmt.Errorf("len is 0")
	}
	for i := 0; i < noOfExpects%15; i++ {
		query, err := f.GetString()
		if err != nil {
			return err
		}
		dbClient.ExpectRequest(query, &sqltypes.Result{}, nil)
	}
	return nil
}

func FuzzEngine(data []byte) int {
	initter.Do(initTesting)
	t := &testing.T{}
	f := fuzz.NewConsumer(data)
	queries, err := createQueries(f)
	if err != nil {
		return 0
	}
	topoServer := memorytopo.NewServer("cell1")
	_ = topoServer
	defer func() { globalStats = &vrStats{} }()

	resetBinlogClient()
	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)

	vre := NewTestEngine(topoServer, "cell1", mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	// Fuzzer fails if this expectation is not made first:
	dbClient.ExpectRequest(sqlparser.BuildParsedQuery("select * from %s.vreplication where db_name='db'",
		sidecardb.GetIdentifier()).Query, &sqltypes.Result{}, nil)
	err = makeExpectations(dbClient, f)
	if err != nil {
		return 0
	}
	vre.Open(context.Background())
	defer vre.Close()
	if !vre.IsOpen() {
		return 0
	}
	_, _ = vre.Exec(queries[0])
	_, _ = vre.Exec(queries[1])
	_, _ = vre.Exec(queries[2])
	_, _ = vre.Exec(queries[3])
	_, _ = vre.Exec(queries[4])

	return 1
}
