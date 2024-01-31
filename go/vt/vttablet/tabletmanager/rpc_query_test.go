/*
Copyright 2020 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestAnalyzeExecuteFetchAsDbaMultiQuery(t *testing.T) {
	tcases := []struct {
		query           string
		count           int
		parseable       bool
		allowZeroInDate bool
		allCreate       bool
		expectErr       bool
	}{
		{
			query:     "",
			expectErr: true,
		},
		{
			query:     "select * from t1 ; select * from t2",
			count:     2,
			parseable: true,
		},
		{
			query:     "create table t(id int)",
			count:     1,
			allCreate: true,
			parseable: true,
		},
		{
			query:     "create table t(id int); create view v as select 1 from dual",
			count:     2,
			allCreate: true,
			parseable: true,
		},
		{
			query:     "create table t(id int); create view v as select 1 from dual; drop table t3",
			count:     3,
			allCreate: false,
			parseable: true,
		},
		{
			query:           "create /*vt+ allowZeroInDate=true */ table t (id int)",
			count:           1,
			allCreate:       true,
			allowZeroInDate: true,
			parseable:       true,
		},
		{
			query:           "create table a (id int) ; create /*vt+ allowZeroInDate=true */ table b (id int)",
			count:           2,
			allCreate:       true,
			allowZeroInDate: true,
			parseable:       true,
		},
		{
			query:     "stop replica; start replica",
			count:     2,
			parseable: false,
		},
		{
			query:     "create table a (id int) ; --comment ; what",
			count:     3,
			parseable: false,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.query, func(t *testing.T) {
			parser := sqlparser.NewTestParser()
			queries, parseable, countCreate, allowZeroInDate, err := analyzeExecuteFetchAsDbaMultiQuery(tcase.query, parser)
			if tcase.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tcase.count, len(queries))
				assert.Equal(t, tcase.parseable, parseable)
				assert.Equal(t, tcase.allCreate, (countCreate == len(queries)))
				assert.Equal(t, tcase.allowZeroInDate, allowZeroInDate)
			}
		})
	}
}

func TestTabletManager_ExecuteFetchAsDba(t *testing.T) {
	ctx := context.Background()
	cp := mysql.ConnParams{}
	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})
	daemon := mysqlctl.NewFakeMysqlDaemon(db)

	dbName := " escap`e me "
	tm := &TabletManager{
		MysqlDaemon:            daemon,
		DBConfigs:              dbconfigs.NewTestDBConfigs(cp, cp, dbName),
		QueryServiceControl:    tabletservermock.NewController(),
		_waitForGrantsComplete: make(chan struct{}),
		Env:                    vtenv.NewTestEnv(),
	}
	close(tm._waitForGrantsComplete)

	_, err := tm.ExecuteFetchAsDba(ctx, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
		Query:   []byte("select 42"),
		DbName:  dbName,
		MaxRows: 10,
	})
	require.NoError(t, err)
	want := []string{
		"use ` escap``e me `",
		"select 42",
	}
	got := strings.Split(db.QueryLog(), ";")
	for _, w := range want {
		require.Contains(t, got, w)
	}
}
