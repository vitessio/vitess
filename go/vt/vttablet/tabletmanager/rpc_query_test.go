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
	"testing"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/mysql/fakesqldb"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	"github.com/stretchr/testify/require"
)

func TestTabletManager_ExecuteFetchAsDba(t *testing.T) {
	ctx := context.Background()
	cp := mysql.ConnParams{}
	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})
	daemon := fakemysqldaemon.NewFakeMysqlDaemon(db)

	dbName := " escap`e me "
	tm := &TabletManager{
		MysqlDaemon:         daemon,
		DBConfigs:           dbconfigs.NewTestDBConfigs(cp, cp, dbName),
		QueryServiceControl: tabletservermock.NewController(),
	}

	_, err := tm.ExecuteFetchAsDba(ctx, []byte("select 42"), dbName, 10, false, false)
	require.NoError(t, err)
	require.Equal(t, "use ` escap``e me `;select 42", db.QueryLog())
}
