package tabletmanager

import (
	"context"
	"strings"
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
	want := []string{
		"use ` escap``e me `",
		"select 42",
	}
	got := strings.Split(db.QueryLog(), ";")
	for _, w := range want {
		require.Contains(t, got, w)
	}
}
