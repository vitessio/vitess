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

package vstreamer

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	_flag "vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"
)

var (
	engine *Engine
	env    *testenv.Env

	ignoreKeyspaceShardInFieldAndRowEvents bool
	testRowEventFlags                      bool
)

func TestMain(m *testing.M) {
	_flag.ParseFlagsForTest()
	ignoreKeyspaceShardInFieldAndRowEvents = true

	exitCode := func() int {
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		env, err = testenv.Init(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer env.Close()

		// engine cannot be initialized in testenv because it introduces
		// circular dependencies
		engine = NewEngine(env.TabletEnv, env.SrvTopo, env.SchemaEngine, nil, env.Cells[0])
		engine.InitDBConfig(env.KeyspaceName, env.ShardName)
		engine.Open()
		defer engine.Close()

		return m.Run()
	}()
	os.Exit(exitCode)
}

func newEngine(t *testing.T, ctx context.Context, binlogRowImage string) {
	if engine != nil {
		engine.Close()
	}
	if env != nil {
		env.Close()
	}
	var err error
	env, err = testenv.Init(ctx)
	require.NoError(t, err)

	setBinlogRowImage(t, binlogRowImage)

	// engine cannot be initialized in testenv because it introduces
	// circular dependencies
	engine = NewEngine(env.TabletEnv, env.SrvTopo, env.SchemaEngine, nil, env.Cells[0])
	engine.InitDBConfig(env.KeyspaceName, env.ShardName)
	engine.Open()
}

func customEngine(t *testing.T, modifier func(mysql.ConnParams) mysql.ConnParams) *Engine {
	original, err := env.Dbcfgs.AppWithDB().MysqlParams()
	require.NoError(t, err)
	modified := modifier(*original)
	cfg := env.TabletEnv.Config().Clone()
	cfg.DB = dbconfigs.NewTestDBConfigs(modified, modified, modified.DbName)

	engine := NewEngine(tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "VStreamerTest"), env.SrvTopo, env.SchemaEngine, nil, env.Cells[0])
	engine.InitDBConfig(env.KeyspaceName, env.ShardName)
	engine.Open()
	return engine
}

func setBinlogRowImage(t *testing.T, mode string) {
	execStatements(t, []string{
		fmt.Sprintf("set @@binlog_row_image='%s'", mode),
		fmt.Sprintf("set @@session.binlog_row_image='%s'", mode),
		fmt.Sprintf("set @@global.binlog_row_image='%s'", mode),
	})

}
