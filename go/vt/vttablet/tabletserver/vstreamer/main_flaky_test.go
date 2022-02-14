package vstreamer

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"
)

var (
	engine *Engine
	env    *testenv.Env

	ignoreKeyspaceShardInFieldAndRowEvents bool
)

func TestMain(m *testing.M) {
	flag.Parse() // Do not remove this comment, import into google3 depends on it
	ignoreKeyspaceShardInFieldAndRowEvents = true
	if testing.Short() {
		os.Exit(m.Run())
	}

	exitCode := func() int {
		var err error
		env, err = testenv.Init()
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

func customEngine(t *testing.T, modifier func(mysql.ConnParams) mysql.ConnParams) *Engine {
	original, err := env.Dbcfgs.AppWithDB().MysqlParams()
	require.NoError(t, err)
	modified := modifier(*original)
	config := env.TabletEnv.Config().Clone()
	config.DB = dbconfigs.NewTestDBConfigs(modified, modified, modified.DbName)

	engine := NewEngine(tabletenv.NewEnv(config, "VStreamerTest"), env.SrvTopo, env.SchemaEngine, nil, env.Cells[0])
	engine.InitDBConfig(env.KeyspaceName, env.ShardName)
	engine.Open()
	return engine
}
