/*
Copyright 2017 Google Inc.

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
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"
)

var (
	engine *Engine
	env    *testenv.Env
)

func TestMain(m *testing.M) {
	flag.Parse() // Do not remove this comment, import into google3 depends on it

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
		// circular dependencies.
		engine = NewEngine(servenv.NewEmbedder("test", ""), env.SrvTopo, env.SchemaEngine)
		engine.InitDBConfig(env.Dbcfgs)
		engine.Open(env.KeyspaceName, env.Cells[0])
		defer engine.Close()

		return m.Run()
	}()
	os.Exit(exitCode)
}
