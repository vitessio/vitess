/*
Copyright 2023 The Vitess Authors.

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

package aggregation

import (
	_ "embed"
	"flag"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "test"

	//go:embed schema.sql
	schemaSQL string

	//go:embed vschema.json
	vschema string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: schemaSQL,
			VSchema:   vschema,
		}
		clusterInstance.VtGateExtraArgs = []string{"--no_scatter=true"}
		err = clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 0, false)
		if err != nil {
			return 1
		}
		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = clusterInstance.GetVTParams(keyspaceName)

		return m.Run()
	}()
	os.Exit(exitCode)
}
