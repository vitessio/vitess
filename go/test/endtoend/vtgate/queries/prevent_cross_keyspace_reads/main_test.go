/*
Copyright 2026 The Vitess Authors.

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

package preventcrosskeyspacereads

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
	ks1Name         = "ks1"
	ks2Name         = "ks2"
	cell            = "test"

	//go:embed ks1_schema.sql
	ks1SchemaSQL string

	//go:embed ks2_schema.sql
	ks2SchemaSQL string

	//go:embed ks1_vschema.json
	ks1VSchema string

	//go:embed ks2_vschema.json
	ks2VSchema string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start ks1 (has prevent_cross_keyspace_reads: true in vschema)
		ks1 := &cluster.Keyspace{
			Name:      ks1Name,
			SchemaSQL: ks1SchemaSQL,
			VSchema:   ks1VSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*ks1, 0, false, cell)
		if err != nil {
			return 1
		}

		// Start ks2 (no cross-keyspace join restriction)
		ks2 := &cluster.Keyspace{
			Name:      ks2Name,
			SchemaSQL: ks2SchemaSQL,
			VSchema:   ks2VSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*ks2, 0, false, cell)
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}
