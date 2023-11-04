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

package dml

import (
	"context"
	_ "embed"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	sKs             = "sks"
	uKs             = "uks"
	cell            = "test"

	//go:embed sharded_schema.sql
	sSchemaSQL string

	//go:embed vschema.json
	sVSchema string
)

var (
	shards4 = []string{
		"-40", "40-80", "80-c0", "c0-",
	}

	shards8 = []string{
		"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-",
	}

	shards16 = []string{
		"-10", "10-20", "20-30", "30-40", "40-50", "50-60", "60-70", "70-80", "80-90", "90-a0", "a0-b0", "b0-c0", "c0-d0", "d0-e0", "e0-f0", "f0-",
	}

	shards32 = []string{
		"-05", "05-10", "10-15", "15-20", "20-25", "25-30", "30-35", "35-40", "40-45", "45-50", "50-55", "55-60", "60-65", "65-70", "70-75", "75-80",
		"80-85", "85-90", "90-95", "95-a0", "a0-a5", "a5-b0", "b0-b5", "b5-c0", "c0-c5", "c5-d0", "d0-d5", "d5-e0", "e0-e5", "e5-f0", "f0-f5", "f5-",
	}
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

		// Start sharded keyspace
		sKeyspace := &cluster.Keyspace{
			Name:      sKs,
			SchemaSQL: sSchemaSQL,
			VSchema:   sVSchema,
		}

		err = clusterInstance.StartKeyspace(*sKeyspace, shards4, 0, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = clusterInstance.GetVTParams(sKs)

		return m.Run()
	}()
	os.Exit(exitCode)
}

func start(b *testing.B) (*mysql.Conn, func()) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(b, err)

	deleteAll := func() {
		tables := []string{"tbl_no_lkp_vdx"}
		for _, table := range tables {
			_, _ = utils.ExecAllowError(b, conn, "delete from "+table)
		}
	}

	deleteAll()

	return conn, func() {
		deleteAll()
		conn.Close()
		cluster.PanicHandler(b)
	}
}
