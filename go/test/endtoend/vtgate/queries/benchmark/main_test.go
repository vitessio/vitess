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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	mysqlParams     mysql.ConnParams
	sKs1            = "sks1"
	sKs2            = "sks2"
	sKs3            = "sks3"
	cell            = "test"

	//go:embed sharded_schema1.sql
	sSchemaSQL1 string

	//go:embed vschema1.json
	sVSchema1 string

	//go:embed sharded_schema2.sql
	sSchemaSQL2 string

	//go:embed vschema2.json
	sVSchema2 string

	//go:embed sharded_schema3.sql
	sSchemaSQL3 string

	//go:embed vschema3.json
	sVSchema3 string
)

var shards4 = []string{
	"-40", "40-80", "80-c0", "c0-",
}

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

		// Start sharded keyspace 1
		sKeyspace1 := &cluster.Keyspace{
			Name:      sKs1,
			SchemaSQL: sSchemaSQL1,
			VSchema:   sVSchema1,
		}

		err = clusterInstance.StartKeyspace(*sKeyspace1, shards4, 0, false)
		if err != nil {
			return 1
		}

		// Start sharded keyspace 2
		sKeyspace2 := &cluster.Keyspace{
			Name:      sKs2,
			SchemaSQL: sSchemaSQL2,
			VSchema:   sVSchema2,
		}

		err = clusterInstance.StartKeyspace(*sKeyspace2, shards4, 0, false)
		if err != nil {
			return 1
		}

		// Start sharded keyspace 3
		sKeyspace3 := &cluster.Keyspace{
			Name:      sKs3,
			SchemaSQL: sSchemaSQL3,
			VSchema:   sVSchema3,
		}

		err = clusterInstance.StartKeyspace(*sKeyspace3, shards4, 0, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = clusterInstance.GetVTParams("@primary")

		return m.Run()
	}()
	os.Exit(exitCode)
}

func start(b *testing.B) (*mysql.Conn, func()) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(b, err)

	deleteAll := func() {
		tables := []string{
			fmt.Sprintf("%s.tbl_no_lkp_vdx", sKs1),
			fmt.Sprintf("%s.mirror_tbl1", sKs1),
			fmt.Sprintf("%s.mirror_tbl2", sKs1),
			fmt.Sprintf("%s.mirror_tbl1", sKs2),
			fmt.Sprintf("%s.mirror_tbl2", sKs3),
		}
		for _, table := range tables {
			_, _ = utils.ExecAllowError(b, conn, "delete from "+table)
		}
	}

	// Make sure all keyspaces are serving.
	pending := map[string]string{
		sKs1: "mirror_tbl1",
		sKs2: "mirror_tbl1",
		sKs3: "mirror_tbl2",
	}
	for len(pending) > 0 {
		for ks, tbl := range pending {
			_, err := conn.ExecuteFetch(
				fmt.Sprintf("SELECT COUNT(id) FROM %s.%s", ks, tbl), 1, false)
			if err != nil {
				b.Logf("waiting for keyspace %s to be serving; last error: %v", ks, err)
				time.Sleep(1 * time.Second)
			} else {
				delete(pending, ks)
			}
		}
	}

	// Delete any pre-existing data.
	deleteAll()

	return conn, func() {
		deleteAll()
		conn.Close()
		cluster.PanicHandler(b)
	}
}
