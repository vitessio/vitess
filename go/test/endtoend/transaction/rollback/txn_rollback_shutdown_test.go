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

package rollback

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "zone1"
	hostname        = "localhost"
	sqlSchema       = `
	create table buffer(
		id BIGINT NOT NULL,
		msg VARCHAR(64) NOT NULL,
		PRIMARY KEY (id)
	) Engine=InnoDB;`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Reserve vtGate port in order to pass it to vtTablet
		clusterInstance.VtgateGrpcPort = clusterInstance.GetAndReservePort()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			panic(err)
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false)
		if err != nil {
			panic(err)
		}

		// Set a short onterm timeout so the test goes faster.
		clusterInstance.VtGateExtraArgs = []string{"--onterm_timeout", "1s"}
		err = clusterInstance.StartVtgate()
		if err != nil {
			panic(err)
		}
		vtParams = clusterInstance.GetVTParams(keyspaceName)
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestTransactionRollBackWhenShutDown(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "insert into buffer(id, msg) values(3,'mark')")
	utils.Exec(t, conn, "insert into buffer(id, msg) values(4,'doug')")

	// start an incomplete transaction
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into buffer(id, msg) values(33,'mark')")

	// Enforce a restart to enforce rollback
	if err = clusterInstance.RestartVtgate(); err != nil {
		t.Errorf("Fail to re-start vtgate: %v", err)
	}

	want := ""

	// Make a new mysql connection to vtGate
	vtParams = clusterInstance.GetVTParams(keyspaceName)
	conn2, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	vtParams = clusterInstance.GetVTParams(keyspaceName)
	// Verify that rollback worked
	qr := utils.Exec(t, conn2, "select id from buffer where msg='mark'")
	got := fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(3)]]`
	assert.Equal(t, want, got)
}

func TestErrorInAutocommitSession(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "set autocommit=true")
	utils.Exec(t, conn, "insert into buffer(id, msg) values(1,'foo')")
	_, err = conn.ExecuteFetch("insert into buffer(id, msg) values(1,'bar')", 1, true)
	require.Error(t, err) // this should fail with duplicate error
	utils.Exec(t, conn, "insert into buffer(id, msg) values(2,'baz')")

	conn2, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn2.Close()
	result := utils.Exec(t, conn2, "select * from buffer order by id")

	// if we have properly working autocommit code, both the successful inserts should be visible to a second
	// connection, even if we have not done an explicit commit
	assert.Equal(t, `[[INT64(1) VARCHAR("foo")] [INT64(2) VARCHAR("baz")] [INT64(3) VARCHAR("mark")] [INT64(4) VARCHAR("doug")]]`, fmt.Sprintf("%v", result.Rows))
}
