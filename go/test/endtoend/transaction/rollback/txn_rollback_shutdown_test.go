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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	clusterInstance *vitesst.Cluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	sqlSchema       = `
	create table buffer(
		id BIGINT NOT NULL,
		msg VARCHAR(64) NOT NULL,
		PRIMARY KEY (id)
	) Engine=InnoDB;`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithKeyspace(keyspaceName).
				WithReplicas(1).
				WithSchema(sqlSchema),
			// Set a short onterm timeout so the test goes faster.
			vitesst.WithVTGateArgs("--onterm-timeout", "1s"),
		)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		cleanup, err := cluster.Start(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer func() {
			if err := cleanup(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "cluster teardown:", err)
			}
		}()

		clusterInstance = cluster
		vtParams = cluster.VTParams(ctx, "")
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestTransactionRollBackWhenShutDown(t *testing.T) {
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "insert into buffer(id, msg) values(3,'mark')")
	vitesst.Exec(t, conn, "insert into buffer(id, msg) values(4,'doug')")

	// start an incomplete transaction
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into buffer(id, msg) values(33,'mark')")

	// Enforce a restart to enforce rollback
	if err = clusterInstance.VTGate().Restart(ctx); err != nil {
		assert.NoError(t, err)
	}

	want := ""

	// Make a new mysql connection to vtGate
	vtParams = clusterInstance.VTParams(ctx, "")
	conn2, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	vtParams = clusterInstance.VTParams(ctx, "")
	// Verify that rollback worked
	qr := vitesst.Exec(t, conn2, "select id from buffer where msg='mark'")
	got := fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(3)]]`
	assert.Equal(t, want, got)
}

func TestErrorInAutocommitSession(t *testing.T) {
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "set autocommit=true")
	vitesst.Exec(t, conn, "insert into buffer(id, msg) values(1,'foo')")
	_, err = conn.ExecuteFetch("insert into buffer(id, msg) values(1,'bar')", 1, true)
	require.Error(t, err) // this should fail with duplicate error
	vitesst.Exec(t, conn, "insert into buffer(id, msg) values(2,'baz')")

	conn2, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn2.Close()
	result := vitesst.Exec(t, conn2, "select * from buffer order by id")

	// if we have properly working autocommit code, both the successful inserts should be visible to a second
	// connection, even if we have not done an explicit commit
	assert.Equal(t, `[[INT64(1) VARCHAR("foo")] [INT64(2) VARCHAR("baz")] [INT64(3) VARCHAR("mark")] [INT64(4) VARCHAR("doug")]]`, fmt.Sprintf("%v", result.Rows))
}
