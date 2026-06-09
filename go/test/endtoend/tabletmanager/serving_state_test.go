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

package tabletmanager

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

const schema = `
	create table test(
		id bigint,
		msg varchar(64),
		primary key(id)
	) Engine=InnoDB;
`

const vschema = `
	{
		"sharded": false,
		"tables": {
			"test": {}
		}
	}
`

// TestChangeTypePrimaryCompletesWithBlockedCommit verifies that a primary
// tablet can leave serving while a COMMIT is in flight.
func TestChangeTypePrimaryCompletesWithBlockedCommit(t *testing.T) {
	clusterInstance := cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	err := clusterInstance.StartTopo()
	require.NoError(t, err)

	keyspace := cluster.Keyspace{
		Name:             "ks",
		SchemaSQL:        schema,
		VSchema:          vschema,
		DurabilityPolicy: policy.DurabilitySemiSync,
	}

	err = clusterInstance.StartUnshardedKeyspace(keyspace, 1, false, clusterInstance.Cell)
	require.NoError(t, err)

	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	ctx := t.Context()

	conn, err := mysql.Connect(ctx, &mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	})
	require.NoError(t, err)
	defer conn.Close()

	require.NotEmpty(t, clusterInstance.Keyspaces)
	require.NotEmpty(t, clusterInstance.Keyspaces[0].Shards)

	tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
	require.NotEmpty(t, tablets)

	var primary *cluster.Vttablet
	var replicas []*cluster.Vttablet

	for _, tablet := range tablets {
		switch tablet.Type {
		case "primary":
			require.Nil(t, primary, "expected only one primary tablet")
			primary = tablet

		case "replica":
			replicas = append(replicas, tablet)
		}
	}

	require.NotNil(t, primary)
	require.NotEmpty(t, replicas)

	// Stop every replica so COMMITs are blocked waiting on semi-sync.
	for _, tablet := range replicas {
		err := tablet.VttabletProcess.TearDownWithTimeout(30 * time.Second)
		require.NoError(t, err)

		err = tablet.MysqlctlProcess.Stop()
		require.NoError(t, err)
	}

	_, err = conn.ExecuteFetch("begin", 0, false)
	require.NoError(t, err)

	query := "insert into test(id, msg) values (1, 'test 1')"
	_, err = conn.ExecuteFetch(query, 0, false)
	require.NoError(t, err)

	commitErr := make(chan error, 1)

	// Issue the COMMIT, which should block waiting on semi-sync. The expectation is that
	// it will be killed after the grace period as part of the `ChangeType` call.
	go func() {
		_, err := conn.ExecuteFetch("commit", 0, false)
		commitErr <- err
	}()

	// Wait until the commit is stuck waiting on semi-sync.
	require.Eventually(t, func() bool {
		qr, err := primary.VttabletProcess.QueryTablet(
			"select State, Info from information_schema.processlist",
			keyspace.Name,
			false,
		)
		if err != nil {
			return false
		}

		for _, row := range qr.Rows {
			if len(row) != 2 {
				continue
			}

			if strings.EqualFold(row[0].ToString(), "Waiting for semi-sync ACK from replica") &&
				strings.EqualFold(row[1].ToString(), "commit") {
				return true
			}
		}

		return false
	}, 20*time.Second, 100*time.Millisecond, "query with state not in processlist")

	oldPrimary, err := clusterInstance.VtctldClientProcess.GetTablet(primary.Alias)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

	tmClient := tmc.NewClient()
	defer tmClient.Close()

	// Change the primary to spare, which should stop the query service and drain any
	// active queries, including the COMMIT.
	err = tmClient.ChangeType(ctx, oldPrimary, topodatapb.TabletType_SPARE, false)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(commitErr) > 0
	}, 5*time.Second, 100*time.Millisecond, "timed out waiting for COMMIT to return after ChangeType")

	err = <-commitErr
	require.ErrorContains(t, err, "code = Canceled")
	require.ErrorContains(t, err, "QueryList.TerminateAll()")
}
