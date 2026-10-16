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
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

// blockedCommitSchema is the schema of the keyspace in the blocked COMMIT test.
const blockedCommitSchema = `
	create table test(
		id bigint,
		msg varchar(64),
		primary key(id)
	) Engine=InnoDB;
`

// blockedCommitVSchema is the vschema of the keyspace in the blocked COMMIT test.
const blockedCommitVSchema = `
	{
		"sharded": false,
		"tables": {
			"test": {}
		}
	}
`

// TestChangeTypePrimaryCompletesWithBlockedCommit verifies that a primary
// tablet can leave serving while a COMMIT is blocked on a semi-sync ACK.
// SPARE stops the query service. REPLICA keeps it serving read-only, which is
// the transition that SetReplicationSource does on a demoted primary.
func TestChangeTypePrimaryCompletesWithBlockedCommit(t *testing.T) {
	if topoFlavor := *clusterInstance.TopoFlavorString(); topoFlavor != "etcd2" {
		t.Skipf("requires etcd2 topology, got %s", topoFlavor)
	}

	for _, tabletType := range []topodatapb.TabletType{
		topodatapb.TabletType_SPARE,
		topodatapb.TabletType_REPLICA,
	} {
		t.Run(tabletType.String(), func(t *testing.T) {
			testChangeTypePrimaryWithBlockedCommit(t, tabletType)
		})
	}
}

// testChangeTypePrimaryWithBlockedCommit starts a cluster, blocks a COMMIT on
// the primary on a semi-sync ACK, and changes the primary to tabletType. It
// checks that the change completes and that the shutdown grace period kills
// the COMMIT.
func testChangeTypePrimaryWithBlockedCommit(t *testing.T, tabletType topodatapb.TabletType) {
	localCluster := cluster.NewCluster(cell, hostname)
	t.Cleanup(localCluster.Teardown)

	// Raise the query and transaction timeouts. Otherwise they end the blocked
	// COMMIT before the ChangeType deadline, and the change completes without
	// the fix.
	localCluster.VtTabletExtraArgs = append(localCluster.VtTabletExtraArgs,
		"--queryserver-config-query-timeout", "10m",
		"--queryserver-config-transaction-timeout", "10m",
		"--shutdown-grace-period", "3s",
	)

	err := localCluster.StartTopo()
	require.NoError(t, err)

	keyspace := cluster.Keyspace{
		Name:             "ks",
		SchemaSQL:        blockedCommitSchema,
		VSchema:          blockedCommitVSchema,
		DurabilityPolicy: policy.DurabilitySemiSync,
	}

	err = localCluster.StartUnshardedKeyspace(keyspace, 1, false, localCluster.Cell)
	require.NoError(t, err)

	err = localCluster.StartVtgate()
	require.NoError(t, err)

	ctx := t.Context()

	conn, err := mysql.Connect(ctx, &mysql.ConnParams{
		Host: localCluster.Hostname,
		Port: localCluster.VtgateMySQLPort,
	})
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	require.NotEmpty(t, localCluster.Keyspaces)
	require.NotEmpty(t, localCluster.Keyspaces[0].Shards)

	tablets := localCluster.Keyspaces[0].Shards[0].Vttablets
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

	// Issue the COMMIT in the background. It blocks on semi-sync until the
	// grace period of the `ChangeType` transition kills it.
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
	}, 30*time.Second, 100*time.Millisecond, "COMMIT never waited for a semi-sync ACK")

	oldPrimary, err := localCluster.VtctldClientProcess.GetTablet(primary.Alias)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	t.Cleanup(cancel)

	localTMClient := tmc.NewClient()
	t.Cleanup(localTMClient.Close)

	// Change the primary type. The transition drains active queries, including
	// the COMMIT.
	err = localTMClient.ChangeType(ctx, oldPrimary, tabletType, false)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(commitErr) > 0
	}, 30*time.Second, 100*time.Millisecond, "timed out waiting for COMMIT to return after ChangeType")

	err = <-commitErr
	require.ErrorContains(t, err, "code = Canceled")
	require.ErrorContains(t, err, "QueryList.TerminateAll()")
	require.ErrorContains(t, err, "COMMIT was killed and its outcome is unknown")
}
