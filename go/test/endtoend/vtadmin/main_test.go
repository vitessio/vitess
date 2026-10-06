/*
Copyright 2024 The Vitess Authors.

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

package vtadmin

import (
	_ "embed"
	"flag"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	uks             = "uks"
	cell            = "test_misc"

	uschemaSQL = `
create table u_a
(
    id bigint,
    a  bigint,
    primary key (id)
) Engine = InnoDB;

create table u_b
(
    id bigint,
    b  varchar(50),
    primary key (id)
) Engine = InnoDB;

`
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

		// Start Unsharded keyspace
		ukeyspace := &cluster.Keyspace{
			Name:      uks,
			SchemaSQL: uschemaSQL,
		}
		err = clusterInstance.StartUnshardedKeyspace(*ukeyspace, 0, false, clusterInstance.Cell)
		if err != nil {
			return 1
		}

		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		clusterInstance.NewVTAdminProcess()
		// Override the default cluster ID to include an underscore to
		// exercise the gRPC name resolver handling of non-RFC3986 schemes.
		clusterInstance.VtadminProcess.ClusterID = "local_test"
		err = clusterInstance.VtadminProcess.Setup()
		if err != nil {
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

// TestVtadminAPIs tests the vtadmin APIs.
func TestVtadminAPIs(t *testing.T) {
	// Test the vtadmin APIs
	t.Run("keyspaces api", func(t *testing.T) {
		resp := clusterInstance.VtadminProcess.MakeAPICallRetry(t, "api/keyspaces")
		require.Contains(t, resp, uks)
	})

	// VExplain is authorized as a read, so a VEXPLAIN that runs the statement it
	// explains must not change data, even through a stored function that runs with
	// its definer's privileges.
	t.Run("vexplain cannot write through a definer function", func(t *testing.T) {
		tablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet().VttabletProcess
		for _, query := range []string{
			"insert into u_a (id, a) values (1, 1)",
			"create table vexplain_audit (id bigint auto_increment primary key)",
			"create function vexplain_write() returns int deterministic modifies sql data sql security definer begin insert into vexplain_audit () values (); return 1; end",
		} {
			_, err := tablet.QueryTablet(query, uks, true)
			require.NoError(t, err, query)
		}

		for _, sql := range []string{
			"vexplain trace select vexplain_write() from u_a where id = 1",
			"vexplain queries select vexplain_write() from u_a where id = 1",
			// ALL also has MySQL EXPLAIN the query, which materializes a
			// non-mergeable derived table and so executes the function again.
			"vexplain all select * from (select vexplain_write() as w from u_a where id = 1 limit 1) as d",
			"vexplain mysqlplan select vexplain_write() from u_a where id = 1",
		} {
			params := url.Values{
				"cluster_id": {clusterInstance.VtadminProcess.ClusterID},
				"keyspace":   {uks},
				"sql":        {sql},
			}
			_, _, err := clusterInstance.VtadminProcess.MakeAPICall("api/vexplain?" + params.Encode())
			require.NoError(t, err)

			qr, err := tablet.QueryTablet("select count(*) from vexplain_audit", uks, true)
			require.NoError(t, err)
			assert.Equal(t, "0", qr.Rows[0][0].ToString(), "the definer function must not have written: %s", sql)
		}
	})
}
