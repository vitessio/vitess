/*
Copyright 2022 The Vitess Authors.

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

package reference

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	cell            = "zone1"
	hostname        = "localhost"
	vtParams        mysql.ConnParams

	unshardedKeyspaceName = "uks"
	unshardedSQLSchema    = `
		CREATE TABLE IF NOT EXISTS zip(
			id BIGINT NOT NULL AUTO_INCREMENT,
			code5 INT(5) NOT NULL,
			PRIMARY KEY(id)
		) ENGINE=InnoDB;

		INSERT INTO zip(id, code5)
		VALUES (1, 47107),
			   (2, 82845),
			   (3, 11237);

		CREATE TABLE IF NOT EXISTS zip_detail(
			id BIGINT NOT NULL AUTO_INCREMENT,
			zip_id BIGINT NOT NULL,
			discontinued_at DATE,
			PRIMARY KEY(id)
		) ENGINE=InnoDB;

	`
	unshardedVSchema = `
		{
			"sharded":false,
			"tables": {
				"zip": {},
				"zip_detail": {}
			}
		}
	`
	shardedKeyspaceName = "sks"
	shardedSQLSchema    = `
		CREATE TABLE IF NOT EXISTS delivery_failure (
			id BIGINT NOT NULL,
			zip_detail_id BIGINT NOT NULL,
			reason VARCHAR(255),
			PRIMARY KEY(id)
		) ENGINE=InnoDB;
	`
	shardedVSchema = `
		{
			"sharded": true,
			"vindexes": {
				"hash": {
					"type": "hash"
				}
			},
			"tables": {
				"delivery_failure": {
					"columnVindexes": [
						{
							"column": "id",
							"name": "hash"
						}
					]
				},
				"zip_detail": {
					"type": "reference",
					"source": "` + unshardedKeyspaceName + `.zip_detail"
				}
			}
		}
	`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		uKeyspace := &cluster.Keyspace{
			Name:      unshardedKeyspaceName,
			SchemaSQL: unshardedSQLSchema,
			VSchema:   unshardedVSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*uKeyspace, 0, false); err != nil {
			return 1
		}

		sKeyspace := &cluster.Keyspace{
			Name:      shardedKeyspaceName,
			SchemaSQL: shardedSQLSchema,
			VSchema:   shardedVSchema,
		}
		if err := clusterInstance.StartKeyspace(*sKeyspace, []string{"-80", "80-"}, 0, false); err != nil {
			return 1
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}

		if err := clusterInstance.WaitForTabletsToHealthyInVtgate(); err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: "localhost",
			Port: clusterInstance.VtgateMySQLPort,
		}

		// TODO(maxeng) remove when we have a proper way to check
		// materialization lag and cutover.
		done := make(chan bool, 1)
		expectRows := 2
		go func() {
			ctx := context.Background()
			vtgateAddr := fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateProcess.GrpcPort)
			vtgateConn, err := vtgateconn.Dial(ctx, vtgateAddr)
			if err != nil {
				done <- false
				return
			}
			defer vtgateConn.Close()

			maxWait := time.After(300 * time.Second)
			for _, ks := range clusterInstance.Keyspaces {
				if ks.Name != shardedKeyspaceName {
					continue
				}
				for _, s := range ks.Shards {
					var ok bool
					for !ok {
						select {
						case <-maxWait:
							fmt.Println("Waited too long for materialization, cancelling.")
							done <- false
							return
						default:
						}
						shard := fmt.Sprintf("%s/%s@primary", ks.Name, s.Name)
						session := vtgateConn.Session(shard, nil)
						_, err := session.Execute(ctx, "SHOW CREATE TABLE zip_detail", map[string]*querypb.BindVariable{})
						if err != nil {
							fmt.Fprintf(os.Stderr, "Failed to SHOW CREATE TABLE zip_detail; might not exist yet: %v\n", err)
							time.Sleep(1 * time.Second)
							continue
						}
						qr, err := session.Execute(ctx, "SELECT * FROM zip_detail", map[string]*querypb.BindVariable{})
						if err != nil {
							fmt.Fprintf(os.Stderr, "Failed to query sharded keyspace for zip_detail rows: %v\n", err)
							done <- false
							return
						}
						if len(qr.Rows) != expectRows {
							fmt.Fprintf(os.Stderr, "Shard %s doesn't yet have expected number of zip_detail rows\n", shard)
							time.Sleep(10 * time.Second)
							continue
						}
						fmt.Fprintf(os.Stdout, "Shard %s has expected number of zip_detail rows.\n", shard)
						ok = true
					}
				}
				fmt.Println("All shards have expected number of zip_detail rows.")
				done <- true
			}
		}()

		// Materialize zip_detail to sharded keyspace.
		output, err := clusterInstance.VtctlProcess.ExecuteCommandWithOutput(
			"Materialize",
			"--",
			"--tablet_types",
			"PRIMARY",
			`{
				"workflow": "copy_zip_detail",
				"source_keyspace": "`+unshardedKeyspaceName+`",
				"target_keyspace": "`+shardedKeyspaceName+`",
				"tablet_types": "PRIMARY",
				"table_settings": [
					{
						"target_table": "zip_detail",
						"source_expression": "select * from zip_detail",
						"create_ddl": "copy"
					}
				]
			}`,
		)
		fmt.Fprintf(os.Stderr, "Output from materialize: %s\n", output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Got error trying to start materialize zip_detail: %v\n", err)
			return 1
		}

		ctx := context.Background()
		vtgateAddr := fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateProcess.GrpcPort)
		vtgateConn, err := vtgateconn.Dial(ctx, vtgateAddr)
		if err != nil {
			return 1
		}
		defer vtgateConn.Close()

		session := vtgateConn.Session("@primary", nil)
		// INSERT some zip_detail rows.
		if _, err := session.Execute(ctx, `
			INSERT INTO zip_detail(id, zip_id, discontinued_at)
			VALUES (1, 1, '2022-05-13'),
				   (2, 2, '2022-08-15')
		`, map[string]*querypb.BindVariable{}); err != nil {
			return 1
		}

		// INSERT some delivery_failure rows.
		if _, err := session.Execute(ctx, `
			INSERT INTO delivery_failure(id, zip_detail_id, reason)
			VALUES (1, 1, 'Failed delivery due to discontinued zipcode.'),
			       (2, 2, 'Failed delivery due to discontinued zipcode.'),
			       (3, 3, 'Failed delivery due to unknown reason.');
		`, map[string]*querypb.BindVariable{}); err != nil {
			return 1
		}

		if ok := <-done; !ok {
			fmt.Fprintf(os.Stderr, "Materialize did not succeed.\n")
			return 1
		}

		// Stop materialize zip_detail to sharded keyspace.
		err = clusterInstance.VtctlProcess.ExecuteCommand(
			"Workflow",
			"--",
			shardedKeyspaceName+".copy_zip_detail",
			"delete",
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to stop materialization workflow: %v", err)
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}
