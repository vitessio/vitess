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

package reference

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
	cell                  = "zone1"
	hostname              = "localhost"
	unshardedKeyspaceName = "uks"
	unshardedSQLSchema    = `
		CREATE TABLE zip(
			id BIGINT NOT NULL AUTO_INCREMENT,
			code5 INT(5) NOT NULL,
			PRIMARY KEY(id)
		) ENGINE=InnoDB;

		INSERT INTO zip(id, code5)
		VALUES (1, 47107),
			   (2, 82845),
			   (3, 11237);

		CREATE TABLE zip_detail(
			id BIGINT NOT NULL AUTO_INCREMENT,
			zip_id BIGINT NOT NULL,
			discontinued_at DATE,
			PRIMARY KEY(id)
		) ENGINE=InnoDB;

		INSERT INTO zip_detail(id, zip_id, discontinued_at)
		VALUES (1, 1, '2022-05-13'),
			   (2, 2, '2022-08-15');
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
		CREATE TABLE zip_detail(
			id BIGINT NOT NULL AUTO_INCREMENT,
			zip_id BIGINT NOT NULL,
			discontinued_at DATE,
			PRIMARY KEY(id)
		) ENGINE=InnoDB;

		CREATE TABLE delivery_failure (
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
		if err := clusterInstance.StartUnshardedKeyspace(*uKeyspace, 1, false); err != nil {
			return 1
		}

		sKeyspace := &cluster.Keyspace{
			Name:      shardedKeyspaceName,
			SchemaSQL: shardedSQLSchema,
			VSchema:   shardedVSchema,
		}
		if err := clusterInstance.StartKeyspace(*sKeyspace, []string{"-80", "80-"}, 1, false); err != nil {
			return 1
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}

		// Materialize zip_detail to sharded keyspace.
		if err := clusterInstance.VtctlProcess.ExecuteCommand(
			"Materialize",
			`{
				"workflow": "materialize_zip_detail",
				"source_keyspace": "`+unshardedKeyspaceName+`",
				"target_keyspace": "`+shardedKeyspaceName+`",
				"table_settings": [
					{
						"target_table": "zip_detail",
						"source_expression": "select * from zip_detail"
					}
				],
				"stop_after_copy": true
			}`,
		); err != nil {
			return 1
		}

		ctx := context.Background()
		vtgateAddr := fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateProcess.GrpcPort)
		vtgateConn, err := vtgateconn.Dial(ctx, vtgateAddr)
		if err != nil {
			return 1
		}

		// INSERT some delivery_failure rows.
		session := vtgateConn.Session("@primary", nil)
		if _, err := session.Execute(ctx, `
			INSERT INTO delivery_failure(id, zip_detail_id, reason)
			VALUES (1, 1, 'Failed delivery due to discontinued zipcode.'),
			       (2, 2, 'Failed delivery due to discontinued zipcode.'),
			       (3, 3, 'Failed delivery due to unknown reason.');
		`, map[string]*querypb.BindVariable{}); err != nil {
			return 1
		}

		// TODO(maxeng) remove when we have a proper way to check
		// materialization lag and cutover.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		reader, err := vtgateConn.VStream(
			ctx,
			topodatapb.TabletType_PRIMARY,
			&binlogdatapb.VGtid{
				ShardGtids: []*binlogdatapb.ShardGtid{
					{
						Keyspace: shardedKeyspaceName,
						Shard:    "-80",
						Gtid:     "",
					},
					{
						Keyspace: shardedKeyspaceName,
						Shard:    "80-",
						Gtid:     "",
					},
				},
			},
			&binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{
					{
						Match:  "zip_detail",
						Filter: "select * from zip_detail",
					},
				},
			},
			&vtgatepb.VStreamFlags{},
		)
		if err != nil {
			return 1
		}
		expectRowChanges := 4
		for {
			evs, err := reader.Recv()
			switch err {
			case nil:
				for _, ev := range evs {
					if ev.Type != binlogdatapb.VEventType_ROW {
						continue
					}
					expectRowChanges = expectRowChanges - len(ev.RowEvent.RowChanges)
					if expectRowChanges <= 0 {
						goto MATERIALIZED
					}
				}
			case io.EOF:
				cancel()
			}
		}
	MATERIALIZED:
		vtgateConn.Close()

		deleted := false
		for !deleted {
			// Stop materialize zip_detail to sharded keyspace.
			if err := clusterInstance.VtctlProcess.ExecuteCommand(
				"Workflow",
				"--",
				shardedKeyspaceName+".copy_zip_detail",
				"delete",
			); err != nil {
				return 1
			}

			// Verify workflow is deleted.
			err := clusterInstance.VtctlProcess.ExecuteCommand(
				"Workflow",
				"--",
				shardedKeyspaceName+".copy_zip_detail",
				"show",
			)
			if err != nil {
				deleted = true
			}
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

// TestGlobalReferenceRouting tests that unqualified queries for reference
// tables go to the right place.
//
//		Given:
//		* Unsharded keyspace `uks` and sharded keyspace `sks`.
//		* Source table `uks.zip_detail` and a reference table `sks.zip_detail`,
//	   initially with the same rows.
//		* Unsharded table `uks.zip` and sharded table `sks.delivery_failure`.
//
//		When: we execute `INSERT INTO zip_detail ...`,
//		Then: `zip_detail` should be routed to `uks`.
//
//		When: we execute `UPDATE zip_detail ...`,
//		Then: `zip_detail` should be routed to `uks`.
//
//		When: we execute `SELECT ... FROM zip JOIN zip_detail ...`,
//		Then: `zip_detail` should be routed to `uks`.
//
//		When: we execute `SELECT ... FROM delivery_failure JOIN zip_detail ...`,
//		Then: `zip_detail` should be routed to `sks`.
//
//		When: we execute `DELETE FROM zip_detail ...`,
//		Then: `zip_detail` should be routed to `uks`.
func TestReferenceRouting(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	// INSERT should route an unqualified zip_detail to unsharded keyspace.
	utils.Exec(t, conn,
		"INSERT INTO zip_detail(id, zip_id, discontinued_at) VALUES(3, 1, DATE('2022-12-03'))")
	// Verify with qualified zip_detail queries to each keyspace. The unsharded
	// keyspace should have an extra row.
	uqr := utils.Exec(t, conn,
		"SELECT COUNT(zd.id) FROM "+unshardedKeyspaceName+".zip_detail zd WHERE id = 3")
	if got, want := fmt.Sprintf("%v", uqr.Rows), `[[INT64(1)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}
	sqr := utils.Exec(t, conn,
		"SELECT COUNT(zd.id) FROM "+shardedKeyspaceName+".zip_detail zd WHERE id = 3")
	if got, want := fmt.Sprintf("%v", sqr.Rows), `[[INT64(0)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}

	// UPDATE should route an unqualified zip_detail to unsharded keyspace.
	utils.Exec(t, conn,
		"UPDATE zip_detail SET discontinued_at = NULL WHERE id = 2")
	// Verify with qualified zip_detail queries to each keyspace. The unsharded
	// keyspace should have a matching row, but not the sharded keyspace.
	uqr = utils.Exec(t, conn,
		"SELECT COUNT(id) FROM "+unshardedKeyspaceName+".zip_detail WHERE discontinued_at IS NULL")
	if got, want := fmt.Sprintf("%v", uqr.Rows), `[[INT64(1)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}
	sqr = utils.Exec(t, conn,
		"SELECT COUNT(id) FROM "+shardedKeyspaceName+".zip_detail WHERE discontinued_at IS NULL")
	if got, want := fmt.Sprintf("%v", sqr.Rows), `[[INT64(0)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}

	// SELECT a table in unsharded keyspace and JOIN unqualified zip_detail.
	qr := utils.Exec(t, conn,
		"SELECT COUNT(zd.id) FROM zip z JOIN zip_detail zd ON z.id = zd.zip_id WHERE zd.id = 3")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}

	// SELECT a table in sharded keyspace and JOIN unqualified zip_detail.
	// Use gen4 planner to avoid errors from gen3 planner.
	qr = utils.Exec(t, conn,
		`SELECT /*vt+ PLANNER=gen4 */ COUNT(zd.id)
		 FROM delivery_failure df
		 JOIN zip_detail zd ON zd.id = df.zip_detail_id WHERE zd.id = 3`)
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(0)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}

	// DELETE should route an unqualified zip_detail to unsharded keyspace.
	utils.Exec(t, conn,
		"DELETE FROM zip_detail")
	// Verify with qualified zip_detail queries to each keyspace. The unsharded
	// keyspace should not have any rows; the sharded keyspace should.
	uqr = utils.Exec(t, conn,
		"SELECT COUNT(id) FROM "+unshardedKeyspaceName+".zip_detail")
	if got, want := fmt.Sprintf("%v", uqr.Rows), `[[INT64(0)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}
	sqr = utils.Exec(t, conn,
		"SELECT COUNT(id) FROM "+shardedKeyspaceName+".zip_detail")
	if got, want := fmt.Sprintf("%v", sqr.Rows), `[[INT64(2)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}
}
