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

package onlineddl

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	gosqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttest"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var (
	localCluster *vttest.LocalCluster
	mysqlAddress string
)

const keyspaceName = "ks"

const tableSchema = `
CREATE TABLE test_table (
  id BIGINT UNSIGNED NOT NULL,
  msg VARCHAR(64),
  keyspace_id BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB
`

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		topology := &vttestpb.VTTestTopology{
			Keyspaces: []*vttestpb.Keyspace{
				{
					Name: keyspaceName,
					Shards: []*vttestpb.Shard{
						{Name: "-80"},
						{Name: "80-"},
					},
				},
			},
		}

		vschema := &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"hash": {Type: "hash"},
			},
			Tables: map[string]*vschemapb.Table{
				"test_table": {
					ColumnVindexes: []*vschemapb.ColumnVindex{
						{Column: "keyspace_id", Name: "hash"},
					},
				},
			},
		}

		var cfg vttest.Config
		cfg.Topology = topology
		cfg.EnableOnlineDDL = true
		cfg.PerShardSidecar = true
		cfg.MigrationCheckInterval = 5 * time.Second

		if err := cfg.InitSchemas(keyspaceName, tableSchema, vschema); err != nil {
			return 1, err
		}
		defer os.RemoveAll(cfg.SchemaDir)

		localCluster = &vttest.LocalCluster{Config: cfg}
		if err := localCluster.Setup(); err != nil {
			return 1, err
		}
		defer localCluster.TearDown()

		mysqlAddress = fmt.Sprintf("localhost:%d", localCluster.Env.PortForProtocol("vtcombo_mysql_port", ""))

		return m.Run(), nil
	}()
	if err != nil {
		log.Errorf("top level error: %v\n", err)
		os.Exit(1)
	}
	os.Exit(exitcode)
}

func mysqlConn(t *testing.T) *sql.DB {
	t.Helper()
	cfg := gosqldriver.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = mysqlAddress
	cfg.DBName = keyspaceName + "@primary"
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestOnlineDDLSharded(t *testing.T) {
	ctx := context.Background()
	db := mysqlConn(t)

	// Insert a row to prove the table is working across shards.
	_, err := db.ExecContext(ctx, "INSERT INTO test_table (id, msg, keyspace_id) VALUES (1, 'hello', 1)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO test_table (id, msg, keyspace_id) VALUES (2, 'world', 2)")
	require.NoError(t, err)

	// Submit an online DDL to add a column.
	_, err = db.ExecContext(ctx, "SET @@ddl_strategy='online'")
	require.NoError(t, err)

	result, err := db.QueryContext(ctx, "ALTER TABLE test_table ADD COLUMN extra VARCHAR(64)")
	require.NoError(t, err)
	defer result.Close()

	// The ALTER returns a UUID for the migration.
	require.True(t, result.Next())
	var uuid string
	require.NoError(t, result.Scan(&uuid))
	t.Logf("migration UUID: %s", uuid)
	require.NotEmpty(t, uuid)

	// Wait for the migration to complete on all shards.
	require.Eventually(t, func() bool {
		rows, err := db.QueryContext(ctx, fmt.Sprintf("SHOW VITESS_MIGRATIONS LIKE '%s'", uuid))
		if err != nil {
			return false
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		statusIdx := -1
		for i, c := range cols {
			if c == "migration_status" {
				statusIdx = i
				break
			}
		}
		if statusIdx < 0 {
			return false
		}

		allComplete := true
		count := 0
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return false
			}
			count++
			status := fmt.Sprintf("%s", vals[statusIdx])
			if !strings.EqualFold(status, "complete") {
				allComplete = false
			}
		}
		// Expect one migration row per shard.
		return count == 2 && allComplete
	}, 2*time.Minute, 2*time.Second, "migration %s did not complete on all shards", uuid)

	// Verify the column was added by inserting a row that uses it.
	_, err = db.ExecContext(ctx, "INSERT INTO test_table (id, msg, keyspace_id, extra) VALUES (3, 'test', 3, 'new_col_value')")
	require.NoError(t, err)

	// Verify we can read back the new column.
	var extra string
	err = db.QueryRowContext(ctx, "SELECT extra FROM test_table WHERE id = 3").Scan(&extra)
	require.NoError(t, err)
	assert.Equal(t, "new_col_value", extra)
}
