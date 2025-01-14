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

package vschema

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	configFile      string
	vtParams        mysql.ConnParams
	hostname        = "localhost"
	keyspaceName    = "ks"
	cell            = "zone1"
	sqlSchema       = `
		create table vt_user (
			id bigint,
			name varchar(64),
			primary key (id)
		) Engine=InnoDB;
			
		create table main (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;
`
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// List of users authorized to execute vschema ddl operations
		if utils.BinaryIsAtLeastAtVersion(22, "vtgate") {
			timeNow := time.Now().Unix()
			configFile = path.Join(os.TempDir(), fmt.Sprintf("vtgate-config-%d.json", timeNow))
			err := writeConfig(configFile, map[string]string{
				"vschema_ddl_authorized_users": "%",
			})
			if err != nil {
				return 1, err
			}
			defer os.Remove(configFile)

			clusterInstance.VtGateExtraArgs = []string{fmt.Sprintf("--config-file=%s", configFile), "--schema_change_signal=false"}
		} else {
			clusterInstance.VtGateExtraArgs = []string{"--vschema_ddl_authorized_users=%", "--schema_change_signal=false"}
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false); err != nil {
			return 1, err
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1, err
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}

}

func writeConfig(path string, cfg map[string]string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewEncoder(file).Encode(cfg)
}

func TestVSchema(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Test the empty database with no vschema
	utils.Exec(t, conn, "insert into vt_user (id,name) values(1,'test1'), (2,'test2'), (3,'test3'), (4,'test4')")

	utils.AssertMatches(t, conn, "select id, name from vt_user order by id",
		`[[INT64(1) VARCHAR("test1")] [INT64(2) VARCHAR("test2")] [INT64(3) VARCHAR("test3")] [INT64(4) VARCHAR("test4")]]`)

	utils.AssertMatches(t, conn, "delete from vt_user", `[]`)
	utils.AssertMatches(t, conn, "SHOW VSCHEMA TABLES", `[]`)

	// Use the DDL to create an unsharded vschema and test again

	// Create VSchema and do a Select to force update VSCHEMA
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "ALTER VSCHEMA ADD TABLE vt_user")
	utils.Exec(t, conn, "select * from  vt_user")
	utils.Exec(t, conn, "commit")

	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "ALTER VSCHEMA ADD TABLE main")
	utils.Exec(t, conn, "select * from  main")
	utils.Exec(t, conn, "commit")

	// Test Showing Tables
	utils.AssertMatches(t, conn, "SHOW VSCHEMA TABLES", `[[VARCHAR("main")] [VARCHAR("vt_user")]]`)

	// Test Showing Vindexes
	utils.AssertMatches(t, conn, "SHOW VSCHEMA VINDEXES", `[]`)

	// Test DML operations
	utils.Exec(t, conn, "insert into vt_user (id,name) values(1,'test1'), (2,'test2'), (3,'test3'), (4,'test4')")
	utils.AssertMatches(t, conn, "select id, name from vt_user order by id",
		`[[INT64(1) VARCHAR("test1")] [INT64(2) VARCHAR("test2")] [INT64(3) VARCHAR("test3")] [INT64(4) VARCHAR("test4")]]`)

	utils.AssertMatches(t, conn, "delete from vt_user", `[]`)

	if utils.BinaryIsAtLeastAtVersion(22, "vtgate") {
		writeConfig(configFile, map[string]string{
			"vschema_ddl_authorized_users": "",
		})

		require.EventuallyWithT(t, func(t *assert.CollectT) {
			_, err = conn.ExecuteFetch("ALTER VSCHEMA DROP TABLE main", 1000, false)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "is not authorized to perform vschema operations")
		}, 5*time.Second, 100*time.Millisecond)
	}
}

// TestVSchemaSQLAPIConcurrency tests that we prevent lost writes when we have
// concurrent vschema changes being made via the SQL API.
func TestVSchemaSQLAPIConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.AssertMatches(t, conn, "SHOW VSCHEMA TABLES", `[]`)
	baseTableName := "t"
	numTables := 1000
	mysqlConns := make([]*mysql.Conn, numTables)
	for i := 0; i < numTables; i++ {
		c, err := mysql.Connect(ctx, &vtParams)
		require.NoError(t, err)
		mysqlConns[i] = c
		defer c.Close()
	}

	wg := sync.WaitGroup{}
	preventedLostWrites := false
	for i := 0; i < numTables; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(1000) * int(time.Nanosecond)))
			tableName := fmt.Sprintf("%s%d", baseTableName, i)
			_, err = mysqlConns[i].ExecuteFetch(fmt.Sprintf("ALTER VSCHEMA ADD TABLE %s", tableName), -1, false)
			if err != nil {
				// The error we get is an SQL error so we have to do string matching.
				if err != nil && strings.Contains(err.Error(), "failed to update vschema as the session's version was stale") {
					preventedLostWrites = true
				}
			} else {
				require.NoError(t, err)
				time.Sleep(time.Duration(rand.Intn(1000) * int(time.Nanosecond)))
				_, err = mysqlConns[i].ExecuteFetch(fmt.Sprintf("ALTER VSCHEMA DROP TABLE %s", tableName), -1, false)
				// The error we get is an SQL error so we have to do string matching.
				if err != nil && strings.Contains(err.Error(), "failed to update vschema as the session's version was stale") {
					preventedLostWrites = true
				} else {
					require.NoError(t, err)
				}
			}
		}()
	}
	wg.Wait()
	require.True(t, preventedLostWrites)

	// Cleanup any tables that were not dropped because the DROP query
	// failed due to a bad node version.
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("%s%d", baseTableName, i)
		_, _ = mysqlConns[i].ExecuteFetch(fmt.Sprintf("ALTER VSCHEMA DROP TABLE %s", tableName), -1, false)
	}
	utils.AssertMatches(t, conn, "SHOW VSCHEMA TABLES", `[]`)
}
