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
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/vtgate"
)

// vtgateConfigPath is the config file each vtgate watches inside its
// container. It is staged world-writable so setAuthorizedDDLUsers can rewrite
// it as the unprivileged container user.
const vtgateConfigPath = "/vt/files/vtgate.json"

var (
	keyspaceName = "ks"
	sqlSchema    = `
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

func setup(t *testing.T) (*vitesst.Cluster, mysql.ConnParams) {
	t.Helper()

	ctx := t.Context()
	cluster, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace(keyspaceName).
			WithSchema(sqlSchema),
		vitesst.WithVTGateArgs("--schema-change-signal=false"),
		vitesst.WithVTGateFiles(vitesst.ContainerFile{
			Content:       []byte("{}\n"),
			ContainerPath: vtgateConfigPath,
			Mode:          0o666,
		}),
	)
	require.NoError(t, err)
	cleanup, err := cluster.Start(t, ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		require.NoError(t, cleanup(cleanupCtx))
	})
	require.NoError(t, setAuthorizedDDLUsers(ctx, cluster, "%"))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		require.NoError(t, setAuthorizedDDLUsers(cleanupCtx, cluster, "%"))
	})

	return cluster, cluster.VTParams(ctx, "")
}

// setAuthorizedDDLUsers rewrites the vtgate config file with the given list of
// users authorized to execute vschema ddl operations, then waits for the
// running vtgate to hot-reload the new value.
func setAuthorizedDDLUsers(ctx context.Context, cluster *vitesst.Cluster, users string) error {
	g := cluster.VTGate()
	content := fmt.Sprintf("{%q:%q}\n", "vschema_ddl_authorized_users", users)
	if err := g.WriteConfig(ctx, content); err != nil {
		return err
	}
	_, _, err := g.MakeAPICallRetry(ctx, "/debug/config?format=json", 30*time.Second, func(status int, body string) bool {
		if status != http.StatusOK {
			return false
		}
		var cfg map[string]any
		if err := json.Unmarshal([]byte(body), &cfg); err != nil {
			return false
		}
		value, _ := cfg["vschema_ddl_authorized_users"].(string)
		return value == users
	})
	return err
}

func TestVSchema(t *testing.T) {
	ctx := t.Context()
	clusterInstance, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Test the empty database with no vschema
	vitesst.Exec(t, conn, "insert into vt_user (id,name) values(1,'test1'), (2,'test2'), (3,'test3'), (4,'test4')")

	vitesst.AssertMatches(t, conn, "select id, name from vt_user order by id",
		`[[INT64(1) VARCHAR("test1")] [INT64(2) VARCHAR("test2")] [INT64(3) VARCHAR("test3")] [INT64(4) VARCHAR("test4")]]`)

	vitesst.AssertMatches(t, conn, "delete from vt_user", `[]`)
	vitesst.AssertMatches(t, conn, "SHOW VSCHEMA TABLES", `[]`)

	// Use the DDL to create an unsharded vschema and test again

	// Create VSchema and do a Select to force update VSCHEMA
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "ALTER VSCHEMA ADD TABLE vt_user")
	vitesst.Exec(t, conn, "select * from  vt_user")
	vitesst.Exec(t, conn, "commit")

	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "ALTER VSCHEMA ADD TABLE main")
	vitesst.Exec(t, conn, "select * from  main")
	vitesst.Exec(t, conn, "commit")

	// Test Showing Tables
	vitesst.AssertMatches(t, conn, "SHOW VSCHEMA TABLES", `[[VARCHAR("main")] [VARCHAR("vt_user")]]`)

	// Test Showing Vindexes
	vitesst.AssertMatches(t, conn, "SHOW VSCHEMA VINDEXES", `[]`)

	// Test DML operations
	vitesst.Exec(t, conn, "insert into vt_user (id,name) values(1,'test1'), (2,'test2'), (3,'test3'), (4,'test4')")
	vitesst.AssertMatches(t, conn, "select id, name from vt_user order by id",
		`[[INT64(1) VARCHAR("test1")] [INT64(2) VARCHAR("test2")] [INT64(3) VARCHAR("test3")] [INT64(4) VARCHAR("test4")]]`)

	vitesst.AssertMatches(t, conn, "delete from vt_user", `[]`)

	// Don't allow any users to modify the vschema via the SQL API
	// in order to test that behavior.
	require.NoError(t, setAuthorizedDDLUsers(ctx, clusterInstance, ""))
	// Allow anyone to modify the vschema via the SQL API again when
	// the test completes.
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		_, err = conn.ExecuteFetch("ALTER VSCHEMA DROP TABLE main", 1000, false)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "is not authorized to perform vschema operations")
	}, 5*time.Second, 100*time.Millisecond)
}

// TestVSchemaSQLAPIConcurrency tests that we prevent lost writes when we have
// concurrent vschema changes being made via the SQL API.
func TestVSchemaSQLAPIConcurrency(t *testing.T) {
	_, vtParams := setup(t)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	initialVSchema, err := conn.ExecuteFetch("SHOW VSCHEMA TABLES", -1, false)
	require.NoError(t, err)
	baseTableName := "t"
	numTables := 1000
	mysqlConns := make([]*mysql.Conn, numTables)
	for i := range numTables {
		c, err := mysql.Connect(ctx, &vtParams)
		require.NoError(t, err)
		mysqlConns[i] = c
		defer c.Close()
	}

	isVersionMismatchErr := func(err error) bool {
		// The error we get is an SQL error so we have to do string matching.
		return err != nil && strings.Contains(err.Error(), vtgate.ErrStaleVSchema.Error())
	}

	wg := sync.WaitGroup{}
	preventedLostWrites := atomic.Bool{}
	for i := range numTables {
		wg.Go(func() {
			time.Sleep(time.Duration(rand.IntN(100) * int(time.Nanosecond)))
			tableName := fmt.Sprintf("%s%d", baseTableName, i)
			_, err = mysqlConns[i].ExecuteFetch("ALTER VSCHEMA ADD TABLE "+tableName, -1, false)
			if isVersionMismatchErr(err) {
				preventedLostWrites.Store(true)
			} else {
				require.NoError(t, err)
				time.Sleep(time.Duration(rand.IntN(75) * int(time.Nanosecond)))
				_, err = mysqlConns[i].ExecuteFetch("ALTER VSCHEMA DROP TABLE "+tableName, -1, false)
				if isVersionMismatchErr(err) {
					preventedLostWrites.Store(true)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
	wg.Wait()
	require.True(t, preventedLostWrites.Load())

	// Cleanup any tables that were not dropped because the DROP query
	// failed due to a bad node version.
	for i := range numTables {
		tableName := fmt.Sprintf("%s%d", baseTableName, i)
		_, _ = mysqlConns[i].ExecuteFetch("ALTER VSCHEMA DROP TABLE "+tableName, -1, false)
	}
	// Confirm that we're back to the initial state.
	vitesst.AssertMatches(t, conn, "SHOW VSCHEMA TABLES", fmt.Sprintf("%v", initialVSchema.Rows))
}
