/*
Copyright 2021 The Vitess Authors.

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

package schematracker

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	hostname        = "localhost"
	keyspaceName    = "ks"
	cell            = "zone1"
	signalInterval  = 1
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

		create table test_table (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;
`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// List of users authorized to execute vschema ddl operations
		clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
			return 1
		}

		// restart the tablet so that the schema.Engine gets a chance to start with existing schema
		tablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
		tablet.VttabletProcess.ExtraArgs = []string{
			"--queryserver-config-schema-change-signal",
			fmt.Sprintf("--queryserver-config-schema-change-signal-interval=%d", signalInterval),
		}
		if err := tablet.RestartOnlyTablet(); err != nil {
			return 1
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			clusterInstance.VtgateProcess = cluster.VtgateProcess{}
			return 1
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitcode)
}

func TestVSchemaTrackerInit(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := utils.Exec(t, conn, "SHOW VSCHEMA TABLES")
	got := fmt.Sprintf("%v", qr.Rows)
	want := `[[VARCHAR("dual")] [VARCHAR("main")] [VARCHAR("test_table")] [VARCHAR("vt_user")]]`
	assert.Equal(t, want, got)
}

// TestVSchemaTrackerKeyspaceReInit tests that the vschema tracker
// properly handles primary tablet restarts -- meaning that we maintain
// the exact same vschema state as before the restart.
func TestVSchemaTrackerKeyspaceReInit(t *testing.T) {
	defer cluster.PanicHandler(t)

	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()

	// get the vschema prior to the restarts
	var originalResults any
	readVSchema(t, &clusterInstance.VtgateProcess, &originalResults)
	assert.NotNil(t, originalResults)

	// restart the primary tablet so that the vschema gets reloaded for the keyspace
	for i := 0; i < 5; i++ {
		err := primaryTablet.VttabletProcess.TearDownWithTimeout(30 * time.Second)
		require.NoError(t, err)
		err = primaryTablet.VttabletProcess.Setup()
		require.NoError(t, err)
		err = clusterInstance.WaitForTabletsToHealthyInVtgate()
		require.NoError(t, err)
		time.Sleep(time.Duration(signalInterval*2) * time.Second)
		var newResults any
		readVSchema(t, &clusterInstance.VtgateProcess, &newResults)
		assert.Equal(t, originalResults, newResults)
		newResults = nil
	}
}

func readVSchema(t *testing.T, vtgate *cluster.VtgateProcess, results *any) {
	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get(vtgate.VSchemaURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
	json.NewDecoder(resp.Body).Decode(results)
}
