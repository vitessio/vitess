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

package loadkeyspace

import (
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
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

		create table test_table (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;
`
)

func TestLoadKeyspaceWithNoTablet(t *testing.T) {
	defer cluster.PanicHandler(t)
	var err error

	clusterInstance = cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	// Start topo server
	err = clusterInstance.StartTopo()
	require.NoError(t, err)

	// create keyspace
	keyspace := &cluster.Keyspace{
		Name:      keyspaceName,
		SchemaSQL: sqlSchema,
	}
	clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-schema-change-signal"}
	err = clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false)
	require.NoError(t, err)

	// teardown vttablets
	for _, vttablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
		err = vttablet.VttabletProcess.TearDown()
		require.NoError(t, err)
		utils.TimeoutAction(t, 1*time.Minute, "timeout - teardown of VTTablet", func() bool {
			return vttablet.VttabletProcess.GetStatus() == ""
		})
	}

	// Start vtgate with the schema_change_signal flag
	clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}
	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	// After starting VTGate we need to leave enough time for resolveAndLoadKeyspace to reach
	// the schema tracking timeout (5 seconds).
	utils.TimeoutAction(t, 5*time.Minute, "timeout - could not find 'Unable to get initial schema reload' in 'vtgate-stderr.txt'", func() bool {
		logDir := clusterInstance.VtgateProcess.LogDir
		all, _ := os.ReadFile(path.Join(logDir, "vtgate-stderr.txt"))
		return strings.Contains(string(all), "Unable to get initial schema reload")
	})
}

func TestNoInitialKeyspace(t *testing.T) {
	defer cluster.PanicHandler(t)
	var err error

	clusterInstance = cluster.NewCluster(cell, hostname)
	defer clusterInstance.Teardown()

	// Start topo server
	err = clusterInstance.StartTopo()
	require.NoError(t, err)

	// Start vtgate with the schema_change_signal flag
	clusterInstance.VtGateExtraArgs = []string{"--schema_change_signal"}
	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	logDir := clusterInstance.VtgateProcess.LogDir

	// teardown vtgate to flush logs
	err = clusterInstance.VtgateProcess.TearDown()
	require.NoError(t, err)

	// check info logs
	all, err := os.ReadFile(path.Join(logDir, "vtgate.INFO"))
	require.NoError(t, err)
	require.Contains(t, string(all), "No keyspace to load")
}
