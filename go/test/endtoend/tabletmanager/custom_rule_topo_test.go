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
package tabletmanager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

const (
	// topoCustomRuleEmptyFile and topoCustomRuleDenyFile hold the query rule
	// documents inside the vtctld container, where vtctldclient reads them.
	topoCustomRuleEmptyFile = "/vt/files/rules.json"
	topoCustomRuleDenyFile  = "/vt/files/rules_deny.json"

	topoCustomRulePath = "/keyspaces/ks/configs/CustomRules"

	emptyCustomRules = "[]\n"

	denySelectCustomRules = `[{
		"Name": "rule1",
		"Description": "disallow select on table t1",
		"TableNames" : ["t1"],
		"Query" : "(select)|(SELECT)"
	  }]`
)

func TestTopoCustomRule(t *testing.T) {
	setup(t)
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &primaryTabletParams)
	require.NoError(t, err)
	defer conn.Close()
	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	require.NoError(t, err)
	defer replicaConn.Close()

	// Insert data for sanity checks
	vitesst.Exec(t, conn, "delete from t1")
	vitesst.Exec(t, conn, "insert into t1(id, value) values(11,'r'), (12,'s')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("r")] [VARCHAR("s")]]`)

	// Copy the empty config file into topo.
	err = writeTopologyPath(t, topoCustomRulePath, topoCustomRuleEmptyFile)
	require.Nil(t, err, "error should be Nil")

	// Start a new Tablet with the topo custom rule
	rTablet, err := clusterInstance.AddTablet(ctx, cell, keyspaceName, shardName, "replica")
	require.Nil(t, err, "error should be Nil")

	err = rTablet.StopVttablet(ctx)
	require.Nil(t, err, "error should be Nil")
	err = rTablet.StartVttablet(
		ctx,
		"--topocustomrule-path", topoCustomRulePath,
	)
	require.Nil(t, err, "error should be Nil")
	err = rTablet.WaitForTabletStatus(ctx, vttabletStateTimeout, "SERVING")
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "Validate")
	require.Nil(t, err, "error should be Nil")

	// And wait until the query is working.
	// We need a wait here because the instance we have created is a replica
	// It might take a while to replicate the two rows.
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		qr, err := execOnTablet(t.Context(), rTablet, "select id, value from t1", nil, nil)
		if err == nil {
			if len(qr.Rows) == 2 {
				break
			}
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Now update the topocustomrule file.
	err = writeTopologyPath(t, topoCustomRulePath, topoCustomRuleDenyFile)
	require.Nil(t, err, "error should be Nil")

	// And wait until the query fails with the right error.
	timeout = time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		if _, err := execOnTablet(t.Context(), rTablet, "select id, value from t1", nil, nil); err != nil {
			assert.Contains(t, err.Error(), "disallow select on table t1")
			break
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Empty the table
	vitesst.Exec(t, conn, "delete from t1")
	// Tear down custom processes
	killTablets(ctx, rTablet)
}

// writeTopologyPath copies a file of the vtctld container to the given path in
// the topology server.
func writeTopologyPath(t *testing.T, path, file string) error {
	t.Helper()
	args := append([]string{"--server", "internal"}, clusterInstance.TopoFlags()...)
	args = append(args, "WriteTopologyPath", path, file)
	return clusterInstance.Vtctld().ExecuteCommand(t.Context(), args...)
}
