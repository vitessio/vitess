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
	"context"
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestTopoCustomRule(t *testing.T) {

	defer cluster.PanicHandler(t)
	ctx := context.Background()
	masterConn, err := mysql.Connect(ctx, &masterTabletParams)
	require.NoError(t, err)
	defer masterConn.Close()
	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	require.NoError(t, err)
	defer replicaConn.Close()

	// Insert data for sanity checks
	exec(t, masterConn, "delete from t1")
	exec(t, masterConn, "insert into t1(id, value) values(11,'r'), (12,'s')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("r")] [VARCHAR("s")]]`)

	// create empty topoCustomRuleFile.
	topoCustomRuleFile := "/tmp/rules.json"
	topoCustomRulePath := "/keyspaces/ks/configs/CustomRules"
	data := []byte("[]\n")
	err = ioutil.WriteFile(topoCustomRuleFile, data, 0777)
	require.NoError(t, err)

	// Copy config file into topo.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("TopoCp", "-to_topo", topoCustomRuleFile, topoCustomRulePath)
	require.Nil(t, err, "error should be Nil")

	// Set extra tablet args for topo custom rule
	clusterInstance.VtTabletExtraArgs = []string{
		"-topocustomrule_path", topoCustomRulePath,
	}

	// Start a new Tablet
	rTablet := clusterInstance.NewVttabletInstance("replica", 0, "")

	// Start Mysql Processes
	err = cluster.StartMySQL(ctx, rTablet, username, clusterInstance.TmpDirectory)
	require.Nil(t, err, "error should be Nil")

	// Start Vttablet
	err = clusterInstance.StartVttablet(rTablet, "SERVING", false, cell, keyspaceName, hostname, shardName)
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
	require.Nil(t, err, "error should be Nil")

	// Verify that query is working
	result, err := vtctlExec("select id, value from t1", rTablet.Alias)
	require.NoError(t, err)
	resultMap := make(map[string]interface{})
	err = json.Unmarshal([]byte(result), &resultMap)
	require.NoError(t, err)

	rowsAffected := resultMap["rows"].([]interface{})
	assert.EqualValues(t, 2, len(rowsAffected))

	// Now update the topocustomrule file.
	data = []byte(`[{
		"Name": "rule1",
		"Description": "disallow select on table t1",
		"TableNames" : ["t1"],
		"Query" : "(select)|(SELECT)"
	  }]`)
	err = ioutil.WriteFile(topoCustomRuleFile, data, 0777)
	require.NoError(t, err)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("TopoCp", "-to_topo", topoCustomRuleFile, topoCustomRulePath)
	require.Nil(t, err, "error should be Nil")

	// And wait until the query fails with the right error.
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		result, err := vtctlExec("select id, value from t1", rTablet.Alias)
		if err != nil {
			assert.Contains(t, result, "disallow select on table t1")
			break
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Empty the table
	exec(t, masterConn, "delete from t1")
	// Reset the VtTabletExtraArgs
	clusterInstance.VtTabletExtraArgs = []string{}
	// Tear down custom processes
	killTablets(t, rTablet)
}

func vtctlExec(sql string, tabletAlias string) (string, error) {
	args := []string{"VtTabletExecute", "-json", tabletAlias, sql}
	return clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(args...)
}
