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

package recovery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

func VerifyQueriesUsingVtgate(t *testing.T, session *vtgateconn.VTGateSession, query string, value string) {
	qr, err := session.Execute(context.Background(), query, nil)
	assert.Nil(t, err)
	assert.Equal(t, value, fmt.Sprintf("%v", qr.Rows[0][0]))
}

func RestoreTablet(t *testing.T, localCluster *cluster.LocalProcessCluster, tablet *cluster.Vttablet, restoreKSName string, shardName string, keyspaceName string, commonTabletArg []string) {
	tablet.ValidateTabletRestart(t)
	tm := time.Now().UTC()
	tm.Format(time.RFC3339)
	_, err := localCluster.VtctlProcess.ExecuteCommandWithOutput("CreateKeyspace",
		"-keyspace_type=SNAPSHOT", "-base_keyspace="+keyspaceName,
		"-snapshot_time", tm.Format(time.RFC3339), restoreKSName)
	assert.Nil(t, err)

	replicaTabletArgs := commonTabletArg
	replicaTabletArgs = append(replicaTabletArgs, "-disable_active_reparents",
		"-enable_replication_reporter=false",
		"-init_tablet_type", "replica",
		"-init_keyspace", restoreKSName,
		"-init_shard", shardName)
	tablet.VttabletProcess.SupportsBackup = true
	tablet.VttabletProcess.ExtraArgs = replicaTabletArgs

	tablet.VttabletProcess.ServingStatus = ""
	err = tablet.VttabletProcess.Setup()
	assert.Nil(t, err)

	err = tablet.VttabletProcess.WaitForTabletTypesForTimeout([]string{"SERVING"}, 20*time.Second)
	assert.Nil(t, err)
}

func InsertData(t *testing.T, tablet *cluster.Vttablet, index int, keyspaceName string) {
	_, err := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("insert into vt_insert_test (id, msg) values (%d, 'test %d')", index, index), keyspaceName, true)
	assert.Nil(t, err)
}
