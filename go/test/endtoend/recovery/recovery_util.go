/*
Copyright 2020 The Vitess Authors.

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
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

var (
	dbPassword = "VtDbaPass"

	// UseXb flag to use extra backup for recovery teseting.
	UseXb = false
	// XbArgs are the arguments for specifying xtrabackup.
	XbArgs = []string{
		utils.GetFlagVariantForTests("--backup-engine-implementation"), "xtrabackup",
		utils.GetFlagVariantForTests("--xtrabackup-stream-mode") + "=xbstream",
		utils.GetFlagVariantForTests("--xtrabackup-user") + "=vt_dba",
		utils.GetFlagVariantForTests("--xtrabackup-backup-flags"), "--password=" + dbPassword,
	}
)

// VerifyQueriesUsingVtgate verifies queries using vtgate.
func VerifyQueriesUsingVtgate(t *testing.T, session *vtgateconn.VTGateSession, query string, value string) {
	qr, err := session.Execute(context.Background(), query, nil, false)
	require.Nil(t, err)
	assert.Equal(t, value, fmt.Sprintf("%v", qr.Rows[0][0]))
}

// RestoreTablet performs a PITR restore.
func RestoreTablet(t *testing.T, localCluster *cluster.LocalProcessCluster, tablet *cluster.Vttablet, restoreKSName string, shardName string, keyspaceName string, commonTabletArg []string, restoreTime time.Time) {
	tablet.ValidateTabletRestart(t)
	replicaTabletArgs := commonTabletArg

	_, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("GetKeyspace", restoreKSName)

	if restoreTime.IsZero() {
		restoreTime = time.Now().UTC()
	}

	if err != nil {
		_, err := localCluster.VtctldClientProcess.ExecuteCommandWithOutput("CreateKeyspace", restoreKSName,
			"--type=SNAPSHOT", "--base-keyspace="+keyspaceName,
			"--snapshot-timestamp", restoreTime.Format(time.RFC3339))
		require.Nil(t, err)
	}

	if UseXb {
		replicaTabletArgs = append(replicaTabletArgs, XbArgs...)
	}
	replicaTabletArgs = append(replicaTabletArgs,
		utils.GetFlagVariantForTests("--enable-replication-reporter")+"=false",
		utils.GetFlagVariantForTests("--init-tablet-type"), "replica",
		utils.GetFlagVariantForTests("--init-keyspace"), restoreKSName,
		utils.GetFlagVariantForTests("--init-shard"), shardName,
		utils.GetFlagVariantForTests("--init-db-name-override"), "vt_"+keyspaceName,
	)
	tablet.VttabletProcess.SupportsBackup = true
	tablet.VttabletProcess.ExtraArgs = replicaTabletArgs

	tablet.VttabletProcess.ServingStatus = ""
	err = tablet.VttabletProcess.Setup()
	require.Nil(t, err)

	err = tablet.VttabletProcess.WaitForTabletStatusesForTimeout([]string{"SERVING"}, 20*time.Second)
	require.Nil(t, err)
}

// InsertData inserts data.
func InsertData(t *testing.T, tablet *cluster.Vttablet, index int, keyspaceName string) {
	_, err := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("insert into vt_insert_test (id, msg) values (%d, 'test %d')", index, index), keyspaceName, true)
	require.Nil(t, err)
}
