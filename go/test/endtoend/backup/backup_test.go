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

package backup

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	vtInsertTest = `
					create table vt_insert_test (
					  id bigint auto_increment,
					  msg varchar(64),
					  primary key (id)
					  ) Engine=InnoDB`
)

func TestReplicaBackup(t *testing.T) {
	testBackup(t, "replica")
}

//func TestRdonlyBackup(t *testing.T) {
//	testBackup(t, "rdonly")
//}

//
//Test backup flow.
//
//test_backup will:
//- create a shard with master and replica1 only
//- run InitShardMaster
//- bring up tablet_replica2 concurrently, telling it to wait for a backup
//- insert some data
//- take a backup
//- insert more data on the master
//- wait for tablet_replica2 to become SERVING
//- check all data is right (before+after backup data)
//- list the backup, remove it
//
//Args:
//tablet_type: 'replica' or 'rdonly'.
//
//
func testBackup(t *testing.T, tabletType string) {
	restoreWaitForBackup(t)
	_, err := master.VttabletProcess.QueryTablet(vtInsertTest, keyspaceName, true)
	assert.Nil(t, err)
	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test1')", keyspaceName, true)
	assert.Nil(t, err)
	checkData(t, replica1, 1)

	err = localCluster.VtctlclientProcess.ExecuteCommand("Backup", replica1.Alias)
	assert.Nil(t, err)

	backups := listBackups(t)
	assert.Equal(t, len(backups), 1)

	_, err = master.VttabletProcess.QueryTablet("insert into vt_insert_test (msg) values ('test2')", keyspaceName, true)
	assert.Nil(t, err)

	err = replica2.VttabletProcess.WaitForTabletType("SERVING")
	assert.Nil(t, err)

	checkData(t, replica2, 2)

	qr, err := replica2.VttabletProcess.QueryTablet("select * from _vt.local_metadata", keyspaceName, false)
	assert.Nil(t, err)
	fmt.Printf("%v", qr.Rows)
}

// Bring up another replica concurrently, telling it to wait until a backup
// is available instead of starting up empty.
//
// Override the backup engine implementation to a non-existent one for restore.
// This setting should only matter for taking new backups. We should be able
// to restore a previous backup successfully regardless of this setting.
func restoreWaitForBackup(t *testing.T) {
	resetTabletDir(t)
	replicaTabletArgs := commonTabletArg
	replicaTabletArgs = append(replicaTabletArgs, "-backup_engine_implementation", "fake_implementation")
	replicaTabletArgs = append(replicaTabletArgs, "-wait_for_backup_interval", "1s")
	replica2.VttabletProcess.ExtraArgs = replicaTabletArgs
	replica2.VttabletProcess.ServingStatus = ""
	err := replica2.VttabletProcess.Setup()
	assert.Nil(t, err)
}

func resetTabletDir(t *testing.T) {
	replica2.MysqlctlProcess.Stop()
	replica2.VttabletProcess.TearDown()
	os.RemoveAll(replica2.VttabletProcess.Directory)

	err := replica2.MysqlctlProcess.Start()
	assert.Nil(t, err)
}

func listBackups(t *testing.T) []string {
	output, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("ListBackups", shardKsName)
	assert.Nil(t, err)
	result := strings.Split(output, "\n")
	var returnResult []string
	for _, str := range result {
		if str != "" {
			returnResult = append(returnResult, str)
		}
	}
	return returnResult
}

func checkData(t *testing.T, vttablet *cluster.Vttablet, totalRows int) {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		qr, err := vttablet.VttabletProcess.QueryTablet("select * from vt_insert_test", keyspaceName, true)
		assert.Nil(t, err)
		if len(qr.Rows) != totalRows {
			time.Sleep(300 * time.Millisecond)
		} else {
			return
		}
	}
	assert.Fail(t, "expected rows not found.")
}
