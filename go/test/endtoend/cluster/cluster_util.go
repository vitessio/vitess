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

package cluster

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	tmClient = tmc.NewClient()
)

// Restart restarts vttablet and mysql.
func (tablet *Vttablet) Restart() error {
	if tablet.MysqlctlProcess.TabletUID|tablet.MysqlctldProcess.TabletUID == 0 {
		return fmt.Errorf("no mysql process is running")
	}

	if tablet.MysqlctlProcess.TabletUID > 0 {
		tablet.MysqlctlProcess.Stop()
		tablet.VttabletProcess.TearDown()
		os.RemoveAll(tablet.VttabletProcess.Directory)

		return tablet.MysqlctlProcess.Start()
	}

	tablet.MysqlctldProcess.Stop()
	tablet.VttabletProcess.TearDown()
	os.RemoveAll(tablet.VttabletProcess.Directory)

	return tablet.MysqlctldProcess.Start()
}

// ValidateTabletRestart restarts the tablet and validate error if there is any.
func (tablet *Vttablet) ValidateTabletRestart(t *testing.T) {
	require.Nilf(t, tablet.Restart(), "tablet restart failed")
}

// GetMasterPosition gets the master position of required vttablet
func GetMasterPosition(t *testing.T, vttablet Vttablet, hostname string) (string, string) {
	ctx := context.Background()
	vtablet := getTablet(vttablet.GrpcPort, hostname)
	pos, err := tmClient.MasterPosition(ctx, vtablet)
	require.Nil(t, err)
	gtID := strings.SplitAfter(pos, "/")[1]
	return pos, gtID
}

// VerifyRowsInTabletForTable Verify total number of rows in a table
// this is used to check replication caught up the changes from master
func VerifyRowsInTabletForTable(t *testing.T, vttablet *Vttablet, ksName string, expectedRows int, tableName string) {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		// ignoring the error check, if the newly created table is not replicated, then there might be error and we should ignore it
		// but eventually it will catch up and if not caught up in required time, testcase will fail
		qr, _ := vttablet.VttabletProcess.QueryTablet("select * from "+tableName, ksName, true)
		if qr != nil && len(qr.Rows) == expectedRows {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	assert.Fail(t, "expected rows not found.")
}

// VerifyRowsInTablet Verify total number of rows in a tablet
func VerifyRowsInTablet(t *testing.T, vttablet *Vttablet, ksName string, expectedRows int) {
	VerifyRowsInTabletForTable(t, vttablet, ksName, expectedRows, "vt_insert_test")
}

// PanicHandler handles the panic in the testcase.
func PanicHandler(t *testing.T) {
	err := recover()
	if t == nil {
		return
	}
	require.Nilf(t, err, "panic occured in testcase %v", t.Name())
}

// VerifyLocalMetadata Verify Local Metadata of a tablet
func VerifyLocalMetadata(t *testing.T, tablet *Vttablet, ksName string, shardName string, cell string) {
	qr, err := tablet.VttabletProcess.QueryTablet("select * from _vt.local_metadata", ksName, false)
	require.Nil(t, err)
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows[0][1]), fmt.Sprintf(`BLOB("%s")`, tablet.Alias))
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows[1][1]), fmt.Sprintf(`BLOB("%s.%s")`, ksName, shardName))
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows[2][1]), fmt.Sprintf(`BLOB("%s")`, cell))
	if tablet.Type == "replica" {
		assert.Equal(t, fmt.Sprintf("%v", qr.Rows[3][1]), `BLOB("neutral")`)
	} else if tablet.Type == "rdonly" {
		assert.Equal(t, fmt.Sprintf("%v", qr.Rows[3][1]), `BLOB("must_not")`)
	}
}

// ListBackups Lists back preset in shard
func (cluster LocalProcessCluster) ListBackups(shardKsName string) ([]string, error) {
	output, err := cluster.VtctlclientProcess.ExecuteCommandWithOutput("ListBackups", shardKsName)
	if err != nil {
		return nil, err
	}
	result := strings.Split(output, "\n")
	var returnResult []string
	for _, str := range result {
		if str != "" {
			returnResult = append(returnResult, str)
		}
	}
	return returnResult, nil
}

// VerifyBackupCount compares the backup count with expected count.
func (cluster LocalProcessCluster) VerifyBackupCount(t *testing.T, shardKsName string, expected int) []string {
	backups, err := cluster.ListBackups(shardKsName)
	require.Nil(t, err)
	assert.Equalf(t, expected, len(backups), "invalid number of backups")
	return backups
}

// RemoveAllBackups removes all the backup corresponds to list backup.
func (cluster LocalProcessCluster) RemoveAllBackups(t *testing.T, shardKsName string) {
	backups, err := cluster.ListBackups(shardKsName)
	require.Nil(t, err)
	for _, backup := range backups {
		cluster.VtctlclientProcess.ExecuteCommand("RemoveBackup", shardKsName, backup)
	}
}

// ResetTabletDirectory transitions back to tablet state (i.e. mysql process restarts with cleaned directory and tablet is off)
func ResetTabletDirectory(tablet Vttablet) error {
	tablet.VttabletProcess.TearDown()
	tablet.MysqlctlProcess.Stop()
	os.RemoveAll(tablet.VttabletProcess.Directory)

	return tablet.MysqlctlProcess.Start()
}

func getTablet(tabletGrpcPort int, hostname string) *tabletpb.Tablet {
	portMap := make(map[string]int32)
	portMap["grpc"] = int32(tabletGrpcPort)
	return &tabletpb.Tablet{Hostname: hostname, PortMap: portMap}
}

func filterResultWhenRunsForCoverage(input string) string {
	if !*isCoverage {
		return input
	}
	lines := strings.Split(input, "\n")
	var result string
	for _, line := range lines {
		if strings.Contains(line, "=== RUN") {
			continue
		}
		if strings.Contains(line, "--- PASS:") || strings.Contains(line, "PASS") {
			break
		}
		result = result + line + "\n"
	}
	return result
}

// WaitForReplicationPos will wait for replication position to catch-up
func WaitForReplicationPos(t *testing.T, tabletA *Vttablet, tabletB *Vttablet, hostname string, timeout float64) {
	replicationPosA, _ := GetMasterPosition(t, *tabletA, hostname)
	for {
		replicationPosB, _ := GetMasterPosition(t, *tabletB, hostname)
		if positionAtLeast(t, tabletA, replicationPosB, replicationPosA) {
			break
		}
		msg := fmt.Sprintf("%s's replication position to catch up to %s's;currently at: %s, waiting to catch up to: %s", tabletB.Alias, tabletA.Alias, replicationPosB, replicationPosA)
		waitStep(t, msg, timeout, 0.01)
	}
}

func waitStep(t *testing.T, msg string, timeout float64, sleepTime float64) float64 {
	timeout = timeout - sleepTime
	if timeout < 0.0 {
		t.Errorf("timeout waiting for condition '%s'", msg)
	}
	time.Sleep(time.Duration(sleepTime) * time.Second)
	return timeout
}

func positionAtLeast(t *testing.T, tablet *Vttablet, a string, b string) bool {
	isAtleast := false
	val, err := tablet.MysqlctlProcess.ExecuteCommandWithOutput("position", "at_least", a, b)
	require.NoError(t, err)
	if strings.Contains(val, "true") {
		isAtleast = true
	}
	return isAtleast
}

// ExecuteQueriesUsingVtgate sends query to vtgate using vtgate session.
func ExecuteQueriesUsingVtgate(t *testing.T, session *vtgateconn.VTGateSession, query string) {
	_, err := session.Execute(context.Background(), query, nil)
	require.Nil(t, err)
}

// NewConnParams creates ConnParams corresponds to given arguments.
func NewConnParams(port int, password, socketPath, keyspace string) mysql.ConnParams {
	if port != 0 {
		socketPath = ""
	}
	cp := mysql.ConnParams{
		Uname:      "vt_dba",
		Port:       port,
		UnixSocket: socketPath,
		Pass:       password,
	}

	if keyspace != "" {
		cp.DbName = "vt_" + keyspace
	}

	return cp

}
