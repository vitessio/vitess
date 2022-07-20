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
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	tmClient                 = tmc.NewClient()
	dbCredentialFile         string
	InsertTabletTemplateKsID = `insert into %s (id, msg) values (%d, '%s') /* id:%d */`
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

// RestartOnlyTablet restarts vttablet, but not the underlying mysql instance
func (tablet *Vttablet) RestartOnlyTablet() error {
	err := tablet.VttabletProcess.TearDown()
	if err != nil {
		return err
	}

	tablet.VttabletProcess.ServingStatus = "SERVING"

	return tablet.VttabletProcess.Setup()
}

// ValidateTabletRestart restarts the tablet and validate error if there is any.
func (tablet *Vttablet) ValidateTabletRestart(t *testing.T) {
	require.Nilf(t, tablet.Restart(), "tablet restart failed")
}

// GetPrimaryPosition gets the executed replication position of given vttablet
func GetPrimaryPosition(t *testing.T, vttablet Vttablet, hostname string) (string, string) {
	ctx := context.Background()
	vtablet := getTablet(vttablet.GrpcPort, hostname)
	pos, err := tmClient.PrimaryPosition(ctx, vtablet)
	require.Nil(t, err)
	gtID := strings.SplitAfter(pos, "/")[1]
	return pos, gtID
}

// GetReplicationStatus gets the replication status of given vttablet
func GetReplicationStatus(t *testing.T, vttablet *Vttablet, hostname string) *replicationdatapb.Status {
	ctx := context.Background()
	vtablet := getTablet(vttablet.GrpcPort, hostname)
	pos, err := tmClient.ReplicationStatus(ctx, vtablet)
	require.NoError(t, err)
	return pos
}

// VerifyRowsInTabletForTable verifies the total number of rows in a table.
// This is used to check that replication has caught up with the changes on primary.
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

func getTablet(tabletGrpcPort int, hostname string) *topodatapb.Tablet {
	portMap := make(map[string]int32)
	portMap["grpc"] = int32(tabletGrpcPort)
	return &topodatapb.Tablet{Hostname: hostname, PortMap: portMap}
}

func filterResultForWarning(input string) string {
	lines := strings.Split(input, "\n")
	var result string
	for _, line := range lines {
		if strings.Contains(line, "WARNING: vtctl should only be used for VDiff workflows") {
			continue
		}
		result = result + line + "\n"
	}
	return result
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
	replicationPosA, _ := GetPrimaryPosition(t, *tabletA, hostname)
	for {
		replicationPosB, _ := GetPrimaryPosition(t, *tabletB, hostname)
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

func filterDoubleDashArgs(args []string, version int) (filtered []string) {
	if version > 13 {
		return args
	}

	for _, arg := range args {
		if arg == "--" {
			continue
		}

		filtered = append(filtered, arg)
	}

	return filtered
}

// WriteDbCredentialToTmp writes JSON formatted db credentials to the
// specified tmp directory.
func WriteDbCredentialToTmp(tmpDir string) string {
	data := []byte(`{
        "vt_dba": ["VtDbaPass"],
        "vt_app": ["VtAppPass"],
        "vt_allprivs": ["VtAllprivsPass"],
        "vt_repl": ["VtReplPass"],
        "vt_filtered": ["VtFilteredPass"]
	}`)
	dbCredentialFile = path.Join(tmpDir, "db_credentials.json")
	os.WriteFile(dbCredentialFile, data, 0666)
	return dbCredentialFile
}

// GetPasswordUpdateSQL returns the SQL for updating the users' passwords
// to the static creds used throughout tests.
func GetPasswordUpdateSQL(localCluster *LocalProcessCluster) string {
	pwdChangeCmd := `
					# Set real passwords for all users.
					UPDATE mysql.user SET %s = PASSWORD('RootPass')
					  WHERE User = 'root' AND Host = 'localhost';
					UPDATE mysql.user SET %s = PASSWORD('VtDbaPass')
					  WHERE User = 'vt_dba' AND Host = 'localhost';
					UPDATE mysql.user SET %s = PASSWORD('VtAppPass')
					  WHERE User = 'vt_app' AND Host = 'localhost';
					UPDATE mysql.user SET %s = PASSWORD('VtAllprivsPass')
					  WHERE User = 'vt_allprivs' AND Host = 'localhost';
					UPDATE mysql.user SET %s = PASSWORD('VtReplPass')
					  WHERE User = 'vt_repl' AND Host = '%%';
					UPDATE mysql.user SET %s = PASSWORD('VtFilteredPass')
					  WHERE User = 'vt_filtered' AND Host = 'localhost';
					FLUSH PRIVILEGES;
					`
	pwdCol, _ := getPasswordField(localCluster)
	return fmt.Sprintf(pwdChangeCmd, pwdCol, pwdCol, pwdCol, pwdCol, pwdCol, pwdCol)
}

// getPasswordField determines which column is used for user passwords in this MySQL version.
func getPasswordField(localCluster *LocalProcessCluster) (pwdCol string, err error) {
	tablet := &Vttablet{
		Type:            "relpica",
		TabletUID:       100,
		MySQLPort:       15000,
		MysqlctlProcess: *MysqlCtlProcessInstance(100, 15000, localCluster.TmpDirectory),
	}
	if err = tablet.MysqlctlProcess.Start(); err != nil {
		return "", err
	}
	tablet.VttabletProcess = VttabletProcessInstance(tablet.HTTPPort, tablet.GrpcPort, tablet.TabletUID, "", "", "", 0, tablet.Type, localCluster.TopoPort, "", "", nil, false, localCluster.DefaultCharset)
	result, err := tablet.VttabletProcess.QueryTablet("select password from mysql.user limit 0", "", false)
	if err == nil && len(result.Rows) > 0 {
		return "password", nil
	}
	tablet.MysqlctlProcess.Stop()
	os.RemoveAll(path.Join(tablet.VttabletProcess.Directory))
	return "authentication_string", nil

}

// CheckSrvKeyspace confirms that the cell and keyspace contain the expected
// shard mappings.
func CheckSrvKeyspace(t *testing.T, cell string, ksname string, expectedPartition map[topodatapb.TabletType][]string, ci LocalProcessCluster) {
	srvKeyspace := GetSrvKeyspace(t, cell, ksname, ci)

	currentPartition := map[topodatapb.TabletType][]string{}

	for _, partition := range srvKeyspace.Partitions {
		currentPartition[partition.ServedType] = []string{}
		for _, shardRef := range partition.ShardReferences {
			currentPartition[partition.ServedType] = append(currentPartition[partition.ServedType], shardRef.Name)
		}
	}

	assert.True(t, reflect.DeepEqual(currentPartition, expectedPartition))
}

// GetSrvKeyspace returns the SrvKeyspace structure for the cell and keyspace.
func GetSrvKeyspace(t *testing.T, cell string, ksname string, ci LocalProcessCluster) *topodatapb.SrvKeyspace {
	output, err := ci.VtctlclientProcess.ExecuteCommandWithOutput("GetSrvKeyspace", cell, ksname)
	require.Nil(t, err)
	var srvKeyspace topodatapb.SrvKeyspace

	err = json2.Unmarshal([]byte(output), &srvKeyspace)
	require.Nil(t, err)
	return &srvKeyspace
}

// ExecuteOnTablet executes a query on the specified vttablet.
// It should always be called with a primary tablet for a keyspace/shard.
func ExecuteOnTablet(t *testing.T, query string, vttablet Vttablet, ks string, expectFail bool) {
	_, _ = vttablet.VttabletProcess.QueryTablet("begin", ks, true)
	_, err := vttablet.VttabletProcess.QueryTablet(query, ks, true)
	if expectFail {
		require.Error(t, err)
	} else {
		require.Nil(t, err)
	}
	_, _ = vttablet.VttabletProcess.QueryTablet("commit", ks, true)
}
