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

	"github.com/buger/jsonparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	tmClient                 = tmc.NewClient()
	dbCredentialFile         string
	InsertTabletTemplateKsID = `insert into %s (id, msg) values (%d, '%s') /* id:%d */`
	defaultOperationTimeout  = 60 * time.Second
	defeaultRetryDelay       = 1 * time.Second
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

// VerifyRowsInTabletForTable verifies the total number of rows in a table.
// This is used to check that replication has caught up with the changes on primary.
func VerifyRowsInTabletForTable(t *testing.T, vttablet *Vttablet, ksName string, expectedRows int, tableName string) {
	timeout := time.Now().Add(1 * time.Minute)
	lastNumRowsFound := 0
	for time.Now().Before(timeout) {
		// ignoring the error check, if the newly created table is not replicated, then there might be error and we should ignore it
		// but eventually it will catch up and if not caught up in required time, testcase will fail
		qr, _ := vttablet.VttabletProcess.QueryTablet("select * from "+tableName, ksName, true)
		if qr != nil {
			if len(qr.Rows) == expectedRows {
				return
			}
			lastNumRowsFound = len(qr.Rows)
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Equalf(t, expectedRows, lastNumRowsFound, "unexpected number of rows in %s (%s.%s)", vttablet.Alias, ksName, tableName)
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
		if strings.Contains(line, "WARNING: vtctl should only be used for VDiff v1 workflows. Please use VDiff v2 and consider using vtctldclient for all other commands.") {
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
	cp.DbName = keyspace
	if keyspace != "" && keyspace != "_vt" {
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
					SET PASSWORD FOR 'root'@'localhost' = 'RootPass';
					SET PASSWORD FOR 'vt_dba'@'localhost' = 'VtDbaPass';
					SET PASSWORD FOR 'vt_app'@'localhost' = 'VtAppPass';
					SET PASSWORD FOR 'vt_allprivs'@'localhost' = 'VtAllprivsPass';
					SET PASSWORD FOR 'vt_repl'@'%' = 'VtReplPass';
					SET PASSWORD FOR 'vt_filtered'@'localhost' = 'VtFilteredPass';
					SET PASSWORD FOR 'vt_appdebug'@'localhost' = 'VtDebugPass';
					FLUSH PRIVILEGES;
					`
	return pwdChangeCmd
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

func WaitForTabletSetup(vtctlClientProcess *VtctlClientProcess, expectedTablets int, expectedStatus []string) error {
	// wait for both tablet to get into replica state in topo
	waitUntil := time.Now().Add(10 * time.Second)
	for time.Now().Before(waitUntil) {
		result, err := vtctlClientProcess.ExecuteCommandWithOutput("ListAllTablets")
		if err != nil {
			return err
		}

		tabletsFromCMD := strings.Split(result, "\n")
		tabletCountFromCMD := 0

		for _, line := range tabletsFromCMD {
			if len(line) > 0 {
				for _, status := range expectedStatus {
					if strings.Contains(line, status) {
						tabletCountFromCMD = tabletCountFromCMD + 1
						break
					}
				}
			}
		}

		if tabletCountFromCMD >= expectedTablets {
			return nil
		}

		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("all %d tablet are not in expected state %s", expectedTablets, expectedStatus)
}

// GetSidecarDBName returns the sidecar database name configured for
// the keyspace in the topo server.
func (cluster LocalProcessCluster) GetSidecarDBName(keyspace string) (string, error) {
	res, err := cluster.VtctldClientProcess.ExecuteCommandWithOutput("GetKeyspace", keyspace)
	if err != nil {
		return "", err
	}
	sdbn, err := jsonparser.GetString([]byte(res), "sidecar_db_name")
	if err != nil {
		return "", err
	}
	return sdbn, nil
}

// WaitForHealthyShard waits for the given shard info record in the topo
// server to list a tablet (alias and uid) as the primary serving tablet
// for the shard. This is done using "vtctldclient GetShard" and parsing
// its JSON output. All other watchers should then also see this shard
// info status as well.
func WaitForHealthyShard(vtctldclient *VtctldClientProcess, keyspace, shard string) error {
	var (
		tmr  = time.NewTimer(defaultOperationTimeout)
		res  string
		err  error
		json []byte
		cell string
		uid  int64
	)
	for {
		res, err = vtctldclient.ExecuteCommandWithOutput("GetShard", fmt.Sprintf("%s/%s", keyspace, shard))
		if err != nil {
			return err
		}
		json = []byte(res)

		cell, err = jsonparser.GetString(json, "shard", "primary_alias", "cell")
		if err != nil && err != jsonparser.KeyPathNotFoundError {
			return err
		}
		uid, err = jsonparser.GetInt(json, "shard", "primary_alias", "uid")
		if err != nil && err != jsonparser.KeyPathNotFoundError {
			return err
		}

		if cell != "" && uid > 0 {
			return nil
		}

		select {
		case <-tmr.C:
			return fmt.Errorf("timed out waiting for the %s/%s shard to become healthy in the topo after %v; last seen status: %s; last seen error: %v",
				keyspace, shard, defaultOperationTimeout, res, err)
		default:
		}

		time.Sleep(defeaultRetryDelay)
	}
}
