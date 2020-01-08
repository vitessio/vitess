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
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	tmClient = tmc.NewClient()
)

// GetMasterPosition gets the master position of required vttablet
func GetMasterPosition(t *testing.T, vttablet Vttablet, hostname string) (string, string) {
	ctx := context.Background()
	vtablet := getTablet(vttablet.GrpcPort, hostname)
	pos, err := tmClient.MasterPosition(ctx, vtablet)
	require.NoError(t, err)
	gtID := strings.SplitAfter(pos, "/")[1]
	return pos, gtID
}

// Verify total number of rows in a tablet
func VerifyRowsInTablet(t *testing.T, vttablet *Vttablet, ksName string, expectedRows int) {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		qr, err := vttablet.VttabletProcess.QueryTablet("select * from vt_insert_test", ksName, true)
		assert.Nil(t, err)
		if len(qr.Rows) != expectedRows {
			time.Sleep(300 * time.Millisecond)
		} else {
			return
		}
	}
	assert.Fail(t, "expected rows not found.")
}

// Verify Local Metadata of a tablet
func VerifyLocalMetadata(t *testing.T, tablet *Vttablet, ksName string, shardName string, cell string) {
	qr, err := tablet.VttabletProcess.QueryTablet("select * from _vt.local_metadata", ksName, false)
	assert.Nil(t, err)
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows[0][1]), fmt.Sprintf(`BLOB("%s")`, tablet.Alias))
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows[1][1]), fmt.Sprintf(`BLOB("%s.%s")`, ksName, shardName))
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows[2][1]), fmt.Sprintf(`BLOB("%s")`, cell))
	if tablet.Type == "replica" {
		assert.Equal(t, fmt.Sprintf("%v", qr.Rows[3][1]), `BLOB("neutral")`)
	} else if tablet.Type == "rdonly" {
		assert.Equal(t, fmt.Sprintf("%v", qr.Rows[3][1]), `BLOB("must_not")`)
	}
}

//Lists back preset in shard
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

// ResetTabletDirectory transitions back to tablet state (i.e. mysql process restarts with cleaned directory and tablet is off)
func ResetTabletDirectory(tablet Vttablet) error {
	tablet.MysqlctlProcess.Stop()
	tablet.VttabletProcess.TearDown()
	os.RemoveAll(tablet.VttabletProcess.Directory)

	return tablet.MysqlctlProcess.Start()
}

func getTablet(tabletGrpcPort int, hostname string) *tabletpb.Tablet {
	portMap := make(map[string]int32)
	portMap["grpc"] = int32(tabletGrpcPort)
	return &tabletpb.Tablet{Hostname: hostname, PortMap: portMap}
}

func filterResultWhenRunsForCoverage(input string) string {
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
