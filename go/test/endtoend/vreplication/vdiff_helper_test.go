/*
Copyright 2022 The Vitess Authors.

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

package vreplication

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buger/jsonparser"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/wrangler"
)

const (
	vdiffTimeout = time.Second * 60
)

var (
	runVDiffsSideBySide = true
)

func vdiff(t *testing.T, keyspace, workflow, cells string, v1, v2 bool, wantV2Result *expectedVDiff2Result) {
	ksWorkflow := fmt.Sprintf("%s.%s", keyspace, workflow)
	if v1 {
		doVDiff1(t, ksWorkflow, cells)
	}
	if v2 {
		vdiff2(t, keyspace, workflow, cells, wantV2Result)
	}
}

func vdiff1(t *testing.T, ksWorkflow, cells string) {
	if !runVDiffsSideBySide {
		doVDiff1(t, ksWorkflow, cells)
		return
	}
	arr := strings.Split(ksWorkflow, ".")
	keyspace := arr[0]
	workflowName := arr[1]
	vdiff(t, keyspace, workflowName, cells, true, true, nil)
}

func doVDiff1(t *testing.T, ksWorkflow, cells string) {
	t.Run(fmt.Sprintf("vdiff1 %s", ksWorkflow), func(t *testing.T) {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("VDiff", "--", "--tablet_types=primary", "--source_cell="+cells, "--format", "json", ksWorkflow)
		log.Infof("vdiff1 err: %+v, output: %+v", err, output)
		require.Nil(t, err)
		require.NotNil(t, output)
		diffReports := make(map[string]*wrangler.DiffReport)
		err = json.Unmarshal([]byte(output), &diffReports)
		require.Nil(t, err)
		if len(diffReports) < 1 {
			t.Fatal("VDiff did not return a valid json response " + output + "\n")
		}
		require.True(t, len(diffReports) > 0)
		for key, diffReport := range diffReports {
			if diffReport.ProcessedRows != diffReport.MatchingRows {
				require.Failf(t, "vdiff1 failed", "Table %d : %#v\n", key, diffReport)
			}
		}
	})
}

func waitForVDiff2ToComplete(t *testing.T, ksWorkflow, uuid string) *vdiffInfo {
	var info *vdiffInfo
	ch := make(chan bool)
	go func() {
		for {
			time.Sleep(1 * time.Second)
			_, jsonStr := performVDiff2Action(t, ksWorkflow, "show", uuid)
			if info = getVDiffInfo(jsonStr); info.State == "completed" {
				ch <- true
				return
			}
		}
	}()

	select {
	case <-ch:
		return info
	case <-time.After(vdiffTimeout):
		require.FailNowf(t, "VDiff never completed: %s", uuid)
		return nil
	}
}

type expectedVDiff2Result struct {
	state       string
	shards      []string
	hasMismatch bool
}

// todo: use specified cells
func vdiff2(t *testing.T, keyspace, workflow, cells string, want *expectedVDiff2Result) {
	ksWorkflow := fmt.Sprintf("%s.%s", keyspace, workflow)
	t.Run(fmt.Sprintf("vdiff2 %s", ksWorkflow), func(t *testing.T) {
		uuid, _ := performVDiff2Action(t, ksWorkflow, "create", "")
		info := waitForVDiff2ToComplete(t, ksWorkflow, uuid)

		require.Equal(t, workflow, info.Workflow)
		require.Equal(t, keyspace, info.Keyspace)
		if want != nil {
			require.Equal(t, want.state, info.State)
			require.Equal(t, strings.Join(want.shards, ","), info.Shards)
			require.Equal(t, want.hasMismatch, info.HasMismatch)
		} else {
			require.Equal(t, info.State, "completed")
			require.False(t, info.HasMismatch)

		}
	})
}

func performVDiff2Action(t *testing.T, ksWorkflow, action, actionArg string) (uuid string, output string) {
	var err error
	output, err = vc.VtctlClient.ExecuteCommandWithOutput("VDiff", "--", "--v2", "--format", "json", ksWorkflow, action, actionArg)
	log.Infof("vdiff2 output: %+v (err: %+v)", output, err)
	require.Nil(t, err)

	uuid, err = jsonparser.GetString([]byte(output), "UUID")
	require.NoError(t, err)
	require.NotEmpty(t, uuid)
	return uuid, output
}

type vdiffInfo struct {
	Workflow, Keyspace string
	State, Shards      string
	HasMismatch        bool
}

func getVDiffInfo(jsonStr string) *vdiffInfo {
	var info vdiffInfo
	json := []byte(jsonStr)
	info.Workflow, _ = jsonparser.GetString(json, "Workflow")
	info.Keyspace, _ = jsonparser.GetString(json, "Keyspace")
	info.State, _ = jsonparser.GetString(json, "State")
	info.Shards, _ = jsonparser.GetString(json, "Shards")
	info.HasMismatch, _ = jsonparser.GetBoolean(json, "HasMismatch")

	return &info
}
