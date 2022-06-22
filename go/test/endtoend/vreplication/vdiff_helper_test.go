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
	tsFormat     = "2006-01-02 15:04:05"
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

func waitForVDiff2ToComplete(t *testing.T, ksWorkflow, cells, uuid string, completedAtMin time.Time) *vdiffInfo {
	var info *vdiffInfo
	ch := make(chan bool)
	go func() {
		for {
			time.Sleep(1 * time.Second)
			_, jsonStr := performVDiff2Action(t, ksWorkflow, cells, "show", uuid)
			info = getVDiffInfo(jsonStr)
			if info.State == "completed" {
				if !completedAtMin.IsZero() {
					ca := info.CompletedAt
					completedAt, _ := time.Parse(tsFormat, ca)
					if !completedAt.After(completedAtMin) {
						continue
					}
				}
				ch <- true
				return
			} else if info.State == "error" {
				ch <- false
				return
			}
		}
	}()

	select {
	case good := <-ch:
		if !good {
			require.FailNow(t, "VDiff encountered an error")
		}
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

func vdiff2(t *testing.T, keyspace, workflow, cells string, want *expectedVDiff2Result) {
	ksWorkflow := fmt.Sprintf("%s.%s", keyspace, workflow)
	t.Run(fmt.Sprintf("vdiff2 %s", ksWorkflow), func(t *testing.T) {
		uuid, _ := performVDiff2Action(t, ksWorkflow, cells, "create", "")
		info := waitForVDiff2ToComplete(t, ksWorkflow, cells, uuid, time.Time{})

		require.Equal(t, workflow, info.Workflow)
		require.Equal(t, keyspace, info.Keyspace)
		// I'm not sure if we always have rows in every table
		//require.Greater(t, info.RowsCompared, int64(0))
		if want != nil {
			require.Equal(t, want.state, info.State)
			require.Equal(t, strings.Join(want.shards, ","), info.Shards)
			require.Equal(t, want.hasMismatch, info.HasMismatch)
		} else {
			require.Equal(t, "completed", info.State)
			require.False(t, info.HasMismatch)
		}
		createCompletedTS, err := time.Parse(tsFormat, info.CompletedAt)
		require.NoError(t, err)

		uuid, _ = performVDiff2Action(t, ksWorkflow, cells, "resume", uuid)
		// Pass the previous completed time as we need to wait for it to
		// resume -- where it goes from completed-> pending->started -- before
		// waiting for it again to move to completed
		info = waitForVDiff2ToComplete(t, ksWorkflow, cells, uuid, createCompletedTS)
		resumeCompletedTS, err := time.Parse(tsFormat, info.CompletedAt)
		require.NoError(t, err)
		require.False(t, info.HasMismatch)
		require.Greater(t, resumeCompletedTS, createCompletedTS)
	})
}

func performVDiff2Action(t *testing.T, ksWorkflow, cells, action, actionArg string) (uuid string, output string) {
	var err error
	output, err = vc.VtctlClient.ExecuteCommandWithOutput("VDiff", "--", "--v2", "--tablet_types=primary", "--source_cell="+cells, "--format", "json", ksWorkflow, action, actionArg)
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
	RowsCompared       int64
	CompletedAt        string
	HasMismatch        bool
}

func getVDiffInfo(jsonStr string) *vdiffInfo {
	var info vdiffInfo
	json := []byte(jsonStr)
	info.Workflow, _ = jsonparser.GetString(json, "Workflow")
	info.Keyspace, _ = jsonparser.GetString(json, "Keyspace")
	info.State, _ = jsonparser.GetString(json, "State")
	info.Shards, _ = jsonparser.GetString(json, "Shards")
	info.RowsCompared, _ = jsonparser.GetInt(json, "RowsCompared")
	info.CompletedAt, _ = jsonparser.GetString(json, "CompletedAt")
	info.HasMismatch, _ = jsonparser.GetBoolean(json, "HasMismatch")

	return &info
}
