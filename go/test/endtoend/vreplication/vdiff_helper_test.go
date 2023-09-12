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

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	vdiff2 "vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"
	"vitess.io/vitess/go/vt/wrangler"
)

const (
	vdiffTimeout = time.Second * 90 // we can leverage auto retry on error with this longer-than-usual timeout
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
		doVdiff2(t, keyspace, workflow, cells, wantV2Result)
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
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("VDiff", "--", "--v1", "--tablet_types=primary", "--source_cell="+cells, "--format", "json", ksWorkflow)
		log.Infof("vdiff1 err: %+v, output: %+v", err, output)
		require.NoError(t, err)
		require.NotNil(t, output)
		diffReports := make(map[string]*wrangler.DiffReport)
		t.Logf("vdiff1 output: %s", output)
		err = json.Unmarshal([]byte(output), &diffReports)
		require.NoError(t, err)
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
	first := true
	previousProgress := vdiff2.ProgressReport{}
	ch := make(chan bool)
	go func() {
		for {
			time.Sleep(1 * time.Second)
			_, jsonStr := performVDiff2Action(t, ksWorkflow, cells, "show", uuid, false)
			info = getVDiffInfo(jsonStr)
			if info.State == "completed" {
				if !completedAtMin.IsZero() {
					ca := info.CompletedAt
					completedAt, _ := time.Parse(vdiff2.TimestampFormat, ca)
					if !completedAt.After(completedAtMin) {
						continue
					}
				}
				ch <- true
				return
			} else if info.State == "started" { // test the progress report
				// The ETA should always be in the future -- when we're able to estimate
				// it -- and the progress percentage should only increase.
				// The timestamp format allows us to compare them lexicographically.
				// We don't test that the ETA always increases as it can decrease based on how
				// quickly we're doing work.

				// Commenting out this check for now as it is quite flaky in Github CI: we sometimes get a difference of
				// more than 1s between the ETA and the current time, empirically seen 2s when it has failed,
				// but presumably it can be higher. Keeping the code here for now in case we want to re-enable it.

				/*
					if info.Progress.ETA != "" {
						// If we're operating at the second boundary then the ETA can be up
						// to 1 second in the past due to using second based precision.
						loc, _ := time.LoadLocation("UTC")
						require.GreaterOrEqual(t, info.Progress.ETA, time.Now().Add(-time.Second).In(loc).Format(vdiff2.TimestampFormat))
					}
				*/

				if !first {
					require.GreaterOrEqual(t, info.Progress.Percentage, previousProgress.Percentage)
				}
				previousProgress.Percentage = info.Progress.Percentage
				first = false
			}
		}
	}()

	select {
	case <-ch:
		return info
	case <-time.After(vdiffTimeout):
		require.FailNow(t, fmt.Sprintf("VDiff never completed for UUID %s", uuid))
		return nil
	}
}

type expectedVDiff2Result struct {
	state       string
	shards      []string
	hasMismatch bool
}

func doVdiff2(t *testing.T, keyspace, workflow, cells string, want *expectedVDiff2Result) {
	ksWorkflow := fmt.Sprintf("%s.%s", keyspace, workflow)
	t.Run(fmt.Sprintf("vdiff2 %s", ksWorkflow), func(t *testing.T) {
		// update-table-stats is needed in order to test progress reports.
		uuid, _ := performVDiff2Action(t, ksWorkflow, cells, "create", "", false, "--auto-retry", "--update-table-stats")
		info := waitForVDiff2ToComplete(t, ksWorkflow, cells, uuid, time.Time{})

		require.Equal(t, workflow, info.Workflow)
		require.Equal(t, keyspace, info.Keyspace)
		if want != nil {
			require.Equal(t, want.state, info.State)
			require.Equal(t, strings.Join(want.shards, ","), info.Shards)
			require.Equal(t, want.hasMismatch, info.HasMismatch)
		} else {
			require.Equal(t, "completed", info.State, "vdiff results: %+v", info)
			require.False(t, info.HasMismatch, "vdiff results: %+v", info)
		}
		if strings.Contains(t.Name(), "AcrossDBVersions") {
			log.Errorf("VDiff resume cannot be guaranteed between major MySQL versions due to implied collation differences, skipping resume test...")
			return
		}
	})
}

func performVDiff2Action(t *testing.T, ksWorkflow, cells, action, actionArg string, expectError bool, extraFlags ...string) (uuid string, output string) {
	var err error
	args := []string{"VDiff", "--", "--tablet_types=primary", "--source_cell=" + cells, "--format=json"}
	if len(extraFlags) > 0 {
		args = append(args, extraFlags...)
	}
	args = append(args, ksWorkflow, action, actionArg)
	output, err = vc.VtctlClient.ExecuteCommandWithOutput(args...)
	log.Infof("vdiff2 output: %+v (err: %+v)", output, err)
	if !expectError {
		require.Nil(t, err)
		uuid = gjson.Get(output, "UUID").String()
		if action != "delete" && !(action == "show" && actionArg == "all") { // a UUID is not required
			require.NoError(t, err)
			require.NotEmpty(t, uuid)
		}
	}
	return uuid, output
}

type vdiffInfo struct {
	Workflow, Keyspace string
	State, Shards      string
	RowsCompared       int64
	StartedAt          string
	CompletedAt        string
	HasMismatch        bool
	Progress           vdiff2.ProgressReport
}

func getVDiffInfo(json string) *vdiffInfo {
	var info vdiffInfo
	info.Workflow = gjson.Get(json, "Workflow").String()
	info.Keyspace = gjson.Get(json, "Keyspace").String()
	info.State = gjson.Get(json, "State").String()
	info.Shards = gjson.Get(json, "Shards").String()
	info.RowsCompared = gjson.Get(json, "RowsCompared").Int()
	info.StartedAt = gjson.Get(json, "StartedAt").String()
	info.CompletedAt = gjson.Get(json, "CompletedAt").String()
	info.HasMismatch = gjson.Get(json, "HasMismatch").Bool()
	info.Progress.Percentage = gjson.Get(json, "Progress.Percentage").Float()
	info.Progress.ETA = gjson.Get(json, "Progress.ETA").String()
	return &info
}

func encodeString(in string) string {
	var buf strings.Builder
	sqltypes.NewVarChar(in).EncodeSQL(&buf)
	return buf.String()
}

// generateMoreCustomers creates additional test data for better tests
// when needed.
func generateMoreCustomers(t *testing.T, keyspace string, numCustomers int64) {
	log.Infof("Generating more test data with an additional %d customers", numCustomers)
	res := execVtgateQuery(t, vtgateConn, keyspace, "select max(cid) from customer")
	startingID, _ := res.Rows[0][0].ToInt64()
	insert := strings.Builder{}
	insert.WriteString("insert into customer(cid, name, typ) values ")
	i := int64(0)
	for i < numCustomers {
		i++
		insert.WriteString(fmt.Sprintf("(%d, 'Testy (Bot) McTester', 'soho')", startingID+i))
		if i != numCustomers {
			insert.WriteString(", ")
		}
	}
	execVtgateQuery(t, vtgateConn, keyspace, insert.String())
}
