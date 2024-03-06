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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	vdiff2 "vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"
)

const (
	vdiffTimeout             = 120 * time.Second // We can leverage auto retry on error with this longer-than-usual timeout
	vdiffRetryTimeout        = 30 * time.Second
	vdiffStatusCheckInterval = 5 * time.Second
	vdiffRetryInterval       = 5 * time.Second
)

var (
	runVDiffsSideBySide = true
)

func vdiff(t *testing.T, keyspace, workflow, cells string, vtctlclient, vtctldclient bool, wantV2Result *expectedVDiff2Result) {
	if vtctlclient {
		doVtctlclientVDiff(t, keyspace, workflow, cells, wantV2Result)
	}
	if vtctldclient {
		doVtctldclientVDiff(t, keyspace, workflow, cells, wantV2Result)
	}
}

// vdiffSideBySide will run the VDiff command using both vtctlclient
// and vtctldclient.
func vdiffSideBySide(t *testing.T, ksWorkflow, cells string) {
	arr := strings.Split(ksWorkflow, ".")
	keyspace := arr[0]
	workflowName := arr[1]
	if !runVDiffsSideBySide {
		doVtctlclientVDiff(t, keyspace, workflowName, cells, nil)
		return
	}
	vdiff(t, keyspace, workflowName, cells, true, true, nil)
}

func doVtctlclientVDiff(t *testing.T, keyspace, workflow, cells string, want *expectedVDiff2Result) {
	ksWorkflow := fmt.Sprintf("%s.%s", keyspace, workflow)
	t.Run(fmt.Sprintf("vtctlclient vdiff %s", ksWorkflow), func(t *testing.T) {
		// update-table-stats is needed in order to test progress reports.
		uuid, _ := performVDiff2Action(t, true, ksWorkflow, cells, "create", "", false, "--auto-retry", "--update-table-stats")
		info := waitForVDiff2ToComplete(t, true, ksWorkflow, cells, uuid, time.Time{})
		require.NotNil(t, info)
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

func waitForVDiff2ToComplete(t *testing.T, useVtctlclient bool, ksWorkflow, cells, uuid string, completedAtMin time.Time) *vdiffInfo {
	var info *vdiffInfo
	var jsonStr string
	first := true
	previousProgress := vdiff2.ProgressReport{}
	ch := make(chan bool)
	go func() {
		for {
			time.Sleep(vdiffStatusCheckInterval)
			_, jsonStr = performVDiff2Action(t, useVtctlclient, ksWorkflow, cells, "show", uuid, false)
			info = getVDiffInfo(jsonStr)
			require.NotNil(t, info)
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
			} else if info.State == "started" { // Test the progress report
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
		log.Errorf("VDiff never completed for UUID %s. Latest output: %s", uuid, jsonStr)
		require.FailNow(t, fmt.Sprintf("VDiff never completed for UUID %s", uuid))
		return nil
	}
}

type expectedVDiff2Result struct {
	state       string
	shards      []string
	hasMismatch bool
}

func doVtctldclientVDiff(t *testing.T, keyspace, workflow, cells string, want *expectedVDiff2Result, extraFlags ...string) {
	ksWorkflow := fmt.Sprintf("%s.%s", keyspace, workflow)
	t.Run(fmt.Sprintf("vtctldclient vdiff %s", ksWorkflow), func(t *testing.T) {
		// update-table-stats is needed in order to test progress reports.
		flags := []string{"--auto-retry", "--update-table-stats"}
		if len(extraFlags) > 0 {
			flags = append(flags, extraFlags...)
		}
		uuid, _ := performVDiff2Action(t, false, ksWorkflow, cells, "create", "", false, flags...)
		info := waitForVDiff2ToComplete(t, false, ksWorkflow, cells, uuid, time.Time{})
		require.NotNil(t, info)
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

func performVDiff2Action(t *testing.T, useVtctlclient bool, ksWorkflow, cells, action, actionArg string, expectError bool, extraFlags ...string) (uuid string, output string) {
	var err error
	targetKeyspace, workflowName, ok := strings.Cut(ksWorkflow, ".")
	require.True(t, ok, "invalid keyspace.workflow value: %s", ksWorkflow)

	if useVtctlclient {
		// This will always result in us using a PRIMARY tablet, which is all
		// we start in many e2e tests, but it avoids the tablet picker logic
		// where when you ONLY specify the PRIMARY type it then picks the
		// shard's primary and ignores any cell settings.
		args := []string{"VDiff", "--", "--tablet_types=in_order:primary,replica", "--source_cell=" + cells, "--format=json"}
		if len(extraFlags) > 0 {
			args = append(args, extraFlags...)
		}
		args = append(args, ksWorkflow, action, actionArg)
		output, err = execVDiffWithRetry(t, expectError, false, args)
		log.Infof("vdiff output: %+v (err: %+v)", output, err)
		if !expectError {
			require.Nil(t, err)
			uuid = gjson.Get(output, "UUID").String()
			if action != "delete" && !(action == "show" && actionArg == "all") { // A UUID is not required
				require.NoError(t, err)
				require.NotEmpty(t, uuid)
			}
		}
	} else {
		args := []string{"VDiff", "--target-keyspace", targetKeyspace, "--workflow", workflowName, "--format=json", action}
		if strings.ToLower(action) == string(vdiff2.CreateAction) {
			// This will always result in us using a PRIMARY tablet, which is all
			// we start in many e2e tests, but it avoids the tablet picker logic
			// where when you ONLY specify the PRIMARY type it then picks the
			// shard's primary and ignores any cell settings.
			args = append(args, "--tablet-types=primary,replica", "--tablet-types-in-preference-order", "--source-cells="+cells)
		}
		if len(extraFlags) > 0 {
			args = append(args, extraFlags...)
		}
		if actionArg != "" {
			args = append(args, actionArg)
		}

		output, err = execVDiffWithRetry(t, expectError, true, args)
		log.Infof("vdiff output: %+v (err: %+v)", output, err)
		if !expectError {
			require.NoError(t, err)
			ouuid := gjson.Get(output, "UUID").String()
			if action == "create" || (action == "show" && actionArg != "all") { // A UUID is returned
				require.NotEmpty(t, ouuid)
				uuid = ouuid
			}
		}
	}

	return uuid, output
}

// During SwitchTraffic, due to changes in the cluster, vdiff can return transient errors. isVDiffRetryable() is used to
// ignore such errors and retry vdiff expecting the condition to be resolved.
func isVDiffRetryable(str string) bool {
	for _, s := range []string{"Error while dialing", "failed to connect"} {
		if strings.Contains(str, s) {
			return true
		}
	}
	return false
}

type vdiffResult struct {
	output string
	err    error
}

// execVDiffWithRetry will ignore transient errors that can occur during workflow state changes.
func execVDiffWithRetry(t *testing.T, expectError bool, useVtctldClient bool, args []string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), vdiffRetryTimeout)
	defer cancel()
	vdiffResultCh := make(chan vdiffResult)
	go func() {
		var output string
		var err error
		retry := false
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if retry {
				time.Sleep(vdiffRetryInterval)
			}
			retry = false
			if useVtctldClient {
				output, err = vc.VtctldClient.ExecuteCommandWithOutput(args...)
			} else {
				output, err = vc.VtctlClient.ExecuteCommandWithOutput(args...)
			}
			if err != nil {
				if expectError {
					result := vdiffResult{output: output, err: err}
					vdiffResultCh <- result
					return
				}
				log.Infof("vdiff error: %s", err)
				if isVDiffRetryable(err.Error()) {
					retry = true
				} else {
					result := vdiffResult{output: output, err: err}
					vdiffResultCh <- result
					return
				}
			}
			if isVDiffRetryable(output) {
				retry = true
			}
			if !retry {
				result := vdiffResult{output: output, err: nil}
				vdiffResultCh <- result
				return
			}
		}
	}()
	select {
	case <-ctx.Done():
		return "", fmt.Errorf("timed out waiting for vdiff to complete")
	case result := <-vdiffResultCh:
		return result.output, result.err
	}
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
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
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
