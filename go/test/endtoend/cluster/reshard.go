/*
Copyright 2024 The Vitess Authors.

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
	"slices"
	"strings"
	"testing"
	"time"
)

// ReshardWorkflow is used to store the information needed to run
// Reshard commands.
type ReshardWorkflow struct {
	t               *testing.T
	clusterInstance *LocalProcessCluster
	workflowName    string
	targetKs        string
	sourceShards    string
	targetShards    string
}

// NewReshard creates a new ReshardWorkflow.
func NewReshard(t *testing.T, clusterInstance *LocalProcessCluster, workflowName, targetKs, targetShards, srcShards string) *ReshardWorkflow {
	return &ReshardWorkflow{
		t:               t,
		clusterInstance: clusterInstance,
		workflowName:    workflowName,
		targetKs:        targetKs,
		sourceShards:    srcShards,
		targetShards:    targetShards,
	}
}

func (rw *ReshardWorkflow) Create() (string, error) {
	args := []string{"Create"}
	if rw.sourceShards != "" {
		args = append(args, "--source-shards="+rw.sourceShards)
	}
	if rw.targetShards != "" {
		args = append(args, "--target-shards="+rw.targetShards)
	}

	return rw.exec(args...)
}

func (rw *ReshardWorkflow) exec(args ...string) (string, error) {
	args2 := []string{"Reshard", "--workflow=" + rw.workflowName, "--target-keyspace=" + rw.targetKs}
	args2 = append(args2, args...)
	return rw.clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(args2...)
}

func (rw *ReshardWorkflow) SwitchReadsAndWrites() (string, error) {
	return rw.exec("SwitchTraffic")
}

func (rw *ReshardWorkflow) ReverseReadsAndWrites() (string, error) {
	return rw.exec("ReverseTraffic")
}

func (rw *ReshardWorkflow) Cancel() (string, error) {
	return rw.exec("Cancel")
}

func (rw *ReshardWorkflow) Complete() (string, error) {
	return rw.exec("Complete")
}

func (rw *ReshardWorkflow) Show() (string, error) {
	return rw.exec("Show")
}

func (rw *ReshardWorkflow) WaitForVreplCatchup(timeToWait time.Duration) {
	targetShards := strings.Split(rw.targetShards, ",")
	for _, ks := range rw.clusterInstance.Keyspaces {
		if ks.Name != rw.targetKs {
			continue
		}
		for _, shard := range ks.Shards {
			if !slices.Contains(targetShards, shard.Name) {
				continue
			}
			vttablet := shard.FindPrimaryTablet().VttabletProcess
			vttablet.WaitForVReplicationToCatchup(rw.t, rw.workflowName, "vt_"+vttablet.Keyspace, "", timeToWait)
		}
	}
}
