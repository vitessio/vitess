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
	"strings"
	"testing"
	"time"
)

// MoveTablesWorkflow is used to store the information needed to run
// MoveTables commands.
type MoveTablesWorkflow struct {
	t               *testing.T
	clusterInstance *LocalProcessCluster
	workflowName    string
	targetKs        string
	srcKs           string
	tables          string
	tabletTypes     []string
}

// NewMoveTables creates a new MoveTablesWorkflow.
func NewMoveTables(t *testing.T, clusterInstance *LocalProcessCluster, workflowName, targetKs, srcKs, tables string, tabletTypes []string) *MoveTablesWorkflow {
	return &MoveTablesWorkflow{
		t:               t,
		clusterInstance: clusterInstance,
		workflowName:    workflowName,
		tables:          tables,
		targetKs:        targetKs,
		srcKs:           srcKs,
		tabletTypes:     tabletTypes,
	}
}

func (mtw *MoveTablesWorkflow) Create() (string, error) {
	args := []string{"Create", "--source-keyspace=" + mtw.srcKs}
	if mtw.tables != "" {
		args = append(args, "--tables="+mtw.tables)
	} else {
		args = append(args, "--all-tables")
	}
	if len(mtw.tabletTypes) != 0 {
		args = append(args, "--tablet-types")
		args = append(args, strings.Join(mtw.tabletTypes, ","))
	}
	return mtw.exec(args...)
}

func (mtw *MoveTablesWorkflow) exec(args ...string) (string, error) {
	args2 := []string{"MoveTables", "--workflow=" + mtw.workflowName, "--target-keyspace=" + mtw.targetKs}
	args2 = append(args2, args...)
	return mtw.clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(args2...)
}

func (mtw *MoveTablesWorkflow) SwitchReadsAndWrites() (string, error) {
	return mtw.exec("SwitchTraffic")
}

func (mtw *MoveTablesWorkflow) ReverseReadsAndWrites() (string, error) {
	return mtw.exec("ReverseTraffic")
}

func (mtw *MoveTablesWorkflow) Cancel() (string, error) {
	return mtw.exec("Cancel")
}

func (mtw *MoveTablesWorkflow) Complete() (string, error) {
	return mtw.exec("Complete")
}

func (mtw *MoveTablesWorkflow) Show() (string, error) {
	return mtw.exec("Show")
}

func (mtw *MoveTablesWorkflow) WaitForVreplCatchup(timeToWait time.Duration) {
	for _, ks := range mtw.clusterInstance.Keyspaces {
		if ks.Name != mtw.targetKs {
			continue
		}
		for _, shard := range ks.Shards {
			vttablet := shard.PrimaryTablet().VttabletProcess
			vttablet.WaitForVReplicationToCatchup(mtw.t, mtw.workflowName, "vt_"+vttablet.Keyspace, "", timeToWait)
		}
	}
}
