/*
Copyright 2023 The Vitess Authors.

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
	"sync"
	"testing"
	"time"

	"github.com/tidwall/gjson"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestMultipleConcurrentVDiffs(t *testing.T) {
	cellName := "zone1"
	vc = NewVitessCluster(t, nil)
	defer vc.TearDown()

	sourceKeyspace := "product"
	shardName := "0"

	cell := vc.Cells[cellName]
	vc.AddKeyspace(t, []*Cell{cell}, sourceKeyspace, shardName, initialProductVSchema, initialProductSchema, 0, 0, 100, sourceKsOpts)

	verifyClusterHealth(t, vc)
	insertInitialData(t)
	targetTabletId := 200
	targetKeyspace := "customer"
	vc.AddKeyspace(t, []*Cell{cell}, targetKeyspace, shardName, initialProductVSchema, initialProductSchema, 0, 0, targetTabletId, sourceKsOpts)

	index := 1000
	var loadCtx context.Context
	var loadCancel context.CancelFunc
	loadCtx, loadCancel = context.WithCancel(context.Background())
	load := func(tableName string) {
		query := "insert into %s(cid, name) values(%d, 'customer-%d')"
		for {
			select {
			case <-loadCtx.Done():
				log.Infof("load cancelled")
				return
			default:
				index += 1
				vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
				q := fmt.Sprintf(query, tableName, index, index)
				vtgateConn.ExecuteFetch(q, 1000, false)
				vtgateConn.Close()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	targetKs := vc.Cells[cellName].Keyspaces[targetKeyspace]
	targetTab := targetKs.Shards["0"].Tablets[fmt.Sprintf("%s-%d", cellName, targetTabletId)].Vttablet
	require.NotNil(t, targetTab)

	time.Sleep(15 * time.Second) // wait for some rows to be inserted.

	createWorkflow := func(workflowName, tables string) {
		mt := newMoveTables(vc, &moveTablesWorkflow{
			workflowInfo: &workflowInfo{
				vc:             vc,
				workflowName:   workflowName,
				targetKeyspace: targetKeyspace,
				tabletTypes:    "primary",
			},
			sourceKeyspace: sourceKeyspace,
			tables:         tables,
		}, workflowFlavorVtctld)
		mt.Create()
		waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKeyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())
		catchup(t, targetTab, workflowName, "MoveTables")
	}

	createWorkflow("wf1", "customer")
	createWorkflow("wf2", "customer2")

	go load("customer")
	go load("customer2")

	var wg sync.WaitGroup
	wg.Add(2)

	doVdiff := func(workflowName, table string) {
		defer wg.Done()
		vdiff(t, targetKeyspace, workflowName, cellName, true, false, nil)
	}
	go doVdiff("wf1", "customer")
	go doVdiff("wf2", "customer2")
	wg.Wait()
	loadCancel()

	// confirm that show all shows the correct workflow and only that workflow.
	output, err := vc.VtctldClient.ExecuteCommandWithOutput("VDiff", "--format", "json", "--workflow", "wf1", "--target-keyspace", "customer", "show", "all")
	require.NoError(t, err)
	log.Infof("VDiff output: %s", output)
	count := gjson.Get(output, "..#").Int()
	wf := gjson.Get(output, "0.Workflow").String()
	ksName := gjson.Get(output, "0.Keyspace").String()
	require.Equal(t, int64(1), count)
	require.Equal(t, "wf1", wf)
	require.Equal(t, "customer", ksName)
}
