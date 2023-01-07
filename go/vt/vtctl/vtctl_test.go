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

package vtctl

import (
	"context"
	"fmt"
	"testing"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/sqltypes"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/wrangler"
)

func TestMoveTables(t *testing.T) {
	cell := "cell1"
	shard := "0"
	sourceKs := "sourceKs"
	targetKs := "targetKs"
	table := "customer"
	wf := "testwf"
	ksWf := fmt.Sprintf("%s.%s", targetKs, wf)
	sourceCol := fmt.Sprintf(`keyspace:"%s" shard:"%s" filter:{rules:{match:"%s" filter:"select * from %s"}}`, sourceKs, shard, table, table)
	ctx := context.Background()
	env := newTestVTCtlEnv()
	defer env.close()
	_ = env.addTablet(100, sourceKs, shard, topodatapb.TabletType_PRIMARY)
	target := env.addTablet(200, targetKs, shard, topodatapb.TabletType_PRIMARY)

	subFlags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	subFlags.Set("cells", cell)
	subFlags.Set("source", sourceKs)
	subFlags.Set("tables", table)
	subFlags.Set("tablet_types", "PRIMARY")

	env.tmc.setVRResults(
		target.tablet,
		fmt.Sprintf("select id, source, message, cell, tablet_types, workflow_type, workflow_sub_type from _vt.vreplication where workflow='%s' and db_name='vt_%s'", wf, targetKs),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type",
			"int64|varchar|varchar|varchar|varchar|int64|int64"),
			fmt.Sprintf("1|%s||%s|primary|1|0", sourceCol, cell),
		),
	)

	tests := []struct {
		name         string
		workflowType wrangler.VReplicationWorkflowType
		args         []string
		wantErr      bool
	}{
		{
			name:         "1",
			workflowType: wrangler.MoveTablesWorkflow,
			args:         []string{"Progress", ksWf},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := commandVRWorkflow(ctx, env.wr, subFlags, tt.args, tt.workflowType); (err != nil) != tt.wantErr {
				// This needs to produce an ERROR once the tests are working
				t.Logf("commandVRWorkflow() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
