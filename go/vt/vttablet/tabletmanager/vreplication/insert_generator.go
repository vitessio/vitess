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

package vreplication

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/throttler"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// InsertGenerator generates a vreplication insert statement.
type InsertGenerator struct {
	buf    *strings.Builder
	prefix string

	state  string
	dbname string
	now    int64
}

// NewInsertGenerator creates a new InsertGenerator.
func NewInsertGenerator(state binlogdatapb.VReplicationWorkflowState, dbname string) *InsertGenerator {
	buf := &strings.Builder{}
	buf.WriteString("insert into _vt.vreplication(workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys, options) values ")
	return &InsertGenerator{
		buf:    buf,
		state:  state.String(),
		dbname: dbname,
		now:    time.Now().Unix(),
	}
}

// AddRow adds a row to the insert statement. You should use AddRowWithOptions in new code
// if you cannot avoid using this function (you should instead use the new tabletmanager
// RPCs instead whenever possible).
func (ig *InsertGenerator) AddRow(workflow string, bls *binlogdatapb.BinlogSource, pos, cell, tabletTypes string,
	workflowType binlogdatapb.VReplicationWorkflowType, workflowSubType binlogdatapb.VReplicationWorkflowSubType,
	deferSecondaryKeys bool) {
	ig.AddRowWithOptions(workflow, bls, pos, cell, tabletTypes, workflowType, workflowSubType, deferSecondaryKeys, nil)
}
func (ig *InsertGenerator) AddRowWithOptions(workflow string, bls *binlogdatapb.BinlogSource, pos, cell, tabletTypes string,
	workflowType binlogdatapb.VReplicationWorkflowType, workflowSubType binlogdatapb.VReplicationWorkflowSubType,
	deferSecondaryKeys bool, workflowOptions *vtctldatapb.WorkflowOptions) {
	protoutil.SortBinlogSourceTables(bls)
	ob, err := protojson.Marshal(workflowOptions)
	if err != nil || ob == nil {
		log.Errorf("Error marshaling workflow options: %v, value will be ignored\n", err)
		ob = []byte("{}")
	}
	fmt.Fprintf(ig.buf, "%s(%v, %v, %v, %v, %v, %v, %v, %v, 0, '%v', %v, %d, %d, %v, %v)",
		ig.prefix,
		encodeString(workflow),
		encodeString(bls.String()),
		encodeString(pos),
		throttler.MaxRateModuleDisabled,
		throttler.ReplicationLagModuleDisabled,
		encodeString(cell),
		encodeString(tabletTypes),
		ig.now,
		ig.state,
		encodeString(ig.dbname),
		workflowType,
		workflowSubType,
		deferSecondaryKeys,
		fmt.Sprintf("'%s'", string(ob)),
	)
	ig.prefix = ", "
}

// String returns the generated statement.
func (ig *InsertGenerator) String() string {
	return ig.buf.String()
}
