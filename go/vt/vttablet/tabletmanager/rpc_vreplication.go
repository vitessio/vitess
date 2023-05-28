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

package tabletmanager

import (
	"context"
	"strings"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	// Retrieve the current configuration values for a workflow's vreplication stream.
	sqlSelectVRWorkflowConfig = "select id, source, cell, tablet_types from %s.vreplication where workflow = %a"
	// Update the configuration values for a workflow's vreplication stream.
	sqlUpdateVRWorkflowConfig = "update %s.vreplication set source = %a, cell = %a, tablet_types = %a where id = %a"
)

// VReplicationExec executes a vreplication command.
func (tm *TabletManager) VReplicationExec(ctx context.Context, query string) (*querypb.QueryResult, error) {
	// Replace any provided sidecar databsae qualifiers with the correct one.
	uq, err := sqlparser.ReplaceTableQualifiers(query, sidecardb.DefaultName, sidecardb.GetName())
	if err != nil {
		return nil, err
	}
	qr, err := tm.VREngine.ExecWithDBA(uq)
	if err != nil {
		return nil, err
	}
	return sqltypes.ResultToProto3(qr), nil
}

// VReplicationWaitForPos waits for the specified position.
func (tm *TabletManager) VReplicationWaitForPos(ctx context.Context, id int32, pos string) error {
	return tm.VREngine.WaitForPos(ctx, id, pos)
}

// UpdateVRWorkflow updates the sidecar databases's vreplication
// record for this tablet's vreplication workflow stream(s). If there
// is no stream for the given workflow on the tablet then a nil result
// is returned as this is expected e.g. on source tablets of a
// Reshard workflow (source and target are the same keyspace). The
// caller can consider this case an error if they choose to.
// Note: the VReplication engine creates a new controller for the
// workflow stream when the record is updated, so we also in effect
// restart the workflow stream via the update.
func (tm *TabletManager) UpdateVRWorkflow(ctx context.Context, req *tabletmanagerdatapb.UpdateVRWorkflowRequest) (*tabletmanagerdatapb.UpdateVRWorkflowResponse, error) {
	bindVars := map[string]*querypb.BindVariable{
		"wf": sqltypes.StringBindVariable(req.Workflow),
	}
	parsed := sqlparser.BuildParsedQuery(sqlSelectVRWorkflowConfig, sidecardb.GetIdentifier(), ":wf")
	stmt, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	res, err := tm.VREngine.Exec(stmt)
	if err != nil {
		return nil, err
	}
	if res == nil || len(res.Rows) == 0 {
		// No streams on this tablet to update. This is
		// expected e.g. on source tablets for Reshard
		// workflows. If callers want to treat this
		// scenario as an error they can.
		return &tabletmanagerdatapb.UpdateVRWorkflowResponse{Result: nil}, nil
	}

	row := res.Named().Row()
	id := row.AsInt64("id", 0)
	cells := strings.Split(row.AsString("cell", ""), ",")
	tabletTypes := strings.Split(row.AsString("tablet_types", ""), ",")
	bls := &binlogdatapb.BinlogSource{}
	source := row.AsBytes("source", []byte{})
	// For the string based values, we use NULL to differentiate
	// from an empty string. The NULL value indicates that we
	// should keep the existing value.
	if !textutil.ValueIsSimulatedNull(req.Cells) {
		cells = req.Cells
	}
	if !textutil.ValueIsSimulatedNull(req.TabletTypes) {
		tabletTypes = req.TabletTypes
	}
	if err = prototext.Unmarshal(source, bls); err != nil {
		return nil, err
	}
	// If we don't want to update the existing value then pass
	// the simulated NULL value of -1.
	if !textutil.ValueIsSimulatedNull(req.OnDdl) {
		bls.OnDdl = req.OnDdl
	}
	source, err = prototext.Marshal(bls)
	if err != nil {
		return nil, err
	}
	bindVars = map[string]*querypb.BindVariable{
		"sc": sqltypes.StringBindVariable(string(source)),
		"cl": sqltypes.StringBindVariable(strings.Join(cells, ",")),
		"tt": sqltypes.StringBindVariable(strings.Join(tabletTypes, ",")),
		"id": sqltypes.Int64BindVariable(id),
	}
	parsed = sqlparser.BuildParsedQuery(sqlUpdateVRWorkflowConfig, sidecardb.GetIdentifier(), ":sc", ":cl", ":tt", ":id")
	stmt, err = parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	res, err = tm.VREngine.Exec(stmt)

	if err != nil {
		return nil, err
	}
	return &tabletmanagerdatapb.UpdateVRWorkflowResponse{Result: sqltypes.ResultToProto3(res)}, nil
}
