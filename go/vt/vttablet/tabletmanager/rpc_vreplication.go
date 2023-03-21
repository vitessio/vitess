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
	"fmt"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
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

func (tm *TabletManager) UpdateVRWorkflow(ctx context.Context, req *tabletmanagerdatapb.UpdateVRWorkflowRequest) (*tabletmanagerdatapb.UpdateVRWorkflowResponse, error) {
	restart := false
	query := "select id, state, source, cell, tablet_types from %s.vreplication where workflow = %a"
	bindVars := map[string]*querypb.BindVariable{
		"wf": sqltypes.StringBindVariable(req.Workflow),
	}
	parsed := sqlparser.BuildParsedQuery(query, sidecardb.GetIdentifier(), ":wf")
	stmt, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	log.Errorf("UpdateVRWorkflow: %v", stmt)
	res, err := tm.VREngine.Exec(stmt)
	if err != nil {
		return nil, err
	}
	if res == nil || len(res.Rows) == 0 {
		return nil, fmt.Errorf("no stream found for workflow %s", req.Workflow)
	}

	row := res.Named().Row()
	id := row.AsInt64("id", 0)
	state := row.AsString("state", "")
	cells := row.AsString("cells", "")
	tabletTypes := row.AsString("tablet_types", "")
	// If the stream was running then we will stop and restart it.
	if state == "Running" {
		state = "Stopped"
		restart = true
	}
	bls := &binlogdatapb.BinlogSource{}
	source := row.AsBytes("source", []byte{})
	if req.Cells != sqltypes.NULL.String() { // Update the value
		cells = req.Cells
	}
	if tabletTypes != sqltypes.NULL.String() {
		tabletTypes = req.TabletTypes // Update the value
	}
	if err = prototext.Unmarshal(source, bls); err != nil {
		return nil, err
	}
	// If we don't want to update the existing value pass -1 or any
	// other invalid value.
	if _, ok := binlogdatapb.OnDDLAction_name[int32(req.OnDdl)]; ok {
		bls.OnDdl = req.OnDdl
	}
	source, err = prototext.Marshal(bls)
	if err != nil {
		return nil, err
	}
	query = "update %s.vreplication set state = %a, source = %a, cell = %a, tablet_types = %a where id = %a"
	bindVars = map[string]*querypb.BindVariable{
		"st": sqltypes.StringBindVariable(state),
		"sc": sqltypes.StringBindVariable(string(source)),
		"cl": sqltypes.StringBindVariable(cells),
		"tt": sqltypes.StringBindVariable(tabletTypes),
		"id": sqltypes.Int64BindVariable(id),
	}
	parsed = sqlparser.BuildParsedQuery(query, sidecardb.GetIdentifier(), ":st", ":sc", ":cl", ":tt", ":id")
	stmt, err = parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	log.Errorf("UpdateVRWorkflow: %v", stmt)
	res, err = tm.VREngine.Exec(stmt)

	if err != nil {
		return nil, err
	}
	if !restart {
		return &tabletmanagerdatapb.UpdateVRWorkflowResponse{Result: sqltypes.ResultToProto3(res)}, nil
	}

	state = "Running"
	query = "update %s.vreplication set state = %a where id = %a"
	bindVars = map[string]*querypb.BindVariable{
		"st": sqltypes.StringBindVariable(state),
		"id": sqltypes.Int64BindVariable(id),
	}
	parsed = sqlparser.BuildParsedQuery(query, sidecardb.GetIdentifier(), ":st", ":id")
	stmt, err = parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	log.Errorf("UpdateVRWorkflow: %v", stmt)
	res, err = tm.VREngine.Exec(stmt)

	if err != nil {
		return nil, err
	}
	return &tabletmanagerdatapb.UpdateVRWorkflowResponse{Result: sqltypes.ResultToProto3(res)}, nil
}
