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
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtctl/workflow"
	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	// Create a new VReplication workflow record.
	sqlCreateVRWorkflow = "insert into %s.vreplication (workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys) values (%a, %a, '', 0, 0, %a, %a, now(), 0, %a, %a, %a, %a, %a)"
	// Read a VReplication workflow.
	sqlReadVRWorkflow = "select id, source, pos, stop_pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name, rows_copied, tags, time_heartbeat, workflow_type, time_throttled, component_throttled, workflow_sub_type, defer_secondary_keys from %s.vreplication where workflow = %a and db_name = %a"
	// Delete VReplication records for the given workflow.
	sqlDeleteVRWorkflow = "delete from %s.vreplication where workflow = %a and db_name = %a"
	// Retrieve the current configuration values for a workflow's vreplication stream.
	sqlSelectVRWorkflowConfig = "select id, source, cell, tablet_types, state, message from %s.vreplication where workflow = %a"
	// Update the configuration values for a workflow's vreplication stream.
	sqlUpdateVRWorkflowConfig = "update %s.vreplication set state = %a, source = %a, cell = %a, tablet_types = %a where id = %a"
)

func (tm *TabletManager) CreateVRWorkflow(ctx context.Context, req *tabletmanagerdatapb.CreateVRWorkflowRequest) (*tabletmanagerdatapb.CreateVRWorkflowResponse, error) {
	if req == nil || len(req.BinlogSource) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid request, no binlog source specified")
	}
	res := &sqltypes.Result{}
	for _, bls := range req.BinlogSource {
		source, err := prototext.Marshal(bls)
		if err != nil {
			return nil, err
		}
		// Use the local cell if none are specified.
		if len(req.Cells) == 0 || strings.TrimSpace(req.Cells[0]) == "" {
			req.Cells = append(req.Cells, tm.Tablet().Alias.Cell)
		}
		wfState := "Stopped"
		bindVars := map[string]*querypb.BindVariable{
			"wf":  sqltypes.StringBindVariable(req.Workflow),
			"sc":  sqltypes.StringBindVariable(string(source)),
			"cl":  sqltypes.StringBindVariable(strings.Join(req.Cells, ",")),
			"tt":  sqltypes.StringBindVariable(strings.Join(req.TabletTypes, ",")),
			"st":  sqltypes.StringBindVariable(wfState),
			"db":  sqltypes.StringBindVariable(tm.DBConfigs.DBName),
			"wt":  sqltypes.Int64BindVariable(int64(req.WorkflowType)),
			"wst": sqltypes.Int64BindVariable(int64(req.WorkflowSubType)),
			"ds":  sqltypes.BoolBindVariable(req.DeferSecondaryKeys),
		}
		parsed := sqlparser.BuildParsedQuery(sqlCreateVRWorkflow, sidecardb.GetIdentifier(), ":wf", ":sc", ":cl", ":tt", ":st", ":db", ":wt", ":wst", ":ds")
		stmt, err := parsed.GenerateQuery(bindVars, nil)
		if err != nil {
			return nil, err
		}
		streamres, err := tm.VREngine.Exec(stmt)

		if err != nil {
			return nil, err
		}
		res.RowsAffected += streamres.RowsAffected
	}
	return &tabletmanagerdatapb.CreateVRWorkflowResponse{Result: sqltypes.ResultToProto3(res)}, nil
}

func (tm *TabletManager) DeleteVRWorkflow(ctx context.Context, req *tabletmanagerdatapb.DeleteVRWorkflowRequest) (*tabletmanagerdatapb.DeleteVRWorkflowResponse, error) {
	res := &sqltypes.Result{}
	bindVars := map[string]*querypb.BindVariable{
		"wf": sqltypes.StringBindVariable(req.Workflow),
		"db": sqltypes.StringBindVariable(tm.DBConfigs.DBName),
	}
	parsed := sqlparser.BuildParsedQuery(sqlDeleteVRWorkflow, sidecardb.GetIdentifier(), ":wf", ":db")
	stmt, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	streamres, err := tm.VREngine.Exec(stmt)

	if err != nil {
		return nil, err
	}
	res.RowsAffected += streamres.RowsAffected

	return &tabletmanagerdatapb.DeleteVRWorkflowResponse{Result: sqltypes.ResultToProto3(res)}, nil
}

func (tm *TabletManager) ReadVRWorkflow(ctx context.Context, req *tabletmanagerdatapb.ReadVRWorkflowRequest) (*tabletmanagerdatapb.ReadVRWorkflowResponse, error) {
	bindVars := map[string]*querypb.BindVariable{
		"wf": sqltypes.StringBindVariable(req.Workflow),
		"db": sqltypes.StringBindVariable(tm.DBConfigs.DBName),
	}
	parsed := sqlparser.BuildParsedQuery(sqlReadVRWorkflow, sidecardb.GetIdentifier(), ":wf", ":db")
	stmt, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	res, err := tm.VREngine.Exec(stmt)
	if err != nil {
		return nil, err
	}
	if res == nil || len(res.Rows) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "no VReplication workflow found with name %s on tablet %s", req.Workflow, tm.tabletAlias)
	}
	rows := res.Named().Rows
	resp := &tabletmanagerdatapb.ReadVRWorkflowResponse{Workflow: req.Workflow}
	streams := make([]*tabletmanagerdatapb.ReadVRWorkflowResponse_Stream, len(rows))

	// First the things that are common to all streams.
	if resp.Id, err = rows[0]["id"].ToInt32(); err != nil {
		return nil, vterrors.Wrap(err, "error parsing id field from vreplication table record")
	}
	resp.Cell = rows[0]["cell"].ToString()
	resp.TabletTypes = rows[0]["tablet_types"].ToString()
	resp.DbName = rows[0]["db_name"].ToString()
	resp.Tags = rows[0]["tags"].ToString()
	if resp.WorkflowType, err = rows[0]["workflow_type"].ToInt32(); err != nil {
		return nil, vterrors.Wrap(err, "error parsing workflow_type field from vreplication table record")
	}
	if resp.WorkflowSubType, err = rows[0]["workflow_sub_type"].ToInt32(); err != nil {
		return nil, vterrors.Wrap(err, "error parsing workflow_sub_type field from vreplication table record")
	}
	resp.DeferSecondaryKeys = rows[0]["defer_secondary_keys"].ToString() == "1"

	// Now the individual streams (there can be more than 1 with shard merges).
	for i, row := range rows {
		srcBytes, err := row["source"].ToBytes()
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing binlog_source field from vreplication table record")
		}
		blspb := &binlogdatapb.BinlogSource{}
		err = proto.Unmarshal(srcBytes, blspb)
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing binlog_source field from vreplication table record")
		}
		streams[i] = &tabletmanagerdatapb.ReadVRWorkflowResponse_Stream{
			Bls: blspb,
		}
		streams[i].Pos = row["pos"].ToString()
		streams[i].StopPos = row["stop_pos"].ToString()
		if streams[i].MaxTps, err = row["max_tps"].ToInt64(); err != nil {
			return nil, vterrors.Wrap(err, "error parsing max_tps field from vreplication table record")
		}
		if streams[i].MaxReplicationLag, err = row["max_replication_lag"].ToInt64(); err != nil {
			return nil, vterrors.Wrap(err, "error parsing max_replication_lag field from vreplication table record")
		}
		timeUpdated, err := row["time_updated"].ToInt64()
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing time_updated field from vreplication table record")
		}
		streams[i].TimeUpdated = &vttime.Time{Seconds: timeUpdated}
		txTimestamp, err := row["transaction_timestamp"].ToInt64()
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing transaction_timestamp field from vreplication table record")
		}
		streams[i].TransactionTimestamp = &vttime.Time{Seconds: txTimestamp}
		streams[i].State = row["state"].ToString()
		streams[i].Message = row["message"].ToString()
		if streams[i].RowsCopied, err = row["rows_copied"].ToInt64(); err != nil {
			return nil, vterrors.Wrap(err, "error parsing rows_copied field from vreplication table record")
		}
		timeHeartbeat, err := row["time_heartbeat"].ToInt64()
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing time_heartbeat field from vreplication table record")
		}
		streams[i].TimeHeartbeat = &vttime.Time{Seconds: timeHeartbeat}
		timeThrottled, err := row["time_throttled"].ToInt64()
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing time_throttled field from vreplication table record")
		}
		streams[i].TimeThrottled = &vttime.Time{Seconds: timeThrottled}
		streams[i].ComponentThrottled = row["component_throttled"].ToString()
	}

	return resp, nil
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
	state := row.AsString("state", "")
	message := row.AsString("message", "")
	if strings.ToUpper(req.State) == workflow.Running && strings.ToUpper(message) == workflow.Frozen {
		return &tabletmanagerdatapb.UpdateVRWorkflowResponse{Result: nil},
			vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "cannot start a workflow when it is frozen")
	}
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
	if !textutil.ValueIsSimulatedNull(req.State) {
		state = req.State
	}
	bindVars = map[string]*querypb.BindVariable{
		"st": sqltypes.StringBindVariable(state),
		"sc": sqltypes.StringBindVariable(string(source)),
		"cl": sqltypes.StringBindVariable(strings.Join(cells, ",")),
		"tt": sqltypes.StringBindVariable(strings.Join(tabletTypes, ",")),
		"id": sqltypes.Int64BindVariable(id),
	}
	parsed = sqlparser.BuildParsedQuery(sqlUpdateVRWorkflowConfig, sidecardb.GetIdentifier(), ":st", ":sc", ":cl", ":tt", ":id")
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
