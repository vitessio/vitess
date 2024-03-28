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
	"strings"

	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/workflow"
	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	// Create a new VReplication workflow record.
	sqlCreateVReplicationWorkflow = "insert into %s.vreplication (workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys) values (%a, %a, '', 0, 0, %a, %a, now(), 0, %a, %a, %a, %a, %a)"
	sqlHasVReplicationWorkflows   = "select if(count(*) > 0, 1, 0) as has_workflows from %s.vreplication where db_name = %a"
	// Read all VReplication workflows. The final format specifier is used to
	// optionally add any additional predicates to the query.
	sqlReadVReplicationWorkflows = "select workflow, id, source, pos, stop_pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, message, db_name, rows_copied, tags, time_heartbeat, workflow_type, time_throttled, component_throttled, workflow_sub_type, defer_secondary_keys from %s.vreplication where db_name = %a%s group by workflow, id order by workflow, id"
	// Read a VReplication workflow.
	sqlReadVReplicationWorkflow = "select id, source, pos, stop_pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, message, db_name, rows_copied, tags, time_heartbeat, workflow_type, time_throttled, component_throttled, workflow_sub_type, defer_secondary_keys from %s.vreplication where workflow = %a and db_name = %a"
	// Delete VReplication records for the given workflow.
	sqlDeleteVReplicationWorkflow = "delete from %s.vreplication where workflow = %a and db_name = %a"
	// Retrieve the current configuration values for a workflow's vreplication stream(s).
	sqlSelectVReplicationWorkflowConfig = "select id, source, cell, tablet_types, state, message from %s.vreplication where workflow = %a"
	// Update the configuration values for a workflow's vreplication stream.
	sqlUpdateVReplicationWorkflowStreamConfig = "update %s.vreplication set state = %a, source = %a, cell = %a, tablet_types = %a where id = %a"
	// Update field values for multiple workflows. The final format specifier is
	// used to optionally add any additional predicates to the query.
	sqlUpdateVReplicationWorkflows = "update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ %s.vreplication set%s where db_name = '%s'%s"
)

var (
	errNoFieldsToUpdate               = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "no field values provided to update")
	errAllWithIncludeExcludeWorkflows = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot specify all workflows along with either of include or exclude workflows")
)

func (tm *TabletManager) CreateVReplicationWorkflow(ctx context.Context, req *tabletmanagerdatapb.CreateVReplicationWorkflowRequest) (*tabletmanagerdatapb.CreateVReplicationWorkflowResponse, error) {
	if req == nil || len(req.BinlogSource) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid request, no binlog source specified")
	}
	res := &sqltypes.Result{}
	for _, bls := range req.BinlogSource {
		protoutil.SortBinlogSourceTables(bls)
		source, err := prototext.Marshal(bls)
		if err != nil {
			return nil, err
		}
		// Use the local cell if none are specified.
		if len(req.Cells) == 0 || strings.TrimSpace(req.Cells[0]) == "" {
			req.Cells = []string{tm.Tablet().Alias.Cell}
		}
		wfState := binlogdatapb.VReplicationWorkflowState_Stopped.String()
		tabletTypesStr := topoproto.MakeStringTypeCSV(req.TabletTypes)
		if req.TabletSelectionPreference == tabletmanagerdatapb.TabletSelectionPreference_INORDER {
			tabletTypesStr = discovery.InOrderHint + tabletTypesStr
		}
		bindVars := map[string]*querypb.BindVariable{
			"workflow":           sqltypes.StringBindVariable(req.Workflow),
			"source":             sqltypes.StringBindVariable(string(source)),
			"cells":              sqltypes.StringBindVariable(strings.Join(req.Cells, ",")),
			"tabletTypes":        sqltypes.StringBindVariable(tabletTypesStr),
			"state":              sqltypes.StringBindVariable(wfState),
			"dbname":             sqltypes.StringBindVariable(tm.DBConfigs.DBName),
			"workflowType":       sqltypes.Int64BindVariable(int64(req.WorkflowType)),
			"workflowSubType":    sqltypes.Int64BindVariable(int64(req.WorkflowSubType)),
			"deferSecondaryKeys": sqltypes.BoolBindVariable(req.DeferSecondaryKeys),
		}
		parsed := sqlparser.BuildParsedQuery(sqlCreateVReplicationWorkflow, sidecar.GetIdentifier(),
			":workflow", ":source", ":cells", ":tabletTypes", ":state", ":dbname", ":workflowType", ":workflowSubType", ":deferSecondaryKeys",
		)
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
	return &tabletmanagerdatapb.CreateVReplicationWorkflowResponse{Result: sqltypes.ResultToProto3(res)}, nil
}

func (tm *TabletManager) DeleteVReplicationWorkflow(ctx context.Context, req *tabletmanagerdatapb.DeleteVReplicationWorkflowRequest) (*tabletmanagerdatapb.DeleteVReplicationWorkflowResponse, error) {
	if req == nil || req.Workflow == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid request, no workflow provided")
	}
	res := &sqltypes.Result{}
	bindVars := map[string]*querypb.BindVariable{
		"wf": sqltypes.StringBindVariable(req.Workflow),
		"db": sqltypes.StringBindVariable(tm.DBConfigs.DBName),
	}
	parsed := sqlparser.BuildParsedQuery(sqlDeleteVReplicationWorkflow, sidecar.GetIdentifier(), ":wf", ":db")
	stmt, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	streamres, err := tm.VREngine.Exec(stmt)

	if err != nil {
		return nil, err
	}
	res.RowsAffected += streamres.RowsAffected

	return &tabletmanagerdatapb.DeleteVReplicationWorkflowResponse{Result: sqltypes.ResultToProto3(res)}, nil
}

func (tm *TabletManager) HasVReplicationWorkflows(ctx context.Context, req *tabletmanagerdatapb.HasVReplicationWorkflowsRequest) (*tabletmanagerdatapb.HasVReplicationWorkflowsResponse, error) {
	bindVars := map[string]*querypb.BindVariable{
		"db": sqltypes.StringBindVariable(tm.DBConfigs.DBName),
	}
	parsed := sqlparser.BuildParsedQuery(sqlHasVReplicationWorkflows, sidecar.GetIdentifier(), ":db")
	stmt, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	res, err := tm.VREngine.Exec(stmt)
	if err != nil {
		return nil, err
	}
	// This should never occur. Let the caller decide how to treat it.
	if res == nil || len(res.Rows) == 0 {
		return nil, nil
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected response to query %q: expected 1 row with 1 column but got %d row(s) with %d column(s)",
			parsed.Query, len(res.Rows), len(res.Rows[0]))
	}
	has, err := res.Rows[0][0].ToBool()
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected response to query %q: could not convert %q to boolean",
			parsed.Query, res.Rows[0][0].ToString())
	}

	return &tabletmanagerdatapb.HasVReplicationWorkflowsResponse{Has: has}, nil
}

func (tm *TabletManager) ReadVReplicationWorkflows(ctx context.Context, req *tabletmanagerdatapb.ReadVReplicationWorkflowsRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse, error) {
	query, err := tm.buildReadVReplicationWorkflowsQuery(req)
	if err != nil {
		return nil, err
	}
	res, err := tm.VREngine.Exec(query)
	if err != nil {
		return nil, err
	}
	resp := &tabletmanagerdatapb.ReadVReplicationWorkflowsResponse{}
	if res == nil || len(res.Rows) == 0 {
		return resp, nil
	}
	rows := res.Named().Rows
	workflows := make(map[string]*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, len(rows))

	for _, row := range rows {
		workflow := row["workflow"].ToString()
		if workflows[workflow] == nil {
			workflows[workflow] = &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{Workflow: workflow}
		}
		workflows[workflow].Cells = row["cell"].ToString()
		tabletTypes, inorder, err := discovery.ParseTabletTypesAndOrder(row["tablet_types"].ToString())
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing the tablet_types field from vreplication table record")
		}
		workflows[workflow].TabletTypes = tabletTypes
		workflows[workflow].TabletSelectionPreference = tabletmanagerdatapb.TabletSelectionPreference_ANY
		if inorder {
			workflows[workflow].TabletSelectionPreference = tabletmanagerdatapb.TabletSelectionPreference_INORDER
		}
		workflows[workflow].DbName = row["db_name"].ToString()
		workflows[workflow].Tags = row["tags"].ToString()
		wft, err := row["workflow_type"].ToInt32()
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing workflow_type field from vreplication table record")
		}
		workflows[workflow].WorkflowType = binlogdatapb.VReplicationWorkflowType(wft)
		wfst, err := row["workflow_sub_type"].ToInt32()
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing workflow_sub_type field from vreplication table record")
		}
		workflows[workflow].WorkflowSubType = binlogdatapb.VReplicationWorkflowSubType(wfst)
		workflows[workflow].DeferSecondaryKeys = row["defer_secondary_keys"].ToString() == "1"

		// Now the individual streams (there can be more than 1 with shard merges).
		if workflows[workflow].Streams == nil {
			workflows[workflow].Streams = make([]*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream, 0, 1)
		}
		stream := &tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{}
		if stream.Id, err = row["id"].ToInt32(); err != nil {
			return nil, vterrors.Wrap(err, "error parsing id field from vreplication table record")
		}
		srcBytes, err := row["source"].ToBytes()
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing binlog_source field from vreplication table record")
		}
		bls := &binlogdatapb.BinlogSource{}
		err = prototext.Unmarshal(srcBytes, bls)
		if err != nil {
			return nil, vterrors.Wrap(err, "error unmarshaling binlog_source field from vreplication table record")
		}
		stream.Bls = bls
		stream.Pos = row["pos"].ToString()
		stream.StopPos = row["stop_pos"].ToString()
		if stream.MaxTps, err = row["max_tps"].ToInt64(); err != nil {
			return nil, vterrors.Wrap(err, "error parsing max_tps field from vreplication table record")
		}
		if stream.MaxReplicationLag, err = row["max_replication_lag"].ToInt64(); err != nil {
			return nil, vterrors.Wrap(err, "error parsing max_replication_lag field from vreplication table record")
		}
		timeUpdated, err := row["time_updated"].ToInt64()
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing time_updated field from vreplication table record")
		}
		stream.TimeUpdated = &vttime.Time{Seconds: timeUpdated}
		txTimestamp, err := row["transaction_timestamp"].ToInt64()
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing transaction_timestamp field from vreplication table record")
		}
		stream.TransactionTimestamp = &vttime.Time{Seconds: txTimestamp}
		stream.State = binlogdatapb.VReplicationWorkflowState(binlogdatapb.VReplicationWorkflowState_value[row["state"].ToString()])
		stream.Message = row["message"].ToString()
		if stream.RowsCopied, err = row["rows_copied"].ToInt64(); err != nil {
			return nil, vterrors.Wrap(err, "error parsing rows_copied field from vreplication table record")
		}
		timeHeartbeat, err := row["time_heartbeat"].ToInt64()
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing time_heartbeat field from vreplication table record")
		}
		stream.TimeHeartbeat = &vttime.Time{Seconds: timeHeartbeat}
		timeThrottled, err := row["time_throttled"].ToInt64()
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing time_throttled field from vreplication table record")
		}
		stream.TimeThrottled = &vttime.Time{Seconds: timeThrottled}
		stream.ComponentThrottled = row["component_throttled"].ToString()
		workflows[workflow].Streams = append(workflows[workflow].Streams, stream)
	}
	resp.Workflows = maps.Values(workflows)

	return resp, nil
}

func (tm *TabletManager) ReadVReplicationWorkflow(ctx context.Context, req *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
	if req == nil || req.Workflow == "" {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid request, no workflow provided")
	}
	bindVars := map[string]*querypb.BindVariable{
		"wf": sqltypes.StringBindVariable(req.Workflow),
		"db": sqltypes.StringBindVariable(tm.DBConfigs.DBName),
	}
	parsed := sqlparser.BuildParsedQuery(sqlReadVReplicationWorkflow, sidecar.GetIdentifier(), ":wf", ":db")
	stmt, err := parsed.GenerateQuery(bindVars, nil)
	if err != nil {
		return nil, err
	}
	res, err := tm.VREngine.Exec(stmt)
	if err != nil {
		return nil, err
	}
	if res == nil || len(res.Rows) == 0 {
		return nil, nil
	}
	rows := res.Named().Rows
	resp := &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{Workflow: req.Workflow}
	streams := make([]*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream, len(rows))

	// First the things that are common to all streams.
	resp.Cells = rows[0]["cell"].ToString()
	tabletTypes, inorder, err := discovery.ParseTabletTypesAndOrder(rows[0]["tablet_types"].ToString())
	if err != nil {
		return nil, vterrors.Wrap(err, "error parsing the tablet_types field from vreplication table record")
	}
	resp.TabletTypes = tabletTypes
	resp.TabletSelectionPreference = tabletmanagerdatapb.TabletSelectionPreference_ANY
	if inorder {
		resp.TabletSelectionPreference = tabletmanagerdatapb.TabletSelectionPreference_INORDER
	}
	resp.DbName = rows[0]["db_name"].ToString()
	resp.Tags = rows[0]["tags"].ToString()
	wft, err := rows[0]["workflow_type"].ToInt32()
	if err != nil {
		return nil, vterrors.Wrap(err, "error parsing workflow_type field from vreplication table record")
	}
	resp.WorkflowType = binlogdatapb.VReplicationWorkflowType(wft)
	wfst, err := rows[0]["workflow_sub_type"].ToInt32()
	if err != nil {
		return nil, vterrors.Wrap(err, "error parsing workflow_sub_type field from vreplication table record")
	}
	resp.WorkflowSubType = binlogdatapb.VReplicationWorkflowSubType(wfst)
	resp.DeferSecondaryKeys = rows[0]["defer_secondary_keys"].ToString() == "1"

	// Now the individual streams (there can be more than 1 with shard merges).
	for i, row := range rows {
		streams[i] = &tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{}
		if streams[i].Id, err = row["id"].ToInt32(); err != nil {
			return nil, vterrors.Wrap(err, "error parsing id field from vreplication table record")
		}
		srcBytes, err := row["source"].ToBytes()
		if err != nil {
			return nil, vterrors.Wrap(err, "error parsing binlog_source field from vreplication table record")
		}
		blspb := &binlogdatapb.BinlogSource{}
		err = prototext.Unmarshal(srcBytes, blspb)
		if err != nil {
			return nil, vterrors.Wrap(err, "error unmarshaling binlog_source field from vreplication table record")
		}
		streams[i].Bls = blspb
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
		streams[i].State = binlogdatapb.VReplicationWorkflowState(binlogdatapb.VReplicationWorkflowState_value[row["state"].ToString()])
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
	resp.Streams = streams

	return resp, nil
}

// UpdateVReplicationWorkflow updates the sidecar databases's vreplication
// record(s) for this tablet's vreplication workflow stream(s). If there
// are no streams for the given workflow on the tablet then a nil result
// is returned as this is expected e.g. on source tablets of a
// Reshard workflow (source and target are the same keyspace). The
// caller can consider this case an error if they choose to.
// Note: the VReplication engine creates a new controller for the
// workflow stream when the record is updated, so we also in effect
// restart the workflow stream via the update.
func (tm *TabletManager) UpdateVReplicationWorkflow(ctx context.Context, req *tabletmanagerdatapb.UpdateVReplicationWorkflowRequest) (*tabletmanagerdatapb.UpdateVReplicationWorkflowResponse, error) {
	bindVars := map[string]*querypb.BindVariable{
		"wf": sqltypes.StringBindVariable(req.Workflow),
	}
	parsed := sqlparser.BuildParsedQuery(sqlSelectVReplicationWorkflowConfig, sidecar.GetIdentifier(), ":wf")
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
		return &tabletmanagerdatapb.UpdateVReplicationWorkflowResponse{Result: nil}, nil
	}

	for _, row := range res.Named().Rows {
		id := row.AsInt64("id", 0)
		cells := strings.Split(row.AsString("cell", ""), ",")
		for i := range cells {
			cells[i] = strings.TrimSpace(cells[i])
		}
		tabletTypes, inorder, err := discovery.ParseTabletTypesAndOrder(row.AsString("tablet_types", ""))
		if err != nil {
			return nil, err
		}
		bls := &binlogdatapb.BinlogSource{}
		source := row.AsBytes("source", []byte{})
		state := row.AsString("state", "")
		message := row.AsString("message", "")
		if req.State == binlogdatapb.VReplicationWorkflowState_Running && strings.ToUpper(message) == workflow.Frozen {
			return &tabletmanagerdatapb.UpdateVReplicationWorkflowResponse{Result: nil},
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
		tabletTypesStr := topoproto.MakeStringTypeCSV(tabletTypes)
		if (inorder && req.TabletSelectionPreference == tabletmanagerdatapb.TabletSelectionPreference_UNKNOWN) ||
			(req.TabletSelectionPreference == tabletmanagerdatapb.TabletSelectionPreference_INORDER) {
			tabletTypesStr = discovery.InOrderHint + tabletTypesStr
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
			state = binlogdatapb.VReplicationWorkflowState_name[int32(req.State)]
		}
		bindVars = map[string]*querypb.BindVariable{
			"st": sqltypes.StringBindVariable(state),
			"sc": sqltypes.StringBindVariable(string(source)),
			"cl": sqltypes.StringBindVariable(strings.Join(cells, ",")),
			"tt": sqltypes.StringBindVariable(tabletTypesStr),
			"id": sqltypes.Int64BindVariable(id),
		}
		parsed = sqlparser.BuildParsedQuery(sqlUpdateVReplicationWorkflowStreamConfig, sidecar.GetIdentifier(), ":st", ":sc", ":cl", ":tt", ":id")
		stmt, err = parsed.GenerateQuery(bindVars, nil)
		if err != nil {
			return nil, err
		}
		res, err = tm.VREngine.Exec(stmt)
		if err != nil {
			return nil, err
		}
	}

	return &tabletmanagerdatapb.UpdateVReplicationWorkflowResponse{
		Result: &querypb.QueryResult{
			RowsAffected: uint64(len(res.Rows)),
		},
	}, nil
}

// UpdateVReplicationWorkflows operates in much the same way that
// UpdateVReplicationWorkflow does, but it allows you to update the
// metadata/flow control fields -- state, message, and stop_pos -- for
// multiple workflows.
// Note: today this is only used during Reshard as all of the vreplication
// streams need to be migrated from the old shards to the new ones.
func (tm *TabletManager) UpdateVReplicationWorkflows(ctx context.Context, req *tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest) (*tabletmanagerdatapb.UpdateVReplicationWorkflowsResponse, error) {
	query, err := tm.buildUpdateVReplicationWorkflowsQuery(req)
	if err != nil {
		return nil, err
	}
	res, err := tm.VREngine.Exec(query)
	if err != nil {
		return nil, err
	}

	return &tabletmanagerdatapb.UpdateVReplicationWorkflowsResponse{
		Result: &querypb.QueryResult{
			RowsAffected: uint64(len(res.Rows)),
		},
	}, nil
}

// VReplicationExec executes a vreplication command.
func (tm *TabletManager) VReplicationExec(ctx context.Context, query string) (*querypb.QueryResult, error) {
	// Replace any provided sidecar database qualifiers with the correct one.
	uq, err := tm.Env.Parser().ReplaceTableQualifiers(query, sidecar.DefaultName, sidecar.GetName())
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

// buildReadVReplicationWorkflowsQuery builds the SQL query used to read N
// vreplication workflows based on the request.
func (tm *TabletManager) buildReadVReplicationWorkflowsQuery(req *tabletmanagerdatapb.ReadVReplicationWorkflowsRequest) (string, error) {
	bindVars := map[string]*querypb.BindVariable{
		"db": sqltypes.StringBindVariable(tm.DBConfigs.DBName),
	}

	additionalPredicates := strings.Builder{}
	if req.GetExcludeFrozen() {
		additionalPredicates.WriteString(fmt.Sprintf(" and message != '%s'", workflow.Frozen))
	}
	if len(req.GetIncludeIds()) > 0 {
		additionalPredicates.WriteString(" and id in (")
		for i, id := range req.GetIncludeIds() {
			if i > 0 {
				additionalPredicates.WriteByte(',')
			}
			additionalPredicates.WriteString(fmt.Sprintf("%d", id))
		}
		additionalPredicates.WriteByte(')')
	}
	if len(req.GetIncludeWorkflows()) > 0 {
		additionalPredicates.WriteString(" and workflow in (")
		for i, wf := range req.GetIncludeWorkflows() {
			if i > 0 {
				additionalPredicates.WriteByte(',')
			}
			additionalPredicates.WriteString(sqltypes.EncodeStringSQL(wf))
		}
		additionalPredicates.WriteByte(')')
	}
	if len(req.GetExcludeWorkflows()) > 0 {
		additionalPredicates.WriteString(" and workflow not in (")
		for i, wf := range req.GetExcludeWorkflows() {
			if i > 0 {
				additionalPredicates.WriteByte(',')
			}
			additionalPredicates.WriteString(sqltypes.EncodeStringSQL(wf))
		}
		additionalPredicates.WriteByte(')')
	}
	if len(req.GetIncludeStates()) > 0 {
		additionalPredicates.WriteString(" and state in (")
		for i, state := range req.GetIncludeStates() {
			if i > 0 {
				additionalPredicates.WriteByte(',')
			}
			additionalPredicates.WriteString(sqltypes.EncodeStringSQL(state.String()))
		}
		additionalPredicates.WriteByte(')')
	}
	if len(req.GetExcludeStates()) > 0 {
		additionalPredicates.WriteString(" and state not in (")
		for i, state := range req.GetExcludeStates() {
			if i > 0 {
				additionalPredicates.WriteByte(',')
			}
			additionalPredicates.WriteString(sqltypes.EncodeStringSQL(state.String()))
		}
		additionalPredicates.WriteByte(')')
	}

	parsed := sqlparser.BuildParsedQuery(sqlReadVReplicationWorkflows, sidecar.GetIdentifier(), ":db", additionalPredicates.String())
	return parsed.GenerateQuery(bindVars, nil)
}

// buildUpdateVReplicationWorkflowsQuery builds the SQL query used to update
// the metadata/flow control fields for N vreplication workflows based on the
// request.
func (tm *TabletManager) buildUpdateVReplicationWorkflowsQuery(req *tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest) (string, error) {
	if req.GetAllWorkflows() && (len(req.GetIncludeWorkflows()) > 0 || len(req.GetExcludeWorkflows()) > 0) {
		return "", errAllWithIncludeExcludeWorkflows
	}
	if textutil.ValueIsSimulatedNull(req.GetState()) && textutil.ValueIsSimulatedNull(req.GetMessage()) && textutil.ValueIsSimulatedNull(req.GetStopPosition()) {
		return "", errNoFieldsToUpdate
	}
	sets := strings.Builder{}
	predicates := strings.Builder{}

	// First add the SET clauses.
	if !textutil.ValueIsSimulatedNull(req.GetState()) {
		state, ok := binlogdatapb.VReplicationWorkflowState_name[int32(req.GetState())]
		if !ok {
			return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid state value: %v", req.GetState())
		}
		sets.WriteString(" state = ")
		sets.WriteString(sqltypes.EncodeStringSQL(state))
	}
	if !textutil.ValueIsSimulatedNull(req.GetMessage()) {
		if sets.Len() > 0 {
			sets.WriteByte(',')
		}
		sets.WriteString(" message = ")
		sets.WriteString(sqltypes.EncodeStringSQL(req.GetMessage()))
	}
	if !textutil.ValueIsSimulatedNull(req.GetStopPosition()) {
		if sets.Len() > 0 {
			sets.WriteByte(',')
		}
		sets.WriteString(" stop_pos = ")
		sets.WriteString(sqltypes.EncodeStringSQL(req.GetStopPosition()))
	}

	// Now add any WHERE predicate clauses.
	if len(req.GetIncludeWorkflows()) > 0 {
		predicates.WriteString(" and workflow in (")
		for i, wf := range req.GetIncludeWorkflows() {
			if i > 0 {
				predicates.WriteByte(',')
			}
			predicates.WriteString(sqltypes.EncodeStringSQL(wf))
		}
		predicates.WriteByte(')')
	}
	if len(req.GetExcludeWorkflows()) > 0 {
		predicates.WriteString(" and workflow not in (")
		for i, wf := range req.GetExcludeWorkflows() {
			if i > 0 {
				predicates.WriteByte(',')
			}
			predicates.WriteString(sqltypes.EncodeStringSQL(wf))
		}
		predicates.WriteByte(')')
	}

	return sqlparser.BuildParsedQuery(sqlUpdateVReplicationWorkflows, sidecar.GetIdentifier(), sets.String(), tm.DBConfigs.DBName, predicates.String()).Query, nil
}
