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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/movetables"
	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/workflow"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	// Create a new VReplication workflow record.
	sqlCreateVReplicationWorkflow = "insert into %s.vreplication (workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys, options) values (%a, %a, '', 0, 0, %a, %a, now(), 0, %a, %a, %a, %a, %a, %a)"
	sqlHasVReplicationWorkflows   = "select if(count(*) > 0, 1, 0) as has_workflows from %s.vreplication where db_name = %a"
	// Read all VReplication workflows. The final format specifier is used to
	// optionally add any additional predicates to the query.
	sqlReadVReplicationWorkflows = "select workflow, id, source, pos, stop_pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, message, db_name, rows_copied, tags, time_heartbeat, workflow_type, time_throttled, component_throttled, workflow_sub_type, defer_secondary_keys, options from %s.vreplication where db_name = %a%s order by workflow, id"
	// Read a VReplication workflow.
	sqlReadVReplicationWorkflow = "select id, source, pos, stop_pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, message, db_name, rows_copied, tags, time_heartbeat, workflow_type, time_throttled, component_throttled, workflow_sub_type, defer_secondary_keys, options from %s.vreplication where workflow = %a and db_name = %a"
	// Delete VReplication records for the given workflow.
	sqlDeleteVReplicationWorkflow = "delete from %s.vreplication where workflow = %a and db_name = %a"
	// Retrieve the current configuration values for a workflow's vreplication stream(s).
	sqlSelectVReplicationWorkflowConfig = "select id, source, cell, tablet_types, state, message from %s.vreplication where workflow = %a"
	// Update the configuration values for a workflow's vreplication stream.
	sqlUpdateVReplicationWorkflowStreamConfig = "update %s.vreplication set state = %a, source = %a, cell = %a, tablet_types = %a, message = %a %s where id = %a"
	// Update field values for multiple workflows. The final format specifier is
	// used to optionally add any additional predicates to the query.
	sqlUpdateVReplicationWorkflows = "update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ %s.vreplication set%s where db_name = '%s'%s"
	// Check if workflow is still copying.
	sqlGetVReplicationCopyStatus = "select distinct vrepl_id from %s.copy_state where vrepl_id = %d"
	// Validate the minimum set of permissions needed to manage vreplication metadata.
	// This is a simple check for a matching user rather than any specific user@host
	// combination. Also checks for wildcards. Note the, seemingly reverse check, `%a LIKE d.db`,
	// which is required since %a replaces the actual sidecar db name and
	// d.db is where a (potential) wildcard match is specified in a privilege grant.
	sqlValidateVReplicationPermissions = `
select count(*)>0 as good from mysql.user as u
  left join mysql.db as d on (u.user = d.user)
  left join mysql.tables_priv as t on (u.user = t.user)
where u.user = %a
  and (
    (u.select_priv = 'y' and u.insert_priv = 'y' and u.update_priv = 'y' and u.delete_priv = 'y') /* user has global privs */
    or (%a LIKE d.db escape '\\' and d.select_priv = 'y' and d.insert_priv = 'y' and d.update_priv = 'y' and d.delete_priv = 'y') /* user has db privs */
    or (%a LIKE t.db escape '\\' and t.table_name = 'vreplication'
      and find_in_set('select', t.table_priv)
      and find_in_set('insert', t.table_priv)
      and find_in_set('update', t.table_priv)
      and find_in_set('delete', t.table_priv)
    )
  )
limit 1

`
	sqlGetMaxSequenceVal   = "select max(%a) as maxval from %a.%a"
	sqlInitSequenceTable   = "insert into %a.%a (id, next_id, cache) values (0, %d, 1000) on duplicate key update next_id = if(next_id < %d, %d, next_id)"
	sqlCreateSequenceTable = "create table if not exists %a (id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence'"

	// Functional permission testing queries - test each permission individually without accessing mysql.user table
	sqlTestVReplicationSelectPermission = "select count(*) from %s.vreplication limit 1"
	sqlTestVReplicationInsertPermission = "insert into %s.vreplication (workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys, options) values (%a, '', '', 0, 0, '', '', now(), 0, 'Stopped', '__test__', 0, 0, false, '{}')"
	sqlTestVReplicationUpdatePermission = "update %s.vreplication set message = '__test_update__' where workflow = %a and db_name = '__test__'"
	sqlTestVReplicationDeletePermission = "delete from %s.vreplication where workflow = %a and db_name = '__test__'"
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
			"options":            sqltypes.StringBindVariable(req.Options),
		}
		parsed := sqlparser.BuildParsedQuery(sqlCreateVReplicationWorkflow, sidecar.GetIdentifier(),
			":workflow", ":source", ":cells", ":tabletTypes", ":state", ":dbname", ":workflowType", ":workflowSubType",
			":deferSecondaryKeys", ":options",
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

// DeleteTableData will delete data from the given tables (keys in the
// req.Tabletfilters map) using the given filter or WHERE clauses (values
// in the map). It will perform this work in batches of req.BatchSize
// until all matching rows have been deleted in all tables, or the context
// expires.
func (tm *TabletManager) DeleteTableData(ctx context.Context, req *tabletmanagerdatapb.DeleteTableDataRequest) (*tabletmanagerdatapb.DeleteTableDataResponse, error) {
	if req == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid nil request")
	}

	if len(req.TableFilters) == 0 { // Nothing to do
		return &tabletmanagerdatapb.DeleteTableDataResponse{}, nil
	}

	// So that we do them in a predictable and uniform order.
	tables := maps.Keys(req.TableFilters)
	sort.Strings(tables)

	batchSize := req.BatchSize
	if batchSize < 1 {
		batchSize = movetables.DefaultDeleteBatchSize
	}
	limit := &sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral(strconv.FormatInt(batchSize, 10))}
	// We will log some progress info every 100 delete batches.
	progressRows := uint64(batchSize * 100)

	throttledLogger := logutil.NewThrottledLogger("DeleteTableData", 1*time.Minute)
	checkIfCanceled := func() error {
		select {
		case <-ctx.Done():
			return vterrors.Wrap(ctx.Err(), "context expired while deleting data")
		default:
			return nil
		}
	}

	for _, table := range tables {
		stmt, err := tm.Env.Parser().Parse(fmt.Sprintf("delete from %s %s", table, req.TableFilters[table]))
		if err != nil {
			return nil, vterrors.Wrapf(err, "unable to build delete query for table %s", table)
		}
		del, ok := stmt.(*sqlparser.Delete)
		if !ok {
			return nil, vterrors.Wrapf(err, "unable to build delete query for table %s", table)
		}
		del.Limit = limit
		query := sqlparser.String(del)
		rowsDeleted := uint64(0)
		// Delete all of the matching rows from the table, in batches, until we've
		// deleted them all.
		log.Infof("Starting deletion of data from table %s using query %q", table, query)
		for {
			// Back off if we're causing too much load on the database with these
			// batch deletes.
			if _, ok := tm.VREngine.ThrottlerClient().ThrottleCheckOKOrWaitAppName(ctx, throttlerapp.VReplicationName); !ok {
				throttledLogger.Infof("throttling bulk data delete for table %s using query %q",
					table, query)
				if err := checkIfCanceled(); err != nil {
					return nil, err
				}
				continue
			}
			res, err := tm.ExecuteFetchAsAllPrivs(ctx,
				&tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest{
					Query:  []byte(query),
					DbName: tm.DBConfigs.DBName,
				})
			if err != nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "error deleting data using query %q: %v",
					query, err)
			}
			rowsDeleted += res.RowsAffected
			// Log some progress info periodically to give the operator some idea of
			// how much work we've done, how much is left, and how long it may take
			// (considering throttling, system performance, etc).
			if rowsDeleted%progressRows == 0 {
				log.Infof("Successfully deleted %d rows of data from table %s so far, using query %q",
					rowsDeleted, table, query)
			}
			if res.RowsAffected == 0 { // We're done with this table
				break
			}
			if err := checkIfCanceled(); err != nil {
				return nil, err
			}
		}
		log.Infof("Completed deletion of data (%d rows) from table %s using query %q",
			rowsDeleted, table, query)
	}

	return &tabletmanagerdatapb.DeleteTableDataResponse{}, nil
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
		workflows[workflow].Options = row["options"].ToString()
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
	resp.Options = rows[0]["options"].ToString()
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

func isStreamCopying(tm *TabletManager, id int64) (bool, error) {
	query := fmt.Sprintf(sqlGetVReplicationCopyStatus, sidecar.GetIdentifier(), id)
	res, err := tm.VREngine.Exec(query)
	if err != nil {
		return false, err
	}
	if res != nil && len(res.Rows) > 0 {
		return true, nil
	}
	return false, nil
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

	rowsAffected := uint64(0)
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
		if req.State != nil && *req.State == binlogdatapb.VReplicationWorkflowState_Running &&
			strings.ToUpper(message) == workflow.Frozen {
			return &tabletmanagerdatapb.UpdateVReplicationWorkflowResponse{Result: nil},
				vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "cannot start a workflow when it is frozen")
		}
		if !textutil.ValueIsSimulatedNull(req.Cells) {
			cells = req.Cells
		}
		if !textutil.ValueIsSimulatedNull(req.TabletTypes) {
			tabletTypes = req.TabletTypes
		}
		if req.Message != nil {
			message = *req.Message
		}
		tabletTypesStr := topoproto.MakeStringTypeCSV(tabletTypes)
		if req.TabletSelectionPreference != nil &&
			((inorder && *req.TabletSelectionPreference == tabletmanagerdatapb.TabletSelectionPreference_UNKNOWN) ||
				(*req.TabletSelectionPreference == tabletmanagerdatapb.TabletSelectionPreference_INORDER)) {
			tabletTypesStr = discovery.InOrderHint + tabletTypesStr
		}
		if err = prototext.Unmarshal(source, bls); err != nil {
			return nil, err
		}
		// We also need to check for a SimulatedNull here to support older clients and
		// smooth upgrades. All non-slice simulated NULL checks can be removed in v22+.
		if req.OnDdl != nil && *req.OnDdl != binlogdatapb.OnDDLAction(textutil.SimulatedNullInt) {
			bls.OnDdl = *req.OnDdl
		}
		bls.Filter.Rules = append(bls.Filter.Rules, req.FilterRules...)
		source, err = prototext.Marshal(bls)
		if err != nil {
			return nil, err
		}
		// We also need to check for a SimulatedNull here to support older clients and
		// smooth upgrades. All non-slice simulated NULL checks can be removed in v22+.
		if req.State != nil && *req.State != binlogdatapb.VReplicationWorkflowState(textutil.SimulatedNullInt) {
			state = binlogdatapb.VReplicationWorkflowState_name[int32(*req.State)]
		}
		if state == binlogdatapb.VReplicationWorkflowState_Running.String() {
			// `Workflow Start` sets the new state to Running. However, if stream is still copying tables, we should set
			// the state as Copying.
			isCopying, err := isStreamCopying(tm, id)
			if err != nil {
				return nil, err
			}
			if isCopying {
				state = binlogdatapb.VReplicationWorkflowState_Copying.String()
			}
		}
		options := getOptionSetString(req.ConfigOverrides)

		bindVars = map[string]*querypb.BindVariable{
			"st": sqltypes.StringBindVariable(state),
			"sc": sqltypes.StringBindVariable(string(source)),
			"cl": sqltypes.StringBindVariable(strings.Join(cells, ",")),
			"tt": sqltypes.StringBindVariable(tabletTypesStr),
			"ms": sqltypes.StringBindVariable(message),
			"id": sqltypes.Int64BindVariable(id),
		}
		parsed = sqlparser.BuildParsedQuery(sqlUpdateVReplicationWorkflowStreamConfig, sidecar.GetIdentifier(), ":st", ":sc", ":cl", ":tt", ":ms", options, ":id")
		stmt, err = parsed.GenerateQuery(bindVars, nil)
		if err != nil {
			return nil, err
		}
		res, err = tm.VREngine.Exec(stmt)
		if err != nil {
			return nil, err
		}
		rowsAffected += res.RowsAffected
	}

	return &tabletmanagerdatapb.UpdateVReplicationWorkflowResponse{
		Result: &querypb.QueryResult{
			RowsAffected: rowsAffected,
		},
	}, nil
}

// getOptionSetString takes the option keys passed in and creates a sql clause to update the existing options
// field in the vreplication table. The clause is built using the json_set() for new and updated options
// and json_remove() for deleted options, denoted by an empty value.
func getOptionSetString(config map[string]string) string {
	if len(config) == 0 {
		return ""
	}

	var (
		options     string
		deletedKeys []string
		keys        []string
	)
	for k, v := range config {
		if strings.TrimSpace(v) == "" {
			deletedKeys = append(deletedKeys, k)
		} else {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	sort.Strings(deletedKeys)
	clause := "options"
	if len(deletedKeys) > 0 {
		// We need to quote the key in the json functions because flag names can contain hyphens.
		clause = fmt.Sprintf("json_remove(options, '$.config.\"%s\"'", deletedKeys[0])
		for _, k := range deletedKeys[1:] {
			clause += fmt.Sprintf(", '$.config.\"%s\"'", k)
		}
		clause += ")"
	}
	if len(keys) > 0 {
		clause = fmt.Sprintf("json_set(%s, '$.config', json_object(), ", clause)
		for i, k := range keys {
			if i > 0 {
				clause += ", "
			}
			clause += fmt.Sprintf("'$.config.\"%s\"', '%s'", k, strings.TrimSpace(config[k]))
		}
		clause += ")"
	}
	options = ", options = " + clause
	return options
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
			RowsAffected: res.RowsAffected,
		},
	}, nil
}

func (tm *TabletManager) GetMaxValueForSequences(ctx context.Context, req *tabletmanagerdatapb.GetMaxValueForSequencesRequest) (*tabletmanagerdatapb.GetMaxValueForSequencesResponse, error) {
	maxValues := make(map[string]int64, len(req.Sequences))
	mu := sync.Mutex{}
	initGroup, gctx := errgroup.WithContext(ctx)
	for _, sm := range req.Sequences {
		initGroup.Go(func() error {
			maxId, err := tm.getMaxSequenceValue(gctx, sm)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			maxValues[sm.BackingTableName] = maxId
			return nil
		})
	}
	errs := initGroup.Wait()
	if errs != nil {
		return nil, errs
	}
	return &tabletmanagerdatapb.GetMaxValueForSequencesResponse{
		MaxValuesBySequenceTable: maxValues,
	}, nil
}

func (tm *TabletManager) getMaxSequenceValue(ctx context.Context, sm *tabletmanagerdatapb.GetMaxValueForSequencesRequest_SequenceMetadata) (int64, error) {
	for _, val := range []string{sm.UsingTableDbNameEscaped, sm.UsingTableNameEscaped, sm.UsingColEscaped} {
		lv := len(val)
		if lv < 3 || val[0] != '`' || val[lv-1] != '`' {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"the database (%s), table (%s), and column (%s) names must be non-empty escaped values", sm.UsingTableDbNameEscaped, sm.UsingTableNameEscaped, sm.UsingColEscaped)
		}
	}
	query := sqlparser.BuildParsedQuery(sqlGetMaxSequenceVal,
		sm.UsingColEscaped,
		sm.UsingTableDbNameEscaped,
		sm.UsingTableNameEscaped,
	)
	qr, err := tm.ExecuteFetchAsApp(ctx, &tabletmanagerdatapb.ExecuteFetchAsAppRequest{
		Query:   []byte(query.Query),
		MaxRows: 1,
	})
	if err != nil || len(qr.Rows) != 1 {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL,
			"failed to get the max used sequence value for target table %s in order to initialize the backing sequence table: %v", sm.UsingTableNameEscaped, err)
	}
	rawVal := sqltypes.Proto3ToResult(qr).Rows[0][0]
	maxID := int64(0)
	if !rawVal.IsNull() { // If it's NULL then there are no rows and 0 remains the max
		maxID, err = rawVal.ToInt64()
		if err != nil {
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to get the max used sequence value for target table %s in order to initialize the backing sequence table: %v", sm.UsingTableNameEscaped, err)
		}
	}
	return maxID, nil
}

func (tm *TabletManager) UpdateSequenceTables(ctx context.Context, req *tabletmanagerdatapb.UpdateSequenceTablesRequest) (*tabletmanagerdatapb.UpdateSequenceTablesResponse, error) {
	sequenceTables := make([]string, 0, len(req.Sequences))
	for _, sm := range req.Sequences {
		if err := tm.updateSequenceValue(ctx, sm); err != nil {
			return nil, err
		}
		sequenceTables = append(sequenceTables, sm.BackingTableName)
	}

	// It is important to reset in-memory sequence counters on the tables,
	// since it is possible for it to be outdated, this will prevent duplicate
	// key errors.
	err := tm.ResetSequences(ctx, sequenceTables)
	if err != nil {
		return nil, vterrors.Errorf(
			vtrpcpb.Code_INTERNAL, "failed to reset sequences on %q: %v",
			tm.DBConfigs.DBName, err)
	}
	return &tabletmanagerdatapb.UpdateSequenceTablesResponse{}, nil
}

func (tm *TabletManager) updateSequenceValue(ctx context.Context, seq *tabletmanagerdatapb.UpdateSequenceTablesRequest_SequenceMetadata) error {
	nextVal := seq.MaxValue + 1
	if tm.Tablet().DbNameOverride != "" {
		seq.BackingTableDbName = tm.Tablet().DbNameOverride
	}
	backingTableDbNameEscaped, err := sqlescape.EnsureEscaped(seq.BackingTableDbName)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid database name %s specified for sequence backing table: %v",
			seq.BackingTableDbName, err)
	}
	backingTableNameEscaped, err := sqlescape.EnsureEscaped(seq.BackingTableName)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid table name %s specified for sequence backing table: %v",
			seq.BackingTableName, err)
	}
	log.Infof("Updating sequence %s.%s to %d", seq.BackingTableDbName, seq.BackingTableName, nextVal)
	initQuery := sqlparser.BuildParsedQuery(sqlInitSequenceTable,
		backingTableDbNameEscaped,
		backingTableNameEscaped,
		nextVal,
		nextVal,
		nextVal,
	)
	const maxTries = 2

	for i := 0; i < maxTries; i++ {
		// Attempt to initialize the sequence.
		_, err = tm.ExecuteFetchAsApp(ctx, &tabletmanagerdatapb.ExecuteFetchAsAppRequest{
			Query:   []byte(initQuery.Query),
			MaxRows: 1,
		})
		if err == nil {
			return nil
		}

		// If the table doesn't exist, try creating it.
		sqlErr, ok := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
		if !ok || (sqlErr.Num != sqlerror.ERNoSuchTable && sqlErr.Num != sqlerror.ERBadTable) {
			return vterrors.Errorf(
				vtrpcpb.Code_INTERNAL,
				"failed to initialize the backing sequence table %s.%s: %v",
				backingTableDbNameEscaped, backingTableNameEscaped, err,
			)
		}

		if err := tm.createSequenceTable(ctx, backingTableNameEscaped); err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL,
				"failed to create the backing sequence table %s in the global-keyspace %s: %v",
				backingTableNameEscaped, tm.Tablet().Keyspace, err)
		}
		// Table has been created, so we fall through and try again on the next loop iteration.
	}

	return vterrors.Errorf(
		vtrpcpb.Code_INTERNAL, "failed to initialize the backing sequence table %s.%s after retries. Last error: %v",
		backingTableDbNameEscaped, backingTableNameEscaped, err)
}

func (tm *TabletManager) createSequenceTable(ctx context.Context, tableName string) error {
	escapedTableName, err := sqlescape.EnsureEscaped(tableName)
	if err != nil {
		return err
	}
	stmt := sqlparser.BuildParsedQuery(sqlCreateSequenceTable, escapedTableName)
	_, err = tm.ApplySchema(ctx, &tmutils.SchemaChange{
		SQL:                     stmt.Query,
		Force:                   false,
		AllowReplication:        true,
		SQLMode:                 vreplication.SQLMode,
		DisableForeignKeyChecks: true,
	})
	return err
}

// ValidateVReplicationPermissionsOld validates that the --db_filtered_user has
// the minimum permissions required on the sidecardb vreplication table
// needed in order to manage vreplication metadata.
// Switching to use a functional test approach in ValidateVReplicationPermissions below
// instead of querying mysql.user table directly as that requires permissions on the mysql.user table.
// Leaving this here for now in case we want to revert back.
func (tm *TabletManager) ValidateVReplicationPermissionsOld(ctx context.Context, req *tabletmanagerdatapb.ValidateVReplicationPermissionsRequest) (*tabletmanagerdatapb.ValidateVReplicationPermissionsResponse, error) {
	query, err := sqlparser.ParseAndBind(sqlValidateVReplicationPermissions,
		sqltypes.StringBindVariable(tm.DBConfigs.Filtered.User),
		sqltypes.StringBindVariable(sidecar.GetName()),
		sqltypes.StringBindVariable(sidecar.GetName()),
	)
	if err != nil {
		return nil, err
	}
	log.Infof("Validating VReplication permissions on %s using query %s", tm.tabletAlias, query)
	conn, err := tm.MysqlDaemon.GetAllPrivsConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	qr, err := conn.ExecuteFetch(query, 1, false)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 { // Should never happen
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected response to query %s: expected 1 row with 1 column, got: %+v",
			query, qr)
	}
	val, err := qr.Rows[0][0].ToBool()
	if err != nil { // Should never happen
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result for query %s: expected boolean-like value, got: %q",
			query, qr.Rows[0][0].ToString())
	}
	var errorString string
	if !val {
		errorString = fmt.Sprintf("user %s does not have the required set of permissions (select,insert,update,delete) on the %s.vreplication table on tablet %s",
			tm.DBConfigs.Filtered.User, sidecar.GetName(), topoproto.TabletAliasString(tm.tabletAlias))
		log.Errorf("validateVReplicationPermissions returning error: %s. Permission query run was %s", errorString, query)
	}
	return &tabletmanagerdatapb.ValidateVReplicationPermissionsResponse{
		User:  tm.DBConfigs.Filtered.User,
		Ok:    val,
		Error: errorString,
	}, nil
}

// ValidateVReplicationPermissions validates that the --db_filtered_user has
// the minimum permissions required on the sidecardb vreplication table
// using a functional testing approach that doesn't require access to mysql.user table.
func (tm *TabletManager) ValidateVReplicationPermissions(ctx context.Context, req *tabletmanagerdatapb.ValidateVReplicationPermissionsRequest) (*tabletmanagerdatapb.ValidateVReplicationPermissionsResponse, error) {
	log.Infof("Validating VReplication permissions on sidecar db %s", tm.tabletAlias)

	conn, err := tm.MysqlDaemon.GetFilteredConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if _, err := conn.ExecuteFetch("START TRANSACTION", 1, false); err != nil {
		return nil, vterrors.Wrap(err, "failed to start transaction for permission testing")
	}
	defer func() {
		_, err := conn.ExecuteFetch("ROLLBACK", 1, false)
		if err != nil {
			log.Warningf("failed to rollback transaction after permission testing: %v", err)
		}
	}()

	// Create a unique test workflow name to avoid conflicts using timestamp and random component
	testWorkflow := fmt.Sprintf("__permission_test_%d_%d", time.Now().Unix(), time.Now().Nanosecond()%1000000)
	sidecarDB := sidecar.GetName()

	permissionTests := []struct {
		permission  string
		sqlTemplate string
	}{
		{"SELECT", sqlTestVReplicationSelectPermission},
		{"INSERT", sqlTestVReplicationInsertPermission},
		{"UPDATE", sqlTestVReplicationUpdatePermission},
		{"DELETE", sqlTestVReplicationDeletePermission},
	}

	for _, test := range permissionTests {
		var query string
		var err error

		if test.permission == "SELECT" {
			parsed := sqlparser.BuildParsedQuery(test.sqlTemplate, sidecar.GetIdentifier())
			query, err = parsed.GenerateQuery(nil, nil)
		} else {
			parsed := sqlparser.BuildParsedQuery(test.sqlTemplate, sidecar.GetIdentifier(), ":workflow")
			query, err = parsed.GenerateQuery(map[string]*querypb.BindVariable{
				"workflow": sqltypes.StringBindVariable(testWorkflow),
			}, nil)
		}

		if err != nil {
			return nil, vterrors.Wrapf(err, "failed to bind %s query for permission testing", test.permission)
		}

		log.Infof("Testing %s permission using query: %s", test.permission, query)
		if _, err := conn.ExecuteFetch(query, 1, false); err != nil {
			// Check if we got `ERTableAccessDenied` error code from MySQL
			sqlErr, ok := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
			if !ok || sqlErr.Num != sqlerror.ERTableAccessDenied {
				return nil, vterrors.Wrapf(err, "error executing %s permission test query", test.permission)
			}

			return &tabletmanagerdatapb.ValidateVReplicationPermissionsResponse{
				User: tm.DBConfigs.Filtered.User,
				Ok:   false,
				Error: fmt.Sprintf("user %s does not have %s permission on %s.vreplication table on tablet %s: %v",
					tm.DBConfigs.Filtered.User, test.permission, sidecarDB, topoproto.TabletAliasString(tm.tabletAlias), err),
			}, nil
		}
	}

	log.Infof("VReplication sidecardb permission validation succeeded for user %s on tablet %s",
		tm.DBConfigs.Filtered.User, tm.tabletAlias)

	return &tabletmanagerdatapb.ValidateVReplicationPermissionsResponse{
		User:  tm.DBConfigs.Filtered.User,
		Ok:    true,
		Error: "",
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
			additionalPredicates.WriteString(strconv.Itoa(int(id)))
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
	if req.State == nil && req.Message == nil && req.StopPosition == nil {
		return "", errNoFieldsToUpdate
	}
	sets := strings.Builder{}
	predicates := strings.Builder{}

	// First add the SET clauses.
	// We also need to check for a SimulatedNull here to support older clients and
	// smooth upgrades. All non-slice simulated NULL checks can be removed in v22+.
	if req.State != nil && *req.State != binlogdatapb.VReplicationWorkflowState(textutil.SimulatedNullInt) {
		state, ok := binlogdatapb.VReplicationWorkflowState_name[int32(req.GetState())]
		if !ok {
			return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid state value: %v", req.GetState())
		}
		sets.WriteString(" state = ")
		sets.WriteString(sqltypes.EncodeStringSQL(state))
	}
	// We also need to check for a SimulatedNull here to support older clients and
	// smooth upgrades. All non-slice simulated NULL checks can be removed in v22+.
	if req.Message != nil && *req.Message != sqltypes.Null.String() {
		if sets.Len() > 0 {
			sets.WriteByte(',')
		}
		sets.WriteString(" message = ")
		sets.WriteString(sqltypes.EncodeStringSQL(req.GetMessage()))
	}
	// We also need to check for a SimulatedNull here to support older clients and
	// smooth upgrades. All non-slice simulated NULL checks can be removed in v22+.
	if req.StopPosition != nil && *req.StopPosition != sqltypes.Null.String() {
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
