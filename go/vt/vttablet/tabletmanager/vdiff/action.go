/*
Copyright 2022 The Vitess Authors.

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

package vdiff

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type VDiffAction string //nolint

const (
	CreateAction  VDiffAction = "create"
	ShowAction    VDiffAction = "show"
	StopAction    VDiffAction = "stop"
	ResumeAction  VDiffAction = "resume"
	DeleteAction  VDiffAction = "delete"
	AllActionArg              = "all"
	LastActionArg             = "last"
)

var (
	Actions    = []VDiffAction{CreateAction, ShowAction, StopAction, ResumeAction, DeleteAction}
	ActionArgs = []string{AllActionArg, LastActionArg}
)

func (vde *Engine) PerformVDiffAction(ctx context.Context, req *tabletmanagerdatapb.VDiffRequest) (*tabletmanagerdatapb.VDiffResponse, error) {
	if !vde.isOpen {
		return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "vdiff engine is closed")
	}
	if vde.cancelRetry != nil {
		return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "vdiff engine is still trying to open")
	}

	resp := &tabletmanagerdatapb.VDiffResponse{
		Id:     0,
		Output: nil,
	}
	// We use the db_filtered user for vreplication related work
	dbClient := vde.dbClientFactoryFiltered()
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	defer dbClient.Close()

	action := VDiffAction(req.Action)
	switch action {
	case CreateAction, ResumeAction:
		if err := vde.handleCreateResumeAction(ctx, dbClient, action, req, resp); err != nil {
			return nil, err
		}
	case ShowAction:
		if err := vde.handleShowAction(ctx, dbClient, action, req, resp); err != nil {
			return nil, err
		}
	case StopAction:
		if err := vde.handleStopAction(ctx, dbClient, action, req, resp); err != nil {
			return nil, err
		}
	case DeleteAction:
		if err := vde.handleDeleteAction(ctx, dbClient, action, req, resp); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("action %s not supported", action)
	}
	return resp, nil
}

func (vde *Engine) getVDiffSummary(vdiffID int64, dbClient binlogplayer.DBClient) (*query.QueryResult, error) {
	var qr *sqltypes.Result
	var err error

	query, err := sqlparser.ParseAndBind(sqlVDiffSummary, sqltypes.Int64BindVariable(vdiffID))
	if err != nil {
		return nil, err
	}
	if qr, err = dbClient.ExecuteFetch(query, -1); err != nil {
		return nil, err
	}
	return sqltypes.ResultToProto3(qr), nil

}

// Validate vdiff options. Also setup defaults where applicable.
func (vde *Engine) fixupOptions(options *tabletmanagerdatapb.VDiffOptions) (*tabletmanagerdatapb.VDiffOptions, error) {
	// Assign defaults to sourceCell and targetCell if not specified.
	sourceCell := options.PickerOptions.SourceCell
	targetCell := options.PickerOptions.TargetCell
	var defaultCell string
	var err error
	if sourceCell == "" || targetCell == "" {
		defaultCell, err = vde.getDefaultCell()
		if err != nil {
			return nil, err
		}
	}
	if sourceCell == "" {
		sourceCell = defaultCell
	}
	if targetCell == "" {
		targetCell = defaultCell
	}
	options.PickerOptions.SourceCell = sourceCell
	options.PickerOptions.TargetCell = targetCell

	return options, nil
}

func (vde *Engine) getDefaultCell() (string, error) {
	cells, err := vde.ts.GetCellInfoNames(vde.ctx)
	if err != nil {
		return "", err
	}
	if len(cells) == 0 {
		// Unreachable
		return "", fmt.Errorf("there are no cells in the topo")
	}
	return cells[0], nil
}

func (vde *Engine) handleCreateResumeAction(ctx context.Context, dbClient binlogplayer.DBClient, action VDiffAction, req *tabletmanagerdatapb.VDiffRequest, resp *tabletmanagerdatapb.VDiffResponse) error {
	var qr *sqltypes.Result
	options := req.Options

	query, err := sqlparser.ParseAndBind(sqlGetVDiffID, sqltypes.StringBindVariable(req.VdiffUuid))
	if err != nil {
		return err
	}
	if qr, err = dbClient.ExecuteFetch(query, 1); err != nil {
		return err
	}
	recordFound := len(qr.Rows) == 1
	if recordFound && action == CreateAction {
		return fmt.Errorf("vdiff with UUID %s already exists on tablet %v",
			req.VdiffUuid, vde.thisTablet.Alias)
	} else if action == ResumeAction {
		if !recordFound {
			return fmt.Errorf("vdiff with UUID %s not found on tablet %v",
				req.VdiffUuid, vde.thisTablet.Alias)
		}
		if resp.Id, err = qr.Named().Row().ToInt64("id"); err != nil {
			return fmt.Errorf("vdiff found with invalid id on tablet %v: %w",
				vde.thisTablet.Alias, err)
		}
	}
	if options, err = vde.fixupOptions(options); err != nil {
		return err
	}
	optionsJSON, err := json.Marshal(options)
	if err != nil {
		return err
	}
	if action == CreateAction {
		query, err := sqlparser.ParseAndBind(sqlNewVDiff,
			sqltypes.StringBindVariable(req.Keyspace),
			sqltypes.StringBindVariable(req.Workflow),
			sqltypes.StringBindVariable("pending"),
			sqltypes.StringBindVariable(string(optionsJSON)),
			sqltypes.StringBindVariable(vde.thisTablet.Shard),
			sqltypes.StringBindVariable(topoproto.TabletDbName(vde.thisTablet)),
			sqltypes.StringBindVariable(req.VdiffUuid),
		)
		if err != nil {
			return err
		}
		if qr, err = dbClient.ExecuteFetch(query, 1); err != nil {
			return err
		}
		if qr.InsertID == 0 {
			return fmt.Errorf("unable to create vdiff for UUID %s on tablet %v (%w)",
				req.VdiffUuid, vde.thisTablet.Alias, err)
		}
		resp.Id = int64(qr.InsertID)
	} else {
		query, err := sqlparser.ParseAndBind(sqlResumeVDiff,
			sqltypes.StringBindVariable(string(optionsJSON)),
			sqltypes.StringBindVariable(req.VdiffUuid),
		)
		if err != nil {
			return err
		}
		if qr, err = dbClient.ExecuteFetch(query, 1); err != nil {
			return err
		}
		if qr.RowsAffected == 0 {
			msg := fmt.Sprintf("no completed or stopped vdiff found for UUID %s on tablet %v",
				req.VdiffUuid, vde.thisTablet.Alias)
			if err != nil {
				msg = fmt.Sprintf("%s (%v)", msg, err)
			}
			return fmt.Errorf(msg)
		}
	}

	resp.VdiffUuid = req.VdiffUuid
	qr, err = vde.getVDiffByID(ctx, dbClient, resp.Id)
	if err != nil {
		return err
	}
	vde.mu.Lock()
	defer vde.mu.Unlock()
	if err := vde.addController(qr.Named().Row(), options); err != nil {
		return err
	}

	return nil
}

func (vde *Engine) handleShowAction(ctx context.Context, dbClient binlogplayer.DBClient, action VDiffAction, req *tabletmanagerdatapb.VDiffRequest, resp *tabletmanagerdatapb.VDiffResponse) error {
	var qr *sqltypes.Result
	var err error
	vdiffUUID := ""

	if req.ActionArg == LastActionArg {
		query, err := sqlparser.ParseAndBind(sqlGetMostRecentVDiff,
			sqltypes.StringBindVariable(req.Keyspace),
			sqltypes.StringBindVariable(req.Workflow),
		)
		if err != nil {
			return err
		}
		if qr, err = dbClient.ExecuteFetch(query, 1); err != nil {
			return err
		}
		if len(qr.Rows) == 1 {
			row := qr.Named().Row()
			vdiffUUID = row.AsString("vdiff_uuid", "")
		}
	} else {
		if uuidt, err := uuid.Parse(req.ActionArg); err == nil {
			vdiffUUID = uuidt.String()
		}
	}
	if vdiffUUID != "" {
		resp.VdiffUuid = vdiffUUID
		query, err := sqlparser.ParseAndBind(sqlGetVDiffByKeyspaceWorkflowUUID,
			sqltypes.StringBindVariable(req.Keyspace),
			sqltypes.StringBindVariable(req.Workflow),
			sqltypes.StringBindVariable(vdiffUUID),
		)
		if err != nil {
			return err
		}
		if qr, err = dbClient.ExecuteFetch(query, 1); err != nil {
			return err
		}
		switch len(qr.Rows) {
		case 0:
			return fmt.Errorf("no vdiff found for UUID %s keyspace %s and workflow %s on tablet %v",
				vdiffUUID, req.Keyspace, req.Workflow, vde.thisTablet.Alias)
		case 1:
			row := qr.Named().Row()
			vdiffID, _ := row["id"].ToInt64()
			summary, err := vde.getVDiffSummary(vdiffID, dbClient)
			resp.Output = summary
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("too many vdiffs found (%d) for UUID %s keyspace %s and workflow %s on tablet %v",
				len(qr.Rows), vdiffUUID, req.Keyspace, req.Workflow, vde.thisTablet.Alias)
		}
	}
	switch req.ActionArg {
	case AllActionArg:
		if qr, err = dbClient.ExecuteFetch(sqlGetAllVDiffs, -1); err != nil {
			return err
		}
		resp.Output = sqltypes.ResultToProto3(qr)
	case LastActionArg:
	default:
		if _, err := uuid.Parse(req.ActionArg); err != nil {
			return fmt.Errorf("action argument %s not supported", req.ActionArg)
		}
	}

	return nil
}

func (vde *Engine) handleStopAction(ctx context.Context, dbClient binlogplayer.DBClient, action VDiffAction, req *tabletmanagerdatapb.VDiffRequest, resp *tabletmanagerdatapb.VDiffResponse) error {
	vde.mu.Lock()
	defer vde.mu.Unlock()
	for _, controller := range vde.controllers {
		if controller.uuid == req.VdiffUuid {
			controller.Stop()
			if err := controller.markStoppedByRequest(); err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "encountered an error marking vdiff %s as stopped: %v", controller.uuid, err)
			}
			break
		}
	}
	return nil
}

func (vde *Engine) handleDeleteAction(ctx context.Context, dbClient binlogplayer.DBClient, action VDiffAction, req *tabletmanagerdatapb.VDiffRequest, resp *tabletmanagerdatapb.VDiffResponse) error {
	var err error
	query := ""

	switch req.ActionArg {
	case AllActionArg:
		query, err = sqlparser.ParseAndBind(sqlDeleteVDiffs,
			sqltypes.StringBindVariable(req.Keyspace),
			sqltypes.StringBindVariable(req.Workflow),
		)
		if err != nil {
			return err
		}
	default:
		uuid, err := uuid.Parse(req.ActionArg)
		if err != nil {
			return fmt.Errorf("action argument %s not supported", req.ActionArg)
		}
		query, err = sqlparser.ParseAndBind(sqlDeleteVDiffByUUID, sqltypes.StringBindVariable(uuid.String()))
		if err != nil {
			return err
		}
	}
	if _, err = dbClient.ExecuteFetch(query, 1); err != nil {
		return err
	}

	return nil
}
