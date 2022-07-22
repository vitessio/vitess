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
	"strings"

	"github.com/google/uuid"

	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/withddl"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

type VDiffAction string //nolint

const (
	CreateAction  VDiffAction = "create"
	ShowAction    VDiffAction = "show"
	ResumeAction  VDiffAction = "resume"
	DeleteAction  VDiffAction = "delete"
	AllActionArg              = "all"
	LastActionArg             = "last"
)

var (
	Actions    = []VDiffAction{CreateAction, ShowAction, ResumeAction, DeleteAction}
	ActionArgs = []string{AllActionArg, LastActionArg}
)

func (vde *Engine) PerformVDiffAction(ctx context.Context, req *tabletmanagerdatapb.VDiffRequest) (*tabletmanagerdatapb.VDiffResponse, error) {
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

	vde.vdiffSchemaCreateOnce.Do(func() {
		_, _ = withDDL.Exec(ctx, withddl.QueryToTriggerWithDDL, dbClient.ExecuteFetch, dbClient.ExecuteFetch)
	})

	var qr *sqltypes.Result
	var err error
	options := req.Options

	action := VDiffAction(strings.ToLower(req.Command))
	switch action {
	case CreateAction, ResumeAction:
		query := fmt.Sprintf(sqlGetVDiffID, encodeString(req.VdiffUuid))
		if qr, err = dbClient.ExecuteFetch(query, 1); err != nil {
			return nil, err
		}
		recordFound := len(qr.Rows) == 1
		if recordFound && action == CreateAction {
			return nil, fmt.Errorf("vdiff with UUID %s already exists on tablet %v",
				req.VdiffUuid, vde.thisTablet.Alias)
		} else if action == ResumeAction {
			if !recordFound {
				return nil, fmt.Errorf("vdiff with UUID %s not found on tablet %v",
					req.VdiffUuid, vde.thisTablet.Alias)
			}
			if resp.Id, err = qr.Named().Row().ToInt64("id"); err != nil {
				return nil, fmt.Errorf("vdiff found with invalid id on tablet %v: %w",
					vde.thisTablet.Alias, err)
			}
		}
		options, err = vde.fixupOptions(options)
		if err != nil {
			return nil, err
		}
		optionsJSON, err := json.Marshal(options)
		if err != nil {
			return nil, err
		}
		if action == CreateAction {
			query := fmt.Sprintf(sqlNewVDiff,
				encodeString(req.Keyspace), encodeString(req.Workflow), "pending", encodeString(string(optionsJSON)),
				vde.thisTablet.Shard, topoproto.TabletDbName(vde.thisTablet), req.VdiffUuid)
			if qr, err = dbClient.ExecuteFetch(query, 1); err != nil {
				return nil, err
			}
			if qr.InsertID == 0 {
				return nil, fmt.Errorf("unable to create vdiff for UUID %s on tablet %v (%w)",
					req.VdiffUuid, vde.thisTablet.Alias, err)
			}
			resp.Id = int64(qr.InsertID)
		} else {
			query := fmt.Sprintf(sqlResumeVDiff, encodeString(string(optionsJSON)), encodeString(req.VdiffUuid))
			if qr, err = dbClient.ExecuteFetch(query, 1); err != nil {
				return nil, err
			}
			if qr.RowsAffected == 0 {
				msg := fmt.Sprintf("no completed vdiff found for UUID %s on tablet %v",
					req.VdiffUuid, vde.thisTablet.Alias)
				if err != nil {
					msg = fmt.Sprintf("%s (%v)", msg, err)
				}
				return nil, fmt.Errorf(msg)
			}
		}

		resp.VdiffUuid = req.VdiffUuid
		qr, err := vde.getVDiffByID(ctx, dbClient, resp.Id)
		if err != nil {
			return nil, err
		}
		vde.mu.Lock()
		defer vde.mu.Unlock()
		if err := vde.addController(qr.Named().Row(), options); err != nil {
			return nil, err
		}
	case ShowAction:
		vdiffUUID := ""
		if req.SubCommand == LastActionArg {
			query := fmt.Sprintf(sqlGetMostRecentVDiff, encodeString(req.Keyspace), encodeString(req.Workflow))
			if qr, err = dbClient.ExecuteFetch(query, 1); err != nil {
				return nil, err
			}
			if len(qr.Rows) == 1 {
				row := qr.Named().Row()
				vdiffUUID = row.AsString("vdiff_uuid", "")
			}
		} else {
			if uuidt, err := uuid.Parse(req.SubCommand); err == nil {
				vdiffUUID = uuidt.String()
			}
		}
		if vdiffUUID != "" {
			resp.VdiffUuid = vdiffUUID
			query := fmt.Sprintf(sqlGetVDiffByKeyspaceWorkflowUUID, encodeString(req.Keyspace), encodeString(req.Workflow), encodeString(vdiffUUID))
			if qr, err = dbClient.ExecuteFetch(query, 1); err != nil {
				return nil, err
			}
			switch len(qr.Rows) {
			case 0:
				return nil, fmt.Errorf("no vdiff found for UUID %s keyspace %s and workflow %s on tablet %v",
					vdiffUUID, req.Keyspace, req.Workflow, vde.thisTablet.Alias)
			case 1:
				row := qr.Named().Row()
				vdiffID, _ := row["id"].ToInt64()
				summary, err := vde.getVDiffSummary(vdiffID, dbClient)
				resp.Output = summary
				if err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("too many vdiffs found (%d) for UUID %s keyspace %s and workflow %s on tablet %v",
					len(qr.Rows), vdiffUUID, req.Keyspace, req.Workflow, vde.thisTablet.Alias)
			}
		}
		switch req.SubCommand {
		case AllActionArg:
			if qr, err = dbClient.ExecuteFetch(sqlGetAllVDiffs, -1); err != nil {
				return nil, err
			}
			resp.Output = sqltypes.ResultToProto3(qr)
		case LastActionArg:
		default:
			if _, err := uuid.Parse(req.SubCommand); err != nil {
				return nil, fmt.Errorf("action argument %s not supported", req.SubCommand)
			}
		}
	case DeleteAction:
		query := ""
		switch req.SubCommand {
		case AllActionArg:
			query = fmt.Sprintf(sqlDeleteVDiffs, encodeString(req.Keyspace), encodeString(req.Workflow))
		default:
			uuid, err := uuid.Parse(req.SubCommand)
			if err != nil {
				return nil, fmt.Errorf("action argument %s not supported", req.SubCommand)
			}
			query = fmt.Sprintf(sqlDeleteVDiffByUUID, encodeString(uuid.String()))
		}
		if _, err = dbClient.ExecuteFetch(query, 1); err != nil {
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

	query := fmt.Sprintf(sqlVDiffSummary, vdiffID)
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
	if sourceCell == "" && targetCell == "" {
		cells, err := vde.ts.GetCellInfoNames(vde.ctx)
		if err != nil {
			return nil, err
		}
		if len(cells) == 0 {
			// Unreachable
			return nil, fmt.Errorf("there are no cells in the topo")
		}
		sourceCell = cells[0]
		targetCell = sourceCell
	}
	if sourceCell == "" {
		sourceCell = targetCell
	}
	if targetCell == "" {
		targetCell = sourceCell
	}
	options.PickerOptions.SourceCell = sourceCell
	options.PickerOptions.TargetCell = targetCell

	return options, nil
}
