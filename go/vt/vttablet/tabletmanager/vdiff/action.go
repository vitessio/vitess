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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

type VDiffAction string //nolint

const (
	CreateAction  VDiffAction = "create"
	ShowAction    VDiffAction = "show"
	AllActionArg              = "all"
	LastActionArg             = "last"
)

var (
	Actions    = []VDiffAction{CreateAction, ShowAction}
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
	var qr *sqltypes.Result
	var err error
	options := req.Options

	action := VDiffAction(strings.ToLower(req.Command))
	switch action {
	case CreateAction:
		// todo check if vdiff row already exists
		options, err = vde.fixupOptions(options)
		if err != nil {
			return nil, err
		}
		optionsJSON, err := json.Marshal(options)
		if err != nil {
			return nil, err
		}
		query := fmt.Sprintf(sqlNewVDiff,
			encodeString(req.Keyspace), encodeString(req.Workflow), "pending", encodeString(string(optionsJSON)),
			vde.thisTablet.Shard, topoproto.TabletDbName(vde.thisTablet), req.VdiffUuid)
		if qr, err = withDDL.Exec(context.Background(), query, dbClient.ExecuteFetch, dbClient.ExecuteFetch); err != nil {
			return nil, err
		}
		if qr.InsertID == 0 {
			return nil, fmt.Errorf("unable to insert")
		}
		resp.Id = int64(qr.InsertID)
		resp.VdiffUuid = req.VdiffUuid
		qr, err := vde.getVDiffByID(ctx, resp.Id)
		if err != nil {
			return nil, err
		}
		if err := vde.addController(qr.Named().Row(), options); err != nil {
			return nil, err
		}
	case ShowAction:
		vdiffUUID := ""
		if req.SubCommand == LastActionArg {
			query := fmt.Sprintf(sqlGetMostRecentVDiff, encodeString(req.Keyspace), encodeString(req.Workflow))
			if qr, err = withDDL.Exec(context.Background(), query, dbClient.ExecuteFetch, dbClient.ExecuteFetch); err != nil {
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
			if qr, err = withDDL.Exec(context.Background(), query, dbClient.ExecuteFetch, dbClient.ExecuteFetch); err != nil {
				return nil, err
			}
			switch len(qr.Rows) {
			case 0:
				return nil, fmt.Errorf("no VDiff found for keyspace %s and workflow %s: %s", req.Keyspace, req.Workflow, query)
			case 1:
				row := qr.Named().Row()
				vdiffID, _ := row["id"].ToInt64()
				summary, err := vde.getVDiffSummary(vdiffID, dbClient)
				resp.Output = summary
				if err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("error: too many VDiffs found (%d) for keyspace %s and workflow %s", len(qr.Rows), req.Keyspace, req.Workflow)
			}
		}
		switch req.SubCommand {
		case AllActionArg:
			if qr, err = withDDL.Exec(context.Background(), sqlGetAllVDiffs, dbClient.ExecuteFetch, dbClient.ExecuteFetch); err != nil {
				return nil, err
			}
			resp.Output = sqltypes.ResultToProto3(qr)
		case LastActionArg:
		default:
			if _, err := uuid.Parse(req.SubCommand); err != nil {
				return nil, fmt.Errorf("action argument %s not supported", req.SubCommand)
			}
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
	if qr, err = withDDL.Exec(context.Background(), query, dbClient.ExecuteFetch, dbClient.ExecuteFetch); err != nil {
		return nil, err
	}
	return sqltypes.ResultToProto3(qr), nil

}

// Validate vdiff options. Also setup defaults where applicable
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
