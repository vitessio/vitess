/*
Copyright 2025 The Vitess Authors.

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

package vschema

import (
	"context"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type VSchemaAPI struct {
	ts *topo.Server
}

func NewVSchemaAPI(ts *topo.Server) *VSchemaAPI {
	return &VSchemaAPI{
		ts: ts,
	}
}

// SetReference sets up a reference table, which points to a source table in
// another vschema.
func (api *VSchemaAPI) SetReference(ctx context.Context, req *vtctldatapb.VSchemaSetReferenceRequest) error {
	vsInfo, table, err := getVSchemaAndTable(ctx, api.ts, req.VSchemaName, req.TableName)
	if err != nil {
		return err
	}
	if table.Type == vindexes.TypeReference {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "table '%s' is already a reference table", req.TableName)
	}

	if req.Source != "" {
		sourceKs, sourceTableName, err := vindexes.ExtractTableParts(req.Source, false /* allowUnqualified */)
		if err != nil {
			return vterrors.Wrapf(err, "failed to parse source")

		}
		_, sourceTable, err := getVSchemaAndTable(ctx, api.ts, sourceKs, sourceTableName)
		if err != nil {
			return vterrors.Wrapf(err, "invalid reference table source")
		}
		if sourceTable.Type != vindexes.TypeReference {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "table '%s' is not a reference table", sourceTableName)
		}
	}

	table.Source = req.Source
	table.Type = vindexes.TypeReference
	if err := api.ts.SaveVSchema(ctx, vsInfo); err != nil {
		return vterrors.Wrapf(err, "failed to save updated vschema '%v' in the '%s' keyspace",
			vsInfo, req.VSchemaName)
	}
	return nil
}

func getVSchemaAndTable(ctx context.Context, ts *topo.Server, vschemaName string, tableName string) (*topo.KeyspaceVSchemaInfo, *vschemapb.Table, error) {
	vsInfo, err := ts.GetVSchema(ctx, vschemaName)
	if err != nil {
		return nil, nil, vterrors.Wrapf(err, "failed to retrieve vschema for '%s' keyspace:", vschemaName)
	}

	table, ok := vsInfo.Tables[tableName]
	if !ok {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "table '%s' not found in '%s' keyspace", tableName, vschemaName)
	}

	return vsInfo, table, nil
}
