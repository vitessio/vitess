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
	"fmt"
	"strings"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type VSchemaAPI struct {
	ts     *topo.Server
	parser *sqlparser.Parser
}

func NewVSchemaAPI(ts *topo.Server, parser *sqlparser.Parser) *VSchemaAPI {
	return &VSchemaAPI{
		ts:     ts,
		parser: parser,
	}
}

// TODO(beingnoble03): Missing API comments.

func (api *VSchemaAPI) Create(ctx context.Context, req *vtctldatapb.VSchemaCreateRequest) error {
	// TODO(beingnoble03): Add more validation here.
	topoKs := &topodatapb.Keyspace{}
	err := api.ts.CreateKeyspace(ctx, req.VSchemaName, topoKs)
	if err != nil {
		return vterrors.Wrapf(err, "unable to create keyspace '%s'", req.VSchemaName)
	}

	var vsks *vschemapb.Keyspace
	err = json2.UnmarshalPB([]byte(req.VSchemaJson), vsks)
	if err != nil {
		return vterrors.Wrapf(err, "unable to unmarshal vschema JSON")
	}

	_, err = vindexes.BuildKeyspace(vsks, api.parser)
	if err != nil {
		err = vterrors.Wrapf(err, "failed to build vschema '%s'", req.VSchemaName)
		return err
	}
	vsks.Draft = req.Draft
	vsks.Sharded = req.Sharded

	vsInfo := &topo.KeyspaceVSchemaInfo{
		Name:     req.VSchemaName,
		Keyspace: vsks,
	}
	if err := api.ts.SaveVSchema(ctx, vsInfo); err != nil {
		return vterrors.Wrapf(err, "failed to save updated vschema '%v' in the '%s' keyspace",
			vsInfo, req.VSchemaName)
	}
	return err
}

func (api *VSchemaAPI) Update(ctx context.Context, req *vtctldatapb.VSchemaUpdateRequest) error {
	vsInfo, err := api.ts.GetVSchema(ctx, req.VSchemaName)
	if err != nil {
		return vterrors.Wrapf(err, "failed to retrieve vschema for '%s' keyspace", req.VSchemaName)
	}
	if req.Sharded != nil {
		vsInfo.Sharded = *req.Sharded
	}
	if req.ForeignKeyMode != nil {
		fkMode, err := parseForeignKeyMode(*req.ForeignKeyMode)
		if err != nil {
			return err
		}
		vsInfo.ForeignKeyMode = fkMode
	}
	if req.Draft != nil {
		vsInfo.Draft = *req.Draft
	}
	if req.MultiTenant != nil {
		if *req.MultiTenant {
			// If we are updating both tenantIdColumnName and tenantIdColumnType,
			// we can directly replace the entire MultiTenantSpec. However, if
			// we are looking to update only either column type or column name,
			// we should check if MultiTenantSpec existed before. If it doesn't
			// exist, we should return an error that both column name and column
			// type should be provided.
			//
			// Also, if multiTenant was true but neither tenantIdColumnName was
			// specified nor tenantIdColumnType, we shouldn't return any error
			// in that case and do nothing.
			switch {
			case req.TenantIdColumnType != nil && req.TenantIdColumnName != nil:
				if *req.TenantIdColumnName == "" {
					return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "tenant id column name not specified")
				}
				typ, err := parseTenantIdColumnType(*req.TenantIdColumnType)
				if err != nil {
					return err
				}
				vsInfo.MultiTenantSpec = &vschemapb.MultiTenantSpec{
					TenantIdColumnName: *req.TenantIdColumnName,
					TenantIdColumnType: typ,
				}
			case req.TenantIdColumnType != nil:
				if vsInfo.MultiTenantSpec == nil {
					return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "both tenant id column name and column type should be provided")
				}
				typ, err := parseTenantIdColumnType(*req.TenantIdColumnType)
				if err != nil {
					return err
				}
				vsInfo.MultiTenantSpec.TenantIdColumnType = typ
			case req.TenantIdColumnName != nil:
				if vsInfo.MultiTenantSpec == nil {
					return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "both tenant id column name and column type should be provided")
				}
				if *req.TenantIdColumnName == "" {
					return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "tenant id column name not specified")
				}
				vsInfo.MultiTenantSpec.TenantIdColumnName = *req.TenantIdColumnName
			}
		} else {
			vsInfo.MultiTenantSpec = nil
		}
	}

	if err := api.ts.SaveVSchema(ctx, vsInfo); err != nil {
		return vterrors.Wrapf(err, "failed to save updated vschema '%v' in the '%s' keyspace",
			vsInfo, req.VSchemaName)
	}
	return nil
}

func (api *VSchemaAPI) Publish(ctx context.Context, req *vtctldatapb.VSchemaPublishRequest) error {
	vsInfo, err := api.ts.GetVSchema(ctx, req.VSchemaName)
	if err != nil {
		return vterrors.Wrapf(err, "failed to retrieve vschema for '%s' keyspace", req.VSchemaName)
	}
	if !vsInfo.Draft {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vschema '%s' is already published", req.VSchemaName)
	}
	vsInfo.Draft = false
	if err := api.ts.SaveVSchema(ctx, vsInfo); err != nil {
		return vterrors.Wrapf(err, "failed to save updated vschema '%v' in the '%s' keyspace",
			vsInfo, req.VSchemaName)
	}
	return nil
}

// AddVindex adds a vindex in vschema. It doesn't expect it to be a lookup
// vindex, so owner is not set/required.
func (api *VSchemaAPI) AddVindex(ctx context.Context, req *vtctldatapb.VSchemaAddVindexRequest) error {
	vsInfo, err := api.ts.GetVSchema(ctx, req.VSchemaName)
	if err != nil {
		return vterrors.Wrapf(err, "failed to retrieve vschema for '%s' keyspace", req.VSchemaName)
	}
	if err := validateNewVindex(vsInfo, req.VindexName, req.VindexType, req.Params); err != nil {
		return err
	}
	vindex := &vschemapb.Vindex{
		Type:   req.VindexType,
		Params: req.Params,
	}
	vsInfo.Vindexes[req.VindexName] = vindex
	if err := api.ts.SaveVSchema(ctx, vsInfo); err != nil {
		return vterrors.Wrapf(err, "failed to save updated vschema '%v' in the '%s' keyspace",
			vsInfo, req.VSchemaName)
	}
	return nil
}

func (api *VSchemaAPI) RemoveVindex(ctx context.Context, req *vtctldatapb.VSchemaRemoveVindexRequest) error {
	vsInfo, err := api.ts.GetVSchema(ctx, req.VSchemaName)
	if err != nil {
		return vterrors.Wrapf(err, "failed to retrieve vschema for '%s' keyspace", req.VSchemaName)
	}
	if _, ok := vsInfo.Vindexes[req.VindexName]; !ok {
		return vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "vindex '%s' doesn't exist in '%s' vschema",
			req.VindexName, req.VSchemaName)
	}
	delete(vsInfo.Vindexes, req.VindexName)
	if err := api.ts.SaveVSchema(ctx, vsInfo); err != nil {
		return vterrors.Wrapf(err, "failed to save updated vschema '%v' in the '%s' keyspace",
			vsInfo, req.VSchemaName)
	}
	return nil
}

func (api *VSchemaAPI) AddLookupVindex(ctx context.Context, req *vtctldatapb.VSchemaAddLookupVindexRequest) error {
	// TODO(beingnoble03): Check if we also need to add column vindexes on source and target tables.
	vsInfo, err := api.ts.GetVSchema(ctx, req.VSchemaName)
	if err != nil {
		return vterrors.Wrapf(err, "failed to retrieve vschema for '%s' keyspace", req.VSchemaName)
	}
	req.LookupVindexType = strings.ToLower(req.LookupVindexType)
	if !strings.HasPrefix(req.LookupVindexType, "lookup") {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid lookup vindex type: %s", req.LookupVindexType)
	}
	if len(req.FromColumns) == 0 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "at least 1 column should be specified for lookup vindex")
	}
	params := map[string]string{
		"table":        req.TableName,
		"from":         strings.Join(req.FromColumns, ","),
		"to":           "keyspace_id",
		"ignore_nulls": fmt.Sprintf("%t", req.IgnoreNulls),
	}
	if err := validateNewVindex(vsInfo, req.VindexName, req.LookupVindexType, params); err != nil {
		return err
	}
	vindex := &vschemapb.Vindex{
		Type:   req.LookupVindexType,
		Params: params,
	}
	vsInfo.Vindexes[req.VindexName] = vindex
	if err := api.ts.SaveVSchema(ctx, vsInfo); err != nil {
		return vterrors.Wrapf(err, "failed to save updated vschema '%v' in the '%s' keyspace",
			vsInfo, req.VSchemaName)
	}
	return nil
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
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "table '%s' is not a reference table",
				sourceTableName)
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
