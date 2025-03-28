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
	"strings"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

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

func ensureTablesExist(vsInfo *topo.KeyspaceVSchemaInfo, tables []string) error {
	var missingTables []string
	for _, tableName := range tables {
		if _, ok := vsInfo.Tables[tableName]; !ok {
			missingTables = append(missingTables, tableName)
		}
	}
	if len(missingTables) > 0 {
		return vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "table(s) %s not found in '%s' keyspace",
			strings.Join(missingTables, ", "), vsInfo.Name)
	}
	return nil
}

func ensureTablesDoNotExist(vsInfo *topo.KeyspaceVSchemaInfo, tables []string) error {
	var alreadyExistingTables []string
	for _, tableName := range tables {
		if _, ok := vsInfo.Tables[tableName]; ok {
			alreadyExistingTables = append(alreadyExistingTables, tableName)
		}
	}
	if len(alreadyExistingTables) > 0 {
		return vterrors.Errorf(vtrpcpb.Code_ALREADY_EXISTS, "table(s) %s already exist in '%s' keyspace",
			strings.Join(alreadyExistingTables, ", "), vsInfo.Name)
	}
	return nil
}

// validateNewVindex validates if we can create a vindex with given vindexName
// vindexType and params.
func validateNewVindex(vsInfo *topo.KeyspaceVSchemaInfo, vindexName string, vindexType string, params map[string]string) error {
	if _, ok := vsInfo.Vindexes[vindexName]; ok {
		return vterrors.Errorf(vtrpcpb.Code_ALREADY_EXISTS, "vindex '%s' already exists in '%s' vschema",
			vindexName, vsInfo.Name)
	}

	// Validate if we can create the vindex without any errors.
	if _, err := vindexes.CreateVindex(vindexType, vindexName, params); err != nil {
		return vterrors.Wrapf(err, "failed to create vindex '%s'", vindexName)
	}
	return nil
}

func parseForeignKeyMode(foreignKeyMode string) (vschemapb.Keyspace_ForeignKeyMode, error) {
	fkMode := strings.ToLower(foreignKeyMode)
	fkModeValue, ok := vschemapb.Keyspace_ForeignKeyMode_value[fkMode]
	if !ok {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid value provided for foreign key mode: %s", fkMode)
	}
	return vschemapb.Keyspace_ForeignKeyMode(fkModeValue), nil
}

func parseTenantIdColumnType(tenantType string) (querypb.Type, error) {
	tenantIdColType := strings.ToUpper(tenantType)
	typ, ok := querypb.Type_value[tenantIdColType]
	if !ok {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid tenant id column type: %s", tenantType)
	}
	return querypb.Type(typ), nil
}

// validateQualifiedTableType validates that the specified table name is
// qualified and it matches the specified expected type.
func validateQualifiedTableType(ctx context.Context, ts *topo.Server, qualifiedTableName string, expectedType string) error {
	ksName, tableName, err := vindexes.ExtractTableParts(qualifiedTableName, false)
	if err != nil {
		return err
	}
	_, sourceTable, err := getVSchemaAndTable(ctx, ts, ksName, tableName)
	if err != nil {
		return err
	}
	if sourceTable.Type != expectedType {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "table '%s' is not a %s table",
			tableName, expectedType)
	}
	return nil
}
