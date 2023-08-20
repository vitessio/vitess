/*
Copyright 2021 The Vitess Authors.

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

package schematools

import (
	"context"
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// GetSchema makes an RPC to get the schema from a remote tablet, after
// verifying a tablet with that alias exists in the topo.
func GetSchema(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, alias *topodatapb.TabletAlias, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	ti, err := ts.GetTablet(ctx, alias)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "GetTablet(%v) failed: %v", alias, err)
	}

	sd, err := tmc.GetSchema(ctx, ti.Tablet, request)
	if err != nil {
		return nil, vterrors.Wrapf(err, "GetSchema(%v, %v) failed: %v", ti.Tablet, request, err)
	}

	return sd, nil
}

// ParseSchemaMigrationStrategy parses the given strategy into the underlying enum type.
func ParseSchemaMigrationStrategy(name string) (vtctldatapb.SchemaMigration_Strategy, error) {
	if name == "" {
		// backward compatiblity and to handle unspecified values
		return vtctldatapb.SchemaMigration_DIRECT, nil

	}

	upperName := strings.ToUpper(name)
	switch upperName {
	case "GH-OST", "PT-OSC":
		// more compatibility since the protobuf message names don't
		// have the dash.
		upperName = strings.ReplaceAll(upperName, "-", "")
	default:
	}

	strategy, ok := vtctldatapb.SchemaMigration_Strategy_value[upperName]
	if !ok {
		return 0, fmt.Errorf("unknown schema migration strategy: '%v'", name)
	}

	return vtctldatapb.SchemaMigration_Strategy(strategy), nil

}

// ParseSchemaMigrationStatus parses the given status into the underlying enum type.
func ParseSchemaMigrationStatus(name string) (vtctldatapb.SchemaMigration_Status, error) {
	key := strings.ToUpper(name)

	val, ok := vtctldatapb.SchemaMigration_Status_value[key]
	if !ok {
		return 0, fmt.Errorf("unknown enum name for SchemaMigration_Status: %s", name)
	}

	return vtctldatapb.SchemaMigration_Status(val), nil
}

// SchemaMigrationStrategyName returns the text-based form of the strategy.
func SchemaMigrationStrategyName(strategy vtctldatapb.SchemaMigration_Strategy) string {
	name, ok := vtctldatapb.SchemaMigration_Strategy_name[int32(strategy)]
	if !ok {
		return "unknown"
	}

	switch strategy {
	case vtctldatapb.SchemaMigration_GHOST, vtctldatapb.SchemaMigration_PTOSC:
		name = strings.Join([]string{name[:2], name[2:]}, "-")
	}

	return strings.ToLower(name)
}

// SchemaMigrationStatusName returns the text-based form of the status.
func SchemaMigrationStatusName(status vtctldatapb.SchemaMigration_Status) string {
	return strings.ToLower(vtctldatapb.SchemaMigration_Status_name[int32(status)])
}
