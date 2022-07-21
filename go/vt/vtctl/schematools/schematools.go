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

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

// GetSchema makes an RPC to get the schema from a remote tablet, after
// verifying a tablet with that alias exists in the topo.
func GetSchema(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, alias *topodatapb.TabletAlias, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	ti, err := ts.GetTablet(ctx, alias)
	if err != nil {
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "GetTablet(%v) failed: %v", alias, err)
	}

	sd, err := tmc.GetSchema(ctx, ti.Tablet, request)
	if err != nil {
		return nil, vterrors.Wrapf(err, "GetSchema(%v, %v) failed: %v", ti.Tablet, request, err)
	}

	return sd, nil
}
