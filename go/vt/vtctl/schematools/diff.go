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

	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// CompareSchemas returns (nil, nil) if the schema of the two tablets match. If
// there are diffs, they are returned as (diffs []string, nil).
//
// If fetching the schema for either tablet fails, a non-nil error is returned.
func CompareSchemas(
	ctx context.Context,
	ts *topo.Server,
	tmc tmclient.TabletManagerClient,
	source *topodatapb.TabletAlias,
	dest *topodatapb.TabletAlias,
	tables []string,
	excludeTables []string,
	includeViews bool,
) (diffs []string, err error) {
	req := &tabletmanagerdatapb.GetSchemaRequest{Tables: tables, ExcludeTables: excludeTables, IncludeViews: includeViews}
	sourceSchema, err := GetSchema(ctx, ts, tmc, source, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema from tablet %v. err: %v", source, err)
	}

	destSchema, err := GetSchema(ctx, ts, tmc, dest, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema from tablet %v. err: %v", dest, err)
	}

	return tmutils.DiffSchemaToArray("source", sourceSchema, "dest", destSchema), nil
}
