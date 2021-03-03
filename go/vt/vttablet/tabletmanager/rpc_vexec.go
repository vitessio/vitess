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
	"fmt"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/vttablet/vexec"

	"context"
)

// VExec executes a generic VExec command.
func (tm *TabletManager) VExec(ctx context.Context, query, workflow, keyspace string) (*querypb.QueryResult, error) {
	vx := vexec.NewTabletVExec(workflow, keyspace)
	if err := vx.AnalyzeQuery(ctx, query); err != nil {
		return nil, err
	}
	switch vx.TableName {
	case fmt.Sprintf("%s.%s", vexec.TableQualifier, schema.SchemaMigrationsTableName):
		return tm.QueryServiceControl.OnlineDDLExecutor().VExec(ctx, vx)
	default:
		return nil, fmt.Errorf("table not supported by vexec: %v", vx.TableName)
	}
}
