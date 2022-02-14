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
