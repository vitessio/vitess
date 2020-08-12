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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/onlineddl"

	"golang.org/x/net/context"
)

func (tm *TabletManager) extractTableName(stmt sqlparser.Statement) (string, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Update:
		return sqlparser.String(stmt.TableExprs), nil
	case *sqlparser.Delete:
		return sqlparser.String(stmt.TableExprs), nil
	case *sqlparser.Select:
		return sqlparser.String(stmt.From), nil
	}
	return "", fmt.Errorf("query not supported by vexec: %+v", sqlparser.String(stmt))
}

// VExec executes a generic VExec command.
func (tm *TabletManager) VExec(ctx context.Context, query string) (*querypb.QueryResult, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	tableName, err := tm.extractTableName(stmt)
	if err != nil {
		return nil, err
	}
	// TODO(shlomi) do something here!!!
	fmt.Printf("======= VExec query: %x, table: %s \n", query, tableName)
	switch tableName {
	case onlineddl.SchemaMigrationsTableName:
		return tm.QueryServiceControl.OnlineDDLExecutor().VExec(ctx, query, stmt)
	default:
		return nil, fmt.Errorf("table not supported by vexec: %v", tableName)
	}
}
