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

package vexec

import (
	"context"
	"errors"
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

const (
	VExecTableQualifier = "_vt"

	SchemaMigrationsTableName = "schema_migrations"
	VReplicationTableName     = "vreplication"
)

type VExec struct {
	ts  *topo.Server
	tmc tmclient.TabletManagerClient

	keyspace  string
	primaries []*topo.TabletInfo
}

func NewVExec(keyspace string, ts *topo.Server, tmc tmclient.TabletManagerClient) *VExec {
	return &VExec{
		ts:       ts,
		tmc:      tmc,
		keyspace: keyspace,
	}
}

func (vx *VExec) QueryContext(ctx context.Context, query string) (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	if vx.primaries == nil {
		if err := vx.initialize(ctx); err != nil {
			return nil, err
		}
	}

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}

	table, err := ExtractTableName(stmt)
	if err != nil {
		return nil, err
	}

	planner, err := vx.GetPlanner(table)
	if err != nil {
		return nil, err
	}

	qp, err := planner.PlanQuery(stmt)
	if err != nil {
		return nil, err
	}

	return qp.ExecuteScatter(ctx, vx.primaries...)
}

func (vx *VExec) initialize(ctx context.Context) error {
	vx.primaries = nil

	shards, err := vx.ts.GetShardNames(ctx, vx.keyspace)
	if err != nil {
		return err
	}

	if len(shards) == 0 {
		return fmt.Errorf("%w %s", ErrNoShardsForKeyspace, vx.keyspace)
	}

	primaries := make([]*topo.TabletInfo, 0, len(shards))

	for _, shard := range shards {
		si, err := vx.ts.GetShard(ctx, vx.keyspace, shard)
		if err != nil {
			return err
		}

		if si.MasterAlias == nil {
			return fmt.Errorf("%w %s/%s", ErrNoShardPrimary, vx.keyspace, shard)
		}

		primary, err := vx.ts.GetTablet(ctx, si.MasterAlias)
		if err != nil {
			return err
		}

		if primary == nil {
			return fmt.Errorf("%w %s/%s: tablet %v not found", ErrNoShardPrimary, vx.keyspace, shard, topoproto.TabletAliasString(si.MasterAlias))
		}

		primaries = append(primaries, primary)
	}

	vx.primaries = primaries

	return nil
}

func ExtractTableName(stmt sqlparser.Statement) (string, error) { // TODO: private?
	switch stmt := stmt.(type) {
	case *sqlparser.Update:
		return sqlparser.String(stmt.TableExprs), nil
	case *sqlparser.Delete:
		return sqlparser.String(stmt.TableExprs), nil
	case *sqlparser.Insert:
		return sqlparser.String(stmt.Table), nil
	case *sqlparser.Select:
		return sqlparser.String(stmt.From), nil
	}

	return "", fmt.Errorf("%w: %+v", ErrUnsupportedQuery, sqlparser.String(stmt))
}

func (vx *VExec) GetPlanner(table string) (QueryPlanner, error) { // TODO: private?
	switch table {
	case qualifiedTableName(VReplicationTableName):
		return NewVReplicationQueryPlanner(vx.tmc, "", vx.primaries[0].DbName()), nil
	case qualifiedTableName(SchemaMigrationsTableName):
		return nil, errors.New("Schema Migrations not yet supported in new workflow package")
	default:
		return nil, fmt.Errorf("%w: %v", ErrUnsupportedTable, table)
	}
}

func qualifiedTableName(name string) string {
	return fmt.Sprintf("%s.%s", VExecTableQualifier, name)
}
