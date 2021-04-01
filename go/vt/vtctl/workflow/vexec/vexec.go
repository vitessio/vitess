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
	// VExecTableQualifier is the qualifier that all tables supported by vexec
	// are prefixed by.
	VExecTableQualifier = "_vt"

	// SchemaMigrationsTableName is the unqualified name of the schema
	// migrations table supported by vexec.
	SchemaMigrationsTableName = "schema_migrations"
	// VReplicationTableName is the unqualified name of the vreplication table
	// supported by vexec.
	VReplicationTableName = "vreplication"
)

var ( // Topo lookup errors.
	// ErrNoShardPrimary occurs when a shard is found with no serving
	// primary.
	ErrNoShardPrimary = errors.New("no primary found for shard")
	// ErrNoShardsForKeyspace occurs when attempting to run a vexec on an empty
	// keyspace.
	ErrNoShardsForKeyspace = errors.New("no shards found in keyspace")
)

var ( // Query parsing and planning errors.
	// ErrUnsupportedQuery occurs when attempting to run an unsupported query
	// through vexec.
	ErrUnsupportedQuery = errors.New("query not supported by vexec")
	// ErrUnsupportedTable occurs when attempting to run vexec on an unsupported
	// table. At the time of writing, this occurs when attempting to query any
	// table other than _vt.vreplication.
	ErrUnsupportedTable = errors.New("table not supported by vexec")
)

// VExec provides the main interface to planning and executing vexec queries
// (normally, queries on tables in the `_vt` database). It currently supports
// some limited vreplication queries; this set of supported behavior will expand
// over time. It may be extended to support schema_migrations queries as well.
type VExec struct {
	ts  *topo.Server
	tmc tmclient.TabletManagerClient

	keyspace string
	workflow string

	// (TODO:@ajm188) Consider renaming this field to "targets", and then
	// support different Strategy functions for loading target tablets from a
	// topo.Server.
	//
	// For this, I'm currently thinking:
	// 		type TargetStrategy func(ts *topo.Server) ([]*topo.TabletInfo, error)
	//
	// We _may_ want this if we ever want a vexec query to target anything other
	// than "all of the shard primaries in a given keyspace", and I'm not sure
	// about potential future usages yet.
	primaries []*topo.TabletInfo
	// (TODO:@ajm188) Similar to supporting a TargetStrategy for controlling how
	// a VExec picks which tablets to query, we may also want an
	// ExecutionStrategy (I'm far less sure about whether we would want this at
	// all, or what its type definition might look like, than TargetStrategy),
	// to support running in modes like:
	// - Execute serially rather than concurrently.
	// - Only return error if greater than some percentage of the targets fail.
}

// NewVExec returns a new instance suitable for making vexec queries to a given
// keyspace (required) and workflow (optional, omit by providing the empty
// string). The provided topo server is used to look up target tablets for
// queries. A given instance will discover targets exactly once for its
// lifetime, so to force a refresh, create another instance.
func NewVExec(keyspace string, workflow string, ts *topo.Server, tmc tmclient.TabletManagerClient) *VExec {
	return &VExec{
		ts:       ts,
		tmc:      tmc,
		keyspace: keyspace,
		workflow: workflow,
	}
}

// QueryContext executes the given vexec query, returning a mapping of tablet
// to querypb.QueryResult.
//
// On first use, QueryContext will also cause the VExec instance to discover
// target tablets from the topo; that target list will be reused for all future
// queries made by this instance.
//
// For details on query parsing and planning, see GetPlanner and the
// QueryPlanner interface.
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

	table, err := extractTableName(stmt)
	if err != nil {
		return nil, err
	}

	planner, err := vx.GetPlanner(ctx, table)
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

	getShardsCtx, getShardsCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer getShardsCancel()

	shards, err := vx.ts.GetShardNames(getShardsCtx, vx.keyspace)
	if err != nil {
		return err
	}

	if len(shards) == 0 {
		return fmt.Errorf("%w %s", ErrNoShardsForKeyspace, vx.keyspace)
	}

	primaries := make([]*topo.TabletInfo, 0, len(shards))

	for _, shard := range shards {
		ctx, cancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
		defer cancel()

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

// GetPlanner returns an appropriate implementation of a QueryPlanner, depending
// on the table being queried.
//
// On first use, GetPlanner will also cause the VExec instance to discover
// target tablets from the topo; that target list will be reused for all future
// queries made by this instance.
func (vx *VExec) GetPlanner(ctx context.Context, table string) (QueryPlanner, error) { // TODO: private?
	if vx.primaries == nil {
		if err := vx.initialize(ctx); err != nil {
			return nil, fmt.Errorf("error while initializing target list: %w", err)
		}
	}

	switch table {
	case qualifiedTableName(VReplicationTableName):
		return NewVReplicationQueryPlanner(vx.tmc, vx.workflow, vx.primaries[0].DbName()), nil
	case qualifiedTableName(SchemaMigrationsTableName):
		return nil, errors.New("Schema Migrations not yet supported in new workflow package")
	default:
		return nil, fmt.Errorf("%w: %v", ErrUnsupportedTable, table)
	}
}

func extractTableName(stmt sqlparser.Statement) (string, error) {
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

func qualifiedTableName(name string) string {
	return fmt.Sprintf("%s.%s", VExecTableQualifier, name)
}
