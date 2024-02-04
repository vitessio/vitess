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
	"sync"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/workflow/common"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

const (
	// VExecTableQualifier is the qualifier that all tables supported by vexec
	// are prefixed by.
	VExecTableQualifier = "_vt"

	// VReplicationLogTableName is the unqualified name of the vreplication_log
	// table supported by vexec.
	VReplicationLogTableName = "vreplication_log"
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

	parser      *sqlparser.Parser
	shardSubset []string
}

// NewVExec returns a new instance suitable for making vexec queries to a given
// keyspace (required) and workflow (optional, omit by providing the empty
// string). The provided topo server is used to look up target tablets for
// queries. A given instance will discover targets exactly once for its
// lifetime, so to force a refresh, create another instance.
func NewVExec(keyspace string, workflow string, ts *topo.Server, tmc tmclient.TabletManagerClient, parser *sqlparser.Parser) *VExec {
	return &VExec{
		ts:       ts,
		tmc:      tmc,
		keyspace: keyspace,
		workflow: workflow,
		parser:   parser,
	}
}

func (vx *VExec) SetShardSubset(shardSubset []string) {
	vx.shardSubset = shardSubset
}

func (vx *VExec) GetShardSubset() []string {
	return vx.shardSubset
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

	stmt, err := vx.parser.Parse(query)
	if err != nil {
		return nil, err
	}

	table, err := extractTableName(stmt)
	if err != nil {
		return nil, err
	}

	planner, err := vx.getPlanner(ctx, table)
	if err != nil {
		return nil, err
	}

	qp, err := planner.PlanQuery(stmt)
	if err != nil {
		return nil, err
	}

	return qp.ExecuteScatter(ctx, vx.primaries...)
}

// CallbackContext executes the given callback, returning a mapping of tablet
// to querypb.QueryResult.
//
// On first use, QueryContext will also cause the VExec instance to discover
// target tablets from the topo; that target list will be reused for all future
// callbacks executed by this instance.
func (vx *VExec) CallbackContext(ctx context.Context, callback func(context.Context, *topo.TabletInfo) (*querypb.QueryResult, error)) (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	if vx.primaries == nil {
		if err := vx.initialize(ctx); err != nil {
			return nil, err
		}
	}
	return vx.execCallback(ctx, callback)
}

// execCallback runs the provided callback function on backend shard primaries.
// It collects query results from all shards and returns an aggregate (UNION
// ALL -like) result.
// Note: any nil results from the callback are ignored.
func (vx *VExec) execCallback(ctx context.Context, callback func(context.Context, *topo.TabletInfo) (*querypb.QueryResult, error)) (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	var (
		wg sync.WaitGroup
		mu sync.Mutex

		allErrors = &concurrency.AllErrorRecorder{}
		results   = make(map[*topo.TabletInfo]*querypb.QueryResult)
	)
	for _, primary := range vx.primaries {
		wg.Add(1)
		go func(ctx context.Context, primary *topo.TabletInfo) {
			defer wg.Done()
			qr, err := callback(ctx, primary)
			if err != nil {
				allErrors.RecordError(err)
			} else {
				if qr == nil {
					log.Infof("Callback returned nil result for tablet %s-%s", primary.Alias.Cell, primary.Alias.Uid)
					return // no result
				}
				mu.Lock()
				defer mu.Unlock()
				results[primary] = qr
			}
		}(ctx, primary)
	}
	wg.Wait()
	return results, allErrors.AggrError(vterrors.Aggregate)
}

func (vx *VExec) initialize(ctx context.Context) error {
	vx.primaries = nil

	getShardsCtx, getShardsCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer getShardsCancel()

	shards, err := common.GetShards(getShardsCtx, vx.ts, vx.keyspace, vx.shardSubset)
	if err != nil {
		return err
	}

	primaries := make([]*topo.TabletInfo, 0, len(shards))

	for _, shard := range shards {
		ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
		defer cancel()

		si, err := vx.ts.GetShard(ctx, vx.keyspace, shard)
		if err != nil {
			return err
		}

		if si.PrimaryAlias == nil {
			return fmt.Errorf("%w %s/%s", ErrNoShardPrimary, vx.keyspace, shard)
		}

		primary, err := vx.ts.GetTablet(ctx, si.PrimaryAlias)
		if err != nil {
			return err
		}

		if primary == nil {
			return fmt.Errorf("%w %s/%s: tablet %v not found", ErrNoShardPrimary, vx.keyspace, shard, topoproto.TabletAliasString(si.PrimaryAlias))
		}

		primaries = append(primaries, primary)
	}

	vx.primaries = primaries

	return nil
}

// getPlanner returns an appropriate implementation of a QueryPlanner, depending
// on the table being queried.
//
// On first use, getPlanner will also cause the VExec instance to discover
// target tablets from the topo; that target list will be reused for all future
// queries made by this instance.
func (vx *VExec) getPlanner(ctx context.Context, table string) (QueryPlanner, error) {
	if vx.primaries == nil {
		if err := vx.initialize(ctx); err != nil {
			return nil, fmt.Errorf("error while initializing target list: %w", err)
		}
	}

	switch table {
	case qualifiedTableName(VReplicationTableName):
		return NewVReplicationQueryPlanner(vx.tmc, vx.workflow, vx.primaries[0].DbName()), nil
	case qualifiedTableName(VReplicationLogTableName):
		results, err := vx.QueryContext(ctx, "select id from _vt.vreplication")
		if err != nil {
			return nil, err
		}

		tabletStreamIDMap := make(map[string][]int64, len(results))

		for tablet, p3qr := range results {
			qr := sqltypes.Proto3ToResult(p3qr)
			aliasStr := tablet.AliasString()
			tabletStreamIDMap[aliasStr] = make([]int64, len(qr.Rows))

			for i, row := range qr.Rows {
				id, err := row[0].ToCastInt64()
				if err != nil {
					return nil, err
				}

				tabletStreamIDMap[aliasStr][i] = id
			}
		}

		return NewVReplicationLogQueryPlanner(vx.tmc, tabletStreamIDMap), nil
	default:
		return nil, fmt.Errorf("%w: %v", ErrUnsupportedTable, table)
	}
}

// WithWorkflow returns a copy of VExec with the Workflow field updated. Used so
// callers to reuse a VExec's primaries list without needing to initialize a new
// VExec instance.
func (vx *VExec) WithWorkflow(workflow string) *VExec {
	return &VExec{
		ts:        vx.ts,
		tmc:       vx.tmc,
		primaries: vx.primaries,
		parser:    vx.parser,
		workflow:  workflow,
	}
}

func extractTableName(stmt sqlparser.Statement) (string, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Update:
		return sqlparser.ToString(stmt.TableExprs), nil
	case *sqlparser.Delete:
		return sqlparser.ToString(stmt.TableExprs), nil
	case *sqlparser.Insert:
		return sqlparser.String(stmt.Table), nil
	case *sqlparser.Select:
		return sqlparser.ToString(stmt.From), nil
	}

	return "", fmt.Errorf("%w: %+v", ErrUnsupportedQuery, sqlparser.String(stmt))
}

func qualifiedTableName(name string) string {
	return fmt.Sprintf("%s.%s", VExecTableQualifier, name)
}
