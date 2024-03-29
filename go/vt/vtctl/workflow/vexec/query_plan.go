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
	"fmt"
	"sync"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// QueryPlan defines the interface to executing a prepared vexec query on one
// or more tablets. Implementations should ensure that it is safe to call the
// various Execute* methods repeatedly and in multiple goroutines.
type QueryPlan interface {
	// Execute executes the planned query on a single target.
	Execute(ctx context.Context, target *topo.TabletInfo) (*querypb.QueryResult, error)
	// ExecuteScatter executes the planned query on the specified targets concurrently,
	// returning a mapping of the target tablet to a querypb.QueryResult.
	ExecuteScatter(ctx context.Context, targets ...*topo.TabletInfo) (map[*topo.TabletInfo]*querypb.QueryResult, error)
}

// FixedQueryPlan wraps a planned query produced by a QueryPlanner. It executes
// the same query with the same bind vals, regardless of the target.
type FixedQueryPlan struct {
	ParsedQuery *sqlparser.ParsedQuery

	workflow string
	tmc      tmclient.TabletManagerClient
}

// Execute is part of the QueryPlan interface.
func (qp *FixedQueryPlan) Execute(ctx context.Context, target *topo.TabletInfo) (qr *querypb.QueryResult, err error) {
	if qp.ParsedQuery == nil {
		return nil, fmt.Errorf("%w: call PlanQuery on a query planner first", ErrUnpreparedQuery)
	}

	targetAliasStr := target.AliasString()

	defer func() {
		if err != nil {
			log.Warningf("Result on %v: %v", targetAliasStr, err)
			return
		}
	}()

	qr, err = qp.tmc.VReplicationExec(ctx, target.Tablet, qp.ParsedQuery.Query)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

// ExecuteScatter is part of the QueryPlan interface. For a FixedQueryPlan, the
// exact same query is executed on each target, and errors from individual
// targets are aggregated into a singular error.
func (qp *FixedQueryPlan) ExecuteScatter(ctx context.Context, targets ...*topo.TabletInfo) (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	if qp.ParsedQuery == nil {
		// This check is an "optimization" on error handling. We check here,
		// even though we will check this during the individual Execute calls,
		// so that we return one error, rather than the same error aggregated
		// len(targets) times.
		return nil, fmt.Errorf("%w: call PlanQuery on a query planner first", ErrUnpreparedQuery)
	}

	var (
		m       sync.Mutex
		wg      sync.WaitGroup
		rec     concurrency.AllErrorRecorder
		results = make(map[*topo.TabletInfo]*querypb.QueryResult, len(targets))
	)

	for _, target := range targets {
		wg.Add(1)

		go func(ctx context.Context, target *topo.TabletInfo) {
			defer wg.Done()

			qr, err := qp.Execute(ctx, target)
			if err != nil {
				rec.RecordError(err)

				return
			}

			m.Lock()
			defer m.Unlock()

			results[target] = qr
		}(ctx, target)
	}

	wg.Wait()

	return results, rec.AggrError(vterrors.Aggregate)
}

// PerTargetQueryPlan implements the QueryPlan interface. Unlike FixedQueryPlan,
// this implementation implements different queries, keyed by tablet alias, on
// different targets.
//
// It is the callers responsibility to ensure that the shape of the QueryResult
// (i.e. fields returned) is consistent for each target's planned query, but
// this is not enforced.
type PerTargetQueryPlan struct {
	ParsedQueries map[string]*sqlparser.ParsedQuery

	tmc tmclient.TabletManagerClient
}

// Execute is part of the QueryPlan interface.
//
// It returns ErrUnpreparedQuery if there is no ParsedQuery for the target's
// tablet alias.
func (qp *PerTargetQueryPlan) Execute(ctx context.Context, target *topo.TabletInfo) (qr *querypb.QueryResult, err error) {
	if qp.ParsedQueries == nil {
		return nil, fmt.Errorf("%w: call PlanQuery on a query planner first", ErrUnpreparedQuery)
	}

	targetAliasStr := target.AliasString()
	query, ok := qp.ParsedQueries[targetAliasStr]
	if !ok {
		return nil, fmt.Errorf("%w: no prepared query for target %s", ErrUnpreparedQuery, targetAliasStr)
	}

	defer func() {
		if err != nil {
			log.Warningf("Result on %v: %v", targetAliasStr, err)
			return
		}
	}()

	qr, err = qp.tmc.VReplicationExec(ctx, target.Tablet, query.Query)
	if err != nil {
		return nil, err
	}

	return qr, nil
}

// ExecuteScatter is part of the QueryPlan interface.
func (qp *PerTargetQueryPlan) ExecuteScatter(ctx context.Context, targets ...*topo.TabletInfo) (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	if qp.ParsedQueries == nil {
		// This check is an "optimization" on error handling. We check here,
		// even though we will check this during the individual Execute calls,
		// so that we return one error, rather than the same error aggregated
		// len(targets) times.
		return nil, fmt.Errorf("%w: call PlanQuery on a query planner first", ErrUnpreparedQuery)
	}

	var (
		m       sync.Mutex
		wg      sync.WaitGroup
		rec     concurrency.AllErrorRecorder
		results = make(map[*topo.TabletInfo]*querypb.QueryResult, len(targets))
	)

	for _, target := range targets {
		wg.Add(1)

		go func(ctx context.Context, target *topo.TabletInfo) {
			defer wg.Done()

			qr, err := qp.Execute(ctx, target)
			if err != nil {
				rec.RecordError(err)

				return
			}

			m.Lock()
			defer m.Unlock()

			results[target] = qr
		}(ctx, target)
	}

	wg.Wait()

	return results, rec.AggrError(vterrors.Aggregate)
}
