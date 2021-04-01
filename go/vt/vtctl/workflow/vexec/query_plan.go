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

// QueryPlan wraps a planned query produced by a QueryPlanner. It is safe to
// execute a QueryPlan repeatedly and in multiple goroutines.
type QueryPlan struct {
	ParsedQuery *sqlparser.ParsedQuery

	workflow string
	tmc      tmclient.TabletManagerClient
}

// Execute executes a QueryPlan on a single target.
func (qp *QueryPlan) Execute(ctx context.Context, target *topo.TabletInfo) (qr *querypb.QueryResult, err error) {
	if qp.ParsedQuery == nil {
		return nil, fmt.Errorf("%w: call PlanQuery on a query planner first", ErrUnpreparedQuery)
	}

	targetAliasStr := target.AliasString()

	log.Infof("Running %v on %v", qp.ParsedQuery.Query, targetAliasStr)
	defer func() {
		if err != nil {
			log.Warningf("Result on %v: %v", targetAliasStr, err)

			return
		}

		log.Infof("Result on %v: %v", targetAliasStr, qr)
	}()

	qr, err = qp.tmc.VReplicationExec(ctx, target.Tablet, qp.ParsedQuery.Query)
	if err != nil {
		return nil, err
	}

	if qr.RowsAffected == 0 {
		log.Infof("no matching streams found for workflows %s, tablet %s, query %s", qp.workflow, targetAliasStr, qp.ParsedQuery.Query)
	}

	return qr, nil
}

// ExecuteScatter executes a QueryPlan on multiple targets concurrently,
// returning a mapping of target tablet to querypb.QueryResult. Errors from
// individual targets are aggregated into a singular error.
func (qp *QueryPlan) ExecuteScatter(ctx context.Context, targets ...*topo.TabletInfo) (map[*topo.TabletInfo]*querypb.QueryResult, error) {
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
