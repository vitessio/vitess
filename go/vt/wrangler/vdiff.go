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

package wrangler

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

type vdiff struct {
	mi             *migrater
	sourceCell     string
	targetCell     string
	tabletTypesStr string
	differs        map[string]*tableDiffer
	sources        map[string]*dfParams
	targets        map[string]*dfParams
}

type tableDiffer struct {
	targetTable      string
	targetExpression string
	sourceExpression string
	compareCols      []int
	orderBy          []engine.OrderbyParams
}

type dfParams struct {
	master           *topo.TabletInfo
	tablet           *topodatapb.Tablet
	position         mysql.Position
	snapshotPosition string
	result           chan *sqltypes.Result
	err              error
}

// VDiff reports differences between the sources and targets of a vreplication workflow.
func (wr *Wrangler) VDiff(ctx context.Context, targetKeyspace, workflow, sourceCell, targetCell, tabletTypesStr string, filteredReplicationWaitTime time.Duration) error {
	mi, err := wr.buildMigrater(ctx, targetKeyspace, workflow)
	if err != nil {
		wr.Logger().Errorf("buildMigrater failed: %v", err)
		return err
	}
	if err := mi.validate(ctx, false /* isWrite */); err != nil {
		mi.wr.Logger().Errorf("validate failed: %v", err)
		return err
	}
	df := &vdiff{
		mi:             mi,
		sourceCell:     sourceCell,
		targetCell:     targetCell,
		tabletTypesStr: tabletTypesStr,
		sources:        make(map[string]*dfParams),
		targets:        make(map[string]*dfParams),
	}
	for shard, source := range mi.sources {
		df.sources[shard] = &dfParams{
			master: source.master,
		}
	}
	var oneTarget *miTarget
	for shard, target := range mi.targets {
		df.targets[shard] = &dfParams{
			master: target.master,
		}
		oneTarget = target
	}
	var oneFilter *binlogdatapb.Filter
	for _, bls := range oneTarget.sources {
		oneFilter = bls.Filter
		break
	}
	schm, err := wr.GetSchema(ctx, oneTarget.master.Alias, nil, nil, false)
	if err != nil {
		return err
	}
	df.differs, err = buildVDiffPlan(ctx, oneFilter, schm)
	return err
}

func buildVDiffPlan(ctx context.Context, filter *binlogdatapb.Filter, schm *tabletmanagerdatapb.SchemaDefinition) (map[string]*tableDiffer, error) {
	differs := make(map[string]*tableDiffer)
	for _, table := range schm.TableDefinitions {
		rule, err := vreplication.MatchTable(table.Name, filter)
		if err != nil {
			return nil, err
		}
		if rule == nil {
			continue
		}
		query := rule.Filter
		if rule.Filter == "" || key.IsKeyRange(rule.Filter) {
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf("select * from %v", sqlparser.NewTableIdent(table.Name))
			query = buf.String()
		}
		differs[table.Name], err = buildDifferPlan(table, query)
		if err != nil {
			return nil, err
		}
	}
	return differs, nil
}

func buildDifferPlan(table *tabletmanagerdatapb.TableDefinition, query string) (*tableDiffer, error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(statement))
	}
	td := &tableDiffer{
		targetTable: table.Name,
	}
	sourceSelect := &sqlparser.Select{}
	targetSelect := &sqlparser.Select{}
	for _, selExpr := range sel.SelectExprs {
		switch selExpr := selExpr.(type) {
		case *sqlparser.StarExpr:
			for _, fld := range table.Fields {
				aliased := &sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent(fld.Name)}}
				sourceSelect.SelectExprs = append(sourceSelect.SelectExprs, aliased)
				targetSelect.SelectExprs = append(targetSelect.SelectExprs, aliased)
			}
		case *sqlparser.AliasedExpr:
			var targetCol *sqlparser.ColName
			if !selExpr.As.IsEmpty() {
				targetCol = &sqlparser.ColName{Name: selExpr.As}
			} else {
				if colAs, ok := selExpr.Expr.(*sqlparser.ColName); ok {
					targetCol = colAs
				} else {
					return nil, fmt.Errorf("expression needs an alias: %v", sqlparser.String(selExpr))
				}
			}
			sourceSelect.SelectExprs = append(sourceSelect.SelectExprs, selExpr)
			targetSelect.SelectExprs = append(targetSelect.SelectExprs, &sqlparser.AliasedExpr{Expr: targetCol})
		}
	}
	fields := make(map[string]querypb.Type)
	for _, field := range table.Fields {
		fields[strings.ToLower(field.Name)] = field.Type
	}

	td.compareCols = make([]int, len(sourceSelect.SelectExprs))
	for i := range td.compareCols {
		colname := sourceSelect.SelectExprs[i].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Name.Lowered()
		typ, ok := fields[colname]
		if !ok {
			return nil, fmt.Errorf("column %v not found in table %v", colname, table.Name)
		}
		td.compareCols[i] = i
		if sqltypes.IsText(typ) {
			sourceSelect.SelectExprs = append(sourceSelect.SelectExprs, wrapWeightString(sourceSelect.SelectExprs[i]))
			targetSelect.SelectExprs = append(targetSelect.SelectExprs, wrapWeightString(targetSelect.SelectExprs[i]))
			td.compareCols[i] = len(sourceSelect.SelectExprs) - 1
		}
	}

	sourceSelect.From = sel.From
	targetSelect.From = sqlparser.TableExprs{
		&sqlparser.AliasedTableExpr{
			Expr: &sqlparser.TableName{
				Name: sqlparser.NewTableIdent(table.Name),
			},
		},
	}

	var orderby sqlparser.OrderBy
	for _, pk := range table.PrimaryKeyColumns {
		found := false
		for i, selExpr := range targetSelect.SelectExprs {
			colname := selExpr.(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Name.Lowered()
			if pk == colname {
				td.orderBy = append(td.orderBy, engine.OrderbyParams{Col: td.compareCols[i]})
				found = true
				break
			}
		}
		if !found {
			// Unreachable.
			return nil, fmt.Errorf("column %v not found in table %v", pk, table.Name)
		}
		orderby = append(orderby, &sqlparser.Order{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent(pk)}})
	}
	targetSelect.OrderBy = orderby

	sourceSelect.Where = removeKeyrange(sel.Where)
	sourceSelect.GroupBy = sel.GroupBy
	sourceSelect.OrderBy = orderby

	td.sourceExpression = sqlparser.String(sourceSelect)
	td.targetExpression = sqlparser.String(targetSelect)
	return td, nil
}

func (df *vdiff) stopTargetStreams(ctx context.Context) error {
	var mu sync.Mutex

	err := df.forAll(df.targets, func(shard string, target *dfParams) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Stopped', message='for vdiff' where db_name=%s and workflow=%s", encodeString(target.master.DbName()), encodeString(df.mi.workflow))
		_, err := df.mi.wr.tmc.VReplicationExec(ctx, target.master.Tablet, query)
		if err != nil {
			return err
		}
		query = fmt.Sprintf("select source, pos from _vt.vreplication where db_name=%s and workflow=%s", encodeString(target.master.DbName()), encodeString(df.mi.workflow))
		p3qr, err := df.mi.wr.tmc.VReplicationExec(ctx, target.master.Tablet, query)
		if err != nil {
			return err
		}
		qr := sqltypes.Proto3ToResult(p3qr)

		for _, row := range qr.Rows {
			var bls binlogdatapb.BinlogSource
			if err := proto.UnmarshalText(row[0].ToString(), &bls); err != nil {
				return err
			}
			pos, err := mysql.DecodePosition(row[1].ToString())
			if err != nil {
				return err
			}
			func() {
				mu.Lock()
				defer mu.Unlock()

				source, ok := df.sources[bls.Shard]
				if !ok {
					// Unreachable.
					return
				}
				if !source.position.IsZero() && source.position.AtLeast(pos) {
					return
				}
				source.position = pos
			}()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (df *vdiff) selectTablets(ctx context.Context) error {
	var wg sync.WaitGroup
	var err1, err2 error

	// Parallelize all discovery.
	wg.Add(1)
	go func() {
		defer wg.Done()
		err1 = df.forAll(df.sources, func(shard string, source *dfParams) error {
			tp, err := discovery.NewTabletPicker(ctx, df.mi.wr.ts, df.targetCell, df.mi.targetKeyspace, shard, df.tabletTypesStr)
			if err != nil {
				return err
			}
			defer tp.Close()

			tablet, err := tp.PickForStreaming(ctx)
			if err != nil {
				return err
			}
			source.tablet = tablet
			return nil
		})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err2 = df.forAll(df.targets, func(shard string, target *dfParams) error {
			tp, err := discovery.NewTabletPicker(ctx, df.mi.wr.ts, df.targetCell, df.mi.targetKeyspace, shard, df.tabletTypesStr)
			if err != nil {
				return err
			}
			defer tp.Close()

			tablet, err := tp.PickForStreaming(ctx)
			if err != nil {
				return err
			}
			target.tablet = tablet
			return nil
		})
	}()

	wg.Wait()
	if err1 != nil {
		return err1
	}
	return err2
}

func (df *vdiff) startQueryStreams(ctx context.Context, participans map[string]*dfParams, query string) error {
	err := df.forAll(participans, func(shard string, participant *dfParams) error {
		// Iteration for each participant.
		if err := df.mi.wr.tmc.WaitForPosition(ctx, participant.tablet, mysql.EncodePosition(participant.position)); err != nil {
			return err
		}
		participant.result = make(chan *sqltypes.Result, 1)
		gtidch := make(chan string, 1)

		// Start the stream in a separate goroutine.
		go df.streamOne(ctx, shard, participant, query, gtidch)

		// Wait for the gtid to be sent. If it's not received, there was an error
		// which would be stored in participant.err.
		gtid, ok := <-gtidch
		if !ok {
			return participant.err
		}
		// Save the new position, as of when the query executed.
		participant.snapshotPosition = gtid
		return nil
	})
	return err
}

// streamOne is called as a goroutine, and communicates its results through channels.
// It first sends the snapshot gtid to gtidch.
// Then it streams results to participant.result.
// Before returning, it sets participant.err, and closes all channels.
// If any channel is closed, then participant.err can be checked if there was an error.
func (df *vdiff) streamOne(ctx context.Context, shard string, participant *dfParams, query string, gtidch chan string) {
	defer close(participant.result)
	defer close(gtidch)

	// Wrap the streaming in a separate function so we can capture the error.
	// This shows that the error will be set before the channels are closed.
	participant.err = func() error {
		conn, err := tabletconn.GetDialer()(participant.tablet, grpcclient.FailFast(false))
		if err != nil {
			return err
		}
		defer conn.Close(ctx)

		target := &querypb.Target{
			Keyspace:   df.mi.sourceKeyspace,
			Shard:      shard,
			TabletType: participant.tablet.Type,
		}
		var fields []*querypb.Field
		err = conn.VStreamResults(ctx, target, query, func(vrs *binlogdatapb.VStreamResultsResponse) error {
			if vrs.Fields != nil {
				fields = vrs.Fields
				gtidch <- vrs.Gtid
			}
			p3qr := &querypb.QueryResult{
				Fields: fields,
				Rows:   vrs.Rows,
			}
			result := sqltypes.Proto3ToResult(p3qr)
			select {
			case participant.result <- result:
			case <-ctx.Done():
				return io.EOF
			}
			return nil
		})
		return err
	}()
}

func (df *vdiff) syncTargets(ctx context.Context) error {
	err := df.mi.forAllUids(func(target *miTarget, uid uint32) error {
		bls := target.sources[uid]
		pos := df.sources[bls.Shard].snapshotPosition
		query := fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos='%s', message='synchronizing for vdiff' where id=%d", pos, uid)
		if _, err := df.mi.wr.tmc.VReplicationExec(ctx, target.master.Tablet, query); err != nil {
			return err
		}
		return df.mi.wr.tmc.VReplicationWaitForPos(ctx, target.master.Tablet, int(uid), pos)
	})
	if err != nil {
		return err
	}

	err = df.forAll(df.targets, func(shard string, target *dfParams) error {
		pos, err := df.mi.wr.tmc.MasterPosition(ctx, target.master.Tablet)
		if err != nil {
			return err
		}
		mpos, err := mysql.DecodePosition(pos)
		if err != nil {
			return err
		}
		target.position = mpos
		return nil
	})
	return err
}

func (df *vdiff) restartTargets(ctx context.Context) error {
	return df.forAll(df.targets, func(shard string, target *dfParams) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', message='' where db_name=%s and workflow=%s", encodeString(target.master.DbName()), encodeString(df.mi.workflow))
		_, err := df.mi.wr.tmc.VReplicationExec(ctx, target.master.Tablet, query)
		return err
	})
}

func (df *vdiff) forAll(participants map[string]*dfParams, f func(string, *dfParams) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for shard, participant := range participants {
		wg.Add(1)
		go func(shard string, participant *dfParams) {
			defer wg.Done()

			if err := f(shard, participant); err != nil {
				allErrors.RecordError(err)
			}
		}(shard, participant)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

var _ engine.VCursor = (*resultReader)(nil)

// resultReader performs a merge-sorted read from the participants.
type resultReader struct {
	ctx          context.Context
	cancel       func()
	participants map[string]*dfParams
	orderBy      []engine.OrderbyParams
	result       chan *sqltypes.Result
}

func newResultReader(ctx context.Context, participants map[string]*dfParams, orderBy []engine.OrderbyParams) *resultReader {
	ctx, cancel := context.WithCancel(ctx)
	rr := &resultReader{
		ctx:          ctx,
		cancel:       cancel,
		participants: participants,
		orderBy:      orderBy,
		result:       make(chan *sqltypes.Result, 1),
	}
	rss := make([]*srvtopo.ResolvedShard, 0, len(participants))
	for shard := range participants {
		rss = append(rss, &srvtopo.ResolvedShard{
			Target: &querypb.Target{
				Shard: shard,
			},
		})
	}
	go engine.MergeSort(rr, "", rr.orderBy, rss, nil, func(qr *sqltypes.Result) error {
		select {
		case rr.result <- qr:
		case <-rr.ctx.Done():
			return io.EOF
		}
		return nil
	})
	return rr
}

func (rr *resultReader) Next() (*sqltypes.Result, error) {
	select {
	case qr := <-rr.result:
		return qr, nil
	case <-rr.ctx.Done():
		return nil, io.EOF
	}
}

func (rr *resultReader) Close() {
	rr.cancel()
}

func (rr *resultReader) Context() context.Context {
	return nil
}

func (rr *resultReader) MaxMemoryRows() int {
	return 0
}

func (rr *resultReader) SetContextTimeout(timeout time.Duration) context.CancelFunc {
	return nil
}

func (rr *resultReader) RecordWarning(warning *querypb.QueryWarning) {
}

func (rr *resultReader) Execute(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error) {
	return nil, nil
}

func (rr *resultReader) AutocommitApproval() bool {
	return false
}

func (rr *resultReader) ExecuteMultiShard(rss []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, isDML, canAutocommit bool) (*sqltypes.Result, []error) {
	return nil, nil
}

func (rr *resultReader) ExecuteStandalone(query string, bindvars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard) (*sqltypes.Result, error) {
	return nil, nil
}

func (rr *resultReader) StreamExecuteMulti(query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error {
	for result := range rr.participants[rss[0].Target.Shard].result {
		if err := callback(result); err != nil {
			return err
		}
	}
	return rr.participants[rss[0].Target.Shard].err
}

func (rr *resultReader) ExecuteKeyspaceID(keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, isDML, autocommit bool) (*sqltypes.Result, error) {
	return nil, nil
}

func (rr *resultReader) ResolveDestinations(keyspace string, ids []*querypb.Value, destinations []key.Destination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	return nil, nil, nil
}

func removeKeyrange(where *sqlparser.Where) *sqlparser.Where {
	if where == nil {
		return nil
	}
	if isFuncKeyrange(where.Expr) {
		return nil
	}
	where.Expr = removeExprKeyrange(where.Expr)
	return where
}

func removeExprKeyrange(node sqlparser.Expr) sqlparser.Expr {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		if isFuncKeyrange(node.Left) {
			return removeExprKeyrange(node.Right)
		}
		if isFuncKeyrange(node.Right) {
			return removeExprKeyrange(node.Left)
		}
		return &sqlparser.AndExpr{
			Left:  removeExprKeyrange(node.Left),
			Right: removeExprKeyrange(node.Right),
		}
	case *sqlparser.ParenExpr:
		return &sqlparser.ParenExpr{
			Expr: removeExprKeyrange(node.Expr),
		}
	}
	return node
}

func isFuncKeyrange(expr sqlparser.Expr) bool {
	funcExpr, ok := expr.(*sqlparser.FuncExpr)
	return ok && funcExpr.Name.EqualString("in_keyrange")
}

func wrapWeightString(expr sqlparser.SelectExpr) *sqlparser.AliasedExpr {
	return &sqlparser.AliasedExpr{
		Expr: &sqlparser.FuncExpr{
			Name: sqlparser.NewColIdent("weight_string"),
			Exprs: []sqlparser.SelectExpr{
				&sqlparser.AliasedExpr{
					Expr: expr.(*sqlparser.AliasedExpr).Expr,
				},
			},
		},
	}
}
