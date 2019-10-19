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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

// DiffReport is the summary of differences for one table.
type DiffReport struct {
	ProcessedRows   int
	MatchingRows    int
	MismatchedRows  int
	ExtraRowsSource int
	ExtraRowsTarget int
}

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
	sourceExpression string
	targetExpression string
	compareCols      []int
	comparePKs       []int
	sourcePrimitive  engine.Primitive
	targetPrimitive  engine.Primitive
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
func (wr *Wrangler) VDiff(ctx context.Context, targetKeyspace, workflow, sourceCell, targetCell, tabletTypesStr string, filteredReplicationWaitTime time.Duration) (map[string]*DiffReport, error) {
	if sourceCell == "" && targetCell == "" {
		cells, err := wr.ts.GetCellInfoNames(ctx)
		if err != nil {
			return nil, err
		}
		if len(cells) == 0 {
			// Unreachable
			return nil, fmt.Errorf("there are no cells in the topo")
		}
		sourceCell = cells[0]
		targetCell = sourceCell
	}
	if sourceCell == "" {
		sourceCell = targetCell
	}
	if targetCell == "" {
		targetCell = sourceCell
	}
	mi, err := wr.buildMigrater(ctx, targetKeyspace, workflow)
	if err != nil {
		wr.Logger().Errorf("buildMigrater: %v", err)
		return nil, err
	}
	if err := mi.validate(ctx, false /* isWrite */); err != nil {
		mi.wr.Logger().Errorf("validate: %v", err)
		return nil, err
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
		return nil, vterrors.Wrap(err, "GetSchema")
	}
	df.differs, err = buildVDiffPlan(ctx, oneFilter, schm)
	if err != nil {
		return nil, vterrors.Wrap(err, "buildVDiffPlan")
	}
	if err := df.selectTablets(ctx); err != nil {
		return nil, vterrors.Wrap(err, "selectTablets")
	}
	defer func(ctx context.Context) {
		if err := df.restartTargets(ctx); err != nil {
			wr.Logger().Errorf("Could not restart workflow %s: %v, please restart it manually", workflow, err)
		}
	}(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// TODO(sougou): parallelize
	diffReports := make(map[string]*DiffReport)
	for table, td := range df.differs {
		if err := df.stopTargets(ctx); err != nil {
			return nil, vterrors.Wrap(err, "stopTargets")
		}
		sourceReader, err := df.startQueryStreams(ctx, df.mi.sourceKeyspace, df.sources, td.sourceExpression, filteredReplicationWaitTime)
		if err != nil {
			return nil, vterrors.Wrap(err, "startQueryStreams(sources)")
		}
		if err := df.syncTargets(ctx, filteredReplicationWaitTime); err != nil {
			return nil, vterrors.Wrap(err, "syncTargets")
		}
		targetReader, err := df.startQueryStreams(ctx, df.mi.targetKeyspace, df.targets, td.targetExpression, filteredReplicationWaitTime)
		if err != nil {
			return nil, vterrors.Wrap(err, "startQueryStreams(targets)")
		}
		if err := df.restartTargets(ctx); err != nil {
			return nil, vterrors.Wrap(err, "restartTargets")
		}
		dr, err := td.diff(ctx, df.mi.wr, sourceReader, targetReader)
		if err != nil {
			return nil, vterrors.Wrap(err, "diff")
		}
		wr.Logger().Printf("Summary for %v: %+v\n", td.targetTable, *dr)
		diffReports[table] = dr
	}
	return diffReports, nil
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
	var aggregates []engine.AggregateParams
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

			// Check if it's an aggregate expression
			if expr, ok := selExpr.Expr.(*sqlparser.FuncExpr); ok {
				switch fname := expr.Name.Lowered(); fname {
				case "count", "sum":
					aggregates = append(aggregates, engine.AggregateParams{
						Opcode: engine.SupportedAggregates[fname],
						Col:    len(sourceSelect.SelectExprs) - 1,
					})
				}
			}
		default:
			return nil, fmt.Errorf("unexpected: %v", sqlparser.String(statement))
		}
	}
	fields := make(map[string]querypb.Type)
	for _, field := range table.Fields {
		fields[strings.ToLower(field.Name)] = field.Type
	}

	td.compareCols = make([]int, len(sourceSelect.SelectExprs))
	for i := range td.compareCols {
		colname := targetSelect.SelectExprs[i].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Name.Lowered()
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
				td.comparePKs = append(td.comparePKs, td.compareCols[i])
				// We'll be comparing pks seperately. So, remove them from compareCols.
				td.compareCols[i] = -1
				found = true
				break
			}
		}
		if !found {
			// Unreachable.
			return nil, fmt.Errorf("column %v not found in table %v", pk, table.Name)
		}
		orderby = append(orderby, &sqlparser.Order{
			Expr:      &sqlparser.ColName{Name: sqlparser.NewColIdent(pk)},
			Direction: sqlparser.AscScr,
		})
	}
	sourceSelect.Where = removeKeyrange(sel.Where)
	sourceSelect.GroupBy = sel.GroupBy
	sourceSelect.OrderBy = orderby

	targetSelect.OrderBy = orderby

	td.sourceExpression = sqlparser.String(sourceSelect)
	td.targetExpression = sqlparser.String(targetSelect)

	td.sourcePrimitive = newMergeSorter(td.comparePKs)
	td.targetPrimitive = newMergeSorter(td.comparePKs)
	if len(aggregates) != 0 {
		td.sourcePrimitive = &engine.OrderedAggregate{
			Aggregates: aggregates,
			Keys:       td.comparePKs,
			Input:      td.sourcePrimitive,
		}
	}

	return td, nil
}

func (df *vdiff) selectTablets(ctx context.Context) error {
	var wg sync.WaitGroup
	var err1, err2 error

	// Parallelize all discovery.
	wg.Add(1)
	go func() {
		defer wg.Done()
		err1 = df.forAll(df.sources, func(shard string, source *dfParams) error {
			tp, err := discovery.NewTabletPicker(ctx, df.mi.wr.ts, df.sourceCell, df.mi.sourceKeyspace, shard, df.tabletTypesStr)
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

func (df *vdiff) stopTargets(ctx context.Context) error {
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

func (df *vdiff) startQueryStreams(ctx context.Context, keyspace string, participants map[string]*dfParams, query string, filteredReplicationWaitTime time.Duration) (*resultReader, error) {
	waitCtx, cancel := context.WithTimeout(ctx, filteredReplicationWaitTime)
	defer cancel()
	err := df.forAll(participants, func(shard string, participant *dfParams) error {
		// Iteration for each participant.
		if err := df.mi.wr.tmc.WaitForPosition(waitCtx, participant.tablet, mysql.EncodePosition(participant.position)); err != nil {
			return vterrors.Wrapf(err, "WaitForPosition for tablet %v", topoproto.TabletAliasString(participant.tablet.Alias))
		}
		participant.result = make(chan *sqltypes.Result, 1)
		gtidch := make(chan string, 1)

		// Start the stream in a separate goroutine.
		go df.streamOne(ctx, keyspace, shard, participant, query, gtidch)

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
	if err != nil {
		return nil, err
	}
	return newResultReader(ctx, participants), nil
}

// streamOne is called as a goroutine, and communicates its results through channels.
// It first sends the snapshot gtid to gtidch.
// Then it streams results to participant.result.
// Before returning, it sets participant.err, and closes all channels.
// If any channel is closed, then participant.err can be checked if there was an error.
func (df *vdiff) streamOne(ctx context.Context, keyspace, shard string, participant *dfParams, query string, gtidch chan string) {
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
			Keyspace:   keyspace,
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
			// Fields should be received only once, and sent only once.
			if vrs.Fields == nil {
				result.Fields = nil
			}
			select {
			case participant.result <- result:
			case <-ctx.Done():
				return vterrors.Wrap(ctx.Err(), "VStreamResults")
			}
			return nil
		})
		return err
	}()
}

func (df *vdiff) syncTargets(ctx context.Context, filteredReplicationWaitTime time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, filteredReplicationWaitTime)
	defer cancel()
	err := df.mi.forAllUids(func(target *miTarget, uid uint32) error {
		bls := target.sources[uid]
		pos := df.sources[bls.Shard].snapshotPosition
		query := fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos='%s', message='synchronizing for vdiff' where id=%d", pos, uid)
		if _, err := df.mi.wr.tmc.VReplicationExec(ctx, target.master.Tablet, query); err != nil {
			return err
		}
		if err := df.mi.wr.tmc.VReplicationWaitForPos(waitCtx, target.master.Tablet, int(uid), pos); err != nil {
			return vterrors.Wrapf(err, "VReplicationWaitForPos for tablet %v", topoproto.TabletAliasString(target.master.Tablet.Alias))
		}
		return nil
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
		query := fmt.Sprintf("update _vt.vreplication set state='Running', message='', stop_pos='' where db_name=%s and workflow=%s", encodeString(target.master.DbName()), encodeString(df.mi.workflow))
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

//-----------------------------------------------------------------
// primitiveExecutor

type primitiveExecutor struct {
	prim     engine.Primitive
	rows     [][]sqltypes.Value
	resultch chan *sqltypes.Result
	err      error
}

func newPrimitiveExecutor(ctx context.Context, vcursor engine.VCursor, prim engine.Primitive) *primitiveExecutor {
	pe := &primitiveExecutor{
		prim:     prim,
		resultch: make(chan *sqltypes.Result, 1),
	}
	go func() {
		defer close(pe.resultch)
		pe.err = pe.prim.StreamExecute(vcursor, make(map[string]*querypb.BindVariable), false, func(qr *sqltypes.Result) error {
			select {
			case pe.resultch <- qr:
			case <-ctx.Done():
				return vterrors.Wrap(ctx.Err(), "Outer Stream")
			}
			return nil
		})
	}()
	return pe
}

func (pe *primitiveExecutor) next() ([]sqltypes.Value, error) {
	for len(pe.rows) == 0 {
		qr, ok := <-pe.resultch
		if !ok {
			return nil, pe.err
		}
		pe.rows = qr.Rows
	}

	row := pe.rows[0]
	pe.rows = pe.rows[1:]
	return row, nil
}

func (pe *primitiveExecutor) drain(ctx context.Context) (int, error) {
	count := 0
	for {
		row, err := pe.next()
		if err != nil {
			return 0, err
		}
		if row == nil {
			return count, nil
		}
		count++
	}
}

//-----------------------------------------------------------------
// mergeSorter

var _ engine.Primitive = (*mergeSorter)(nil)

// mergeSorter performs a merge-sorted read from the participants.
type mergeSorter struct {
	engine.Primitive
	orderBy []engine.OrderbyParams
}

func newMergeSorter(comparePKs []int) *mergeSorter {
	ob := make([]engine.OrderbyParams, 0, len(comparePKs))
	for _, col := range comparePKs {
		ob = append(ob, engine.OrderbyParams{Col: col})
	}
	return &mergeSorter{
		orderBy: ob,
	}
}

func (ms *mergeSorter) StreamExecute(vcursor engine.VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	rr, ok := vcursor.(*resultReader)
	if !ok {
		return fmt.Errorf("internal error: vcursor is not a resultReader: %T", vcursor)
	}
	rss := make([]*srvtopo.ResolvedShard, 0, len(rr.participants))
	bvs := make([]map[string]*querypb.BindVariable, 0, len(rr.participants))
	for shard := range rr.participants {
		rss = append(rss, &srvtopo.ResolvedShard{
			Target: &querypb.Target{
				Shard: shard,
			},
		})
		bvs = append(bvs, bindVars)
	}
	return engine.MergeSort(vcursor, "", ms.orderBy, rss, bvs, callback)
}

//-----------------------------------------------------------------
// resultReader

// resultReader acts as a VCursor for the wrapping primitives.
type resultReader struct {
	engine.VCursor
	ctx          context.Context
	participants map[string]*dfParams
}

func newResultReader(ctx context.Context, participants map[string]*dfParams) *resultReader {
	return &resultReader{
		ctx:          ctx,
		participants: participants,
	}
}

func (rr *resultReader) Context() context.Context {
	return rr.ctx
}

func (rr *resultReader) StreamExecuteMulti(query string, rss []*srvtopo.ResolvedShard, bindVars []map[string]*querypb.BindVariable, callback func(reply *sqltypes.Result) error) error {
	for result := range rr.participants[rss[0].Target.Shard].result {
		if err := callback(result); err != nil {
			return err
		}
	}
	return rr.participants[rss[0].Target.Shard].err
}

//-----------------------------------------------------------------
// tableDiffer

func (td *tableDiffer) diff(ctx context.Context, wr *Wrangler, sourceReader, targetReader *resultReader) (*DiffReport, error) {
	sourceExecutor := newPrimitiveExecutor(ctx, sourceReader, td.sourcePrimitive)
	targetExecutor := newPrimitiveExecutor(ctx, targetReader, td.targetPrimitive)
	dr := &DiffReport{}
	var sourceRow, targetRow []sqltypes.Value
	var err error
	advanceSource := true
	advanceTarget := true
	for {
		if advanceSource {
			sourceRow, err = sourceExecutor.next()
			if err != nil {
				return nil, err
			}
		}
		if advanceTarget {
			targetRow, err = targetExecutor.next()
			if err != nil {
				return nil, err
			}
		}

		if sourceRow == nil && targetRow == nil {
			return dr, nil
		}

		advanceSource = true
		advanceTarget = true

		if sourceRow == nil {
			// drain target, update count
			wr.Logger().Errorf("Draining extra row(s) found on the target starting with: %v", targetRow)
			count, err := targetExecutor.drain(ctx)
			if err != nil {
				return nil, err
			}
			dr.ExtraRowsTarget += 1 + count
			dr.ProcessedRows += 1 + count
			return dr, nil
		}
		if targetRow == nil {
			// no more rows from the target
			// we know we have rows from source, drain, update count
			wr.Logger().Errorf("Draining extra row(s) found on the source starting with: %v", sourceRow)
			count, err := sourceExecutor.drain(ctx)
			if err != nil {
				return nil, err
			}
			dr.ExtraRowsSource += 1 + count
			dr.ProcessedRows += 1 + count
			return dr, nil
		}

		dr.ProcessedRows++

		// Compare pk values.
		c, err := td.compare(sourceRow, targetRow, td.comparePKs)
		switch {
		case err != nil:
			return nil, err
		case c < 0:
			if dr.ExtraRowsSource < 10 {
				wr.Logger().Errorf("[table=%v] Extra row %v on source: %v", td.targetTable, dr.ExtraRowsSource, sourceRow)
			}
			dr.ExtraRowsSource++
			advanceTarget = false
			continue
		case c > 0:
			if dr.ExtraRowsTarget < 10 {
				wr.Logger().Errorf("[table=%v] Extra row %v on target: %v", td.targetTable, dr.ExtraRowsTarget, targetRow)
			}
			dr.ExtraRowsTarget++
			advanceSource = false
			continue
		}

		// c == 0
		// Compare non-pk values.
		c, err = td.compare(sourceRow, targetRow, td.compareCols)
		switch {
		case err != nil:
			return nil, err
		case c != 0:
			if dr.MismatchedRows < 10 {
				wr.Logger().Errorf("[table=%v] Different content %v in same PK: %v != %v", td.targetTable, dr.MismatchedRows, sourceRow, targetRow)
			}
			dr.MismatchedRows++
		default:
			dr.MatchingRows++
		}
	}
}

func (td *tableDiffer) compare(sourceRow, targetRow []sqltypes.Value, cols []int) (int, error) {
	for _, col := range cols {
		if col == -1 {
			continue
		}
		c, err := sqltypes.NullsafeCompare(sourceRow[col], targetRow[col])
		if err != nil {
			return 0, err
		}
		if c != 0 {
			return c, nil
		}
	}
	return 0, nil
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
