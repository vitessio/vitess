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
	"sort"
	"strings"
	"sync"
	"text/template"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

type streamMigrater struct {
	streams   map[string][]*vrStream
	workflows []string
	templates []*vrStream
	ts        *trafficSwitcher
}

type vrStream struct {
	id       uint32
	workflow string
	bls      *binlogdatapb.BinlogSource
	pos      mysql.Position
}

func buildStreamMigrater(ctx context.Context, ts *trafficSwitcher, cancelMigrate bool) (*streamMigrater, error) {
	sm := &streamMigrater{ts: ts}
	if sm.ts.migrationType == binlogdatapb.MigrationType_TABLES {
		// Source streams should be stopped only for shard migrations.
		return sm, nil
	}
	streams, err := sm.readSourceStreams(ctx, cancelMigrate)
	if err != nil {
		return nil, err
	}
	sm.streams = streams

	// Loop executes only once.
	for _, tabletStreams := range sm.streams {
		tmpl, err := sm.templatize(ctx, tabletStreams)
		if err != nil {
			return nil, err
		}
		sm.workflows = tabletStreamWorkflows(tmpl)
		return sm, nil
	}
	return sm, nil
}

func (sm *streamMigrater) readSourceStreams(ctx context.Context, cancelMigrate bool) (map[string][]*vrStream, error) {
	streams := make(map[string][]*vrStream)
	var mu sync.Mutex
	err := sm.ts.forAllSources(func(source *tsSource) error {
		if !cancelMigrate {
			// This flow protects us from the following scenario: When we create streams,
			// we always do it in two phases. We start them off as Stopped, and then
			// update them to Running. If such an operation fails, we may be left with
			// lingering Stopped streams. They should actually be cleaned up by the user.
			// In the current workflow, we stop streams and restart them.
			// Once existing streams are stopped, there will be confusion about which of
			// them can be restarted because they will be no different from the lingering streams.
			// To prevent this confusion, we first check if there are any stopped streams.
			// If so, we request the operator to clean them up, or restart them before going ahead.
			// This allows us to assume that all stopped streams can be safely restarted
			// if we cancel the operation.
			stoppedStreams, err := sm.readTabletStreams(ctx, source.master, "state = 'Stopped' and message != 'FROZEN'")
			if err != nil {
				return err
			}
			if len(stoppedStreams) != 0 {
				return fmt.Errorf("cannot migrate until all streams are running: %s: %d", source.si.ShardName(), source.master.Alias.Uid)
			}
		}
		tabletStreams, err := sm.readTabletStreams(ctx, source.master, "")
		if err != nil {
			return err
		}
		if len(tabletStreams) == 0 {
			// No VReplication is running. So, we have no work to do.
			return nil
		}
		p3qr, err := sm.ts.wr.tmc.VReplicationExec(ctx, source.master.Tablet, fmt.Sprintf("select vrepl_id from _vt.copy_state where vrepl_id in %s", tabletStreamValues(tabletStreams)))
		if err != nil {
			return err
		}
		if len(p3qr.Rows) != 0 {
			return fmt.Errorf("cannot migrate while vreplication streams in source shards are still copying: %s", source.si.ShardName())
		}

		mu.Lock()
		defer mu.Unlock()
		streams[source.si.ShardName()] = tabletStreams
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Validate that streams match across source shards.
	streams2 := make(map[string][]*vrStream)
	var reference []*vrStream
	var refshard string
	for k, v := range streams {
		if reference == nil {
			refshard = k
			reference = v
			continue
		}
		streams2[k] = append([]*vrStream(nil), v...)
	}
	for shard, tabletStreams := range streams2 {
	nextStream:
		for _, refStream := range reference {
			for i := 0; i < len(tabletStreams); i++ {
				vrs := tabletStreams[i]
				if refStream.workflow == vrs.workflow &&
					refStream.bls.Keyspace == vrs.bls.Keyspace &&
					refStream.bls.Shard == vrs.bls.Shard {
					// Delete the matched item and scan for the next stream.
					tabletStreams = append(tabletStreams[:i], tabletStreams[i+1:]...)
					continue nextStream
				}
			}
			return nil, fmt.Errorf("streams are mismatched across source shards: %s vs %s", refshard, shard)
		}
		if len(tabletStreams) != 0 {
			return nil, fmt.Errorf("streams are mismatched across source shards: %s vs %s", refshard, shard)
		}
	}
	return streams, nil
}

func (sm *streamMigrater) stopStreams(ctx context.Context) ([]string, error) {
	if sm.streams == nil {
		return nil, nil
	}
	if err := sm.stopSourceStreams(ctx); err != nil {
		return nil, err
	}
	positions, err := sm.syncSourceStreams(ctx)
	if err != nil {
		return nil, err
	}
	return sm.verifyStreamPositions(ctx, positions)
}

// blsIsReference is partially copied from streamMigrater.templatize.
// It reuses the constants from that function also.
func (sm *streamMigrater) blsIsReference(bls *binlogdatapb.BinlogSource) (bool, error) {
	streamType := unknown
	for _, rule := range bls.Filter.Rules {
		typ, err := sm.identifyRuleType(rule)
		if err != nil {
			return false, err
		}
		switch typ {
		case sharded:
			if streamType == reference {
				return false, fmt.Errorf("cannot reshard streams with a mix of reference and sharded tables: %v", bls)
			}
			streamType = sharded
		case reference:
			if streamType == sharded {
				return false, fmt.Errorf("cannot reshard streams with a mix of reference and sharded tables: %v", bls)
			}
			streamType = reference
		}
	}
	return streamType == reference, nil
}

func (sm *streamMigrater) identifyRuleType(rule *binlogdatapb.Rule) (int, error) {
	vtable, ok := sm.ts.sourceKSSchema.Tables[rule.Match]
	if !ok {
		return 0, fmt.Errorf("table %v not found in vschema", rule.Match)
	}
	if vtable.Type == vindexes.TypeReference {
		return reference, nil
	}
	// In this case, 'sharded' means that it's not a reference
	// table. We don't care about any other subtleties.
	return sharded, nil
}

func (sm *streamMigrater) readTabletStreams(ctx context.Context, ti *topo.TabletInfo, constraint string) ([]*vrStream, error) {
	var query string
	if constraint == "" {
		query = fmt.Sprintf("select id, workflow, source, pos from _vt.vreplication where db_name=%s and workflow != %s", encodeString(ti.DbName()), encodeString(sm.ts.reverseWorkflow))
	} else {
		query = fmt.Sprintf("select id, workflow, source, pos from _vt.vreplication where db_name=%s and workflow != %s and %s", encodeString(ti.DbName()), encodeString(sm.ts.reverseWorkflow), constraint)
	}
	p3qr, err := sm.ts.wr.tmc.VReplicationExec(ctx, ti.Tablet, query)
	if err != nil {
		return nil, err
	}
	qr := sqltypes.Proto3ToResult(p3qr)

	tabletStreams := make([]*vrStream, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		id, err := evalengine.ToInt64(row[0])
		if err != nil {
			return nil, err
		}
		workflow := row[1].ToString()
		if workflow == "" {
			return nil, fmt.Errorf("VReplication streams must have named workflows for migration: shard: %s:%s, stream: %d", ti.Keyspace, ti.Shard, id)
		}
		if workflow == sm.ts.workflow {
			return nil, fmt.Errorf("VReplication stream has the same workflow name as the resharding workflow: shard: %s:%s, stream: %d", ti.Keyspace, ti.Shard, id)
		}
		var bls binlogdatapb.BinlogSource
		if err := proto.UnmarshalText(row[2].ToString(), &bls); err != nil {
			return nil, err
		}
		isReference, err := sm.blsIsReference(&bls)
		if err != nil {
			return nil, vterrors.Wrap(err, "blsIsReference")
		}
		if isReference {
			sm.ts.wr.Logger().Infof("readTabletStreams: ignoring reference table %+v", bls)
			continue
		}
		pos, err := mysql.DecodePosition(row[3].ToString())
		if err != nil {
			return nil, err
		}
		tabletStreams = append(tabletStreams, &vrStream{
			id:       uint32(id),
			workflow: workflow,
			bls:      &bls,
			pos:      pos,
		})
	}
	return tabletStreams, nil
}

func (sm *streamMigrater) stopSourceStreams(ctx context.Context) error {
	stoppedStreams := make(map[string][]*vrStream)
	var mu sync.Mutex
	err := sm.ts.forAllSources(func(source *tsSource) error {
		tabletStreams := sm.streams[source.si.ShardName()]
		if len(tabletStreams) == 0 {
			return nil
		}
		query := fmt.Sprintf("update _vt.vreplication set state='Stopped', message='for cutover' where id in %s", tabletStreamValues(tabletStreams))
		_, err := sm.ts.wr.tmc.VReplicationExec(ctx, source.master.Tablet, query)
		if err != nil {
			return err
		}
		tabletStreams, err = sm.readTabletStreams(ctx, source.master, fmt.Sprintf("id in %s", tabletStreamValues(tabletStreams)))
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		stoppedStreams[source.si.ShardName()] = tabletStreams
		return nil
	})
	if err != nil {
		return err
	}
	sm.streams = stoppedStreams
	return nil
}

func (sm *streamMigrater) syncSourceStreams(ctx context.Context) (map[string]mysql.Position, error) {
	stopPositions := make(map[string]mysql.Position)
	for _, tabletStreams := range sm.streams {
		for _, vrs := range tabletStreams {
			key := fmt.Sprintf("%s:%s", vrs.bls.Keyspace, vrs.bls.Shard)
			pos, ok := stopPositions[key]
			if !ok || vrs.pos.AtLeast(pos) {
				sm.ts.wr.Logger().Infof("syncSourceStreams setting stopPositions +%s %+v %d", key, vrs.pos, vrs.id)
				stopPositions[key] = vrs.pos
			}
		}
	}
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for shard, tabletStreams := range sm.streams {
		for _, vrs := range tabletStreams {
			key := fmt.Sprintf("%s:%s", vrs.bls.Keyspace, vrs.bls.Shard)
			pos := stopPositions[key]
			sm.ts.wr.Logger().Infof("syncSourceStreams before go func +%s %+v %d", key, pos, vrs.id)
			if vrs.pos.Equal(pos) {
				continue
			}
			wg.Add(1)
			go func(vrs *vrStream, shard string, pos mysql.Position) {
				defer wg.Done()
				sm.ts.wr.Logger().Infof("syncSourceStreams beginning of go func %s %s %+v %d", shard, vrs.bls.Shard, pos, vrs.id)

				si, err := sm.ts.wr.ts.GetShard(ctx, sm.ts.sourceKeyspace, shard)
				if err != nil {
					allErrors.RecordError(err)
					return
				}
				master, err := sm.ts.wr.ts.GetTablet(ctx, si.MasterAlias)
				if err != nil {
					allErrors.RecordError(err)
					return
				}
				query := fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos='%s', message='synchronizing for cutover' where id=%d", mysql.EncodePosition(pos), vrs.id)
				if _, err := sm.ts.wr.tmc.VReplicationExec(ctx, master.Tablet, query); err != nil {
					allErrors.RecordError(err)
					return
				}
				sm.ts.wr.Logger().Infof("Waiting for keyspace:shard: %v:%v, position %v", sm.ts.sourceKeyspace, shard, pos)
				if err := sm.ts.wr.tmc.VReplicationWaitForPos(ctx, master.Tablet, int(vrs.id), mysql.EncodePosition(pos)); err != nil {
					allErrors.RecordError(err)
					return
				}
				sm.ts.wr.Logger().Infof("Position for keyspace:shard: %v:%v reached", sm.ts.sourceKeyspace, shard)
			}(vrs, shard, pos)
		}
	}
	wg.Wait()
	return stopPositions, allErrors.AggrError(vterrors.Aggregate)
}

func (sm *streamMigrater) verifyStreamPositions(ctx context.Context, stopPositions map[string]mysql.Position) ([]string, error) {
	stoppedStreams := make(map[string][]*vrStream)
	var mu sync.Mutex
	err := sm.ts.forAllSources(func(source *tsSource) error {
		tabletStreams := sm.streams[source.si.ShardName()]
		if len(tabletStreams) == 0 {
			return nil
		}
		tabletStreams, err := sm.readTabletStreams(ctx, source.master, fmt.Sprintf("id in %s", tabletStreamValues(tabletStreams)))
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		stoppedStreams[source.si.ShardName()] = tabletStreams
		return nil
	})
	if err != nil {
		return nil, err
	}

	// This is not really required because it's not used later.
	// But we keep it up-to-date for good measure.
	sm.streams = stoppedStreams

	var oneSet []*vrStream
	allErrors := &concurrency.AllErrorRecorder{}
	for _, tabletStreams := range stoppedStreams {
		if oneSet == nil {
			oneSet = tabletStreams
		}
		for _, vrs := range tabletStreams {
			key := fmt.Sprintf("%s:%s", vrs.bls.Keyspace, vrs.bls.Shard)
			pos := stopPositions[key]
			if !vrs.pos.Equal(pos) {
				allErrors.RecordError(fmt.Errorf("%s: stream %d position: %s does not match %s", key, vrs.id, mysql.EncodePosition(vrs.pos), mysql.EncodePosition(pos)))
			}
		}
	}
	if allErrors.HasErrors() {
		return nil, allErrors.AggrError(vterrors.Aggregate)
	}
	sm.templates, err = sm.templatize(ctx, oneSet)
	if err != nil {
		// Unreachable: we've already templatized this before.
		return nil, err
	}
	return tabletStreamWorkflows(sm.templates), allErrors.AggrError(vterrors.Aggregate)
}

func (sm *streamMigrater) migrateStreams(ctx context.Context) error {
	if sm.streams == nil {
		return nil
	}

	// Delete any previous stray workflows that might have been left-over
	// due to a failed migration.
	if err := sm.deleteTargetStreams(ctx); err != nil {
		return err
	}

	return sm.createTargetStreams(ctx, sm.templates)
}

const (
	unknown = iota
	sharded
	reference
)

// templatizeRule replaces keyrange values with {{.}}.
// This can then be used by go's template package to substitute other keyrange values.
func (sm *streamMigrater) templatize(ctx context.Context, tabletStreams []*vrStream) ([]*vrStream, error) {
	tabletStreams = copyTabletStreams(tabletStreams)
	var shardedStreams []*vrStream
	for _, vrs := range tabletStreams {
		streamType := unknown
		for _, rule := range vrs.bls.Filter.Rules {
			typ, err := sm.templatizeRule(ctx, rule)
			if err != nil {
				return nil, err
			}
			switch typ {
			case sharded:
				if streamType == reference {
					return nil, fmt.Errorf("cannot migrate streams with a mix of reference and sharded tables: %v", vrs.bls)
				}
				streamType = sharded
			case reference:
				if streamType == sharded {
					return nil, fmt.Errorf("cannot migrate streams with a mix of reference and sharded tables: %v", vrs.bls)
				}
				streamType = reference
			}
		}
		if streamType == sharded {
			shardedStreams = append(shardedStreams, vrs)
		}
	}
	return shardedStreams, nil
}

func (sm *streamMigrater) templatizeRule(ctx context.Context, rule *binlogdatapb.Rule) (int, error) {
	vtable, ok := sm.ts.sourceKSSchema.Tables[rule.Match]
	if !ok {
		return 0, fmt.Errorf("table %v not found in vschema", rule.Match)
	}
	if vtable.Type == vindexes.TypeReference {
		return reference, nil
	}
	switch {
	case rule.Filter == "":
		return unknown, fmt.Errorf("rule %v does not have a select expression in vreplication", rule)
	case key.IsKeyRange(rule.Filter):
		rule.Filter = "{{.}}"
		return sharded, nil
	case rule.Filter == vreplication.ExcludeStr:
		return unknown, fmt.Errorf("unexpected rule in vreplication: %v", rule)
	default:
		err := sm.templatizeKeyRange(ctx, rule)
		if err != nil {
			return unknown, err
		}
		return sharded, nil
	}
}

func (sm *streamMigrater) templatizeKeyRange(ctx context.Context, rule *binlogdatapb.Rule) error {
	statement, err := sqlparser.Parse(rule.Filter)
	if err != nil {
		return err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return fmt.Errorf("unexpected query: %v", rule.Filter)
	}
	var expr sqlparser.Expr
	if sel.Where != nil {
		expr = sel.Where.Expr
	}
	exprs := sqlparser.SplitAndExpression(nil, expr)
	for _, subexpr := range exprs {
		funcExpr, ok := subexpr.(*sqlparser.FuncExpr)
		if !ok || !funcExpr.Name.EqualString("in_keyrange") {
			continue
		}
		var krExpr sqlparser.SelectExpr
		switch len(funcExpr.Exprs) {
		case 1:
			krExpr = funcExpr.Exprs[0]
		case 3:
			krExpr = funcExpr.Exprs[2]
		default:
			return fmt.Errorf("unexpected in_keyrange parameters: %v", sqlparser.String(funcExpr))
		}
		aliased, ok := krExpr.(*sqlparser.AliasedExpr)
		if !ok {
			return fmt.Errorf("unexpected in_keyrange parameters: %v", sqlparser.String(funcExpr))
		}
		val, ok := aliased.Expr.(*sqlparser.Literal)
		if !ok {
			return fmt.Errorf("unexpected in_keyrange parameters: %v", sqlparser.String(funcExpr))
		}
		if strings.Contains(rule.Filter, "{{") {
			return fmt.Errorf("cannot migrate queries that contain '{{' in their string: %s", rule.Filter)
		}
		val.Val = "{{.}}"
		rule.Filter = sqlparser.String(statement)
		return nil
	}
	// There was no in_keyrange expression. Create a new one.
	vtable := sm.ts.sourceKSSchema.Tables[rule.Match]
	inkr := &sqlparser.FuncExpr{
		Name: sqlparser.NewColIdent("in_keyrange"),
		Exprs: sqlparser.SelectExprs{
			&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: vtable.ColumnVindexes[0].Columns[0]}},
			&sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral(vtable.ColumnVindexes[0].Type)},
			&sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral("{{.}}")},
		},
	}
	sel.AddWhere(inkr)
	rule.Filter = sqlparser.String(statement)
	return nil
}

func (sm *streamMigrater) createTargetStreams(ctx context.Context, tmpl []*vrStream) error {

	if len(tmpl) == 0 {
		return nil
	}
	return sm.ts.forAllTargets(func(target *tsTarget) error {
		tabletStreams := copyTabletStreams(tmpl)
		for _, vrs := range tabletStreams {
			for _, rule := range vrs.bls.Filter.Rules {
				buf := &strings.Builder{}
				t := template.Must(template.New("").Parse(rule.Filter))
				if err := t.Execute(buf, key.KeyRangeString(target.si.KeyRange)); err != nil {
					return err
				}
				rule.Filter = buf.String()
			}
		}

		ig := vreplication.NewInsertGenerator(binlogplayer.BlpStopped, target.master.DbName())
		for _, vrs := range tabletStreams {
			ig.AddRow(vrs.workflow, vrs.bls, mysql.EncodePosition(vrs.pos), "", "")
		}
		_, err := sm.ts.wr.VReplicationExec(ctx, target.master.Alias, ig.String())
		return err
	})
}

func (sm *streamMigrater) cancelMigration(ctx context.Context) {
	if sm.streams == nil {
		return
	}

	// Ignore error. We still want to restart the source streams if deleteTargetStreams fails.
	_ = sm.deleteTargetStreams(ctx)

	err := sm.ts.forAllSources(func(source *tsSource) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos=null, message='' where db_name=%s and workflow != %s", encodeString(source.master.DbName()), encodeString(sm.ts.reverseWorkflow))
		_, err := sm.ts.wr.VReplicationExec(ctx, source.master.Alias, query)
		return err
	})
	if err != nil {
		sm.ts.wr.Logger().Errorf("Cancel migration failed: could not restart source streams: %v", err)
	}
}

func (sm *streamMigrater) deleteTargetStreams(ctx context.Context) error {
	if len(sm.workflows) == 0 {
		return nil
	}
	workflowList := stringListify(sm.workflows)
	err := sm.ts.forAllTargets(func(target *tsTarget) error {
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow in (%s)", encodeString(target.master.DbName()), workflowList)
		_, err := sm.ts.wr.VReplicationExec(ctx, target.master.Alias, query)
		return err
	})
	if err != nil {
		sm.ts.wr.Logger().Warningf("Could not delete migrated streams: %v", err)
	}
	return err
}

// streamMigraterFinalize finalizes the stream migration.
// It's a standalone function because it does not use the streamMigrater state.
func streamMigraterfinalize(ctx context.Context, ts *trafficSwitcher, workflows []string) error {
	if len(workflows) == 0 {
		return nil
	}
	workflowList := stringListify(workflows)
	err := ts.forAllSources(func(source *tsSource) error {
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow in (%s)", encodeString(source.master.DbName()), workflowList)
		_, err := ts.wr.VReplicationExec(ctx, source.master.Alias, query)
		return err
	})
	if err != nil {
		return err
	}
	err = ts.forAllTargets(func(target *tsTarget) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running' where db_name=%s and workflow in (%s)", encodeString(target.master.DbName()), workflowList)
		_, err := ts.wr.VReplicationExec(ctx, target.master.Alias, query)
		return err
	})
	return err
}

func tabletStreamValues(tabletStreams []*vrStream) string {
	buf := &strings.Builder{}
	prefix := "("
	for _, vrs := range tabletStreams {
		fmt.Fprintf(buf, "%s%d", prefix, vrs.id)
		prefix = ", "
	}
	buf.WriteString(")")
	return buf.String()
}

func tabletStreamWorkflows(tabletStreams []*vrStream) []string {
	workflows := make(map[string]bool)
	for _, vrs := range tabletStreams {
		workflows[vrs.workflow] = true
	}
	list := make([]string, 0, len(workflows))
	for k := range workflows {
		list = append(list, k)
	}
	sort.Strings(list)
	return list
}

func stringListify(in []string) string {
	buf := &strings.Builder{}
	prefix := ""
	for _, str := range in {
		fmt.Fprintf(buf, "%s%s", prefix, encodeString(str))
		prefix = ", "
	}
	return buf.String()
}

func copyTabletStreams(in []*vrStream) []*vrStream {
	out := make([]*vrStream, 0, len(in))
	for _, vrs := range in {
		out = append(out, &vrStream{
			id:       vrs.id,
			workflow: vrs.workflow,
			bls:      proto.Clone(vrs.bls).(*binlogdatapb.BinlogSource),
			pos:      vrs.pos,
		})
	}
	return out
}
