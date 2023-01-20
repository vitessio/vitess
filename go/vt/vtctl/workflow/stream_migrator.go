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

package workflow

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"text/template"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// StreamType is an enum representing the kind of stream.
//
// (TODO:@ajm188) This should be made package-private once the last references
// in package wrangler are removed.
type StreamType int

// StreamType values.
const (
	StreamTypeUnknown = StreamType(iota)
	StreamTypeSharded
	StreamTypeReference
)

// StreamMigrator contains information needed to migrate a stream
type StreamMigrator struct {
	streams   map[string][]*VReplicationStream
	workflows []string
	templates []*VReplicationStream
	ts        ITrafficSwitcher
	logger    logutil.Logger
}

// BuildStreamMigrator creates a new StreamMigrator based on the given
// TrafficSwitcher.
func BuildStreamMigrator(ctx context.Context, ts ITrafficSwitcher, cancelMigrate bool) (*StreamMigrator, error) {
	sm := &StreamMigrator{
		ts:     ts,
		logger: ts.Logger(),
	}

	if sm.ts.MigrationType() == binlogdatapb.MigrationType_TABLES {
		// Source streams should be stopped only for shard migrations.
		return sm, nil
	}

	var err error

	sm.streams, err = sm.readSourceStreams(ctx, cancelMigrate)
	if err != nil {
		return nil, err
	}

	// Loop executes only once.
	for _, tabletStreams := range sm.streams {
		tmpl, err := sm.templatize(ctx, tabletStreams)
		if err != nil {
			return nil, err
		}

		sm.workflows = VReplicationStreams(tmpl).Workflows()
		break
	}

	return sm, nil
}

// StreamMigratorFinalize finalizes the stream migration.
//
// (TODO:@ajm88) in the original implementation, "it's a standalone function
// because it does not use the streamMigrater state". That's still true, but
// moving this to a method on StreamMigrator would provide a cleaner namespacing
// in package workflow. But, we would have to update more callers in order to
// do that (*wrangler.switcher's streamMigrateFinalize would need to change its
// signature to also take a *workflow.StreamMigrator), so we will do that in a
// second PR.
func StreamMigratorFinalize(ctx context.Context, ts ITrafficSwitcher, workflows []string) error {
	if len(workflows) == 0 {
		return nil
	}

	workflowList := stringListify(workflows)
	err := ts.ForAllSources(func(source *MigrationSource) error {
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow in (%s)", encodeString(source.GetPrimary().DbName()), workflowList)
		_, err := ts.VReplicationExec(ctx, source.GetPrimary().Alias, query)
		return err
	})

	if err != nil {
		return err
	}

	err = ts.ForAllTargets(func(target *MigrationTarget) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running' where db_name=%s and workflow in (%s)", encodeString(target.GetPrimary().DbName()), workflowList)
		_, err := ts.VReplicationExec(ctx, target.GetPrimary().Alias, query)
		return err
	})

	return err
}

// Streams returns a deep-copy of the StreamMigrator's streams map.
func (sm *StreamMigrator) Streams() map[string][]*VReplicationStream {
	streams := make(map[string][]*VReplicationStream, len(sm.streams))

	for k, v := range sm.streams {
		streams[k] = VReplicationStreams(v).Copy().ToSlice()
	}

	return streams
}

// Templates returns a copy of the StreamMigrator's template streams.
func (sm *StreamMigrator) Templates() []*VReplicationStream {
	return VReplicationStreams(sm.templates).Copy().ToSlice()
}

// CancelMigration cancels a migration
func (sm *StreamMigrator) CancelMigration(ctx context.Context) {
	if sm.streams == nil {
		return
	}

	_ = sm.deleteTargetStreams(ctx)

	err := sm.ts.ForAllSources(func(source *MigrationSource) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos=null, message='' where db_name=%s and workflow != %s", encodeString(source.GetPrimary().DbName()), encodeString(sm.ts.ReverseWorkflowName()))
		_, err := sm.ts.VReplicationExec(ctx, source.GetPrimary().Alias, query)
		return err
	})
	if err != nil {
		sm.logger.Errorf("Cancel migration failed: could not restart source streams: %v", err)
	}
}

// MigrateStreams migrates N streams
func (sm *StreamMigrator) MigrateStreams(ctx context.Context) error {
	if sm.streams == nil {
		return nil
	}

	if err := sm.deleteTargetStreams(ctx); err != nil {
		return err
	}

	return sm.createTargetStreams(ctx, sm.templates)
}

// StopStreams stops streams
func (sm *StreamMigrator) StopStreams(ctx context.Context) ([]string, error) {
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

/* tablet streams */

func (sm *StreamMigrator) readTabletStreams(ctx context.Context, ti *topo.TabletInfo, constraint string) ([]*VReplicationStream, error) {
	query := fmt.Sprintf("select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name=%s and workflow != %s",
		encodeString(ti.DbName()), encodeString(sm.ts.ReverseWorkflowName()))
	if constraint != "" {
		query += fmt.Sprintf(" and %s", constraint)
	}

	p3qr, err := sm.ts.TabletManagerClient().VReplicationExec(ctx, ti.Tablet, query)
	if err != nil {
		return nil, err
	}

	qr := sqltypes.Proto3ToResult(p3qr)
	tabletStreams := make([]*VReplicationStream, 0, len(qr.Rows))

	for _, row := range qr.Named().Rows {
		id, err := row["id"].ToInt64()
		if err != nil {
			return nil, err
		}

		workflowName := row["workflow"].ToString()
		switch workflowName {
		case "":
			return nil, fmt.Errorf("VReplication streams must have named workflows for migration: shard: %s:%s, stream: %d",
				ti.Keyspace, ti.Shard, id)
		case sm.ts.WorkflowName():
			return nil, fmt.Errorf("VReplication stream has the same workflow name as the resharding workflow: shard: %s:%s, stream: %d",
				ti.Keyspace, ti.Shard, id)
		}

		workflowType, err := row["workflow_type"].ToInt64()
		if err != nil {
			return nil, err
		}
		workflowSubType, err := row["workflow_sub_type"].ToInt64()
		if err != nil {
			return nil, err
		}

		deferSecondaryKeys, err := row["defer_secondary_keys"].ToBool()
		if err != nil {
			return nil, err
		}

		var bls binlogdatapb.BinlogSource
		rowBytes, err := row["source"].ToBytes()
		if err != nil {
			return nil, err
		}
		if err := prototext.Unmarshal(rowBytes, &bls); err != nil {
			return nil, err
		}

		isReference, err := sm.blsIsReference(&bls)
		if err != nil {
			return nil, vterrors.Wrap(err, "blsIsReference")
		}

		if isReference {
			sm.ts.Logger().Infof("readTabletStreams: ignoring reference table %+v", &bls)
			continue
		}

		pos, err := mysql.DecodePosition(row["pos"].ToString())
		if err != nil {
			return nil, err
		}

		tabletStreams = append(tabletStreams, &VReplicationStream{
			ID:                 uint32(id),
			Workflow:           workflowName,
			BinlogSource:       &bls,
			Position:           pos,
			WorkflowType:       binlogdatapb.VReplicationWorkflowType(workflowType),
			WorkflowSubType:    binlogdatapb.VReplicationWorkflowSubType(workflowSubType),
			DeferSecondaryKeys: deferSecondaryKeys,
		})
	}
	return tabletStreams, nil
}

/* source streams */

func (sm *StreamMigrator) readSourceStreams(ctx context.Context, cancelMigrate bool) (map[string][]*VReplicationStream, error) {
	var (
		mu      sync.Mutex
		streams = make(map[string][]*VReplicationStream)
	)

	err := sm.ts.ForAllSources(func(source *MigrationSource) error {
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
			stoppedStreams, err := sm.readTabletStreams(ctx, source.GetPrimary(), "state = 'Stopped' and message != 'FROZEN'")
			if err != nil {
				return err
			}

			if len(stoppedStreams) != 0 {
				return fmt.Errorf("cannot migrate until all streams are running: %s: %d", source.GetShard().ShardName(), source.GetPrimary().Alias.Uid)
			}
		}

		tabletStreams, err := sm.readTabletStreams(ctx, source.GetPrimary(), "")
		if err != nil {
			return err
		}

		if len(tabletStreams) == 0 {
			// No VReplication is running. So, we have no work to do.
			return nil
		}

		query := fmt.Sprintf("select distinct vrepl_id from _vt.copy_state where vrepl_id in %s", VReplicationStreams(tabletStreams).Values())
		p3qr, err := sm.ts.TabletManagerClient().VReplicationExec(ctx, source.GetPrimary().Tablet, query)
		switch {
		case err != nil:
			return err
		case len(p3qr.Rows) != 0:
			return fmt.Errorf("cannot migrate while vreplication streams in source shards are still copying: %s", source.GetShard().ShardName())
		}

		mu.Lock()
		defer mu.Unlock()
		streams[source.GetShard().ShardName()] = tabletStreams
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Validate that streams match across source shards.
	var (
		reference []*VReplicationStream
		refshard  string
		streams2  = make(map[string][]*VReplicationStream)
	)

	for k, v := range streams {
		if reference == nil {
			refshard = k
			reference = v
			continue
		}

		streams2[k] = append([]*VReplicationStream(nil), v...)
	}

	for shard, tabletStreams := range streams2 {
		for _, refStream := range reference {
			err := func() error {
				for i := 0; i < len(tabletStreams); i++ {
					vrs := tabletStreams[i]

					if refStream.Workflow == vrs.Workflow &&
						refStream.BinlogSource.Keyspace == vrs.BinlogSource.Keyspace &&
						refStream.BinlogSource.Shard == vrs.BinlogSource.Shard {
						// Delete the matched item and scan for the next stream.
						tabletStreams = append(tabletStreams[:i], tabletStreams[i+1:]...)
						return nil
					}
				}

				return fmt.Errorf("streams are mismatched across source shards: %s vs %s", refshard, shard)
			}()

			if err != nil {
				return nil, err
			}
		}

		if len(tabletStreams) != 0 {
			return nil, fmt.Errorf("streams are mismatched across source shards: %s vs %s", refshard, shard)
		}
	}

	return streams, nil
}

func (sm *StreamMigrator) stopSourceStreams(ctx context.Context) error {
	var (
		mu             sync.Mutex
		stoppedStreams = make(map[string][]*VReplicationStream)
	)

	err := sm.ts.ForAllSources(func(source *MigrationSource) error {
		tabletStreams := sm.streams[source.GetShard().ShardName()]
		if len(tabletStreams) == 0 {
			return nil
		}

		query := fmt.Sprintf("update _vt.vreplication set state='Stopped', message='for cutover' where id in %s", VReplicationStreams(tabletStreams).Values())
		_, err := sm.ts.TabletManagerClient().VReplicationExec(ctx, source.GetPrimary().Tablet, query)
		if err != nil {
			return err
		}

		tabletStreams, err = sm.readTabletStreams(ctx, source.GetPrimary(), fmt.Sprintf("id in %s", VReplicationStreams(tabletStreams).Values()))
		if err != nil {
			return err
		}

		mu.Lock()
		defer mu.Unlock()
		stoppedStreams[source.GetShard().ShardName()] = tabletStreams

		return nil
	})

	if err != nil {
		return err
	}

	sm.streams = stoppedStreams
	return nil
}

func (sm *StreamMigrator) syncSourceStreams(ctx context.Context) (map[string]mysql.Position, error) {
	stopPositions := make(map[string]mysql.Position)

	for _, tabletStreams := range sm.streams {
		for _, vrs := range tabletStreams {
			key := fmt.Sprintf("%s:%s", vrs.BinlogSource.Keyspace, vrs.BinlogSource.Shard)
			if pos, ok := stopPositions[key]; !ok || vrs.Position.AtLeast(pos) {
				sm.ts.Logger().Infof("syncSourceStreams setting stopPositions +%s %+v %d", key, vrs.Position, vrs.ID)
				stopPositions[key] = vrs.Position
			}
		}
	}

	var (
		wg        sync.WaitGroup
		allErrors concurrency.AllErrorRecorder
	)

	for shard, tabletStreams := range sm.streams {
		for _, vrs := range tabletStreams {
			key := fmt.Sprintf("%s:%s", vrs.BinlogSource.Keyspace, vrs.BinlogSource.Shard)
			pos := stopPositions[key]
			sm.ts.Logger().Infof("syncSourceStreams before go func +%s %+v %d", key, pos, vrs.ID)

			if vrs.Position.Equal(pos) {
				continue
			}

			wg.Add(1)
			go func(vrs *VReplicationStream, shard string, pos mysql.Position) {
				defer wg.Done()
				sm.ts.Logger().Infof("syncSourceStreams beginning of go func %s %s %+v %d", shard, vrs.BinlogSource.Shard, pos, vrs.ID)

				si, err := sm.ts.TopoServer().GetShard(ctx, sm.ts.SourceKeyspaceName(), shard)
				if err != nil {
					allErrors.RecordError(err)
					return
				}

				primary, err := sm.ts.TopoServer().GetTablet(ctx, si.PrimaryAlias)
				if err != nil {
					allErrors.RecordError(err)
					return
				}

				query := fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos='%s', message='synchronizing for cutover' where id=%d", mysql.EncodePosition(pos), vrs.ID)
				if _, err := sm.ts.TabletManagerClient().VReplicationExec(ctx, primary.Tablet, query); err != nil {
					allErrors.RecordError(err)
					return
				}

				sm.ts.Logger().Infof("Waiting for keyspace:shard: %v:%v, position %v", sm.ts.SourceKeyspaceName(), shard, pos)
				if err := sm.ts.TabletManagerClient().VReplicationWaitForPos(ctx, primary.Tablet, int(vrs.ID), mysql.EncodePosition(pos)); err != nil {
					allErrors.RecordError(err)
					return
				}

				sm.ts.Logger().Infof("Position for keyspace:shard: %v:%v reached", sm.ts.SourceKeyspaceName(), shard)
			}(vrs, shard, pos)
		}
	}

	wg.Wait()

	return stopPositions, allErrors.AggrError(vterrors.Aggregate)
}

func (sm *StreamMigrator) verifyStreamPositions(ctx context.Context, stopPositions map[string]mysql.Position) ([]string, error) {
	var (
		mu             sync.Mutex
		stoppedStreams = make(map[string][]*VReplicationStream)
	)

	err := sm.ts.ForAllSources(func(source *MigrationSource) error {
		tabletStreams := sm.streams[source.GetShard().ShardName()]
		if len(tabletStreams) == 0 {
			return nil
		}

		tabletStreams, err := sm.readTabletStreams(ctx, source.GetPrimary(), fmt.Sprintf("id in %s", VReplicationStreams(tabletStreams).Values()))
		if err != nil {
			return err
		}

		mu.Lock()
		defer mu.Unlock()
		stoppedStreams[source.GetShard().ShardName()] = tabletStreams

		return nil
	})

	if err != nil {
		return nil, err
	}

	// This is not really required because it's not used later.
	// But we keep it up-to-date for good measure.
	sm.streams = stoppedStreams

	var (
		oneSet    []*VReplicationStream
		allErrors concurrency.AllErrorRecorder
	)

	for _, tabletStreams := range stoppedStreams {
		if oneSet == nil {
			oneSet = tabletStreams
		}

		for _, vrs := range tabletStreams {
			key := fmt.Sprintf("%s:%s", vrs.BinlogSource.Keyspace, vrs.BinlogSource.Shard)
			if pos := stopPositions[key]; !vrs.Position.Equal(pos) {
				allErrors.RecordError(fmt.Errorf("%s: stream %d position: %s does not match %s", key, vrs.ID, mysql.EncodePosition(vrs.Position), mysql.EncodePosition(pos)))
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

	return VReplicationStreams(sm.templates).Workflows(), allErrors.AggrError(vterrors.Aggregate)
}

/* target streams */

func (sm *StreamMigrator) createTargetStreams(ctx context.Context, tmpl []*VReplicationStream) error {
	if len(tmpl) == 0 {
		return nil
	}

	return sm.ts.ForAllTargets(func(target *MigrationTarget) error {
		ig := vreplication.NewInsertGenerator(binlogplayer.BlpStopped, target.GetPrimary().DbName())
		tabletStreams := VReplicationStreams(tmpl).Copy().ToSlice()

		for _, vrs := range tabletStreams {
			for _, rule := range vrs.BinlogSource.Filter.Rules {
				buf := &strings.Builder{}

				t := template.Must(template.New("").Parse(rule.Filter))
				if err := t.Execute(buf, key.KeyRangeString(target.GetShard().KeyRange)); err != nil {
					return err
				}

				rule.Filter = buf.String()
			}

			ig.AddRow(vrs.Workflow, vrs.BinlogSource, mysql.EncodePosition(vrs.Position), "", "",
				int64(vrs.WorkflowType), int64(vrs.WorkflowSubType), vrs.DeferSecondaryKeys)
		}

		_, err := sm.ts.VReplicationExec(ctx, target.GetPrimary().GetAlias(), ig.String())
		return err
	})
}

func (sm *StreamMigrator) deleteTargetStreams(ctx context.Context) error {
	if len(sm.workflows) == 0 {
		return nil
	}

	workflows := stringListify(sm.workflows)
	err := sm.ts.ForAllTargets(func(target *MigrationTarget) error {
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow in (%s)", encodeString(target.GetPrimary().DbName()), workflows)
		_, err := sm.ts.VReplicationExec(ctx, target.GetPrimary().Alias, query)
		return err
	})

	if err != nil {
		sm.logger.Warningf("Could not delete migrated streams: %v", err)
	}

	return err
}

/* templatizing */

func (sm *StreamMigrator) templatize(ctx context.Context, tabletStreams []*VReplicationStream) ([]*VReplicationStream, error) {
	var shardedStreams []*VReplicationStream

	tabletStreams = VReplicationStreams(tabletStreams).Copy().ToSlice()
	for _, vrs := range tabletStreams {
		streamType := StreamTypeUnknown

		for _, rule := range vrs.BinlogSource.Filter.Rules {
			typ, err := sm.templatizeRule(ctx, rule)
			if err != nil {
				return nil, err
			}

			switch typ {
			case StreamTypeSharded:
				if streamType == StreamTypeReference {
					return nil, fmt.Errorf("cannot migrate streams with a mix of reference and sharded tables: %v", vrs.BinlogSource)
				}
				streamType = StreamTypeSharded
			case StreamTypeReference:
				if streamType == StreamTypeSharded {
					return nil, fmt.Errorf("cannot migrate streams with a mix of reference and sharded tables: %v", vrs.BinlogSource)
				}
				streamType = StreamTypeReference
			}
		}

		if streamType == StreamTypeSharded {
			shardedStreams = append(shardedStreams, vrs)
		}
	}

	return shardedStreams, nil
}

// templatizeRule replaces keyrange values with {{.}}.
// This can then be used by go's template package to substitute other keyrange values.
func (sm *StreamMigrator) templatizeRule(ctx context.Context, rule *binlogdatapb.Rule) (StreamType, error) {
	vtable, ok := sm.ts.SourceKeyspaceSchema().Tables[rule.Match]
	if !ok && !schema.IsInternalOperationTableName(rule.Match) {
		return StreamTypeUnknown, fmt.Errorf("table %v not found in vschema", rule.Match)
	}

	if vtable != nil && vtable.Type == vindexes.TypeReference {
		return StreamTypeReference, nil
	}

	switch {
	case rule.Filter == "":
		return StreamTypeUnknown, fmt.Errorf("rule %v does not have a select expression in vreplication", rule)
	case key.IsKeyRange(rule.Filter):
		rule.Filter = "{{.}}"
		return StreamTypeSharded, nil
	case rule.Filter == vreplication.ExcludeStr:
		return StreamTypeUnknown, fmt.Errorf("unexpected rule in vreplication: %v", rule)
	default:
		if err := sm.templatizeKeyRange(ctx, rule); err != nil {
			return StreamTypeUnknown, err
		}

		return StreamTypeSharded, nil
	}
}

func (sm *StreamMigrator) templatizeKeyRange(ctx context.Context, rule *binlogdatapb.Rule) error {
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
	vtable := sm.ts.SourceKeyspaceSchema().Tables[rule.Match]
	inkr := &sqlparser.FuncExpr{
		Name: sqlparser.NewIdentifierCI("in_keyrange"),
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

/* misc */

func (sm *StreamMigrator) blsIsReference(bls *binlogdatapb.BinlogSource) (bool, error) {
	streamType := StreamTypeUnknown
	for _, rule := range bls.Filter.Rules {
		typ, err := sm.identifyRuleType(rule)
		if err != nil {
			return false, err
		}

		switch typ {
		case StreamTypeSharded:
			if streamType == StreamTypeReference {
				return false, fmt.Errorf("cannot reshard streams with a mix of reference and sharded tables: %v", bls)
			}

			streamType = StreamTypeSharded
		case StreamTypeReference:
			if streamType == StreamTypeSharded {
				return false, fmt.Errorf("cannot reshard streams with a mix of reference and sharded tables: %v", bls)
			}

			streamType = StreamTypeReference
		}
	}

	return streamType == StreamTypeReference, nil
}

func (sm *StreamMigrator) identifyRuleType(rule *binlogdatapb.Rule) (StreamType, error) {
	vtable, ok := sm.ts.SourceKeyspaceSchema().Tables[rule.Match]
	if !ok && !schema.IsInternalOperationTableName(rule.Match) {
		return 0, fmt.Errorf("table %v not found in vschema", rule.Match)
	}

	if vtable != nil && vtable.Type == vindexes.TypeReference {
		return StreamTypeReference, nil
	}

	// In this case, 'sharded' means that it's not a reference
	// table. We don't care about any other subtleties.
	return StreamTypeSharded, nil
}

func stringListify(ss []string) string {
	var buf strings.Builder

	prefix := ""
	for _, s := range ss {
		fmt.Fprintf(&buf, "%s%s", prefix, encodeString(s))
		prefix = ", "
	}

	return buf.String()
}
