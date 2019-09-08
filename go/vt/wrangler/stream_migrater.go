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
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

type streamMigrater struct {
	mi *migrater
}

type vrStream struct {
	id       uint32
	workflow string
	bls      *binlogdatapb.BinlogSource
	pos      mysql.Position
}

func (sm *streamMigrater) stopStreams(ctx context.Context) ([]*vrStream, error) {
	if sm.mi.migrationType == binlogdatapb.MigrationType_TABLES {
		// Source streams should be stopped only for shard migrations.
		return nil, nil
	}
	streams, err := sm.readSourceStreams(ctx)
	if err != nil {
		return nil, err
	}
	streams, err = sm.stopSourceStreams(ctx, streams)
	if err != nil {
		return nil, err
	}
	positions, err := sm.syncSourceStreams(ctx, streams)
	if err != nil {
		return nil, err
	}
	return sm.verifyStreamPositions(ctx, streams, positions)
}

func (sm *streamMigrater) readSourceStreams(ctx context.Context) (map[string][]*vrStream, error) {
	streams := make(map[string][]*vrStream)
	var mu sync.Mutex
	err := sm.mi.forAllSources(func(source *miSource) error {
		stoppedStreams, err := sm.readTabletStreams(ctx, source.master, "state = 'Stopped'")
		if err != nil {
			return err
		}
		if len(stoppedStreams) != 0 {
			return fmt.Errorf("cannot migrate until all strems are running: %s", source.si.ShardName())
		}
		tabletStreams, err := sm.readTabletStreams(ctx, source.master, "")
		if err != nil {
			return err
		}
		if len(tabletStreams) == 0 {
			return nil
		}
		p3qr, err := sm.mi.wr.tmc.VReplicationExec(ctx, source.master.Tablet, fmt.Sprintf("select vrepl_id from _vt.copy_state where vrepl_id in %s", tabletStreamValues(tabletStreams)))
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

func (sm *streamMigrater) readTabletStreams(ctx context.Context, ti *topo.TabletInfo, constraint string) ([]*vrStream, error) {
	var query string
	if constraint == "" {
		query = fmt.Sprintf("select id, workflow, source, pos from _vt.vreplication where db_name=%s", encodeString(ti.DbName()))
	} else {
		query = fmt.Sprintf("select id, workflow, source, pos from _vt.vreplication where db_name=%s and %s", encodeString(ti.DbName()), constraint)
	}
	p3qr, err := sm.mi.wr.tmc.VReplicationExec(ctx, ti.Tablet, query)
	if err != nil {
		return nil, err
	}
	qr := sqltypes.Proto3ToResult(p3qr)

	tabletStreams := make([]*vrStream, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		id, err := sqltypes.ToInt64(row[0])
		if err != nil {
			return nil, err
		}
		workflow := row[1].ToString()
		if workflow == "" {
			return nil, fmt.Errorf("VReplication streams must have named workflows for migration: shard: %s:%s, stream: %d", ti.Keyspace, ti.Shard, id)
		}
		if workflow == sm.mi.workflow {
			return nil, fmt.Errorf("VReplication stream has the same workflow name as the resharding workflow: shard: %s:%s, stream: %d", ti.Keyspace, ti.Shard, id)
		}
		var bls binlogdatapb.BinlogSource
		if err := proto.UnmarshalText(row[2].ToString(), &bls); err != nil {
			return nil, err
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

func (sm *streamMigrater) stopSourceStreams(ctx context.Context, streams map[string][]*vrStream) (map[string][]*vrStream, error) {
	stoppedStreams := make(map[string][]*vrStream)
	var mu sync.Mutex
	err := sm.mi.forAllSources(func(source *miSource) error {
		tabletStreams := streams[source.si.ShardName()]
		if len(tabletStreams) == 0 {
			return nil
		}
		query := fmt.Sprintf("update _vt.vreplication set state='Stopped', message='for cutover' where id in %s", tabletStreamValues(tabletStreams))
		_, err := sm.mi.wr.tmc.VReplicationExec(ctx, source.master.Tablet, query)
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
		return nil, err
	}
	return stoppedStreams, nil
}

func (sm *streamMigrater) syncSourceStreams(ctx context.Context, streams map[string][]*vrStream) (map[string]mysql.Position, error) {
	stopPositions := make(map[string]mysql.Position)
	for _, tabletStreams := range streams {
		for _, vrs := range tabletStreams {
			key := fmt.Sprintf("%s:%s", vrs.bls.Keyspace, vrs.bls.Shard)
			pos, ok := stopPositions[key]
			if !ok || vrs.pos.AtLeast(pos) {
				stopPositions[key] = vrs.pos
			}
		}
	}
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, tabletStreams := range streams {
		for _, vrs := range tabletStreams {
			key := fmt.Sprintf("%s:%s", vrs.bls.Keyspace, vrs.bls.Shard)
			pos := stopPositions[key]
			if vrs.pos.Equal(pos) {
				continue
			}
			wg.Add(1)
			go func(vrs *vrStream) {
				defer wg.Done()
				si, err := sm.mi.wr.ts.GetShard(ctx, vrs.bls.Keyspace, vrs.bls.Shard)
				if err != nil {
					allErrors.RecordError(err)
					return
				}
				master, err := sm.mi.wr.ts.GetTablet(ctx, si.MasterAlias)
				if err != nil {
					allErrors.RecordError(err)
					return
				}
				query := fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos='%s', message='synchronizing for cutover' where id=%d", mysql.EncodePosition(pos), vrs.id)
				if _, err := sm.mi.wr.tmc.VReplicationExec(ctx, master.Tablet, query); err != nil {
					allErrors.RecordError(err)
					return
				}
				if err := sm.mi.wr.tmc.VReplicationWaitForPos(ctx, master.Tablet, int(vrs.id), mysql.EncodePosition(pos)); err != nil {
					allErrors.RecordError(err)
					return
				}
			}(vrs)
		}
	}
	wg.Wait()
	return stopPositions, allErrors.AggrError(vterrors.Aggregate)
}

func (sm *streamMigrater) verifyStreamPositions(ctx context.Context, streams map[string][]*vrStream, stopPositions map[string]mysql.Position) ([]*vrStream, error) {
	stoppedStreams := make(map[string][]*vrStream)
	var mu sync.Mutex
	err := sm.mi.forAllSources(func(source *miSource) error {
		tabletStreams := streams[source.si.ShardName()]
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
	return oneSet, allErrors.AggrError(vterrors.Aggregate)
}

func (sm *streamMigrater) migrateStreams(ctx context.Context, tabletStreams []*vrStream) ([]string, error) {
	if sm.mi.migrationType == binlogdatapb.MigrationType_TABLES {
		return nil, nil
	}

	// Delete any previous stray workflows that might have been left-over
	// due to a failed migration.
	if err := sm.deleteTargetStreams(ctx, tabletStreams); err != nil {
		return nil, err
	}

	tmpl, err := sm.templatize(ctx, tabletStreams)
	if err != nil {
		return nil, err
	}
	workflows := tabletStreamWorkflows(tmpl)
	if err := sm.createTargetStreams(ctx, tmpl); err != nil {
		return nil, err
	}
	return workflows, nil
}

const (
	unknown = iota
	sharded
	reference
)

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
	switch {
	case rule.Filter == "":
		return reference, nil
	case key.IsKeyRange(rule.Filter):
		rule.Filter = "{{.}}"
		return sharded, nil
	case rule.Filter == vreplication.ExcludeStr:
		return unknown, nil
	default:
		templatized, err := sm.checkShardedQuery(ctx, rule.Filter)
		if err != nil {
			return unknown, err
		}
		if templatized != "" {
			rule.Filter = templatized
			return sharded, nil
		}
		return reference, nil
	}
}

func (sm *streamMigrater) checkShardedQuery(ctx context.Context, query string) (string, error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return "", err
	}
	sel, ok := statement.(*sqlparser.Select)
	if !ok {
		return "", fmt.Errorf("unexpected query: %v", query)
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
			return "", fmt.Errorf("unexpected in_keyrange parameters: %v", sqlparser.String(funcExpr))
		}
		aliased, ok := krExpr.(*sqlparser.AliasedExpr)
		if !ok {
			return "", fmt.Errorf("unexpected in_keyrange parameters: %v", sqlparser.String(funcExpr))
		}
		val, ok := aliased.Expr.(*sqlparser.SQLVal)
		if !ok {
			return "", fmt.Errorf("unexpected in_keyrange parameters: %v", sqlparser.String(funcExpr))
		}
		if strings.Contains(query, "{{") {
			return "", fmt.Errorf("cannot migrate queries that contain '{{' in their string: %s", query)
		}
		val.Val = []byte("{{.}}")
		return sqlparser.String(statement), nil
	}
	return "", nil
}

func (sm *streamMigrater) createTargetStreams(ctx context.Context, tmpl []*vrStream) error {
	if len(tmpl) == 0 {
		return nil
	}
	return sm.mi.forAllTargets(func(target *miTarget) error {
		tabletStreams := copyTabletStreams(tmpl)
		for _, vrs := range tabletStreams {
			for _, rule := range vrs.bls.Filter.Rules {
				buf := &strings.Builder{}
				t := template.Must(template.New("").Parse(rule.Filter))
				err := t.Execute(buf, target.si.ShardName())
				if err != nil {
					return err
				}
				rule.Filter = buf.String()
			}
		}

		buf := &strings.Builder{}
		buf.WriteString("insert into _vt.vreplication(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name) values ")
		prefix := ""
		for _, vrs := range tabletStreams {
			fmt.Fprintf(buf, "%s(%v, %v, %v, %v, %v, %v, 0, '%v', %v)",
				prefix,
				encodeString(vrs.workflow),
				encodeString(vrs.bls.String()),
				encodeString(mysql.EncodePosition(vrs.pos)),
				throttler.MaxRateModuleDisabled,
				throttler.ReplicationLagModuleDisabled,
				time.Now().Unix(),
				binlogplayer.BlpStopped,
				encodeString(target.master.DbName()))
			prefix = ", "
		}
		_, err := sm.mi.wr.VReplicationExec(ctx, target.master.Alias, buf.String())
		return err
	})
}

func (sm *streamMigrater) cancelMigration(ctx context.Context) {
	if sm.mi.migrationType == binlogdatapb.MigrationType_TABLES {
		return
	}
	tabletStreams, err := sm.readSourceStreamsForCancel(ctx)
	if err != nil {
		sm.mi.wr.Logger().Errorf("Cancel migration failed: could not read streams metadata: %v", err)
		return
	}

	// Ignore error. We still want to restart the source streams if deleteTargetStreans fails.
	_ = sm.deleteTargetStreams(ctx, tabletStreams)

	workflows := tabletStreamWorkflows(tabletStreams)
	if len(workflows) == 0 {
		return
	}
	workflowList := stringListify(workflows)
	err = sm.mi.forAllSources(func(source *miSource) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos=null, message='' where db_name=%s and workflow in (%s)", encodeString(source.master.DbName()), workflowList)
		_, err := sm.mi.wr.VReplicationExec(ctx, source.master.Alias, query)
		return err
	})
	if err != nil {
		sm.mi.wr.Logger().Errorf("Cancel migration failed: could not restart source streams: %v", err)
	}
}

func (sm *streamMigrater) deleteTargetStreams(ctx context.Context, tabletStreams []*vrStream) error {
	workflows := tabletStreamWorkflows(tabletStreams)
	if len(workflows) == 0 {
		return nil
	}
	workflowList := stringListify(workflows)
	err := sm.mi.forAllTargets(func(target *miTarget) error {
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow in (%s)", encodeString(target.master.DbName()), workflowList)
		_, err := sm.mi.wr.VReplicationExec(ctx, target.master.Alias, query)
		return err
	})
	if err != nil {
		sm.mi.wr.Logger().Warningf("Could not delete migrated streams: %v", err)
	}
	return err
}

func (sm *streamMigrater) readSourceStreamsForCancel(ctx context.Context) ([]*vrStream, error) {
	streams := make(map[string][]*vrStream)
	var mu sync.Mutex
	err := sm.mi.forAllSources(func(source *miSource) error {
		tabletStreams, err := sm.readTabletStreams(ctx, source.master, "")
		if err != nil {
			return err
		}
		if len(tabletStreams) == 0 {
			return nil
		}

		mu.Lock()
		defer mu.Unlock()
		streams[source.si.ShardName()] = tabletStreams
		return nil
	})
	if err != nil {
		return nil, err
	}
	var oneSet []*vrStream
	for _, tabletStream := range streams {
		oneSet = tabletStream
		break
	}
	return oneSet, nil
}

func (sm *streamMigrater) finalize(ctx context.Context, workflows []string) error {
	if sm.mi.migrationType == binlogdatapb.MigrationType_TABLES {
		return nil
	}
	if len(workflows) == 0 {
		return nil
	}
	workflowList := stringListify(workflows)
	err := sm.mi.forAllTargets(func(target *miTarget) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running' where db_name=%s and workflow in (%s)", encodeString(target.master.DbName()), workflowList)
		_, err := sm.mi.wr.VReplicationExec(ctx, target.master.Alias, query)
		return err
	})
	if err != nil {
		return err
	}
	return sm.mi.forAllSources(func(source *miSource) error {
		query := fmt.Sprintf("delete from _vt.vreplication where db_name=%s and workflow in (%s)", encodeString(source.master.DbName()), workflowList)
		_, err := sm.mi.wr.VReplicationExec(ctx, source.master.Alias, query)
		return err
	})
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
