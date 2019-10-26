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
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

type resharder struct {
	wr           *Wrangler
	keyspace     string
	workflow     string
	sourceShards []*topo.ShardInfo
	targetShards []*topo.ShardInfo
	vschema      *vschemapb.Keyspace
	refStreams   map[string]*refStream
}

type refStream struct {
	workflow    string
	bls         *binlogdatapb.BinlogSource
	cell        string
	tabletTypes string
}

// Reshard initiates a resharding workflow.
func (wr *Wrangler) Reshard(ctx context.Context, keyspace, workflow string, sources, targets []string) error {
	if err := wr.validateNewWorkflow(ctx, keyspace, workflow); err != nil {
		return err
	}

	rs, err := wr.buildResharder(ctx, keyspace, workflow, sources, targets)
	if err != nil {
		return vterrors.Wrap(err, "buildResharder")
	}
	if err := wr.refreshMasters(ctx, rs.targetShards); err != nil {
		return errors.Wrap(err, "refreshMasters")
	}
	return nil
}

func (wr *Wrangler) buildResharder(ctx context.Context, keyspace, workflow string, sources, targets []string) (*resharder, error) {
	rs := &resharder{
		wr:       wr,
		keyspace: keyspace,
		workflow: workflow,
	}
	for _, shard := range sources {
		si, err := wr.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return nil, vterrors.Wrapf(err, "GetShard(%s) failed", shard)
		}
		rs.sourceShards = append(rs.sourceShards, si)
	}
	for _, shard := range targets {
		si, err := wr.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return nil, vterrors.Wrapf(err, "GetShard(%s) failed", shard)
		}
		rs.targetShards = append(rs.targetShards, si)
	}
	if err := topotools.ValidateForReshard(rs.sourceShards, rs.targetShards); err != nil {
		return nil, vterrors.Wrap(err, "ValidateForReshard")
	}

	vschema, err := wr.ts.GetVSchema(ctx, keyspace)
	if err != nil {
		return nil, vterrors.Wrap(err, "GetVSchema")
	}
	rs.vschema = vschema

	if err := rs.readRefStreams(ctx); err != nil {
		return nil, vterrors.Wrap(err, "readRefStreams")
	}
	return rs, nil
}

func (rs *resharder) readRefStreams(ctx context.Context) error {
	var mu sync.Mutex
	err := rs.forAll(rs.sourceShards, func(source *topo.ShardInfo) error {
		sourceMaster, err := rs.wr.ts.GetTablet(ctx, source.MasterAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v)", source.MasterAlias)
		}

		query := fmt.Sprintf("select workflow, source, cell, tablet_types from _vt.vreplication where db_name=%s", encodeString(sourceMaster.DbName()))
		p3qr, err := rs.wr.tmc.VReplicationExec(ctx, sourceMaster.Tablet, query)
		if err != nil {
			return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", source.MasterAlias, query)
		}
		qr := sqltypes.Proto3ToResult(p3qr)

		mu.Lock()
		defer mu.Unlock()

		mustCreate := false
		var ref map[string]bool
		if rs.refStreams == nil {
			rs.refStreams = make(map[string]*refStream)
			mustCreate = true
		} else {
			// Copy the ref streams for comparison.
			ref = make(map[string]bool, len(rs.refStreams))
			for k := range rs.refStreams {
				ref[k] = true
			}
		}
		for _, row := range qr.Rows {
			workflow := row[0].ToString()
			if workflow == "" {
				return fmt.Errorf("VReplication streams must have named workflows for migration: shard: %s:%s", source.Keyspace(), source.ShardName())
			}
			var bls binlogdatapb.BinlogSource
			if err := proto.UnmarshalText(row[1].ToString(), &bls); err != nil {
				return vterrors.Wrapf(err, "UnmarshalText: %v", row)
			}
			isReference, err := rs.blsIsReference(&bls)
			if err != nil {
				return vterrors.Wrap(err, "blsIsReference")
			}
			if !isReference {
				continue
			}
			key := fmt.Sprintf("%s:%s:%s", workflow, bls.Keyspace, bls.Shard)
			if mustCreate {
				rs.refStreams[key] = &refStream{
					workflow:    workflow,
					bls:         &bls,
					cell:        row[2].ToString(),
					tabletTypes: row[3].ToString(),
				}
			} else {
				if !ref[key] {
					return fmt.Errorf("streams are mismatched across source shards for workflow: %s", workflow)
				}
				delete(ref, key)
			}
		}
		if len(ref) != 0 {
			return fmt.Errorf("streams are mismatched across source shards: %v", ref)
		}
		return nil
	})
	return err
}

// blsIsReference is partially copied from streamMigrater.templatize.
// It reuses the constants from that function also.
func (rs *resharder) blsIsReference(bls *binlogdatapb.BinlogSource) (bool, error) {
	streamType := unknown
	for _, rule := range bls.Filter.Rules {
		typ, err := rs.identifyRuleType(rule)
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

func (rs *resharder) identifyRuleType(rule *binlogdatapb.Rule) (int, error) {
	vtable, ok := rs.vschema.Tables[rule.Match]
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
		return sharded, nil
	case rule.Filter == vreplication.ExcludeStr:
		return unknown, fmt.Errorf("unexpected rule in vreplication: %v", rule)
	default:
		return sharded, nil
	}
}

func (rs *resharder) createStreams(ctx context.Context) error {
	var excludeRules []*binlogdatapb.Rule
	for tableName, table := range rs.vschema.Tables {
		if table.Type == vindexes.TypeReference {
			excludeRules = append(excludeRules, &binlogdatapb.Rule{
				Match:  tableName,
				Filter: "exclude",
			})
		}
	}

	err := rs.forAll(rs.targetShards, func(target *topo.ShardInfo) error {
		master, err := rs.wr.ts.GetTablet(ctx, target.MasterAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", target.MasterAlias)
		}

		buf := &strings.Builder{}
		buf.WriteString("insert into _vt.vreplication(workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name) values ")
		prefix := ""

		addLine := func(workflow string, bls *binlogdatapb.BinlogSource, cell, tabletTypes string) {
			fmt.Fprintf(buf, "%s(%v, %v, '', %v, %v, %v, %v, %v, 0, '%v', %v)",
				prefix,
				encodeString(workflow),
				encodeString(bls.String()),
				throttler.MaxRateModuleDisabled,
				throttler.ReplicationLagModuleDisabled,
				encodeString(cell),
				encodeString(tabletTypes),
				time.Now().Unix(),
				binlogplayer.BlpStopped,
				encodeString(master.DbName()))
			prefix = ", "
		}

		// copy excludeRules to prevent data race.
		copyExcludeRules := append([]*binlogdatapb.Rule(nil), excludeRules...)
		for _, source := range rs.sourceShards {
			if !key.KeyRangesIntersect(target.KeyRange, source.KeyRange) {
				continue
			}
			filter := &binlogdatapb.Filter{
				Rules: append(copyExcludeRules, &binlogdatapb.Rule{
					Match:  "/.*",
					Filter: key.KeyRangeString(target.KeyRange),
				}),
			}
			bls := &binlogdatapb.BinlogSource{
				Keyspace: rs.keyspace,
				Shard:    source.ShardName(),
				Filter:   filter,
			}
			addLine(rs.workflow, bls, "", "")
		}

		for _, rstream := range rs.refStreams {
			addLine(rstream.workflow, rstream.bls, rstream.cell, rstream.tabletTypes)
		}
		query := buf.String()
		if _, err := rs.wr.tmc.VReplicationExec(ctx, master.Tablet, query); err != nil {
			return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", target.MasterAlias, query)
		}
		return nil
	})

	return err
}

func (rs *resharder) startStreaming(ctx context.Context) error {
	workflows := make(map[string]bool)
	workflows[rs.workflow] = true
	for _, rstream := range rs.refStreams {
		workflows[rstream.workflow] = true
	}
	list := make([]string, 0, len(workflows))
	for k := range workflows {
		list = append(list, k)
	}
	sort.Strings(list)
	err := rs.forAll(rs.targetShards, func(target *topo.ShardInfo) error {
		master, err := rs.wr.ts.GetTablet(ctx, target.MasterAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", target.MasterAlias)
		}
		query := fmt.Sprintf("update _vt.vreplication set state='Running' where db_name=%s and workflow in (%s)", encodeString(master.DbName()), stringListify(list))
		if _, err := rs.wr.tmc.VReplicationExec(ctx, master.Tablet, query); err != nil {
			return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", target.MasterAlias, query)
		}
		return nil
	})
	return err
}

func (rs *resharder) forAll(shards []*topo.ShardInfo, f func(*topo.ShardInfo) error) error {
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, shard := range shards {
		wg.Add(1)
		go func(shard *topo.ShardInfo) {
			defer wg.Done()

			if err := f(shard); err != nil {
				allErrors.RecordError(err)
			}
		}(shard)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}
