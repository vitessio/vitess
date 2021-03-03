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
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

type resharder struct {
	wr            *Wrangler
	keyspace      string
	workflow      string
	sourceShards  []*topo.ShardInfo
	sourceMasters map[string]*topo.TabletInfo
	targetShards  []*topo.ShardInfo
	targetMasters map[string]*topo.TabletInfo
	vschema       *vschemapb.Keyspace
	refStreams    map[string]*refStream
	cell          string //single cell or cellsAlias or comma-separated list of cells/cellsAliases
	tabletTypes   string
	stopAfterCopy bool
}

type refStream struct {
	workflow    string
	bls         *binlogdatapb.BinlogSource
	cell        string
	tabletTypes string
}

// Reshard initiates a resharding workflow.
func (wr *Wrangler) Reshard(ctx context.Context, keyspace, workflow string, sources, targets []string,
	skipSchemaCopy bool, cell, tabletTypes string, autoStart, stopAfterCopy bool) error {
	if err := wr.validateNewWorkflow(ctx, keyspace, workflow); err != nil {
		return err
	}
	if err := wr.ts.ValidateSrvKeyspace(ctx, keyspace, cell); err != nil {
		err2 := vterrors.Wrapf(err, "SrvKeyspace for keyspace %s is corrupt in cell %s", keyspace, cell)
		log.Errorf("%w", err2)
		return err2
	}

	rs, err := wr.buildResharder(ctx, keyspace, workflow, sources, targets, cell, tabletTypes)
	if err != nil {
		return vterrors.Wrap(err, "buildResharder")
	}
	rs.stopAfterCopy = stopAfterCopy
	if !skipSchemaCopy {
		if err := rs.copySchema(ctx); err != nil {
			return vterrors.Wrap(err, "copySchema")
		}
	}
	if err := rs.createStreams(ctx); err != nil {
		return vterrors.Wrap(err, "createStreams")
	}

	if autoStart {
		if err := rs.startStreams(ctx); err != nil {
			return vterrors.Wrap(err, "startStreams")
		}
	} else {
		wr.Logger().Infof("Streams will not be started since -auto_start is set to false")
	}
	return nil
}

func (wr *Wrangler) buildResharder(ctx context.Context, keyspace, workflow string, sources, targets []string, cell, tabletTypes string) (*resharder, error) {
	rs := &resharder{
		wr:            wr,
		keyspace:      keyspace,
		workflow:      workflow,
		sourceMasters: make(map[string]*topo.TabletInfo),
		targetMasters: make(map[string]*topo.TabletInfo),
		cell:          cell,
		tabletTypes:   tabletTypes,
	}
	for _, shard := range sources {
		si, err := wr.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return nil, vterrors.Wrapf(err, "GetShard(%s) failed", shard)
		}
		if !si.IsMasterServing {
			return nil, fmt.Errorf("source shard %v is not in serving state", shard)
		}
		rs.sourceShards = append(rs.sourceShards, si)
		master, err := wr.ts.GetTablet(ctx, si.MasterAlias)
		if err != nil {
			return nil, vterrors.Wrapf(err, "GetTablet(%s) failed", si.MasterAlias)
		}
		rs.sourceMasters[si.ShardName()] = master
	}
	for _, shard := range targets {
		si, err := wr.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return nil, vterrors.Wrapf(err, "GetShard(%s) failed", shard)
		}
		if si.IsMasterServing {
			return nil, fmt.Errorf("target shard %v is in serving state", shard)
		}
		rs.targetShards = append(rs.targetShards, si)
		master, err := wr.ts.GetTablet(ctx, si.MasterAlias)
		if err != nil {
			return nil, vterrors.Wrapf(err, "GetTablet(%s) failed", si.MasterAlias)
		}
		rs.targetMasters[si.ShardName()] = master
	}
	if err := topotools.ValidateForReshard(rs.sourceShards, rs.targetShards); err != nil {
		return nil, vterrors.Wrap(err, "ValidateForReshard")
	}
	if err := rs.validateTargets(ctx); err != nil {
		return nil, vterrors.Wrap(err, "validateTargets")
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

func (rs *resharder) validateTargets(ctx context.Context) error {
	err := rs.forAll(rs.targetShards, func(target *topo.ShardInfo) error {
		targetMaster := rs.targetMasters[target.ShardName()]
		query := fmt.Sprintf("select 1 from _vt.vreplication where db_name=%s", encodeString(targetMaster.DbName()))
		p3qr, err := rs.wr.tmc.VReplicationExec(ctx, targetMaster.Tablet, query)
		if err != nil {
			return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", targetMaster.Tablet, query)
		}
		if len(p3qr.Rows) != 0 {
			return errors.New("some streams already exist in the target shards, please clean them up and retry the command")
		}
		return nil
	})
	return err
}

func (rs *resharder) readRefStreams(ctx context.Context) error {
	var mu sync.Mutex
	err := rs.forAll(rs.sourceShards, func(source *topo.ShardInfo) error {
		sourceMaster := rs.sourceMasters[source.ShardName()]

		query := fmt.Sprintf("select workflow, source, cell, tablet_types from _vt.vreplication where db_name=%s and message != 'FROZEN'", encodeString(sourceMaster.DbName()))
		p3qr, err := rs.wr.tmc.VReplicationExec(ctx, sourceMaster.Tablet, query)
		if err != nil {
			return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", sourceMaster.Tablet, query)
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
	// In this case, 'sharded' means that it's not a reference
	// table. We don't care about any other subtleties.
	return sharded, nil
}

func (rs *resharder) copySchema(ctx context.Context) error {
	oneSource := rs.sourceShards[0].MasterAlias
	err := rs.forAll(rs.targetShards, func(target *topo.ShardInfo) error {
		return rs.wr.CopySchemaShard(ctx, oneSource, []string{"/.*"}, nil, false, rs.keyspace, target.ShardName(), 1*time.Second, false)
	})
	return err
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
		targetMaster := rs.targetMasters[target.ShardName()]

		ig := vreplication.NewInsertGenerator(binlogplayer.BlpStopped, targetMaster.DbName())

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
				Keyspace:      rs.keyspace,
				Shard:         source.ShardName(),
				Filter:        filter,
				StopAfterCopy: rs.stopAfterCopy,
			}
			ig.AddRow(rs.workflow, bls, "", rs.cell, rs.tabletTypes)
		}

		for _, rstream := range rs.refStreams {
			ig.AddRow(rstream.workflow, rstream.bls, "", rstream.cell, rstream.tabletTypes)
		}
		query := ig.String()
		if _, err := rs.wr.tmc.VReplicationExec(ctx, targetMaster.Tablet, query); err != nil {
			return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", targetMaster.Tablet, query)
		}
		return nil
	})

	return err
}

func (rs *resharder) startStreams(ctx context.Context) error {
	err := rs.forAll(rs.targetShards, func(target *topo.ShardInfo) error {
		targetMaster := rs.targetMasters[target.ShardName()]
		query := fmt.Sprintf("update _vt.vreplication set state='Running' where db_name=%s", encodeString(targetMaster.DbName()))
		if _, err := rs.wr.tmc.VReplicationExec(ctx, targetMaster.Tablet, query); err != nil {
			return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", targetMaster.Tablet, query)
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
