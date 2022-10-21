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
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/proto/binlogdata"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/concurrency"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	// DefaultFilteredReplicationWaitTime is the default value for argument filteredReplicationWaitTime.
	DefaultFilteredReplicationWaitTime = 30 * time.Second
)

// keyspace related methods for Wrangler

// validateNewWorkflow ensures that the specified workflow doesn't already exist
// in the keyspace.
func (wr *Wrangler) validateNewWorkflow(ctx context.Context, keyspace, workflow string) error {
	allshards, err := wr.ts.FindAllShardsInKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, si := range allshards {
		if si.PrimaryAlias == nil {
			allErrors.RecordError(fmt.Errorf("shard has no primary: %v", si.ShardName()))
			continue
		}
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()

			primary, err := wr.ts.GetTablet(ctx, si.PrimaryAlias)
			if err != nil {
				allErrors.RecordError(vterrors.Wrap(err, "validateWorkflowName.GetTablet"))
				return
			}
			validations := []struct {
				query string
				msg   string
			}{{
				fmt.Sprintf("select 1 from _vt.vreplication where db_name=%s and workflow=%s", encodeString(primary.DbName()), encodeString(workflow)),
				fmt.Sprintf("workflow %s already exists in keyspace %s on tablet %d", workflow, keyspace, primary.Alias.Uid),
			}, {
				fmt.Sprintf("select 1 from _vt.vreplication where db_name=%s and message='FROZEN' and workflow_sub_type != %d", encodeString(primary.DbName()), binlogdata.VReplicationWorkflowSubType_Partial),
				fmt.Sprintf("found previous frozen workflow on tablet %d, please review and delete it first before creating a new workflow",
					primary.Alias.Uid),
			}}
			for _, validation := range validations {
				p3qr, err := wr.tmc.VReplicationExec(ctx, primary.Tablet, validation.query)
				if err != nil {
					allErrors.RecordError(vterrors.Wrap(err, "validateWorkflowName.VReplicationExec"))
					return
				}
				if p3qr != nil && len(p3qr.Rows) != 0 {
					allErrors.RecordError(vterrors.Wrap(fmt.Errorf(validation.msg), "validateWorkflowName.VReplicationExec"))
					return
				}
			}
		}(si)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}

func (wr *Wrangler) printShards(ctx context.Context, si []*topo.ShardInfo) error {
	for _, si := range si {
		wr.Logger().Printf("    Shard: %v\n", si.ShardName())
		if len(si.SourceShards) != 0 {
			wr.Logger().Printf("      Source Shards: %v\n", si.SourceShards)
		}
		ti, err := wr.ts.GetTablet(ctx, si.PrimaryAlias)
		if err != nil {
			return err
		}
		qr, err := wr.tmc.VReplicationExec(ctx, ti.Tablet, fmt.Sprintf("select * from _vt.vreplication where db_name=%v", encodeString(ti.DbName())))
		if err != nil {
			return err
		}
		res := sqltypes.Proto3ToResult(qr)
		if len(res.Rows) != 0 {
			wr.Logger().Printf("      VReplication:\n")
			for _, row := range res.Rows {
				wr.Logger().Printf("        %v\n", row)
			}
		}
		wr.Logger().Printf("      Is Primary Serving: %v\n", si.IsPrimaryServing)
		if len(si.TabletControls) != 0 {
			wr.Logger().Printf("      Tablet Controls: %v\n", si.TabletControls)
		}
	}
	return nil
}

func (wr *Wrangler) getPrimaryPositions(ctx context.Context, shards []*topo.ShardInfo) (map[*topo.ShardInfo]string, error) {
	mu := sync.Mutex{}
	result := make(map[*topo.ShardInfo]string)

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range shards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()
			wr.Logger().Infof("Gathering primary position for %v", topoproto.TabletAliasString(si.PrimaryAlias))
			ti, err := wr.ts.GetTablet(ctx, si.PrimaryAlias)
			if err != nil {
				rec.RecordError(err)
				return
			}

			pos, err := wr.tmc.PrimaryPosition(ctx, ti.Tablet)
			if err != nil {
				rec.RecordError(err)
				return
			}

			wr.Logger().Infof("Got primary position for %v", topoproto.TabletAliasString(si.PrimaryAlias))
			mu.Lock()
			result[si] = pos
			mu.Unlock()
		}(si)
	}
	wg.Wait()
	return result, rec.Error()
}

func (wr *Wrangler) waitForFilteredReplication(ctx context.Context, sourcePositions map[*topo.ShardInfo]string, destinationShards []*topo.ShardInfo, waitTime time.Duration) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range destinationShards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, waitTime)
			defer cancel()

			var pos string
			for _, sourceShard := range si.SourceShards {
				// find the position it should be at
				for s, sp := range sourcePositions {
					if s.Keyspace() == sourceShard.Keyspace && s.ShardName() == sourceShard.Shard {
						pos = sp
						break
					}
				}

				// and wait for it
				wr.Logger().Infof("Waiting for %v to catch up", topoproto.TabletAliasString(si.PrimaryAlias))
				ti, err := wr.ts.GetTablet(ctx, si.PrimaryAlias)
				if err != nil {
					rec.RecordError(err)
					return
				}

				if err := wr.tmc.VReplicationWaitForPos(ctx, ti.Tablet, int(sourceShard.Uid), pos); err != nil {
					if strings.Contains(err.Error(), "not found") {
						wr.Logger().Infof("%v stream %d was not found. Skipping wait.", topoproto.TabletAliasString(si.PrimaryAlias), sourceShard.Uid)
					} else {
						rec.RecordError(err)
					}
				} else {
					wr.Logger().Infof("%v caught up", topoproto.TabletAliasString(si.PrimaryAlias))
				}
			}
		}(si)
	}
	wg.Wait()
	return rec.Error()
}

// refreshPrimaryTablets will just RPC-ping all the primary tablets with RefreshState
func (wr *Wrangler) refreshPrimaryTablets(ctx context.Context, shards []*topo.ShardInfo) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range shards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()
			wr.Logger().Infof("RefreshState primary %v", topoproto.TabletAliasString(si.PrimaryAlias))
			ti, err := wr.ts.GetTablet(ctx, si.PrimaryAlias)
			if err != nil {
				rec.RecordError(err)
				return
			}

			if err := wr.tmc.RefreshState(ctx, ti.Tablet); err != nil {
				rec.RecordError(err)
			} else {
				wr.Logger().Infof("%v responded", topoproto.TabletAliasString(si.PrimaryAlias))
			}
		}(si)
	}
	wg.Wait()
	return rec.Error()
}

// updateShardRecords updates the shard records based on 'from' or 'to' direction.
func (wr *Wrangler) updateShardRecords(ctx context.Context, keyspace string, shards []*topo.ShardInfo, cells []string, servedType topodatapb.TabletType, isFrom bool, clearSourceShards bool) (err error) {
	return topotools.UpdateShardRecords(ctx, wr.ts, wr.tmc, keyspace, shards, cells, servedType, isFrom, clearSourceShards, wr.Logger())
}

// updateFrozenFlag sets or unsets the Frozen flag for primary migration. This is performed
// for all primary tablet control records.
func (wr *Wrangler) updateFrozenFlag(ctx context.Context, shards []*topo.ShardInfo, value bool) (err error) {
	for i, si := range shards {
		updatedShard, err := wr.ts.UpdateShardFields(ctx, si.Keyspace(), si.ShardName(), func(si *topo.ShardInfo) error {
			tc := si.GetTabletControl(topodatapb.TabletType_PRIMARY)
			if tc != nil {
				tc.Frozen = value
				return nil
			}
			// This shard does not have a tablet control record, adding one to set frozen flag
			tc = &topodatapb.Shard_TabletControl{
				TabletType: topodatapb.TabletType_PRIMARY,
				Frozen:     value,
			}
			si.TabletControls = append(si.TabletControls, tc)
			return nil
		})
		if err != nil {
			return err
		}

		shards[i] = updatedShard
	}
	return nil
}

func encodeString(in string) string {
	buf := bytes.NewBuffer(nil)
	sqltypes.NewVarChar(in).EncodeSQL(buf)
	return buf.String()
}
