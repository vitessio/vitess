/*
Copyright 2023 The Vitess Authors.

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
	"slices"
	"sync"
	"time"

	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type resharder struct {
	s               *Server
	keyspace        string
	workflow        string
	sourceShards    []*topo.ShardInfo
	sourcePrimaries map[string]*topo.TabletInfo
	targetShards    []*topo.ShardInfo
	targetPrimaries map[string]*topo.TabletInfo
	vschema         *vschemapb.Keyspace
	refStreams      map[string]*refStream
	// This can be single cell name or cell alias but it can
	// also be a comma-separated list of cells.
	cell               string
	tabletTypes        string
	stopAfterCopy      bool
	onDDL              string
	deferSecondaryKeys bool
}

type refStream struct {
	workflow        string
	bls             *binlogdatapb.BinlogSource
	cell            string
	tabletTypes     string
	workflowType    binlogdatapb.VReplicationWorkflowType
	workflowSubType binlogdatapb.VReplicationWorkflowSubType
}

func (s *Server) buildResharder(ctx context.Context, keyspace, workflow string, sources, targets []string, cell, tabletTypes string) (*resharder, error) {
	rs := &resharder{
		s:               s,
		keyspace:        keyspace,
		workflow:        workflow,
		sourcePrimaries: make(map[string]*topo.TabletInfo),
		targetPrimaries: make(map[string]*topo.TabletInfo),
		cell:            cell,
		tabletTypes:     tabletTypes,
	}
	for _, shard := range sources {
		si, err := s.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return nil, vterrors.Wrapf(err, "GetShard(%s) failed", shard)
		}
		if !si.IsPrimaryServing {
			return nil, fmt.Errorf("source shard %v is not in serving state", shard)
		}
		rs.sourceShards = append(rs.sourceShards, si)
		primary, err := s.ts.GetTablet(ctx, si.PrimaryAlias)
		if err != nil {
			return nil, vterrors.Wrapf(err, "GetTablet(%s) failed", si.PrimaryAlias)
		}
		rs.sourcePrimaries[si.ShardName()] = primary
	}
	for _, shard := range targets {
		si, err := s.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return nil, vterrors.Wrapf(err, "GetShard(%s) failed", shard)
		}
		if si.IsPrimaryServing {
			return nil, fmt.Errorf("target shard %v is in serving state", shard)
		}
		rs.targetShards = append(rs.targetShards, si)
		primary, err := s.ts.GetTablet(ctx, si.PrimaryAlias)
		if err != nil {
			return nil, vterrors.Wrapf(err, "GetTablet(%s) failed", si.PrimaryAlias)
		}
		rs.targetPrimaries[si.ShardName()] = primary
	}
	if err := topotools.ValidateForReshard(rs.sourceShards, rs.targetShards); err != nil {
		return nil, vterrors.Wrap(err, "ValidateForReshard")
	}
	if err := rs.validateTargets(ctx); err != nil {
		return nil, vterrors.Wrap(err, "validateTargets")
	}

	vschema, err := s.ts.GetVSchema(ctx, keyspace)
	if err != nil {
		return nil, vterrors.Wrap(err, "GetVSchema")
	}
	rs.vschema = vschema

	if err := rs.readRefStreams(ctx); err != nil {
		return nil, vterrors.Wrap(err, "readRefStreams")
	}
	return rs, nil
}

// validateTargets ensures that the target shards have no existing
// VReplication workflow streams as that is an invalid starting
// state for the non-serving shards involved in a Reshard.
func (rs *resharder) validateTargets(ctx context.Context) error {
	err := rs.forAll(rs.targetShards, func(target *topo.ShardInfo) error {
		targetPrimary := rs.targetPrimaries[target.ShardName()]
		res, err := rs.s.tmc.HasVReplicationWorkflows(ctx, targetPrimary.Tablet, &tabletmanagerdatapb.HasVReplicationWorkflowsRequest{})
		if err != nil {
			return vterrors.Wrapf(err, "HasVReplicationWorkflows(%v)", targetPrimary.Tablet)
		}
		if res.Has {
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "some streams already exist in the target shards, please clean them up and retry the command")
		}
		return nil
	})
	return err
}

func (rs *resharder) readRefStreams(ctx context.Context) error {
	var mu sync.Mutex
	err := rs.forAll(rs.sourceShards, func(source *topo.ShardInfo) error {
		sourcePrimary := rs.sourcePrimaries[source.ShardName()]

		req := &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{
			ExcludeFrozen: true,
		}
		res, err := rs.s.tmc.ReadVReplicationWorkflows(ctx, sourcePrimary.Tablet, req)
		if err != nil {
			return vterrors.Wrapf(err, "ReadVReplicationWorkflows(%v, %+v)", sourcePrimary.Tablet, req)
		}

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
		for _, workflow := range res.Workflows {
			if workflow.Workflow == "" {
				return fmt.Errorf("VReplication streams must have named workflows for migration: shard: %s:%s", source.Keyspace(), source.ShardName())
			}
			for _, stream := range workflow.Streams {
				bls := stream.Bls
				isReference, err := rs.blsIsReference(bls)
				if err != nil {
					return vterrors.Wrap(err, "blsIsReference")
				}
				if !isReference {
					continue
				}
				refKey := fmt.Sprintf("%s:%s:%s", workflow.Workflow, bls.Keyspace, bls.Shard)
				if mustCreate {
					rs.refStreams[refKey] = &refStream{
						workflow:        workflow.Workflow,
						bls:             bls,
						cell:            workflow.Cells,
						tabletTypes:     discovery.BuildTabletTypesString(workflow.TabletTypes, workflow.TabletSelectionPreference),
						workflowType:    workflow.WorkflowType,
						workflowSubType: workflow.WorkflowSubType,
					}
				} else {
					if !ref[refKey] {
						return fmt.Errorf("streams are mismatched across source shards for workflow: %s", workflow)
					}
					delete(ref, refKey)
				}
			}
			if len(ref) != 0 {
				return fmt.Errorf("streams are mismatched across source shards: %v", ref)
			}
		}
		return nil
	})
	return err
}

// blsIsReference is partially copied from streamMigrater.templatize.
// It reuses the constants from that function also.
func (rs *resharder) blsIsReference(bls *binlogdatapb.BinlogSource) (bool, error) {
	streamType := StreamTypeUnknown
	for _, rule := range bls.Filter.Rules {
		typ, err := rs.identifyRuleType(rule)
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

func (rs *resharder) identifyRuleType(rule *binlogdatapb.Rule) (StreamType, error) {
	vtable, ok := rs.vschema.Tables[rule.Match]
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

func (rs *resharder) copySchema(ctx context.Context) error {
	oneSource := rs.sourceShards[0].PrimaryAlias
	err := rs.forAll(rs.targetShards, func(target *topo.ShardInfo) error {
		return rs.s.CopySchemaShard(ctx, oneSource, []string{"/.*"}, nil, false, rs.keyspace, target.ShardName(), 1*time.Second, false)
	})
	return err
}

// createStreams creates all of the VReplication streams that
// need to now exist on the new shards.
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
		targetPrimary := rs.targetPrimaries[target.ShardName()]

		ig := vreplication.NewInsertGenerator(binlogdatapb.VReplicationWorkflowState_Stopped, targetPrimary.DbName())

		// Clone excludeRules to prevent data races.
		copyExcludeRules := slices.Clone(excludeRules)
		for _, source := range rs.sourceShards {
			if !key.KeyRangeIntersect(target.KeyRange, source.KeyRange) {
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
				OnDdl:         binlogdatapb.OnDDLAction(binlogdatapb.OnDDLAction_value[rs.onDDL]),
			}
			ig.AddRow(rs.workflow, bls, "", rs.cell, rs.tabletTypes,
				binlogdatapb.VReplicationWorkflowType_Reshard,
				binlogdatapb.VReplicationWorkflowSubType_None,
				rs.deferSecondaryKeys)
		}

		for _, rstream := range rs.refStreams {
			ig.AddRow(rstream.workflow, rstream.bls, "", rstream.cell, rstream.tabletTypes,
				rstream.workflowType,
				rstream.workflowSubType,
				rs.deferSecondaryKeys)
		}
		query := ig.String()
		if _, err := rs.s.tmc.VReplicationExec(ctx, targetPrimary.Tablet, query); err != nil {
			return vterrors.Wrapf(err, "VReplicationExec(%v, %s)", targetPrimary.Tablet, query)
		}
		return nil
	})

	return err
}

func (rs *resharder) startStreams(ctx context.Context) error {
	err := rs.forAll(rs.targetShards, func(target *topo.ShardInfo) error {
		targetPrimary := rs.targetPrimaries[target.ShardName()]
		// This is the rare case where we truly want to update every stream/record
		// because we've already confirmed that there were no existing workflows
		// on the shards when we started, and we want to start all of the ones
		// that we've created on the new shards as we're migrating them.
		req := &tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest{
			AllWorkflows: true,
			State:        binlogdatapb.VReplicationWorkflowState_Running,
			// We don't want to update anything else so use simulated NULLs.
			Message:      textutil.SimulatedNullString,
			StopPosition: textutil.SimulatedNullString,
		}
		if _, err := rs.s.tmc.UpdateVReplicationWorkflows(ctx, targetPrimary.Tablet, req); err != nil {
			return vterrors.Wrapf(err, "UpdateVReplicationWorkflows(%v, 'state='%s')",
				targetPrimary.Tablet, binlogdatapb.VReplicationWorkflowState_Running.String())
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
