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

	"golang.org/x/net/context"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/topo"
)

type materializer struct {
	wr            *Wrangler
	ms            *vtctldatapb.MaterializeSettings
	targetVSchema *vschemapb.Keyspace
	sourceShards  []*topo.ShardInfo
	targetShards  []*topo.ShardInfo
}

// Materialize performs the steps needed to materialize a list of tables based on the materialization specs.
func (wr *Wrangler) Materialize(ctx context.Context, ms *vtctldatapb.MaterializeSettings) error {
	if err := wr.validateNewWorkflow(ctx, ms.TargetKeyspace, ms.Workflow); err != nil {
		return err
	}
	mz, err := wr.buildMaterializer(ctx, ms)
	if err != nil {
		return err
	}
	if err := mz.deploySchema(ctx); err != nil {
		return err
	}
	return nil
}

func (wr *Wrangler) buildMaterializer(ctx context.Context, ms *vtctldatapb.MaterializeSettings) (*materializer, error) {
	// TODO(sougou): if createddl=="copy", make sure source and target table names match.
	// TODO(sougou): ensure sources and targets have MasterAlias set.
	// TODO(sougou): ensure source tables exist.
	targetVSchema, err := wr.ts.GetVSchema(ctx, ms.TargetKeyspace)
	if err != nil {
		return nil, err
	}
	for _, ts := range ms.TableSettings {
		_, ok := targetVSchema.Tables[ts.TargetTable]
		if !ok {
			if targetVSchema.Sharded {
				return nil, fmt.Errorf("table %s not found in vschema for keyspace %s", ts.TargetTable, ms.TargetKeyspace)
			}
		}
	}

	sourceShards, err := wr.ts.GetServingShards(ctx, ms.SourceKeyspace)
	if err != nil {
		return nil, err
	}
	targetShards, err := wr.ts.GetServingShards(ctx, ms.TargetKeyspace)
	if err != nil {
		return nil, err
	}
	return &materializer{
		wr:            wr,
		ms:            ms,
		targetVSchema: targetVSchema,
		sourceShards:  sourceShards,
		targetShards:  targetShards,
	}, nil
}

func (mz *materializer) deploySchema(ctx context.Context) error {
	for _, target := range mz.targetShards {
		for _, ts := range mz.ms.TableSettings {
			tableSchema, err := mz.wr.GetSchema(ctx, target.MasterAlias, []string{ts.TargetTable}, nil, false)
			if err != nil {
				return err
			}
			if len(tableSchema.TableDefinitions) != 0 {
				// Table already exists.
				continue
			}
			if ts.CreateDdl == "" {
				return fmt.Errorf("target table %v does not exist and there is no create ddl defined", ts.TargetTable)
			}
			createddl := ts.CreateDdl
			if createddl == "copy" {
				sourceSchema, err := mz.wr.GetSchema(ctx, mz.sourceShards[0].MasterAlias, []string{ts.TargetTable}, nil, false)
				if err != nil {
					return err
				}
				if len(sourceSchema.TableDefinitions) == 0 {
					return fmt.Errorf("source table %v does not exist", ts.TargetTable)
				}
				createddl = sourceSchema.TableDefinitions[0].Schema
			}
			targetTablet, err := mz.wr.ts.GetTablet(ctx, target.MasterAlias)
			if err != nil {
				return err
			}
			if _, err := mz.wr.tmc.ExecuteFetchAsDba(ctx, targetTablet.Tablet, false, []byte(createddl), 0, false, true); err != nil {
				return err
			}
		}
	}
	return nil
}
