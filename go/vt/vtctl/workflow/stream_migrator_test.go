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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func TestTemplatize(t *testing.T) {
	tests := []struct {
		in  []*VReplicationStream
		out string
		err string
	}{{
		// First test contains all fields.
		in: []*VReplicationStream{{
			ID:       1,
			Workflow: "test",
			BinlogSource: &binlogdatapb.BinlogSource{
				Keyspace: "ks",
				Shard:    "80-",
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1 where in_keyrange('-80')",
					}},
				},
			},
		}},
		out: `[{"ID":1,"Workflow":"test","BinlogSource":{"keyspace":"ks","shard":"80-","filter":{"rules":[{"match":"t1","filter":"select * from t1 where in_keyrange('{{.}}')"}]}}}]`,
	}, {
		// Reference table.
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "ref",
						Filter: "",
					}},
				},
			},
		}},
		out: "",
	}, {
		// Sharded table.
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "-80",
					}},
				},
			},
		}},
		out: `[{"ID":0,"Workflow":"","BinlogSource":{"filter":{"rules":[{"match":"t1","filter":"{{.}}"}]}}}]`,
	}, {
		// table not found
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match: "t3",
					}},
				},
			},
		}},
		err: `table t3 not found in vschema`,
	}, {
		// sharded table with no filter
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match: "t1",
					}},
				},
			},
		}},
		err: `rule match:"t1"  does not have a select expression in vreplication`,
	}, {
		// Excluded table.
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: vreplication.ExcludeStr,
					}},
				},
			},
		}},
		err: `unexpected rule in vreplication: match:"t1" filter:"exclude" `,
	}, {
		// Sharded table and ref table
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "-80",
					}, {
						Match:  "ref",
						Filter: "",
					}},
				},
			},
		}},
		err: `cannot migrate streams with a mix of reference and sharded tables: filter:<rules:<match:"t1" filter:"{{.}}" > rules:<match:"ref" > > `,
	}, {
		// Ref table and sharded table (different code path)
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "ref",
						Filter: "",
					}, {
						Match:  "t2",
						Filter: "-80",
					}},
				},
			},
		}},
		err: `cannot migrate streams with a mix of reference and sharded tables: filter:<rules:<match:"ref" > rules:<match:"t2" filter:"{{.}}" > > `,
	}, {
		// Ref table with select expression
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "ref",
						Filter: "select * from t1",
					}},
				},
			},
		}},
		out: "",
	}, {
		// Select expresstion with no keyrange value
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1",
					}},
				},
			},
		}},
		out: `[{"ID":0,"Workflow":"","BinlogSource":{"filter":{"rules":[{"match":"t1","filter":"select * from t1 where in_keyrange(c1, 'hash', '{{.}}')"}]}}}]`,
	}, {
		// Select expresstion with one keyrange value
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1 where in_keyrange('-80')",
					}},
				},
			},
		}},
		out: `[{"ID":0,"Workflow":"","BinlogSource":{"filter":{"rules":[{"match":"t1","filter":"select * from t1 where in_keyrange('{{.}}')"}]}}}]`,
	}, {
		// Select expresstion with three keyrange values
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1 where in_keyrange(col, vdx, '-80')",
					}},
				},
			},
		}},
		out: `[{"ID":0,"Workflow":"","BinlogSource":{"filter":{"rules":[{"match":"t1","filter":"select * from t1 where in_keyrange(col, vdx, '{{.}}')"}]}}}]`,
	}, {
		// syntax error
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "bad syntax",
					}},
				},
			},
		}},
		err: "syntax error at position 4 near 'bad'",
	}, {
		// invalid statement
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "update t set a=1",
					}},
				},
			},
		}},
		err: "unexpected query: update t set a=1",
	}, {
		// invalid in_keyrange
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1 where in_keyrange(col, vdx, '-80', extra)",
					}},
				},
			},
		}},
		err: "unexpected in_keyrange parameters: in_keyrange(col, vdx, '-80', extra)",
	}, {
		// * in_keyrange
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1 where in_keyrange(*)",
					}},
				},
			},
		}},
		err: "unexpected in_keyrange parameters: in_keyrange(*)",
	}, {
		// non-string in_keyrange
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select * from t1 where in_keyrange(aa)",
					}},
				},
			},
		}},
		err: "unexpected in_keyrange parameters: in_keyrange(aa)",
	}, {
		// '{{' in query
		in: []*VReplicationStream{{
			BinlogSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: "select '{{' from t1 where in_keyrange('-80')",
					}},
				},
			},
		}},
		err: "cannot migrate queries that contain '{{' in their string: select '{{' from t1 where in_keyrange('-80')",
	}}
	vs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"thash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Columns: []string{"c1"},
					Name:    "thash",
				}},
			},
			"t2": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Columns: []string{"c1"},
					Name:    "thash",
				}},
			},
			"ref": {
				Type: vindexes.TypeReference,
			},
		},
	}
	ksschema, err := vindexes.BuildKeyspaceSchema(vs, "ks", sqlparser.NewTestParser())
	require.NoError(t, err, "could not create test keyspace %+v", vs)

	ts := &testTrafficSwitcher{
		sourceKeyspaceSchema: ksschema,
	}
	for _, tt := range tests {
		sm := &StreamMigrator{ts: ts}
		out, err := sm.templatize(context.Background(), tt.in)
		if tt.err != "" {
			assert.Error(t, err, "templatize(%v) expected to get err=%s, got %+v", stringifyVRS(tt.in), tt.err, err)
		}

		got := stringifyVRS(out)
		assert.Equal(t, tt.out, got, "templatize(%v) mismatch", stringifyVRS(tt.in))
	}
}

func stringifyVRS(streams []*VReplicationStream) string {
	if len(streams) == 0 {
		return ""
	}

	type testVRS struct {
		ID           int32
		Workflow     string
		BinlogSource *binlogdatapb.BinlogSource
	}

	converted := make([]*testVRS, len(streams))
	for i, stream := range streams {
		converted[i] = &testVRS{
			ID:           stream.ID,
			Workflow:     stream.Workflow,
			BinlogSource: stream.BinlogSource,
		}
	}

	b, _ := json.Marshal(converted)
	return string(b)
}

var testVSchema = &vschemapb.Keyspace{
	Sharded: true,
	Vindexes: map[string]*vschemapb.Vindex{
		"xxhash": {
			Type: "xxhash",
		},
	},
	Tables: map[string]*vschemapb.Table{
		"t1": {
			ColumnVindexes: []*vschemapb.ColumnVindex{{
				Columns: []string{"c1"},
				Name:    "xxhash",
			}},
		},
		"t2": {
			ColumnVindexes: []*vschemapb.ColumnVindex{{
				Columns: []string{"c1"},
				Name:    "xxhash",
			}},
		},
		"ref": {
			Type: vindexes.TypeReference,
		},
	},
}

var (
	commerceKeyspace = &testKeyspace{
		KeyspaceName: "commerce",
		ShardNames:   []string{"0"},
	}
	customerUnshardedKeyspace = &testKeyspace{
		KeyspaceName: "customer",
		ShardNames:   []string{"0"},
	}
	customerShardedKeyspace = &testKeyspace{
		KeyspaceName: "customer",
		ShardNames:   []string{"-80", "80-"},
	}
)

type streamMigratorEnv struct {
	tenv            *testEnv
	ts              *testTrafficSwitcher
	sourceTabletIds []int
	targetTabletIds []int
}

func (env *streamMigratorEnv) close() {
	env.tenv.close()
}

func (env *streamMigratorEnv) addSourceQueries(queries []string) {
	for _, id := range env.sourceTabletIds {
		for _, q := range queries {
			env.tenv.tmc.expectVRQuery(id, q, &sqltypes.Result{})
		}
	}
}

func (env *streamMigratorEnv) addTargetQueries(queries []string) {
	for _, id := range env.targetTabletIds {
		for _, q := range queries {
			env.tenv.tmc.expectVRQuery(id, q, &sqltypes.Result{})
		}
	}
}

func newStreamMigratorEnv(ctx context.Context, t *testing.T, sourceKeyspace, targetKeyspace *testKeyspace) *streamMigratorEnv {
	tenv := newTestEnv(t, ctx, "cell1", sourceKeyspace, targetKeyspace)
	env := &streamMigratorEnv{tenv: tenv}

	ksschema, err := vindexes.BuildKeyspaceSchema(testVSchema, "ks", sqlparser.NewTestParser())
	require.NoError(t, err, "could not create test keyspace %+v", testVSchema)
	sources := make(map[string]*MigrationSource, len(sourceKeyspace.ShardNames))
	targets := make(map[string]*MigrationTarget, len(targetKeyspace.ShardNames))
	for i, shard := range sourceKeyspace.ShardNames {
		tablet := tenv.tablets[sourceKeyspace.KeyspaceName][startingSourceTabletUID+(i*tabletUIDStep)]
		kr, _ := key.ParseShardingSpec(shard)
		sources[shard] = &MigrationSource{
			si: topo.NewShardInfo(sourceKeyspace.KeyspaceName, shard, &topodatapb.Shard{KeyRange: kr[0]}, nil),
			primary: &topo.TabletInfo{
				Tablet: tablet,
			},
		}
		env.sourceTabletIds = append(env.sourceTabletIds, int(tablet.Alias.Uid))
	}
	for i, shard := range targetKeyspace.ShardNames {
		tablet := tenv.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID+(i*tabletUIDStep)]
		kr, _ := key.ParseShardingSpec(shard)
		targets[shard] = &MigrationTarget{
			si: topo.NewShardInfo(targetKeyspace.KeyspaceName, shard, &topodatapb.Shard{KeyRange: kr[0]}, nil),
			primary: &topo.TabletInfo{
				Tablet: tablet,
			},
		}
		env.targetTabletIds = append(env.targetTabletIds, int(tablet.Alias.Uid))
	}
	ts := &testTrafficSwitcher{
		trafficSwitcher: trafficSwitcher{
			migrationType:  binlogdatapb.MigrationType_SHARDS,
			workflow:       "wf1",
			id:             1,
			sources:        sources,
			targets:        targets,
			sourceKeyspace: sourceKeyspace.KeyspaceName,
			targetKeyspace: targetKeyspace.KeyspaceName,
			sourceKSSchema: ksschema,
			workflowType:   binlogdatapb.VReplicationWorkflowType_Reshard,
			ws:             tenv.ws,
		},
		sourceKeyspaceSchema: ksschema,
	}
	env.ts = ts

	return env
}

func addMaterializeWorkflow(t *testing.T, env *streamMigratorEnv, id int32, sourceShard string) {
	var wfs tabletmanagerdata.ReadVReplicationWorkflowsResponse
	wfName := "wfMat1"
	wfs.Workflows = append(wfs.Workflows, &tabletmanagerdata.ReadVReplicationWorkflowResponse{
		Workflow:     wfName,
		WorkflowType: binlogdatapb.VReplicationWorkflowType_Materialize,
	})
	wfs.Workflows[0].Streams = append(wfs.Workflows[0].Streams, &tabletmanagerdata.ReadVReplicationWorkflowResponse_Stream{
		Id: id,
		Bls: &binlogdatapb.BinlogSource{
			Keyspace: env.tenv.sourceKeyspace.KeyspaceName,
			Shard:    sourceShard,
			Filter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{
					{Match: "t1", Filter: "select * from t1"},
				},
			},
		},
		Pos:   position,
		State: binlogdatapb.VReplicationWorkflowState_Running,
	})
	workflowKey := env.tenv.tmc.GetWorkflowKey(env.tenv.sourceKeyspace.KeyspaceName, sourceShard)
	workflowResponses := []*tabletmanagerdata.ReadVReplicationWorkflowsResponse{
		nil,              // this is the response for getting stopped workflows
		&wfs, &wfs, &wfs, // return the full list for subsequent GetWorkflows calls
	}
	for _, resp := range workflowResponses {
		env.tenv.tmc.AddVReplicationWorkflowsResponse(workflowKey, resp)
	}
	queries := []string{
		fmt.Sprintf("select distinct vrepl_id from _vt.copy_state where vrepl_id in (%d)", id),
		fmt.Sprintf("update _vt.vreplication set state='Stopped', message='for cutover' where id in (%d)", id),
		fmt.Sprintf("delete from _vt.vreplication where db_name='vt_%s' and workflow in ('%s')",
			env.tenv.sourceKeyspace.KeyspaceName, wfName),
	}
	env.addSourceQueries(queries)
	queries = []string{
		fmt.Sprintf("delete from _vt.vreplication where db_name='vt_%s' and workflow in ('%s')",
			env.tenv.sourceKeyspace.KeyspaceName, wfName),
	}
	env.addTargetQueries(queries)

}

func addReferenceWorkflow(t *testing.T, env *streamMigratorEnv, id int32, sourceShard string) {
	var wfs tabletmanagerdata.ReadVReplicationWorkflowsResponse
	wfName := "wfRef1"
	wfs.Workflows = append(wfs.Workflows, &tabletmanagerdata.ReadVReplicationWorkflowResponse{
		Workflow:     wfName,
		WorkflowType: binlogdatapb.VReplicationWorkflowType_Materialize,
	})
	wfs.Workflows[0].Streams = append(wfs.Workflows[0].Streams, &tabletmanagerdata.ReadVReplicationWorkflowResponse_Stream{
		Id: id,
		Bls: &binlogdatapb.BinlogSource{
			Keyspace: env.tenv.sourceKeyspace.KeyspaceName,
			Shard:    sourceShard,
			Filter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{
					{Match: "ref", Filter: "select * from ref"},
				},
			},
		},
		Pos:   position,
		State: binlogdatapb.VReplicationWorkflowState_Running,
	})
	workflowKey := env.tenv.tmc.GetWorkflowKey(env.tenv.sourceKeyspace.KeyspaceName, sourceShard)
	workflowResponses := []*tabletmanagerdata.ReadVReplicationWorkflowsResponse{
		nil,              // this is the response for getting stopped workflows
		&wfs, &wfs, &wfs, // return the full list for subsequent GetWorkflows calls
	}
	for _, resp := range workflowResponses {
		env.tenv.tmc.AddVReplicationWorkflowsResponse(workflowKey, resp)
	}
}

func TestBuildStreamMigratorOneMaterialize(t *testing.T) {
	ctx := context.Background()
	env := newStreamMigratorEnv(ctx, t, customerUnshardedKeyspace, customerShardedKeyspace)
	defer env.close()
	tmc := env.tenv.tmc

	addMaterializeWorkflow(t, env, 100, "0")

	// FIXME: Note: currently it is not optimal: we create two streams for each shard from all the
	// shards even if the key ranges don't intersect. TBD
	getInsert := func(shard string) string {
		s := "/insert into _vt.vreplication.*"
		s += fmt.Sprintf("shard:\"-80\".*in_keyrange.*c1.*%s.*", shard)
		s += fmt.Sprintf("shard:\"80-\".*in_keyrange.*c1.*%s.*", shard)
		return s
	}
	tmc.expectVRQuery(200, getInsert("-80"), &sqltypes.Result{})
	tmc.expectVRQuery(210, getInsert("80-"), &sqltypes.Result{})

	sm, err := BuildStreamMigrator(ctx, env.ts, false, sqlparser.NewTestParser())
	require.NoError(t, err)
	require.NotNil(t, sm)
	require.NotNil(t, sm.streams)
	require.Equal(t, 1, len(sm.streams))

	workflows, err := sm.StopStreams(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(workflows))
	require.NoError(t, sm.MigrateStreams(ctx))
	require.Len(t, sm.templates, 1)
	env.addTargetQueries([]string{
		fmt.Sprintf("update _vt.vreplication set state='Running' where db_name='vt_%s' and workflow in ('%s')",
			env.tenv.sourceKeyspace.KeyspaceName, "wfMat1"),
	})
	require.NoError(t, StreamMigratorFinalize(ctx, env.ts, []string{"wfMat1"}))
}

func TestBuildStreamMigratorNoStreams(t *testing.T) {
	ctx := context.Background()
	env := newStreamMigratorEnv(ctx, t, customerUnshardedKeyspace, customerShardedKeyspace)
	defer env.close()

	sm, err := BuildStreamMigrator(ctx, env.ts, false, sqlparser.NewTestParser())
	require.NoError(t, err)
	require.NotNil(t, sm)
	require.NotNil(t, sm.streams)
	require.Equal(t, 0, len(sm.streams))

	workflows, err := sm.StopStreams(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(workflows))
	require.NoError(t, sm.MigrateStreams(ctx))
	require.Len(t, sm.templates, 0)
}

func TestBuildStreamMigratorRefStream(t *testing.T) {
	ctx := context.Background()
	env := newStreamMigratorEnv(ctx, t, customerUnshardedKeyspace, customerShardedKeyspace)
	defer env.close()

	addReferenceWorkflow(t, env, 100, "0")

	sm, err := BuildStreamMigrator(ctx, env.ts, false, sqlparser.NewTestParser())
	require.NoError(t, err)
	require.NotNil(t, sm)
	require.NotNil(t, sm.streams)
	require.Equal(t, 0, len(sm.streams))

	workflows, err := sm.StopStreams(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(workflows))
	require.NoError(t, sm.MigrateStreams(ctx))
	require.Len(t, sm.templates, 0)
}
