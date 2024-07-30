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
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
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

func TestBuildStreamMigrator(t *testing.T) {
	ctx := context.Background()
	sourceKeyspaceName := "sourceks"
	targetKeyspaceName := "sourceks"
	sourceKeyspace := &testKeyspace{
		KeyspaceName: sourceKeyspaceName,
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: targetKeyspaceName,
		ShardNames:   []string{"-80", "80-"},
	}
	env := newTestEnv(t, ctx, "cell1", sourceKeyspace, targetKeyspace)
	defer env.close()
	vs := &vschemapb.Keyspace{
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
	initialKeyRange, err := key.ParseShardingSpec("-")
	if err != nil || len(initialKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(initialKeyRange))
	}
	leftKeyRange, err := key.ParseShardingSpec("-80")
	if err != nil || len(leftKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(leftKeyRange))
	}

	rightKeyRange, err := key.ParseShardingSpec("80-")
	if err != nil || len(rightKeyRange) != 1 {
		t.Fatalf("ParseShardingSpec failed. Expected non error and only one element. Got err: %v, len(%v)", err, len(rightKeyRange))
	}
	ksschema, err := vindexes.BuildKeyspaceSchema(vs, "ks", sqlparser.NewTestParser())
	require.NoError(t, err, "could not create test keyspace %+v", vs)
	sources := make(map[string]*MigrationSource, len(sourceKeyspace.ShardNames))
	targets := make(map[string]*MigrationTarget, len(targetKeyspace.ShardNames))
	for i, shard := range sourceKeyspace.ShardNames {
		tablet := env.tablets[sourceKeyspace.KeyspaceName][startingSourceTabletUID+(i*tabletUIDStep)]
		kr, _ := key.ParseShardingSpec(shard)
		sources[shard] = &MigrationSource{
			si: topo.NewShardInfo(sourceKeyspaceName, shard, &topodatapb.Shard{KeyRange: kr[0]}, nil),
			primary: &topo.TabletInfo{
				Tablet: tablet,
			},
		}
	}
	for i, shard := range targetKeyspace.ShardNames {
		tablet := env.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID+(i*tabletUIDStep)]
		kr, _ := key.ParseShardingSpec(shard)
		targets[shard] = &MigrationTarget{
			si: topo.NewShardInfo(targetKeyspaceName, shard, &topodatapb.Shard{KeyRange: kr[0]}, nil),
			primary: &topo.TabletInfo{
				Tablet: tablet,
			},
		}
	}
	ts := &testTrafficSwitcher{
		trafficSwitcher: trafficSwitcher{
			migrationType:  binlogdatapb.MigrationType_SHARDS,
			workflow:       "wf1",
			id:             1,
			sources:        sources,
			targets:        targets,
			sourceKeyspace: sourceKeyspaceName,
			targetKeyspace: targetKeyspaceName,
			sourceKSSchema: ksschema,
			workflowType:   binlogdatapb.VReplicationWorkflowType_Reshard,
			ws:             env.ws,
		},
		sourceKeyspaceSchema: ksschema,
	}
	parser := sqlparser.NewTestParser()

	var wfs tabletmanagerdata.ReadVReplicationWorkflowsResponse
	wfName := "wfMat1"
	wfs.Workflows = append(wfs.Workflows, &tabletmanagerdata.ReadVReplicationWorkflowResponse{
		Workflow:     wfName,
		WorkflowType: binlogdatapb.VReplicationWorkflowType_Materialize,
	})
	id := int32(1)
	wfs.Workflows[0].Streams = append(wfs.Workflows[0].Streams, &tabletmanagerdata.ReadVReplicationWorkflowResponse_Stream{
		Id: id,
		Bls: &binlogdatapb.BinlogSource{
			Keyspace: sourceKeyspaceName,
			Shard:    "0",
			Filter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{
					{Match: "t1", Filter: "select * from t1"},
				},
			},
		},
		Pos:   "MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
		State: binlogdatapb.VReplicationWorkflowState_Running,
	})
	workflowKey := env.tmc.GetWorkflowKey(sourceKeyspaceName, "0")
	workflowResponses := []*tabletmanagerdata.ReadVReplicationWorkflowsResponse{
		nil, &wfs, &wfs, &wfs,
	}
	for _, resp := range workflowResponses {
		env.tmc.AddVReplicationWorkflowsResponse(workflowKey, resp)
	}
	sourceTabletIds := []int{100}

	for _, id := range sourceTabletIds {
		queries := []string{
			"select distinct vrepl_id from _vt.copy_state where vrepl_id in (1)",
			"update _vt.vreplication set state='Stopped', message='for cutover' where id in (1)",
		}
		for _, q := range queries {
			env.tmc.expectVRQuery(id, q, &sqltypes.Result{})
		}
	}

	// Shard -80
	env.tmc.expectVRQuery(200, "delete from _vt.vreplication where db_name='vt_sourceks' and workflow in ('wfMat1')", &sqltypes.Result{})
	// FIXME: Note: currently it is not optimal: we create two streams for each shard from all the shards even if the key ranges don't intersect. TBD
	env.tmc.expectVRQuery(200, "/insert into _vt.vreplication.*shard:\"-80\".*in_keyrange.*c1.*-80.*shard:\"80-\".*in_keyrange.*c1.*-80.*", &sqltypes.Result{})

	// Shard 80-
	env.tmc.expectVRQuery(210, "delete from _vt.vreplication where db_name='vt_sourceks' and workflow in ('wfMat1')", &sqltypes.Result{})
	env.tmc.expectVRQuery(210, "/insert into _vt.vreplication.*shard:\"-80\".*in_keyrange.*c1.*80-.*shard:\"80-\".*in_keyrange.*c1.*80-.*", &sqltypes.Result{})

	sm, err := BuildStreamMigrator(ctx, ts, false, parser)
	require.NoError(t, err)
	require.NotNil(t, sm)
	require.NotNil(t, sm.streams)
	require.Equal(t, 1, len(sm.streams))

	workflows, err := sm.StopStreams(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(workflows))
	require.NoError(t, sm.MigrateStreams(ctx))
}
