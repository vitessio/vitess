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
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type testTrafficSwitcher struct {
	trafficSwitcher
	sourceKeyspaceSchema *vindexes.KeyspaceSchema
}

func (tts *testTrafficSwitcher) SourceKeyspaceSchema() *vindexes.KeyspaceSchema {
	return tts.sourceKeyspaceSchema
}

func TestReverseWorkflowName(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{
			in:  "aa",
			out: "aa_reverse",
		},
		{
			in:  "aa_reverse",
			out: "aa",
		},
		{
			in:  "aa_reverse_aa",
			out: "aa_reverse_aa_reverse",
		},
	}
	for _, test := range tests {
		got := ReverseWorkflowName(test.in)
		assert.Equal(t, test.out, got)
	}
}

func TestGetTargetSequenceMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cell := "cell1"
	workflow := "wf1"
	table := "t1"
	sourceKeyspace := &testKeyspace{
		KeyspaceName: "source-ks",
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: "targetks",
		ShardNames:   []string{"-80", "80-"},
	}
	vindexes := map[string]*vschema.Vindex{
		"xxhash": {
			Type: "xxhash",
		},
		"unicode_loose_xxhash": {
			Type: "unicode_loose_xxhash",
		},
	}
	env := newTestEnv(t, ctx, cell, sourceKeyspace, targetKeyspace)
	defer env.close()
	/*
		env.tmc.schema = map[string]*tabletmanagerdatapb.SchemaDefinition{
			"t1": {
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name: "t1",
						Columns: []string{
							"my-col",
						},
					},
				},
			},
		}
	*/

	type testCase struct {
		name          string
		sourceVSchema *vschema.Keyspace
		targetVSchema *vschema.Keyspace
		want          map[string]*sequenceMetadata
		err           string
	}
	tests := []testCase{
		{
			name: "no sequences",
			want: nil,
		},
		{
			name: "sequences with backticks all over",
			sourceVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					"`my-seq1`": {
						Type: "sequence",
					},
				},
			},
			targetVSchema: &vschema.Keyspace{
				Vindexes: vindexes,
				Tables: map[string]*vschema.Table{
					table: {
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "`my-col`",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "`my-col`",
							Sequence: fmt.Sprintf("`%s`.`my-seq1`", sourceKeyspace.KeyspaceName),
						},
					},
				},
			},
			// key: ```my-seq1```, value: &{```my-seq1``` ```source-ks``` vt_`source-ks` t1 vt_targetks 0x14000758640}
			// column_vindexes:{column:"`my-col`" name:"xxhash"} auto_increment:{column:"`my-col`" sequence:"`source-ks`.`my-seq1`"}
			want: map[string]*sequenceMetadata{
				"my-seq1": {
					backingTableName:     "my-seq1",
					backingTableKeyspace: "source-ks",
					backingTableDBName:   "vt_source-ks",
					usingTableName:       table,
					usingTableDBName:     "vt_targetks",
					usingTableDefinition: &vschema.Table{
						ColumnVindexes: []*vschema.ColumnVindex{
							{
								Column: "`my-col`",
								Name:   "xxhash",
							},
						},
						AutoIncrement: &vschema.AutoIncrement{
							Column:   "`my-col`",
							Sequence: fmt.Sprintf("`%s`.`my-seq1`", sourceKeyspace.KeyspaceName),
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := env.ts.SaveVSchema(ctx, sourceKeyspace.KeyspaceName, tc.sourceVSchema)
			require.NoError(t, err)
			err = env.ts.SaveVSchema(ctx, targetKeyspace.KeyspaceName, tc.targetVSchema)
			require.NoError(t, err)
			err = env.ts.RebuildSrvVSchema(ctx, nil)
			require.NoError(t, err)
			sources := make(map[string]*MigrationSource, len(sourceKeyspace.ShardNames))
			targets := make(map[string]*MigrationTarget, len(targetKeyspace.ShardNames))
			for i, shard := range sourceKeyspace.ShardNames {
				tablet := env.tablets[sourceKeyspace.KeyspaceName][startingSourceTabletUID+(i*tabletUIDStep)]
				sources[shard] = &MigrationSource{
					primary: &topo.TabletInfo{
						Tablet: tablet,
					},
				}
			}
			for i, shard := range targetKeyspace.ShardNames {
				tablet := env.tablets[targetKeyspace.KeyspaceName][startingTargetTabletUID+(i*tabletUIDStep)]
				targets[shard] = &MigrationTarget{
					primary: &topo.TabletInfo{
						Tablet: tablet,
					},
				}
			}
			ts := &trafficSwitcher{
				id:             1,
				ws:             env.ws,
				workflow:       workflow,
				tables:         []string{table},
				sourceKeyspace: sourceKeyspace.KeyspaceName,
				targetKeyspace: targetKeyspace.KeyspaceName,
				sources:        sources,
				targets:        targets,
			}
			//t.Logf("DEBUG: ts: %+v", ts)
			got, err := ts.getTargetSequenceMetadata(ctx)
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
			require.True(t, reflect.DeepEqual(tc.want, got), "want: %v, got: %v", tc.want, got)
		})
	}
}
