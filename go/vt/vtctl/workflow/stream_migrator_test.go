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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	ksschema, err := vindexes.BuildKeyspaceSchema(vs, "ks")
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
