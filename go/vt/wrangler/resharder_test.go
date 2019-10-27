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
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestResharderSimple(t *testing.T) {
	sources := []string{"0"}
	targets := []string{"-80", "80-"}
	env := newTestResharderEnv(sources, targets)
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	env.tmc.setVRResultsRE(
		env.tablets[200],
		`insert into _vt.vreplication\(workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name\) values `+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"/.*\\" filter:\\"-80\\" > > ', '', 9223372036854775807, 9223372036854775807, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\)`,
		&sqltypes.Result{},
	)
	env.tmc.setVRResultsRE(
		env.tablets[210],
		`insert into _vt.vreplication\(workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name\) values `+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"/.*\\" filter:\\"80-\\" > > ', '', 9223372036854775807, 9223372036854775807, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\)`,
		&sqltypes.Result{},
	)

	env.tmc.setVRResults(env.tablets[200], "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})
	env.tmc.setVRResults(env.tablets[210], "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true)
	assert.NoError(t, err)
}
