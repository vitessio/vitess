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

package vstreamer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestUVStreamerNoCopyWithGTID(t *testing.T) {
	execStatements(t, []string{
		"create table t1(id int, val varchar(128), primary key(id))",
		"insert into t1 values (1, 'val1')",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	ctx := context.Background()
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}},
	}
	pos := primaryPosition(t)
	options := &binlogdatapb.VStreamOptions{
		TablesToCopy: []string{"t1"},
	}
	uvs := newUVStreamer(ctx, engine, env.Dbcfgs.DbaWithDB(), env.SchemaEngine, pos,
		nil, filter, testLocalVSchema, throttlerapp.VStreamerName,
		func([]*binlogdatapb.VEvent) error { return nil }, options)
	err := uvs.init()
	require.NoError(t, err)
	require.Empty(t, uvs.plans, "Should not build table plans when startPos is set")
}
