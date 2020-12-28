/*
Copyright 2020 The Vitess Authors.

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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestMergeUnshardedJoins(t *testing.T) {
	ks := &vindexes.Keyspace{Name: "apa", Sharded: false}
	ks2 := &vindexes.Keyspace{Name: "banan", Sharded: false}
	r1 := &routePlan{
		routeOpCode: engine.SelectUnsharded,
		solved:      1,
		keyspace:    ks,
	}
	r2 := &routePlan{
		routeOpCode: engine.SelectUnsharded,
		solved:      2,
		keyspace:    ks,
	}
	r3 := &routePlan{
		routeOpCode: engine.SelectUnsharded,
		solved:      4,
		keyspace:    ks2,
	}
	assert.NotNil(t, tryMerge(r1, r2, []sqlparser.Expr{}))
	assert.Nil(t, tryMerge(r1, r3, []sqlparser.Expr{}))
	assert.Nil(t, tryMerge(r2, r3, []sqlparser.Expr{}))
}
