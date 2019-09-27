/*
Copyright 2018 The Vitess Authors.

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

package engine

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestExplainExecute(t *testing.T) {
	source := &fakePrimitive{jsonObj: "the cake is a lie"}

	explain := &Explain{
		Input: source,
	}
	r, err := explain.Execute(noopVCursor{}, make(map[string]*querypb.BindVariable), true)
	assert.NoError(t, err)
	expectResult(t, "jn.Execute", r, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"column",
			"varchar",
		),
		"\"the cake is a lie\"",
	))

}
