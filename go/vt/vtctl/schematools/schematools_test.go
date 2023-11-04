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

package schematools

import (
	"testing"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"

	"github.com/stretchr/testify/assert"
)

func TestSchemaMigrationStrategyName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in  vtctldatapb.SchemaMigration_Strategy
		out string
	}{
		{
			in:  vtctldatapb.SchemaMigration_ONLINE,
			out: "vitess",
		},
		{
			in:  vtctldatapb.SchemaMigration_VITESS,
			out: "vitess",
		},
		{
			in:  vtctldatapb.SchemaMigration_GHOST,
			out: "gh-ost",
		},
		{
			in:  vtctldatapb.SchemaMigration_PTOSC,
			out: "pt-osc",
		},
		{
			in:  vtctldatapb.SchemaMigration_DIRECT,
			out: "direct",
		},
		{
			in:  vtctldatapb.SchemaMigration_Strategy(-1),
			out: "unknown",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.out, func(t *testing.T) {
			t.Parallel()

			out := SchemaMigrationStrategyName(test.in)
			assert.Equal(t, test.out, out)
		})
	}
}
