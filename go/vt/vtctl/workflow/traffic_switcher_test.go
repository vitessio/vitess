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
	"testing"

	"github.com/stretchr/testify/assert"

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
