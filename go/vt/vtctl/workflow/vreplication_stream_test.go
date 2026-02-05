/*
Copyright 2024 The Vitess Authors.

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
	"fmt"
	"reflect"
	"testing"
)

// TestVReplicationStreams tests various methods of VReplicationStreams.
func TestVReplicationStreams(t *testing.T) {
	var streams VReplicationStreams
	for i := 1; i <= 3; i++ {
		streams = append(streams, &VReplicationStream{ID: int32(i), Workflow: fmt.Sprintf("workflow%d", i)})
	}

	tests := []struct {
		name           string
		funcUnderTest  func(VReplicationStreams) interface{}
		expectedResult interface{}
	}{
		{"Test IDs", func(s VReplicationStreams) interface{} { return s.IDs() }, []int32{1, 2, 3}},
		{"Test Values", func(s VReplicationStreams) interface{} { return s.Values() }, "(1, 2, 3)"},
		{"Test Workflows", func(s VReplicationStreams) interface{} { return s.Workflows() }, []string{"workflow1", "workflow2", "workflow3"}},
		{"Test Copy", func(s VReplicationStreams) interface{} { return s.Copy() }, streams.Copy()},
		{"Test ToSlice", func(s VReplicationStreams) interface{} { return s.ToSlice() }, []*VReplicationStream{streams[0], streams[1], streams[2]}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.funcUnderTest(streams)
			if !reflect.DeepEqual(result, tt.expectedResult) {
				t.Errorf("Failed %s: expected %v, got %v", tt.name, tt.expectedResult, result)
			}
		})
	}
}
