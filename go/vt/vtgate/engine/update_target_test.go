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

package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestUpdateTargetTable(t *testing.T) {
	type testCase struct {
		targetString     string
		expectedQueryLog []string
	}

	tests := []testCase{
		{
			targetString: "ks:-80@replica",
			expectedQueryLog: []string{
				`Target set to ks:-80@replica`,
			},
		},
		{
			targetString: "",
			expectedQueryLog: []string{
				`Target set to `,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.targetString, func(t *testing.T) {
			updateTarget := &UpdateTarget{
				Target: tc.targetString,
			}
			vc := &loggingVCursor{}
			_, err := updateTarget.Execute(vc, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			vc.ExpectLog(t, tc.expectedQueryLog)

			vc = &loggingVCursor{}
			_, err = wrapStreamExecute(updateTarget, vc, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			vc.ExpectLog(t, tc.expectedQueryLog)
		})
	}
}

func TestUpdateTargetGetFields(t *testing.T) {
	updateTarget := &UpdateTarget{}
	vc := &noopVCursor{}
	_, err := updateTarget.GetFields(vc, map[string]*querypb.BindVariable{})
	require.EqualError(t, err, "[BUG] GetFields not reachable for use statement")
}
