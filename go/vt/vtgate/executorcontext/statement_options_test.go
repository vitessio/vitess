/*
Copyright 2026 The Vitess Authors.

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

package executorcontext

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// TestExecuteOptions checks that the options of a call to vttablet are the
// session's options with the statement's options applied, and that the
// session's options stay as they were.
func TestExecuteOptions(t *testing.T) {
	timeout := int64(100)
	sessionOptions := &querypb.ExecuteOptions{
		WorkloadName: "session",
		Priority:     "10",
		Consolidator: querypb.ExecuteOptions_CONSOLIDATOR_DISABLED,
		Timeout:      &querypb.ExecuteOptions_AuthoritativeTimeout{AuthoritativeTimeout: 5},
		Workload:     querypb.ExecuteOptions_OLAP,
	}

	tests := []struct {
		name           string
		sessionOptions *querypb.ExecuteOptions
		stmtOptions    *StatementOptions
		want           *querypb.ExecuteOptions
	}{
		{
			name: "no options",
		},
		{
			name:           "session options without statement options",
			sessionOptions: sessionOptions,
			want:           sessionOptions,
		},
		{
			name:        "statement options without session options",
			stmtOptions: &StatementOptions{WorkloadName: "stmt", Priority: "50", Consolidator: querypb.ExecuteOptions_CONSOLIDATOR_ENABLED, timeout: &timeout},
			want: &querypb.ExecuteOptions{
				WorkloadName: "stmt",
				Priority:     "50",
				Consolidator: querypb.ExecuteOptions_CONSOLIDATOR_ENABLED,
				Timeout:      &querypb.ExecuteOptions_AuthoritativeTimeout{AuthoritativeTimeout: 100},
			},
		},
		{
			name:        "empty statement options without session options",
			stmtOptions: &StatementOptions{},
		},
		{
			name:           "statement options replace the session's",
			sessionOptions: sessionOptions,
			stmtOptions:    &StatementOptions{WorkloadName: "stmt", Priority: "50", Consolidator: querypb.ExecuteOptions_CONSOLIDATOR_ENABLED, timeout: &timeout},
			want: &querypb.ExecuteOptions{
				WorkloadName: "stmt",
				Priority:     "50",
				Consolidator: querypb.ExecuteOptions_CONSOLIDATOR_ENABLED,
				Timeout:      &querypb.ExecuteOptions_AuthoritativeTimeout{AuthoritativeTimeout: 100},
				Workload:     querypb.ExecuteOptions_OLAP,
			},
		},
		{
			name:           "options that the statement doesn't set keep the session's, except the timeout",
			sessionOptions: sessionOptions,
			stmtOptions:    &StatementOptions{},
			want: &querypb.ExecuteOptions{
				WorkloadName: "session",
				Priority:     "10",
				Consolidator: querypb.ExecuteOptions_CONSOLIDATOR_DISABLED,
				Workload:     querypb.ExecuteOptions_OLAP,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			session := NewSafeSession(&vtgatepb.Session{Options: tc.sessionOptions.CloneVT()})
			ctx := t.Context()
			if tc.stmtOptions != nil {
				ctx = WithStatementOptions(ctx, tc.stmtOptions)
			}

			got := ExecuteOptions(ctx, session)
			if tc.want == nil {
				require.Nil(t, got)
			} else {
				assert.Truef(t, proto.Equal(tc.want, got), "got %v, want %v", got, tc.want)
			}
			assert.Truef(t, proto.Equal(tc.sessionOptions, session.Options), "session options changed to %v", session.Options)
		})
	}
}
