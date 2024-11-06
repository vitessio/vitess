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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

func TestGetStreamState(t *testing.T) {
	testCases := []struct {
		name    string
		stream  *vtctldatapb.Workflow_Stream
		rstream *tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream
		want    string
	}{
		{
			name: "error state",
			stream: &vtctldatapb.Workflow_Stream{
				Message: "test error",
			},
			want: "Error",
		},
		{
			name: "copying state",
			stream: &vtctldatapb.Workflow_Stream{
				State: "Running",
				CopyStates: []*vtctldatapb.Workflow_Stream_CopyState{
					{
						Table: "table1",
					},
				},
			},
			want: "Copying",
		},
		{
			name: "lagging state",
			stream: &vtctldatapb.Workflow_Stream{
				State: "Running",
			},
			rstream: &tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
				TimeUpdated: &vttime.Time{
					Seconds: int64(time.Now().Second()) - 11,
				},
			},
			want: "Lagging",
		},
		{
			name: "non-running and error free",
			stream: &vtctldatapb.Workflow_Stream{
				State: "Stopped",
			},
			rstream: &tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
				State: binlogdata.VReplicationWorkflowState_Stopped,
			},
			want: "Stopped",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			state := getStreamState(tt.stream, tt.rstream)
			assert.Equal(t, tt.want, state)
		})
	}
}
