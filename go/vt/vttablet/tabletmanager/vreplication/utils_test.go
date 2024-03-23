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

package vreplication

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestInsertLogTruncation(t *testing.T) {
	dbClient := binlogplayer.NewMockDBClient(t)
	defer dbClient.Close()
	dbClient.RemoveInvariant("insert into _vt.vreplication_log") // Otherwise the insert will be ignored
	stats := binlogplayer.NewStats()
	defer stats.Stop()
	vdbClient := newVDBClient(dbClient, stats)
	defer vdbClient.Close()
	vrID := int32(1)
	typ := "Testing"
	state := binlogdatapb.VReplicationWorkflowState_Error.String()

	insertStmtf := "insert into _vt.vreplication_log(vrepl_id, type, state, message) values(%d, '%s', '%s', %s)"

	tests := []struct {
		message          string
		expectTruncation bool
	}{
		{
			message: "Simple message that's not truncated",
		},
		{
			message:          "Simple message that needs to be truncated " + strings.Repeat("a", 80000) + " cuz it's long",
			expectTruncation: true,
		},
		{
			message: "Simple message that doesn't need to be truncated " + strings.Repeat("b", 64000) + " cuz it's not quite too long",
		},
		{
			message: "Message that is just barely short enough " + strings.Repeat("c", maxVReplicationLogMessageLen-(len("Message that is just barely short enough ")+len(" so it doesn't get truncated"))) + " so it doesn't get truncated",
		},
		{
			message:          "Message that is just barely too long " + strings.Repeat("d", maxVReplicationLogMessageLen-(len("Message that is just barely too long ")+len(" so it gets truncated"))+1) + " so it gets truncated",
			expectTruncation: true,
		},
		{
			message:          "Super long message brosef wut r ya doin " + strings.Repeat("e", 60000) + strings.Repeat("f", 60000) + " so maybe don't do that to yourself and your friends",
			expectTruncation: true,
		},
		{
			message:          "Super duper long message brosef wut r ya doin " + strings.Repeat("g", 120602) + strings.Repeat("h", 120001) + " so maybe really don't do that to yourself and your friends",
			expectTruncation: true,
		},
	}
	for _, tc := range tests {
		t.Run("insertLog", func(t *testing.T) {
			var (
				messageOut string
				err        error
			)
			if tc.expectTruncation {
				messageOut, err = textutil.TruncateText(tc.message, maxVReplicationLogMessageLen, binlogplayer.TruncationLocation, binlogplayer.TruncationIndicator)
				require.NoError(t, err)
				require.True(t, strings.HasPrefix(messageOut, tc.message[:1024]))                 // Confirm we still have the same beginning
				require.True(t, strings.HasSuffix(messageOut, tc.message[len(tc.message)-1024:])) // Confirm we still have the same end
				require.True(t, strings.Contains(messageOut, binlogplayer.TruncationIndicator))   // Confirm we have the truncation text
				t.Logf("Original message length: %d, truncated message length: %d", len(tc.message), len(messageOut))
			} else {
				messageOut = tc.message
			}
			require.LessOrEqual(t, len(messageOut), maxVReplicationLogMessageLen)
			dbClient.ExpectRequest(fmt.Sprintf(insertStmtf, vrID, typ, state, encodeString(messageOut)), &sqltypes.Result{}, nil)
			insertLog(vdbClient, typ, vrID, state, tc.message)
			dbClient.Wait()
		})
	}
}
