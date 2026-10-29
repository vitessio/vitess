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

package command_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cmd/vtctldclient/command"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/vtctl/localvtctldclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

type recordingEmergencyReparentServer struct {
	vtctlservicepb.UnimplementedVtctldServer

	requests []*vtctldatapb.EmergencyReparentShardRequest
}

func (s *recordingEmergencyReparentServer) EmergencyReparentShard(_ context.Context, req *vtctldatapb.EmergencyReparentShardRequest) (*vtctldatapb.EmergencyReparentShardResponse, error) {
	s.requests = append(s.requests, req)

	return &vtctldatapb.EmergencyReparentShardResponse{}, nil
}

func resetEmergencyReparentShardFlags(t *testing.T) {
	t.Helper()

	for _, name := range []string{"allow-split-brain-promotion", "new-primary", "required-position"} {
		flag := command.EmergencyReparentShard.Flags().Lookup(name)
		require.NotNil(t, flag)
		require.NoError(t, flag.Value.Set(flag.DefValue))
		flag.Changed = false
	}
}

// TestERSRequiredPositionFlag checks that --required-position reaches the request unchanged.
func TestERSRequiredPositionFlag(t *testing.T) {
	originalProtocol := command.VtctldClientProtocol
	resetEmergencyReparentShardFlags(t)
	command.VtctldClientProtocol = "local"
	t.Cleanup(func() {
		command.Root.SetArgs(nil)
		command.VtctldClientProtocol = originalProtocol
		resetEmergencyReparentShardFlags(t)
	})

	server := &recordingEmergencyReparentServer{}
	localvtctldclient.SetServer(server)

	const position = "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
	command.Root.SetArgs([]string{"EmergencyReparentShard", "--required-position", position, "commerce/0"})

	require.NoError(t, command.Root.Execute())
	require.Len(t, server.requests, 1)
	assert.Equal(t, position, server.requests[0].GetRequiredPosition())
}

func TestERSSplitBrainPromotionFlags(t *testing.T) {
	originalProtocol := command.VtctldClientProtocol
	resetEmergencyReparentShardFlags(t)
	command.VtctldClientProtocol = "local"
	t.Cleanup(func() {
		command.Root.SetArgs(nil)
		command.VtctldClientProtocol = originalProtocol
		resetEmergencyReparentShardFlags(t)
	})

	t.Run("rejects missing new primary before RPC", func(t *testing.T) {
		resetEmergencyReparentShardFlags(t)
		server := &recordingEmergencyReparentServer{}
		localvtctldclient.SetServer(server)
		command.Root.SetArgs([]string{
			"EmergencyReparentShard",
			"--allow-split-brain-promotion",
			"commerce/0",
		})

		err := command.Root.Execute()
		require.ErrorContains(t, err, "--allow-split-brain-promotion requires --new-primary")
		assert.Empty(t, server.requests)
	})

	t.Run("sends override with new primary", func(t *testing.T) {
		resetEmergencyReparentShardFlags(t)
		server := &recordingEmergencyReparentServer{}
		localvtctldclient.SetServer(server)
		command.Root.SetArgs([]string{
			"EmergencyReparentShard",
			"--new-primary", "zone1-0000000100",
			"--allow-split-brain-promotion",
			"commerce/0",
		})

		err := command.Root.Execute()
		require.NoError(t, err)
		require.Len(t, server.requests, 1)
		require.NotNil(t, server.requests[0].GetNewPrimary())
		assert.Equal(t, "zone1", server.requests[0].GetNewPrimary().GetCell())
		assert.Equal(t, uint32(100), server.requests[0].GetNewPrimary().GetUid())
		assert.True(t, server.requests[0].GetAllowSplitBrainPromotion())
	})
}

type initShardPrimaryServer struct {
	vtctlservicepb.UnimplementedVtctldServer

	t      *testing.T
	events []*logutilpb.Event
	logs   *logtest.Logs
}

func (s *initShardPrimaryServer) InitShardPrimary(context.Context, *vtctldatapb.InitShardPrimaryRequest) (*vtctldatapb.InitShardPrimaryResponse, error) {
	// The command's PersistentPreRunE replaces the logger, so capture the log
	// once the RPC is made.
	s.logs = logtest.Capture(s.t, slog.LevelInfo)
	return &vtctldatapb.InitShardPrimaryResponse{Events: s.events}, nil
}

// TestInitShardPrimaryLogsEvents checks that each event the RPC returns is
// logged with the keyspace and shard, its level, and every other set field of
// the event as its own attribute, the text under message.
func TestInitShardPrimaryLogsEvents(t *testing.T) {
	originalProtocol := command.VtctldClientProtocol
	command.VtctldClientProtocol = "local"
	t.Cleanup(func() {
		command.Root.SetArgs(nil)
		command.VtctldClientProtocol = originalProtocol
	})

	eventTime := time.Date(2026, 10, 29, 1, 2, 3, 123456789, time.UTC)
	events := []*logutilpb.Event{{
		Time:  protoutil.TimeToProto(eventTime),
		Level: logutilpb.Level_WARNING,
		File:  "reparent.go",
		Line:  12,
		Value: "promoting zone1-0000000100",
	}, {
		Level: logutilpb.Level_CONSOLE,
		Value: "console text",
	}, {
		Level: logutilpb.Level_INFO,
		File:  "reparent.go",
	}}
	server := &initShardPrimaryServer{t: t, events: events}
	localvtctldclient.SetServer(server)
	command.Root.SetArgs([]string{"InitShardPrimary", "commerce/0", "zone1-0000000100"})
	require.NoError(t, command.Root.Execute())

	require.NotNil(t, server.logs)
	var records []map[string]any
	for _, record := range server.logs.Records(t) {
		if record["msg"] == "InitShardPrimary event" {
			records = append(records, record)
		}
	}
	require.Len(t, records, 3)

	record := records[0]
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "commerce", record["keyspace"])
	assert.Equal(t, "0", record["shard"])
	assert.Equal(t, "WARNING", record["event_level"])
	gotTime, err := time.Parse(time.RFC3339Nano, record["event_time"].(string))
	require.NoError(t, err)
	assert.True(t, gotTime.Equal(eventTime), "event_time %v", record["event_time"])
	assert.Equal(t, "reparent.go", record["event_file"])
	assert.EqualValues(t, 12, record["event_line"])
	assert.Equal(t, "promoting zone1-0000000100", record["message"])

	record = records[1]
	assert.Equal(t, "CONSOLE", record["event_level"])
	assert.Equal(t, "console text", record["message"])
	assert.NotContains(t, record, "event_time")
	assert.NotContains(t, record, "event_file")
	assert.NotContains(t, record, "event_line")

	record = records[2]
	assert.Equal(t, "INFO", record["event_level"])
	assert.Equal(t, "reparent.go", record["event_file"])
	assert.NotContains(t, record, "message")
}
