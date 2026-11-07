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

package command

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// fakeBackupStream is an in-memory backupResponseStream for tests.
type fakeBackupStream struct {
	resps []*vtctldatapb.BackupResponse
	err   error // returned after resps instead of io.EOF, if set
	i     int
}

func (f *fakeBackupStream) Recv() (*vtctldatapb.BackupResponse, error) {
	if f.i >= len(f.resps) {
		if f.err != nil {
			return nil, f.err
		}
		return nil, io.EOF
	}
	r := f.resps[f.i]
	f.i++
	return r, nil
}

func eventResp() *vtctldatapb.BackupResponse {
	return &vtctldatapb.BackupResponse{
		TabletAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Keyspace:    "ks",
		Shard:       "-",
		Event:       &logutilpb.Event{Value: "backing up"},
	}
}

// TestConsumeBackupStream_JSONUsable verifies JSON mode separates progress from
// the machine-readable usable-backup result and inlines the MANIFEST.
func TestConsumeBackupStream_JSONUsable(t *testing.T) {
	manifest := `{"BackupName":"2026-01-01.000000.zone1-0000000100","Incremental":false}`
	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{
		eventResp(),
		{Manifest: manifest, Status: tabletmanagerdatapb.BackupResponse_USABLE},
	}}

	var out, errOut bytes.Buffer
	err := consumeBackupStream(stream, true /* outputJSON */, &out, &errOut)
	require.NoError(t, err)

	// Log events go to errOut in JSON mode, keeping stdout clean.
	assert.Contains(t, errOut.String(), "backing up")

	// stdout is a single JSON object with the manifest inlined (not escaped).
	var got struct {
		Status   string          `json:"status"`
		Manifest json.RawMessage `json:"manifest"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	assert.Equal(t, "USABLE", got.Status)

	var m map[string]any
	require.NoError(t, json.Unmarshal(got.Manifest, &m))
	assert.Equal(t, "2026-01-01.000000.zone1-0000000100", m["BackupName"])
}

// TestConsumeBackupStream_JSONEmpty verifies an empty incremental backup is
// represented by EMPTY status and a null MANIFEST.
func TestConsumeBackupStream_JSONEmpty(t *testing.T) {
	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{
		eventResp(),
		{Status: tabletmanagerdatapb.BackupResponse_EMPTY},
	}}

	var out, errOut bytes.Buffer
	err := consumeBackupStream(stream, true /* outputJSON */, &out, &errOut)
	require.NoError(t, err)

	var got struct {
		Status   string          `json:"status"`
		Manifest json.RawMessage `json:"manifest"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	assert.Equal(t, "EMPTY", got.Status)
	assert.Equal(t, "null", string(got.Manifest))
}

// TestConsumeBackupStream_JSONOlderServerNoStatus verifies that a stream with no
// terminal message, as from a vtctld or tablet that predates include_outcome,
// surfaces as "UNKNOWN", not EMPTY.
func TestConsumeBackupStream_JSONOlderServerNoStatus(t *testing.T) {
	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{eventResp(), eventResp()}}

	var out, errOut bytes.Buffer
	err := consumeBackupStream(stream, true /* outputJSON */, &out, &errOut)
	require.NoError(t, err)

	var got struct {
		Status     string          `json:"status"`
		BackupName string          `json:"backup_name"`
		Manifest   json.RawMessage `json:"manifest"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	assert.Equal(t, "UNKNOWN", got.Status)
	assert.Empty(t, got.BackupName)
	assert.Equal(t, "null", string(got.Manifest))
}

// TestConsumeBackupStream_JSONManifestNotJSON verifies an invalid MANIFEST is
// reported as null without turning an already-stored backup into a failure.
func TestConsumeBackupStream_JSONManifestNotJSON(t *testing.T) {
	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{
		eventResp(),
		{Manifest: "not json", BackupName: "2026-01-01.000000.zone1-0000000100", Status: tabletmanagerdatapb.BackupResponse_USABLE},
	}}

	var out, errOut bytes.Buffer
	err := consumeBackupStream(stream, true /* outputJSON */, &out, &errOut)
	require.NoError(t, err, "an unparseable manifest must not fail the command")

	var got struct {
		Status     string          `json:"status"`
		BackupName string          `json:"backup_name"`
		Manifest   json.RawMessage `json:"manifest"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &got), "stdout must still be valid JSON")
	assert.Equal(t, "USABLE", got.Status)
	assert.Equal(t, "2026-01-01.000000.zone1-0000000100", got.BackupName)
	assert.Equal(t, "null", string(got.Manifest))
	assert.Contains(t, errOut.String(), "not valid JSON")
}

// TestConsumeBackupStream_JSONManifestDropped verifies that the warning vtctld
// puts on the terminal message when it drops the MANIFEST goes to stderr, and
// that the outcome is still printed with a null manifest.
func TestConsumeBackupStream_JSONManifestDropped(t *testing.T) {
	warning := &logutilpb.Event{Level: logutilpb.Level_WARNING, Value: "its MANIFEST would put the message over vtctld's --grpc-max-message-size"}
	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{
		eventResp(),
		{Event: warning, BackupName: "2026-01-01.000000.zone1-0000000100", Status: tabletmanagerdatapb.BackupResponse_USABLE},
	}}

	var out, errOut bytes.Buffer
	require.NoError(t, consumeBackupStream(stream, true /* outputJSON */, &out, &errOut))
	assert.Contains(t, errOut.String(), "--grpc-max-message-size")
	assert.NotContains(t, out.String(), "--grpc-max-message-size")

	var got struct {
		Status     string          `json:"status"`
		BackupName string          `json:"backup_name"`
		Manifest   json.RawMessage `json:"manifest"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	assert.Equal(t, "USABLE", got.Status)
	assert.Equal(t, "2026-01-01.000000.zone1-0000000100", got.BackupName)
	assert.Equal(t, "null", string(got.Manifest))
}

// TestConsumeBackupStream_TextUsable verifies that text mode prints exactly the
// progress events, as it did before --json, even if a terminal message arrives.
func TestConsumeBackupStream_TextUsable(t *testing.T) {
	event := eventResp()
	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{
		event,
		{
			Manifest:   `{"BackupName":"2026-01-01.000000.zone1-0000000100"}`,
			BackupName: "2026-01-01.000000.zone1-0000000100",
			Status:     tabletmanagerdatapb.BackupResponse_USABLE,
		},
	}}

	var out, errOut bytes.Buffer
	require.NoError(t, consumeBackupStream(stream, false /* outputJSON */, &out, &errOut))
	assert.Equal(t, fmt.Sprintf("ks/- (zone1-0000000100): %v\n", event.Event), out.String())
	assert.Empty(t, errOut.String())
}

// TestConsumeBackupStream_JSONStoredThenError verifies that when the stream fails
// after reporting a stored backup, the outcome is printed and the error returned.
func TestConsumeBackupStream_JSONStoredThenError(t *testing.T) {
	stream := &fakeBackupStream{
		resps: []*vtctldatapb.BackupResponse{
			eventResp(),
			{BackupName: "2026-01-01.000000.zone1-0000000100", Status: tabletmanagerdatapb.BackupResponse_USABLE},
		},
		err: errors.New("backup completed, but tablet restoration failed"),
	}

	var out, errOut bytes.Buffer
	err := consumeBackupStream(stream, true /* outputJSON */, &out, &errOut)
	require.ErrorContains(t, err, "tablet restoration failed")

	var got struct {
		Status     string `json:"status"`
		BackupName string `json:"backup_name"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	assert.Equal(t, "USABLE", got.Status)
	assert.Equal(t, "2026-01-01.000000.zone1-0000000100", got.BackupName)
}

// TestConsumeBackupStream_TextStoredThenError verifies that text mode prints only
// the progress events, and returns the error, when the stream fails after a
// stored backup's outcome.
func TestConsumeBackupStream_TextStoredThenError(t *testing.T) {
	event := eventResp()
	stream := &fakeBackupStream{
		resps: []*vtctldatapb.BackupResponse{
			event,
			{BackupName: "2026-01-01.000000.zone1-0000000100", Status: tabletmanagerdatapb.BackupResponse_USABLE},
		},
		err: errors.New("backup completed, but tablet restoration failed"),
	}

	var out, errOut bytes.Buffer
	err := consumeBackupStream(stream, false /* outputJSON */, &out, &errOut)
	require.ErrorContains(t, err, "tablet restoration failed")
	assert.Equal(t, fmt.Sprintf("ks/- (zone1-0000000100): %v\n", event.Event), out.String())
	assert.Empty(t, errOut.String())
}

// TestConsumeBackupStream_Error verifies that a stream error before any outcome
// is returned, in both output modes, without printing a result.
func TestConsumeBackupStream_Error(t *testing.T) {
	for _, outputJSON := range []bool{false, true} {
		stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{eventResp()}, err: io.ErrClosedPipe}
		var out, errOut bytes.Buffer
		err := consumeBackupStream(stream, outputJSON, &out, &errOut)
		require.ErrorIs(t, err, io.ErrClosedPipe, "outputJSON=%v", outputJSON)
		assert.NotContains(t, out.String(), `"status"`, "outputJSON=%v", outputJSON)
	}
}

// failingWriter fails every write, like stdout on a full disk.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("no space left on device")
}

// TestConsumeBackupStream_WriteError verifies that a failed write of the JSON
// object is returned, also after a stored backup's stream error, so the command
// doesn't exit 0 without its result.
func TestConsumeBackupStream_WriteError(t *testing.T) {
	stored := &vtctldatapb.BackupResponse{BackupName: "2026-01-01.000000.zone1-0000000100", Status: tabletmanagerdatapb.BackupResponse_USABLE}

	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{stored}}
	err := consumeBackupStream(stream, true /* outputJSON */, failingWriter{}, io.Discard)
	require.ErrorContains(t, err, "no space left on device")

	stream = &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{stored}, err: errors.New("tablet restoration failed")}
	err = consumeBackupStream(stream, true /* outputJSON */, failingWriter{}, io.Discard)
	require.ErrorContains(t, err, "tablet restoration failed")
	require.ErrorContains(t, err, "no space left on device")
}
