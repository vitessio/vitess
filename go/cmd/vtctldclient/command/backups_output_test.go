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
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/mysqlctl"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// fakeBackupStream is an in-memory backupResponseStream for tests.
type fakeBackupStream struct {
	resps []*vtctldatapb.BackupResponse
	i     int
}

func (f *fakeBackupStream) Recv() (*vtctldatapb.BackupResponse, error) {
	if f.i >= len(f.resps) {
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

func TestConsumeBackupStream_JSONUsable(t *testing.T) {
	manifest := `{"BackupName":"2026-01-01.000000.zone1-0000000100","Incremental":false}`
	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{
		eventResp(),
		{Manifest: manifest, Status: tabletmanagerdatapb.BackupResponse_USABLE},
	}}

	var out, errOut bytes.Buffer
	status, err := consumeBackupStream(stream, true /* outputJSON */, &out, &errOut)
	require.NoError(t, err)
	assert.Equal(t, tabletmanagerdatapb.BackupResponse_USABLE, status)

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

func TestConsumeBackupStream_JSONEmpty(t *testing.T) {
	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{
		eventResp(),
		{Status: tabletmanagerdatapb.BackupResponse_EMPTY},
	}}

	var out, errOut bytes.Buffer
	status, err := consumeBackupStream(stream, true /* outputJSON */, &out, &errOut)
	require.NoError(t, err)
	assert.Equal(t, tabletmanagerdatapb.BackupResponse_EMPTY, status)

	var got struct {
		Status   string          `json:"status"`
		Manifest json.RawMessage `json:"manifest"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	assert.Equal(t, "EMPTY", got.Status)
	assert.Equal(t, "null", string(got.Manifest))
}

// A peer predating these fields sends log events only, leaving Status at
// STATUS_UNSPECIFIED. That must surface as "UNKNOWN", not EMPTY.
func TestConsumeBackupStream_JSONOlderServerNoStatus(t *testing.T) {
	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{eventResp(), eventResp()}}

	var out, errOut bytes.Buffer
	status, err := consumeBackupStream(stream, true /* outputJSON */, &out, &errOut)
	require.NoError(t, err)
	assert.Equal(t, tabletmanagerdatapb.BackupResponse_STATUS_UNSPECIFIED, status)

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

// The half callers feel: if an unknown outcome set the flag, Backup --json
// against an N-1 peer would exit 2 mid-rolling-upgrade and scripts would skip
// follow-up work.
func TestHandleBackupStream_OlderServerIsNotEmpty(t *testing.T) {
	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{eventResp(), eventResp()}}

	require.NoError(t, handleBackupStream(stream, true /* outputJSON */))
	require.False(t, EmptyBackup(), "STATUS_UNSPECIFIED must not be reported as an empty backup")
}

// A manifest that is not valid JSON -- truncated read, or an engine that does
// not write JSON -- must not turn an already-stored backup into a failed
// command. Report null and keep the status.
func TestConsumeBackupStream_JSONManifestNotJSON(t *testing.T) {
	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{
		eventResp(),
		{Manifest: "not json", BackupName: "2026-01-01.000000.zone1-0000000100", Status: tabletmanagerdatapb.BackupResponse_USABLE},
	}}

	var out, errOut bytes.Buffer
	status, err := consumeBackupStream(stream, true /* outputJSON */, &out, &errOut)
	require.NoError(t, err, "an unparseable manifest must not fail the command")
	assert.Equal(t, tabletmanagerdatapb.BackupResponse_USABLE, status)

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

func TestConsumeBackupStream_TextUsable(t *testing.T) {
	manifest := `{"BackupName":"2026-01-01.000000.zone1-0000000100"}`
	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{
		eventResp(),
		{Manifest: manifest, Status: tabletmanagerdatapb.BackupResponse_USABLE},
	}}

	var out, errOut bytes.Buffer
	status, err := consumeBackupStream(stream, false /* outputJSON */, &out, &errOut)
	require.NoError(t, err)
	assert.Equal(t, tabletmanagerdatapb.BackupResponse_USABLE, status)

	// In text mode, progress goes to stdout but the MANIFEST is NOT printed --
	// it is surfaced only via --json, so text output matches prior releases.
	assert.Contains(t, out.String(), "backing up")
	assert.NotContains(t, out.String(), manifest)
	assert.Empty(t, errOut.String())
}

func TestConsumeBackupStream_TextEmpty(t *testing.T) {
	// A real empty incremental backup reports "no new data" as a log event, then
	// sends a terminal EMPTY message. In text mode the terminal message must not
	// print a duplicate summary line: the outcome is already in the log stream.
	emptyEvent := &vtctldatapb.BackupResponse{
		TabletAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Keyspace:    "ks",
		Shard:       "-",
		Event:       &logutilpb.Event{Value: mysqlctl.EmptyBackupMessage},
	}
	stream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{
		emptyEvent,
		{Status: tabletmanagerdatapb.BackupResponse_EMPTY},
	}}

	var out, errOut bytes.Buffer
	status, err := consumeBackupStream(stream, false /* outputJSON */, &out, &errOut)
	require.NoError(t, err)
	assert.Equal(t, tabletmanagerdatapb.BackupResponse_EMPTY, status)

	// The empty message appears exactly once (from the log event), not duplicated
	// by a terminal summary line.
	assert.Equal(t, 1, strings.Count(out.String(), mysqlctl.EmptyBackupMessage))
	assert.Empty(t, errOut.String())
}

// An empty backup in --json mode is a success reported via EmptyBackup, never an
// error -- an error would make cobra skip PersistentPostRunE. Without --json it
// stays a plain success and does not set the flag.
func TestHandleBackupStream_EmptyReporting(t *testing.T) {
	emptyStream := func() *fakeBackupStream {
		return &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{
			{Status: tabletmanagerdatapb.BackupResponse_EMPTY},
		}}
	}

	err := handleBackupStream(emptyStream(), true /* outputJSON */)
	require.NoError(t, err, "an empty backup is a success, not an error")
	require.True(t, EmptyBackup(), "--json empty backup must be reported via EmptyBackup")

	err = handleBackupStream(emptyStream(), false /* outputJSON */)
	require.NoError(t, err, "without --json an empty backup stays a plain success")
	require.False(t, EmptyBackup(), "without --json an empty backup must not set the flag")

	// A subsequent usable backup must clear the flag rather than inherit it.
	usableStream := &fakeBackupStream{resps: []*vtctldatapb.BackupResponse{
		{Status: tabletmanagerdatapb.BackupResponse_USABLE, Manifest: "{}"},
	}}
	require.NoError(t, handleBackupStream(usableStream, true /* outputJSON */))
	require.False(t, EmptyBackup(), "a usable backup must reset the flag")
}

func TestConsumeBackupStream_Error(t *testing.T) {
	// A terminal error from the stream is returned to the caller.
	stream := &erroringBackupStream{}
	var out, errOut bytes.Buffer
	_, err := consumeBackupStream(stream, false, &out, &errOut)
	require.Error(t, err)
}

type erroringBackupStream struct{}

func (e *erroringBackupStream) Recv() (*vtctldatapb.BackupResponse, error) {
	return nil, errStreamClosed
}

// errStreamClosed is the error erroringBackupStream reports from Recv, standing
// in for any transport-level failure mid-stream.
var errStreamClosed = io.ErrClosedPipe
