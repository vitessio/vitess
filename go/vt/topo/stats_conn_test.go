/*
Copyright 2019 The Vitess Authors.

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

package topo

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// The fakeConn is a wrapper for a Conn that emits stats for every operation
type fakeConn struct {
	v        Version
	readOnly bool
}

// ListDir is part of the Conn interface
func (st *fakeConn) ListDir(ctx context.Context, dirPath string, full bool) (res []DirEntry, err error) {
	if dirPath == "error" {
		return res, fmt.Errorf("Dummy error")

	}
	return res, err
}

// Create is part of the Conn interface
func (st *fakeConn) Create(ctx context.Context, filePath string, contents []byte) (ver Version, err error) {
	if st.readOnly {
		return nil, vterrors.Errorf(vtrpc.Code_READ_ONLY, "topo server connection is read-only")
	}
	if filePath == "error" {
		return ver, fmt.Errorf("Dummy error")

	}
	return ver, err
}

// Update is part of the Conn interface
func (st *fakeConn) Update(ctx context.Context, filePath string, contents []byte, version Version) (ver Version, err error) {
	if st.readOnly {
		return nil, vterrors.Errorf(vtrpc.Code_READ_ONLY, "topo server connection is read-only")
	}
	if filePath == "error" {
		return ver, fmt.Errorf("Dummy error")

	}
	return ver, err
}

// Get is part of the Conn interface
func (st *fakeConn) Get(ctx context.Context, filePath string) (bytes []byte, ver Version, err error) {
	if filePath == "error" {
		return bytes, ver, fmt.Errorf("Dummy error")

	}
	return bytes, ver, err
}

// List is part of the Conn interface
func (st *fakeConn) List(ctx context.Context, filePathPrefix string) (bytes []KVInfo, err error) {
	if filePathPrefix == "error" {
		return bytes, fmt.Errorf("Dummy error")
	}
	return bytes, err
}

// Delete is part of the Conn interface
func (st *fakeConn) Delete(ctx context.Context, filePath string, version Version) (err error) {
	if st.readOnly {
		return vterrors.Errorf(vtrpc.Code_READ_ONLY, "topo server connection is read-only")
	}
	if filePath == "error" {
		return fmt.Errorf("dummy error")
	}
	return err
}

// Lock is part of the Conn interface
func (st *fakeConn) Lock(ctx context.Context, dirPath, contents string) (lock LockDescriptor, err error) {
	if st.readOnly {
		return nil, vterrors.Errorf(vtrpc.Code_READ_ONLY, "topo server connection is read-only")
	}
	if dirPath == "error" {
		return lock, fmt.Errorf("dummy error")

	}
	return lock, err
}

// Watch is part of the Conn interface
func (st *fakeConn) Watch(ctx context.Context, filePath string) (current *WatchData, changes <-chan *WatchData, err error) {
	return current, changes, err
}

// WatchRecursive is part of the Conn interface
func (st *fakeConn) WatchRecursive(ctx context.Context, path string) (current []*WatchDataRecursive, changes <-chan *WatchDataRecursive, err error) {
	return current, changes, err
}

// NewLeaderParticipation is part of the Conn interface
func (st *fakeConn) NewLeaderParticipation(name, id string) (mp LeaderParticipation, err error) {
	if name == "error" {
		return mp, fmt.Errorf("dummy error")

	}
	return mp, err
}

// Close is part of the Conn interface
func (st *fakeConn) Close() {
}

// SetReadOnly with true prevents any write operations from being made on the topo connection
func (st *fakeConn) SetReadOnly(readOnly bool) {
	st.readOnly = readOnly
}

// IsReadOnly allows you to check the access type for the topo connection
func (st *fakeConn) IsReadOnly() bool {
	return st.readOnly
}

// TestStatsConnTopoListDir emits stats on ListDir
func TestStatsConnTopoListDir(t *testing.T) {
	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn)
	ctx := context.Background()

	statsConn.ListDir(ctx, "", true)
	timingCounts := topoStatsConnTimings.Counts()["ListDir.global"]
	if got, want := timingCounts, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	// error is zero before getting an error
	errorCount := topoStatsConnErrors.Counts()["ListDir.global"]
	if got, want := errorCount, int64(0); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	statsConn.ListDir(ctx, "error", true)

	// error stats gets emitted
	errorCount = topoStatsConnErrors.Counts()["ListDir.global"]
	if got, want := errorCount, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}
}

// TestStatsConnTopoCreate emits stats on Create
func TestStatsConnTopoCreate(t *testing.T) {
	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn)
	ctx := context.Background()

	statsConn.Create(ctx, "", []byte{})
	timingCounts := topoStatsConnTimings.Counts()["Create.global"]
	if got, want := timingCounts, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	// error is zero before getting an error
	errorCount := topoStatsConnErrors.Counts()["Create.global"]
	if got, want := errorCount, int64(0); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	statsConn.Create(ctx, "error", []byte{})

	// error stats gets emitted
	errorCount = topoStatsConnErrors.Counts()["Create.global"]
	if got, want := errorCount, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}
}

// TestStatsConnTopoUpdate emits stats on Update
func TestStatsConnTopoUpdate(t *testing.T) {
	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn)
	ctx := context.Background()

	statsConn.Update(ctx, "", []byte{}, conn.v)
	timingCounts := topoStatsConnTimings.Counts()["Update.global"]
	if got, want := timingCounts, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	// error is zero before getting an error
	errorCount := topoStatsConnErrors.Counts()["Update.global"]
	if got, want := errorCount, int64(0); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	statsConn.Update(ctx, "error", []byte{}, conn.v)

	// error stats gets emitted
	errorCount = topoStatsConnErrors.Counts()["Update.global"]
	if got, want := errorCount, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}
}

// TestStatsConnTopoGet emits stats on Get
func TestStatsConnTopoGet(t *testing.T) {
	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn)
	ctx := context.Background()

	statsConn.Get(ctx, "")
	timingCounts := topoStatsConnTimings.Counts()["Get.global"]
	if got, want := timingCounts, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	// error is zero before getting an error
	errorCount := topoStatsConnErrors.Counts()["Get.global"]
	if got, want := errorCount, int64(0); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	statsConn.Get(ctx, "error")

	// error stats gets emitted
	errorCount = topoStatsConnErrors.Counts()["Get.global"]
	if got, want := errorCount, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}
}

// TestStatsConnTopoDelete emits stats on Delete
func TestStatsConnTopoDelete(t *testing.T) {
	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn)
	ctx := context.Background()

	statsConn.Delete(ctx, "", conn.v)
	timingCounts := topoStatsConnTimings.Counts()["Delete.global"]
	if got, want := timingCounts, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	// error is zero before getting an error
	errorCount := topoStatsConnErrors.Counts()["Delete.global"]
	if got, want := errorCount, int64(0); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	statsConn.Delete(ctx, "error", conn.v)

	// error stats gets emitted
	errorCount = topoStatsConnErrors.Counts()["Delete.global"]
	if got, want := errorCount, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}
}

// TestStatsConnTopoLock emits stats on Lock
func TestStatsConnTopoLock(t *testing.T) {
	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn)
	ctx := context.Background()

	statsConn.Lock(ctx, "", "")
	timingCounts := topoStatsConnTimings.Counts()["Lock.global"]
	if got, want := timingCounts, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	// error is zero before getting an error
	errorCount := topoStatsConnErrors.Counts()["Lock.global"]
	if got, want := errorCount, int64(0); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	statsConn.Lock(ctx, "error", "")

	// error stats gets emitted
	errorCount = topoStatsConnErrors.Counts()["Lock.global"]
	if got, want := errorCount, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}
}

// TestStatsConnTopoWatch emits stats on Watch
func TestStatsConnTopoWatch(t *testing.T) {
	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn)
	ctx := context.Background()

	statsConn.Watch(ctx, "")
	timingCounts := topoStatsConnTimings.Counts()["Watch.global"]
	if got, want := timingCounts, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

}

// TestStatsConnTopoNewLeaderParticipation emits stats on NewLeaderParticipation
func TestStatsConnTopoNewLeaderParticipation(t *testing.T) {
	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn)

	_, _ = statsConn.NewLeaderParticipation("", "")
	timingCounts := topoStatsConnTimings.Counts()["NewLeaderParticipation.global"]
	if got, want := timingCounts, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	// error is zero before getting an error
	errorCount := topoStatsConnErrors.Counts()["NewLeaderParticipation.global"]
	if got, want := errorCount, int64(0); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}

	_, _ = statsConn.NewLeaderParticipation("error", "")

	// error stats gets emitted
	errorCount = topoStatsConnErrors.Counts()["NewLeaderParticipation.global"]
	if got, want := errorCount, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}
}

// TestStatsConnTopoClose emits stats on Close
func TestStatsConnTopoClose(t *testing.T) {
	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn)

	statsConn.Close()
	timingCounts := topoStatsConnTimings.Counts()["Close.global"]
	if got, want := timingCounts, int64(1); got != want {
		t.Errorf("stats were not properly recorded: got = %d, want = %d", got, want)
	}
}
