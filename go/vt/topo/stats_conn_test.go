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
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// testStatsConnReadSem is a semaphore for unit tests.
// It intentionally has a concurrency limit of '1' to
// allow semaphore contention in tests.
var testStatsConnReadSem = semaphore.NewWeighted(1)

// testStatsConnStatsReset resets StatsConn-based stats.
func testStatsConnStatsReset() {
	topoStatsConnErrors.ResetAll()
	topoStatsConnReadWaitTimings.Reset()
	topoStatsConnTimings.Reset()
}

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

// GetVersion is part of the Conn interface
func (st *fakeConn) GetVersion(ctx context.Context, filePath string, version int64) (bytes []byte, err error) {
	if filePath == "error" {
		return bytes, fmt.Errorf("Dummy error")

	}
	return bytes, err
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

// LockWithTTL is part of the Conn interface.
func (st *fakeConn) LockWithTTL(ctx context.Context, dirPath, contents string, _ time.Duration) (lock LockDescriptor, err error) {
	if st.readOnly {
		return nil, vterrors.Errorf(vtrpc.Code_READ_ONLY, "topo server connection is read-only")
	}
	if dirPath == "error" {
		return lock, fmt.Errorf("dummy error")
	}
	return lock, err
}

// LockName is part of the Conn interface.
func (st *fakeConn) LockName(ctx context.Context, dirPath, contents string) (lock LockDescriptor, err error) {
	if st.readOnly {
		return nil, vterrors.Errorf(vtrpc.Code_READ_ONLY, "topo server connection is read-only")
	}
	if dirPath == "error" {
		return lock, fmt.Errorf("dummy error")
	}
	return lock, err
}

// TryLock is part of the topo.Conn interface.
// As of today it provides same functionality as Lock
func (st *fakeConn) TryLock(ctx context.Context, dirPath, contents string) (lock LockDescriptor, err error) {
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

// createTestReadSemaphoreContention simulates semaphore contention on the test read semaphore.
func createTestReadSemaphoreContention(ctx context.Context, duration time.Duration, semAcquiredChan chan struct{}) {
	if err := testStatsConnReadSem.Acquire(ctx, 1); err != nil {
		panic(err)
	}
	defer testStatsConnReadSem.Release(1)
	semAcquiredChan <- struct{}{}
	time.Sleep(duration)
}

// TestStatsConnTopoListDir emits stats on ListDir
func TestStatsConnTopoListDir(t *testing.T) {
	testStatsConnStatsReset()
	defer testStatsConnStatsReset()

	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn, testStatsConnReadSem)
	ctx := context.Background()

	semAcquiredChan := make(chan struct{})
	go createTestReadSemaphoreContention(ctx, 100*time.Millisecond, semAcquiredChan)
	<-semAcquiredChan
	statsConn.ListDir(ctx, "", true)
	require.Equal(t, int64(1), topoStatsConnTimings.Counts()["ListDir.global"])
	require.NotZero(t, topoStatsConnTimings.Time())

	require.Equal(t, int64(1), topoStatsConnReadWaitTimings.Counts()["ListDir.global"])
	require.NotZero(t, topoStatsConnReadWaitTimings.Time())

	// error is zero before getting an error
	require.Zero(t, topoStatsConnErrors.Counts()["ListDir.global"])

	statsConn.ListDir(ctx, "error", true)

	// error stats gets emitted
	require.Equal(t, int64(1), topoStatsConnErrors.Counts()["ListDir.global"])
}

// TestStatsConnTopoCreate emits stats on Create
func TestStatsConnTopoCreate(t *testing.T) {
	testStatsConnStatsReset()
	defer testStatsConnStatsReset()

	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn, testStatsConnReadSem)
	ctx := context.Background()

	statsConn.Create(ctx, "", []byte{})
	require.Equal(t, int64(1), topoStatsConnTimings.Counts()["Create.global"])
	require.NotZero(t, topoStatsConnTimings.Time())
	require.Zero(t, topoStatsConnReadWaitTimings.Time())

	// error is zero before getting an error
	require.Zero(t, topoStatsConnErrors.Counts()["Create.global"])

	statsConn.Create(ctx, "error", []byte{})

	// error stats gets emitted
	require.Equal(t, int64(1), topoStatsConnErrors.Counts()["Create.global"])
}

// TestStatsConnTopoUpdate emits stats on Update
func TestStatsConnTopoUpdate(t *testing.T) {
	testStatsConnStatsReset()
	defer testStatsConnStatsReset()

	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn, testStatsConnReadSem)
	ctx := context.Background()

	statsConn.Update(ctx, "", []byte{}, conn.v)
	require.Equal(t, int64(1), topoStatsConnTimings.Counts()["Update.global"])
	require.NotZero(t, topoStatsConnTimings.Time())
	require.Zero(t, topoStatsConnReadWaitTimings.Time())

	// error is zero before getting an error
	require.Zero(t, topoStatsConnErrors.Counts()["Update.global"])

	statsConn.Update(ctx, "error", []byte{}, conn.v)

	// error stats gets emitted
	require.Equal(t, int64(1), topoStatsConnErrors.Counts()["Update.global"])
}

// TestStatsConnTopoGet emits stats on Get
func TestStatsConnTopoGet(t *testing.T) {
	testStatsConnStatsReset()
	defer testStatsConnStatsReset()

	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn, testStatsConnReadSem)
	ctx := context.Background()

	semAcquiredChan := make(chan struct{})
	go createTestReadSemaphoreContention(ctx, time.Millisecond*100, semAcquiredChan)
	<-semAcquiredChan
	statsConn.Get(ctx, "")
	require.Equal(t, int64(1), topoStatsConnTimings.Counts()["Get.global"])
	require.NotZero(t, topoStatsConnTimings.Time())

	require.Equal(t, int64(1), topoStatsConnReadWaitTimings.Counts()["Get.global"])
	require.NotZero(t, topoStatsConnReadWaitTimings.Time())

	// error is zero before getting an error
	require.Zero(t, topoStatsConnErrors.Counts()["Get.global"])

	statsConn.Get(ctx, "error")

	// error stats gets emitted
	require.Equal(t, int64(1), topoStatsConnErrors.Counts()["Get.global"])
}

// TestStatsConnTopoDelete emits stats on Delete
func TestStatsConnTopoDelete(t *testing.T) {
	testStatsConnStatsReset()
	defer testStatsConnStatsReset()

	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn, testStatsConnReadSem)
	ctx := context.Background()

	statsConn.Delete(ctx, "", conn.v)
	require.Equal(t, int64(1), topoStatsConnTimings.Counts()["Delete.global"])
	require.NotZero(t, topoStatsConnTimings.Time())
	require.Zero(t, topoStatsConnReadWaitTimings.Time())

	// error is zero before getting an error
	require.Zero(t, topoStatsConnErrors.Counts()["Delete.global"])

	statsConn.Delete(ctx, "error", conn.v)

	// error stats gets emitted
	require.Equal(t, int64(1), topoStatsConnErrors.Counts()["Delete.global"])
}

// TestStatsConnTopoLock emits stats on Lock
func TestStatsConnTopoLock(t *testing.T) {
	testStatsConnStatsReset()
	defer testStatsConnStatsReset()

	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn, testStatsConnReadSem)
	ctx := context.Background()

	statsConn.Lock(ctx, "", "")
	require.Equal(t, int64(1), topoStatsConnTimings.Counts()["Lock.global"])
	require.NotZero(t, topoStatsConnTimings.Time())
	require.Zero(t, topoStatsConnReadWaitTimings.Time())

	statsConn.LockWithTTL(ctx, "", "", time.Second)
	require.Equal(t, int64(1), topoStatsConnTimings.Counts()["LockWithTTL.global"])

	statsConn.LockName(ctx, "", "")
	require.Equal(t, int64(1), topoStatsConnTimings.Counts()["LockName.global"])

	// Error is zero before getting an error.
	require.Zero(t, topoStatsConnErrors.Counts()["Lock.global"])

	statsConn.Lock(ctx, "error", "")

	// Error stats gets emitted.
	require.Equal(t, int64(1), topoStatsConnErrors.Counts()["Lock.global"])
}

// TestStatsConnTopoWatch emits stats on Watch
func TestStatsConnTopoWatch(t *testing.T) {
	testStatsConnStatsReset()
	defer testStatsConnStatsReset()

	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn, testStatsConnReadSem)
	ctx := context.Background()

	statsConn.Watch(ctx, "")
	require.Equal(t, int64(1), topoStatsConnTimings.Counts()["Watch.global"])
	require.NotZero(t, topoStatsConnTimings.Time())
	require.Zero(t, topoStatsConnReadWaitTimings.Time())
}

// TestStatsConnTopoNewLeaderParticipation emits stats on NewLeaderParticipation
func TestStatsConnTopoNewLeaderParticipation(t *testing.T) {
	testStatsConnStatsReset()
	defer testStatsConnStatsReset()

	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn, testStatsConnReadSem)

	_, _ = statsConn.NewLeaderParticipation("", "")
	require.Equal(t, int64(1), topoStatsConnTimings.Counts()["NewLeaderParticipation.global"])
	require.NotZero(t, topoStatsConnTimings.Time())
	require.Zero(t, topoStatsConnReadWaitTimings.Time())

	// error is zero before getting an error
	require.Zero(t, topoStatsConnErrors.Counts()["NewLeaderParticipation.global"])

	_, _ = statsConn.NewLeaderParticipation("error", "")

	// error stats gets emitted
	require.Equal(t, int64(1), topoStatsConnErrors.Counts()["NewLeaderParticipation.global"])
}

// TestStatsConnTopoClose emits stats on Close
func TestStatsConnTopoClose(t *testing.T) {
	testStatsConnStatsReset()
	defer testStatsConnStatsReset()

	conn := &fakeConn{}
	statsConn := NewStatsConn("global", conn, testStatsConnReadSem)

	statsConn.Close()
	require.Equal(t, int64(1), topoStatsConnTimings.Counts()["Close.global"])
	require.NotZero(t, topoStatsConnTimings.Time())
	require.Zero(t, topoStatsConnReadWaitTimings.Time())
}
