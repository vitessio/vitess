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

package tabletserver

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDiskHealthMonitor_noStall(t *testing.T) {
	ctx := t.Context()
	mockFileWriter := &sequencedMockWriter{}
	diskHealthMonitor := newPollingDiskHealthMonitor(ctx, mockFileWriter.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)

	require.Eventually(t, func() bool {
		return mockFileWriter.getTotalCreateCalls() >= 5
	}, 500*time.Millisecond, 10*time.Millisecond)
	require.False(t, diskHealthMonitor.IsDiskStalled(), "expected isStalled to be false")
	require.False(t, diskHealthMonitor.IsDiskFull(), "expected isFull to be false")
}

func TestAttemptFileWriteUsesFreshProbeFile(t *testing.T) {
	oldStalledDiskWriteDir := stalledDiskWriteDir
	stalledDiskWriteDir = t.TempDir()
	t.Cleanup(func() {
		stalledDiskWriteDir = oldStalledDiskWriteDir
	})

	reusableProbePath := filepath.Join(stalledDiskWriteDir, ".stalled_disk_check")
	require.NoError(t, os.WriteFile(reusableProbePath, []byte("preserve"), 0o600))

	require.NoError(t, attemptFileWrite())

	probeContents, err := os.ReadFile(reusableProbePath)
	require.NoError(t, err)
	require.Equal(t, "preserve", string(probeContents))
	entries, err := os.ReadDir(stalledDiskWriteDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestDiskHealthMonitor_stallAndRecover(t *testing.T) {
	ctx := t.Context()
	mockFileWriter := &sequencedMockWriter{sequencedWriteFunctions: []writeFunction{delayedWriteFunction(10*time.Millisecond, nil), delayedWriteFunction(300*time.Millisecond, nil)}}
	diskHealthMonitor := newPollingDiskHealthMonitor(ctx, mockFileWriter.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	totalCreateCalls := mockFileWriter.getTotalCreateCalls()
	require.Equalf(t, 2, totalCreateCalls, "expected 2 calls to createFile, got %d", totalCreateCalls)
	require.True(t, diskHealthMonitor.IsDiskStalled(), "expected isStalled to be true")
	require.False(t, diskHealthMonitor.IsDiskFull(), "expected isFull to be false")

	time.Sleep(300 * time.Millisecond)
	totalCreateCalls = mockFileWriter.getTotalCreateCalls()
	require.GreaterOrEqualf(t, totalCreateCalls, 5, "expected at least 5 calls to createFile, got %d", totalCreateCalls)
	require.False(t, diskHealthMonitor.IsDiskStalled(), "expected isStalled to be false")
	require.False(t, diskHealthMonitor.IsDiskFull(), "expected isFull to be false")
}

func TestDiskHealthMonitor_stallDetected(t *testing.T) {
	ctx := t.Context()
	mockFileWriter := &sequencedMockWriter{defaultWriteFunction: delayedWriteFunction(10*time.Millisecond, errors.New("test error"))}
	diskHealthMonitor := newPollingDiskHealthMonitor(ctx, mockFileWriter.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	totalCreateCalls := mockFileWriter.getTotalCreateCalls()
	require.Equalf(t, 5, totalCreateCalls, "expected 5 calls to createFile, got %d", totalCreateCalls)
	require.True(t, diskHealthMonitor.IsDiskStalled(), "expected isStalled to be true")
	require.False(t, diskHealthMonitor.IsDiskFull(), "expected isFull to be false")
}

// TestDiskHealthMonitor_diskFullAndRecover exercises both ENOSPC (filesystem
// out of space) and EDQUOT (quota exceeded on xfs/ext4/NFS) as disk-full
// signals — without EDQUOT handling, quota-bound tablets would be misrouted
// through the stalled-disk recovery flag.
func TestDiskHealthMonitor_diskFullAndRecover(t *testing.T) {
	ctx := t.Context()
	mockFileWriter := &sequencedMockWriter{
		sequencedWriteFunctions: []writeFunction{
			delayedWriteFunction(10*time.Millisecond, syscall.ENOSPC),
			delayedWriteFunction(10*time.Millisecond, nil),
			delayedWriteFunction(10*time.Millisecond, syscall.EDQUOT),
			delayedWriteFunction(10*time.Millisecond, nil),
		},
	}
	diskHealthMonitor := newPollingDiskHealthMonitor(ctx, mockFileWriter.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)

	require.Eventually(t, func() bool {
		return diskHealthMonitor.IsDiskFull() && !diskHealthMonitor.IsDiskStalled()
	}, 300*time.Millisecond, 10*time.Millisecond, "expected IsDiskFull=true after ENOSPC")

	require.Eventually(t, func() bool {
		return !diskHealthMonitor.IsDiskFull() && !diskHealthMonitor.IsDiskStalled()
	}, 300*time.Millisecond, 10*time.Millisecond, "expected IsDiskFull=false after ENOSPC recovery")

	require.Eventually(t, func() bool {
		return diskHealthMonitor.IsDiskFull() && !diskHealthMonitor.IsDiskStalled()
	}, 300*time.Millisecond, 10*time.Millisecond, "expected IsDiskFull=true after EDQUOT")

	require.Eventually(t, func() bool {
		return !diskHealthMonitor.IsDiskFull() && !diskHealthMonitor.IsDiskStalled()
	}, 300*time.Millisecond, 10*time.Millisecond, "expected IsDiskFull=false after EDQUOT recovery")
}

type sequencedMockWriter struct {
	defaultWriteFunction    writeFunction
	sequencedWriteFunctions []writeFunction

	totalCreateCalls      int
	totalCreateCallsMutex sync.RWMutex
}

func (smw *sequencedMockWriter) mockWriteFunction() error {
	functionIndex := smw.getTotalCreateCalls()
	smw.incrementTotalCreateCalls()

	if functionIndex >= len(smw.sequencedWriteFunctions) {
		if smw.defaultWriteFunction != nil {
			return smw.defaultWriteFunction()
		}
		return delayedWriteFunction(10*time.Millisecond, nil)()
	}

	return smw.sequencedWriteFunctions[functionIndex]()
}

func (smw *sequencedMockWriter) incrementTotalCreateCalls() {
	smw.totalCreateCallsMutex.Lock()
	defer smw.totalCreateCallsMutex.Unlock()
	smw.totalCreateCalls += 1
}

func (smw *sequencedMockWriter) getTotalCreateCalls() int {
	smw.totalCreateCallsMutex.RLock()
	defer smw.totalCreateCallsMutex.RUnlock()
	return smw.totalCreateCalls
}

func delayedWriteFunction(delay time.Duration, err error) writeFunction {
	return func() error {
		time.Sleep(delay)
		return err
	}
}
