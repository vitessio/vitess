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
	"sync"
	"testing"
	"time"
)

func TestDiskHealthMonitor_noStall(t *testing.T) {
	ctx := t.Context()
	mockFileWriter := &sequencedMockWriter{}
	diskHealthMonitor := newPollingDiskHealthMonitor(ctx, mockFileWriter.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	if totalCreateCalls := mockFileWriter.getTotalCreateCalls(); totalCreateCalls != 5 {
		t.Fatalf("expected 5 calls to createFile, got %d", totalCreateCalls)
	}
	if isStalled := diskHealthMonitor.IsDiskStalled(); isStalled {
		t.Fatalf("expected isStalled to be false")
	}
}

func TestDiskHealthMonitor_stallAndRecover(t *testing.T) {
	ctx := t.Context()
	mockFileWriter := &sequencedMockWriter{sequencedWriteFunctions: []writeFunction{delayedWriteFunction(10*time.Millisecond, nil), delayedWriteFunction(300*time.Millisecond, nil)}}
	diskHealthMonitor := newPollingDiskHealthMonitor(ctx, mockFileWriter.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	if totalCreateCalls := mockFileWriter.getTotalCreateCalls(); totalCreateCalls != 2 {
		t.Fatalf("expected 2 calls to createFile, got %d", totalCreateCalls)
	}
	if isStalled := diskHealthMonitor.IsDiskStalled(); !isStalled {
		t.Fatalf("expected isStalled to be true")
	}

	time.Sleep(300 * time.Millisecond)
	if totalCreateCalls := mockFileWriter.getTotalCreateCalls(); totalCreateCalls < 5 {
		t.Fatalf("expected at least 5 calls to createFile, got %d", totalCreateCalls)
	}
	if isStalled := diskHealthMonitor.IsDiskStalled(); isStalled {
		t.Fatalf("expected isStalled to be false")
	}
}

func TestDiskHealthMonitor_stallDetected(t *testing.T) {
	ctx := t.Context()
	mockFileWriter := &sequencedMockWriter{defaultWriteFunction: delayedWriteFunction(10*time.Millisecond, errors.New("test error"))}
	diskHealthMonitor := newPollingDiskHealthMonitor(ctx, mockFileWriter.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	if totalCreateCalls := mockFileWriter.getTotalCreateCalls(); totalCreateCalls != 5 {
		t.Fatalf("expected 5 calls to createFile, got %d", totalCreateCalls)
	}
	if isStalled := diskHealthMonitor.IsDiskStalled(); !isStalled {
		t.Fatalf("expected isStalled to be true")
	}
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
