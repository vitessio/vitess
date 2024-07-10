package tabletmanager

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFileSystemManager_noStall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockFileWriter := &sequencedMockWriter{}
	filesystemManager := newPollingFileSystemManager(ctx, mockFileWriter.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	if mockFileWriter.totalCreateCalls != 5 {
		t.Fatalf("expected 5 calls to createFile, got %d", mockFileWriter.totalCreateCalls)
	}
	if isStalled := filesystemManager.IsDiskStalled(); isStalled {
		t.Fatalf("expected isStalled to be false")
	}
}

func TestFileSystemManager_stallAndRecover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockFileWriter := &sequencedMockWriter{sequencedWriteFunctions: []writeFunction{delayedWriteFunction(10*time.Millisecond, nil), delayedWriteFunction(300*time.Millisecond, nil)}}
	filesystemManager := newPollingFileSystemManager(ctx, mockFileWriter.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	if mockFileWriter.totalCreateCalls != 2 {
		t.Fatalf("expected 2 calls to createFile, got %d", mockFileWriter.totalCreateCalls)
	}
	if isStalled := filesystemManager.IsDiskStalled(); !isStalled {
		t.Fatalf("expected isStalled to be true")
	}

	time.Sleep(300 * time.Millisecond)
	if mockFileWriter.totalCreateCalls < 5 {
		t.Fatalf("expected at least 5 calls to createFile, got %d", mockFileWriter.totalCreateCalls)
	}
	if isStalled := filesystemManager.IsDiskStalled(); isStalled {
		t.Fatalf("expected isStalled to be false")
	}
}

func TestFileSystemManager_errorIsStall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockFileWriter := &sequencedMockWriter{defaultWriteFunction: delayedWriteFunction(10*time.Millisecond, errors.New("test error"))}
	filesystemManager := newPollingFileSystemManager(ctx, mockFileWriter.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	if mockFileWriter.totalCreateCalls != 5 {
		t.Fatalf("expected 5 calls to createFile, got %d", mockFileWriter.totalCreateCalls)
	}
	if isStalled := filesystemManager.IsDiskStalled(); !isStalled {
		t.Fatalf("expected isStalled to be true")
	}
}

type sequencedMockWriter struct {
	defaultWriteFunction    writeFunction
	sequencedWriteFunctions []writeFunction

	totalCreateCalls int
}

func (smw *sequencedMockWriter) mockWriteFunction() error {
	functionIndex := smw.totalCreateCalls
	smw.totalCreateCalls += 1

	if functionIndex >= len(smw.sequencedWriteFunctions) {
		if smw.defaultWriteFunction != nil {
			return smw.defaultWriteFunction()
		}
		return delayedWriteFunction(10*time.Millisecond, nil)()
	}

	return smw.sequencedWriteFunctions[functionIndex]()
}

func delayedWriteFunction(delay time.Duration, err error) writeFunction {
	return func() error {
		time.Sleep(delay)
		return err
	}
}
