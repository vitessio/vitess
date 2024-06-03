package tabletmanager

import (
	"context"
	"errors"
	"testing"
	"time"
)

type mockWriter struct {
	ctx                   context.Context
	totalCreateCalls      int
	blockAfterCreateCalls int
}

func (mfc *mockWriter) mockWriteFunction() error {
	mfc.totalCreateCalls += 1
	if mfc.blockAfterCreateCalls > 0 && mfc.totalCreateCalls >= mfc.blockAfterCreateCalls {
		for {
			select {
			case <-mfc.ctx.Done():
				return errors.New("context cancelled")
			}
		}
	}
	return nil
}

func TestFileSystemManager_noStall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockFileCreator := &mockWriter{ctx: ctx}
	filesystemManager := newPollingFileSystemManager(context.Background(), mockFileCreator.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)
	go filesystemManager.poll(ctx)

	time.Sleep(300 * time.Millisecond)
	if mockFileCreator.totalCreateCalls < 2 {
		t.Fatalf("expected at least 2 calls to createFile, got %d", mockFileCreator.totalCreateCalls)
	}
	if isStalled := filesystemManager.IsDiskStalled(); isStalled {
		t.Fatalf("expected IsDiskStalled to be false")
	}
	cancel()
}

func TestFileSystemManager_stall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockFileCreator := &mockWriter{ctx: ctx, blockAfterCreateCalls: 2}
	filesystemManager := newPollingFileSystemManager(context.Background(), mockFileCreator.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	if mockFileCreator.totalCreateCalls < 2 {
		t.Fatalf("expected at least 2 calls to createFile, got %d", mockFileCreator.totalCreateCalls)
	}
	if isStalled := filesystemManager.IsDiskStalled(); !isStalled {
		t.Fatalf("expected IsDiskStalled to be true")
	}
}
