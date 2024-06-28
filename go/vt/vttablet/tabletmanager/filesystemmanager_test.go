package tabletmanager

import (
	"context"
	"errors"
	"testing"
	"time"
)

type mockWriter struct {
	ctx             context.Context
	totalCalls      int
	blockAfterCalls int
}

func (mfc *mockWriter) mockWriteFunction() error {
	mfc.totalCalls += 1
	if mfc.blockAfterCalls > 0 && mfc.totalCalls >= mfc.blockAfterCalls {
		<-mfc.ctx.Done()
		return errors.New("context cancelled")
	}
	return nil
}

func TestFileSystemManager_IsDiskStalled_noStall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockFileCreator := &mockWriter{ctx: ctx}
	filesystemManager := newPollingFileSystemManager(context.Background(), mockFileCreator.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)
	go filesystemManager.poll(ctx)

	time.Sleep(300 * time.Millisecond)
	if mockFileCreator.totalCalls < 2 {
		t.Fatalf("expected at least 2 calls to createFile, got %d", mockFileCreator.totalCalls)
	}
	if isStalled := filesystemManager.IsDiskStalled(); isStalled {
		t.Fatalf("expected IsDiskStalled to be false")
	}
	cancel()
}

func TestFileSystemManager_IsDiskStalled_stall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockFileCreator := &mockWriter{ctx: ctx, blockAfterCalls: 2}
	filesystemManager := newPollingFileSystemManager(context.Background(), mockFileCreator.mockWriteFunction, 50*time.Millisecond, 25*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	if mockFileCreator.totalCalls < 2 {
		t.Fatalf("expected at least 2 calls to createFile, got %d", mockFileCreator.totalCalls)
	}
	if isStalled := filesystemManager.IsDiskStalled(); !isStalled {
		t.Fatalf("expected IsDiskStalled to be true")
	}
}
