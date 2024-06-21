package tabletmanager

import (
	"context"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/env"
)

type FileSystemManager interface {
	// IsDiskStalled returns true if the disk is stalled or rejecting writes.
	IsDiskStalled() bool
}

func newFileSystemManager(ctx context.Context) FileSystemManager {
	if !enableStalledDiskCheck {
		return newNoopFilesystemManager()
	}

	return newPollingFileSystemManager(ctx, attemptFileWrite, stalledDiskWriteInterval, stalledDiskWriteTimeout)
}

type writeFunction func() error

func attemptFileWrite() error {
	file, err := os.Create(path.Join(env.VtDataRoot(), ".stalled_disk_check"))
	if err != nil {
		return err
	}
	_, err = file.WriteString(strconv.FormatInt(time.Now().UnixNano(), 10))
	if err != nil {
		return err
	}
	err = file.Sync()
	if err != nil {
		return err
	}
	return file.Close()
}

type pollingFileSystemManager struct {
	mu              sync.RWMutex
	stalled         bool
	writeFunc       writeFunction
	pollingInterval time.Duration
	writeTimeout    time.Duration
}

var _ FileSystemManager = &pollingFileSystemManager{}

func newPollingFileSystemManager(ctx context.Context, writeFunc writeFunction, pollingInterval, waitPeriod time.Duration) *pollingFileSystemManager {
	fs := &pollingFileSystemManager{
		mu:              sync.RWMutex{},
		stalled:         false,
		writeFunc:       writeFunc,
		pollingInterval: pollingInterval,
		writeTimeout:    waitPeriod,
	}
	go fs.poll(ctx)
	return fs
}

func (f *pollingFileSystemManager) poll(ctx context.Context) {
	checkInProgress := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(f.pollingInterval):
			f.mu.Lock()
			if checkInProgress {
				f.mu.Unlock()
				continue
			}
			checkInProgress = true
			f.mu.Unlock()

			ch := make(chan error, 1)
			go func() {
				ch <- f.writeFunc()
			}()

			select {
			case <-time.After(f.writeTimeout):
				f.mu.Lock()
				f.stalled = true
				checkInProgress = false
				f.mu.Unlock()
			case err := <-ch:
				f.mu.Lock()
				f.stalled = err != nil
				checkInProgress = false
				f.mu.Unlock()
			}
		}
	}
}

func (fs *pollingFileSystemManager) IsDiskStalled() bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.stalled
}

type noopFileSystemManager struct{}

var _ FileSystemManager = &noopFileSystemManager{}

func newNoopFilesystemManager() FileSystemManager {
	return &noopFileSystemManager{}
}

func (fs *noopFileSystemManager) IsDiskStalled() bool {
	return false
}
