package tabletmanager

import (
	"context"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

type FileSystemManager interface {
	// IsDiskStalled returns true if the disk is stalled or rejecting writes.
	IsDiskStalled() bool
}

func newFileSystemManager(ctx context.Context) FileSystemManager {
	if stalledDiskWriteDir == "" {
		return newNoopFilesystemManager()
	}

	return newPollingFileSystemManager(ctx, attemptFileWrite, stalledDiskWriteInterval, stalledDiskWriteTimeout)
}

type writeFunction func() error

func attemptFileWrite() error {
	file, err := os.Create(path.Join(stalledDiskWriteDir, ".stalled_disk_check"))
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
	stalledMutex         sync.RWMutex
	stalled              bool
	writeInProgressMutex sync.RWMutex
	writeInProgress      bool
	writeFunc            writeFunction
	pollingInterval      time.Duration
	writeTimeout         time.Duration
}

var _ FileSystemManager = &pollingFileSystemManager{}

func newPollingFileSystemManager(ctx context.Context, writeFunc writeFunction, pollingInterval, writeTimeout time.Duration) *pollingFileSystemManager {
	fs := &pollingFileSystemManager{
		stalledMutex:         sync.RWMutex{},
		stalled:              false,
		writeInProgressMutex: sync.RWMutex{},
		writeInProgress:      false,
		writeFunc:            writeFunc,
		pollingInterval:      pollingInterval,
		writeTimeout:         writeTimeout,
	}
	go fs.poll(ctx)
	return fs
}

func (fs *pollingFileSystemManager) poll(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(fs.pollingInterval):
			if fs.isWriteInProgress() {
				continue
			}

			ch := make(chan error, 1)
			go func() {
				fs.setIsWriteInProgress(true)
				err := fs.writeFunc()
				fs.setIsWriteInProgress(false)
				ch <- err
			}()

			select {
			case <-time.After(fs.writeTimeout):
				fs.setIsDiskStalled(true)
			case err := <-ch:
				fs.setIsDiskStalled(err != nil)
			}
		}
	}
}

func (fs *pollingFileSystemManager) IsDiskStalled() bool {
	fs.stalledMutex.RLock()
	defer fs.stalledMutex.RUnlock()
	return fs.stalled
}

func (fs *pollingFileSystemManager) setIsDiskStalled(isStalled bool) {
	fs.stalledMutex.Lock()
	defer fs.stalledMutex.Unlock()
	fs.stalled = isStalled
}

func (fs *pollingFileSystemManager) isWriteInProgress() bool {
	fs.writeInProgressMutex.RLock()
	defer fs.writeInProgressMutex.RUnlock()
	return fs.writeInProgress
}

func (fs *pollingFileSystemManager) setIsWriteInProgress(isInProgress bool) {
	fs.writeInProgressMutex.Lock()
	defer fs.writeInProgressMutex.Unlock()
	fs.writeInProgress = isInProgress
}

type noopFileSystemManager struct{}

var _ FileSystemManager = &noopFileSystemManager{}

func newNoopFilesystemManager() FileSystemManager {
	return &noopFileSystemManager{}
}

func (fs *noopFileSystemManager) IsDiskStalled() bool {
	return false
}
