package tabletmanager

import (
	"context"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

type DiskHealthMonitor interface {
	// IsDiskStalled returns true if the disk is stalled or rejecting writes.
	IsDiskStalled() bool
}

func newDiskHealthMonitor(ctx context.Context) DiskHealthMonitor {
	if stalledDiskWriteDir == "" {
		return newNoopDiskHealthMonitor()
	}

	return newPollingDiskHealthMonitor(ctx, attemptFileWrite, stalledDiskWriteInterval, stalledDiskWriteTimeout)
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

type pollingDiskHealthMonitor struct {
	stalledMutex         sync.RWMutex
	stalled              bool
	writeInProgressMutex sync.RWMutex
	writeInProgress      bool
	writeFunc            writeFunction
	pollingInterval      time.Duration
	writeTimeout         time.Duration
}

var _ DiskHealthMonitor = &pollingDiskHealthMonitor{}

func newPollingDiskHealthMonitor(ctx context.Context, writeFunc writeFunction, pollingInterval, writeTimeout time.Duration) *pollingDiskHealthMonitor {
	fs := &pollingDiskHealthMonitor{
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

func (fs *pollingDiskHealthMonitor) poll(ctx context.Context) {
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

func (fs *pollingDiskHealthMonitor) IsDiskStalled() bool {
	fs.stalledMutex.RLock()
	defer fs.stalledMutex.RUnlock()
	return fs.stalled
}

func (fs *pollingDiskHealthMonitor) setIsDiskStalled(isStalled bool) {
	fs.stalledMutex.Lock()
	defer fs.stalledMutex.Unlock()
	fs.stalled = isStalled
}

func (fs *pollingDiskHealthMonitor) isWriteInProgress() bool {
	fs.writeInProgressMutex.RLock()
	defer fs.writeInProgressMutex.RUnlock()
	return fs.writeInProgress
}

func (fs *pollingDiskHealthMonitor) setIsWriteInProgress(isInProgress bool) {
	fs.writeInProgressMutex.Lock()
	defer fs.writeInProgressMutex.Unlock()
	fs.writeInProgress = isInProgress
}

type noopDiskHealthMonitor struct{}

var _ DiskHealthMonitor = &noopDiskHealthMonitor{}

func newNoopDiskHealthMonitor() DiskHealthMonitor {
	return &noopDiskHealthMonitor{}
}

func (fs *noopDiskHealthMonitor) IsDiskStalled() bool {
	return false
}
