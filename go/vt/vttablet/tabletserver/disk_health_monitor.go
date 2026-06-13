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
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

var (
	stalledDiskWriteDir      = ""
	stalledDiskWriteTimeout  = 30 * time.Second
	stalledDiskWriteInterval = 5 * time.Second
)

func init() {
	servenv.OnParseFor("vtcombo", registerInitFlags)
	servenv.OnParseFor("vttablet", registerInitFlags)
}

func registerInitFlags(fs *pflag.FlagSet) {
	fs.StringVar(&stalledDiskWriteDir, "disk-write-dir", stalledDiskWriteDir, "if provided, tablet will attempt to write a file to this directory to check if the disk is stalled")
	fs.DurationVar(&stalledDiskWriteTimeout, "disk-write-timeout", stalledDiskWriteTimeout, "if writes exceed this duration, the disk is considered stalled")
	fs.DurationVar(&stalledDiskWriteInterval, "disk-write-interval", stalledDiskWriteInterval, "how often to write to the disk to check whether it is stalled")
}

type DiskHealthMonitor interface {
	// IsDiskStalled returns true if the disk is stalled or rejecting writes.
	IsDiskStalled() bool
	// IsDiskFull returns true if the disk is rejecting writes with ENOSPC.
	IsDiskFull() bool
}

func newDiskHealthMonitor(ctx context.Context) DiskHealthMonitor {
	if stalledDiskWriteDir == "" {
		return newNoopDiskHealthMonitor()
	}

	return newPollingDiskHealthMonitor(ctx, attemptFileWrite, stalledDiskWriteInterval, stalledDiskWriteTimeout)
}

type writeFunction func() error

func attemptFileWrite() error {
	file, err := os.CreateTemp(stalledDiskWriteDir, ".stalled_disk_check_")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())

	_, err = file.WriteString(strconv.FormatInt(time.Now().UnixNano(), 10))
	if err != nil {
		_ = file.Close()
		return err
	}
	err = file.Sync()
	if err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

type pollingDiskHealthMonitor struct {
	healthMutex          sync.RWMutex
	stalled              bool
	full                 bool
	writeInProgressMutex sync.RWMutex
	writeInProgress      bool
	writeFunc            writeFunction
	pollingInterval      time.Duration
	writeTimeout         time.Duration
}

var _ DiskHealthMonitor = &pollingDiskHealthMonitor{}

func newPollingDiskHealthMonitor(ctx context.Context, writeFunc writeFunction, pollingInterval, writeTimeout time.Duration) *pollingDiskHealthMonitor {
	fs := &pollingDiskHealthMonitor{
		healthMutex:          sync.RWMutex{},
		stalled:              false,
		full:                 false,
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
				fs.setDiskHealth(true, false)
			case err := <-ch:
				fs.setDiskHealthFromWriteError(err)
			}
		}
	}
}

func (fs *pollingDiskHealthMonitor) IsDiskStalled() bool {
	fs.healthMutex.RLock()
	defer fs.healthMutex.RUnlock()
	return fs.stalled
}

func (fs *pollingDiskHealthMonitor) IsDiskFull() bool {
	fs.healthMutex.RLock()
	defer fs.healthMutex.RUnlock()
	return fs.full
}

// setDiskHealthFromWriteError sets disk health from a probe-write outcome.
// Invariant: stalled and full are mutually exclusive — downstream consumers
// rely on this, do not introduce a state where both are true.
func (fs *pollingDiskHealthMonitor) setDiskHealthFromWriteError(err error) {
	switch {
	case err == nil:
		fs.setDiskHealth(false, false)
	case errors.Is(err, syscall.ENOSPC):
		fs.setDiskHealth(false, true)
	default:
		fs.setDiskHealth(true, false)
	}
}

func (fs *pollingDiskHealthMonitor) setDiskHealth(isStalled, isFull bool) {
	fs.healthMutex.Lock()
	defer fs.healthMutex.Unlock()
	fs.stalled = isStalled
	fs.full = isFull
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

func (fs *noopDiskHealthMonitor) IsDiskFull() bool {
	return false
}
