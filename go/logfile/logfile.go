// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package logfile

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

type Logfile struct {
	mu      sync.Mutex
	handle  *os.File
	curName string
	curSize int64

	// These vars cannot be changed after Logfile is created
	baseName  string
	frequency int64
	maxSize   int64
	maxFiles  int64
}

func Open(baseName string, frequency, maxSize, maxFiles int64) (file io.WriteCloser, err error) {
	if frequency <= 0 && maxSize <= 0 && maxFiles <= 0 {
		return os.OpenFile(baseName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	}
	lf := &Logfile{baseName: baseName, frequency: frequency, maxSize: maxSize, maxFiles: maxFiles}
	// Open the first logfile
	err = lf.rotate()
	if err != nil {
		return nil, err
	}
	go lf.logRotator()
	return lf, nil
}

func (lf *Logfile) Close() error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	// This will close the current file, if any
	return lf.switchFile(nil, "")
}

func (lf *Logfile) Write(b []byte) (n int, err error) {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if lf.handle == nil {
		return 0, os.ErrInvalid
	}
	n, err = lf.handle.Write(b)
	lf.curSize += int64(n)
	if lf.maxSize > 0 && lf.curSize > lf.maxSize {
		lf.rotate()
	}
	return n, err
}

func (lf *Logfile) logRotator() {
	if lf.frequency <= 0 {
		return
	}
	nanoFreq := lf.frequency * 1e9
	for {
		now := time.Now().UnixNano()
		next := (now/nanoFreq)*nanoFreq + nanoFreq
		<-time.After(time.Duration(next - now))
		lf.mu.Lock()
		if lf.handle == nil {
			lf.mu.Unlock()
			return
		} else {
			lf.rotate()
			lf.mu.Unlock()
		}
	}
}

func (lf *Logfile) rotate() error {
	newName := fmt.Sprintf("%s.%s", lf.baseName, time.Now().Format("20060102.150405"))
	if newName == lf.curName {
		return nil
	}
	handle, err := os.Create(newName)
	if err != nil {
		return err
	}
	lf.switchFile(handle, newName)
	os.Remove(lf.baseName)
	os.Symlink(path.Base(newName), lf.baseName)
	go lf.logPurge()
	return nil
}

func (lf *Logfile) switchFile(newHandle *os.File, newName string) error {
	oldHandle := lf.handle
	lf.handle = newHandle
	lf.curName = newName
	lf.curSize = 0
	if oldHandle != nil {
		return oldHandle.Close()
	}
	return nil
}

func (lf *Logfile) logPurge() {
	if lf.maxFiles <= 0 {
		return
	}
	files, _ := filepath.Glob(fmt.Sprintf("%s.*", lf.baseName))
	for i := 0; i < len(files)-int(lf.maxFiles); i++ {
		os.Remove(files[i])
	}
}
