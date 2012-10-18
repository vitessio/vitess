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
	sync.Mutex
	baseName  string
	frequency int64
	maxSize   int64
	maxFiles  int64
	handle    *os.File
	curName   string
	curSize   int64
}

func Open(baseName string, frequency, maxSize, maxFiles int64) (file io.WriteCloser, err error) {
	if frequency <= 0 && maxSize <= 0 && maxFiles <= 0 {
		return os.OpenFile(baseName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	}
	self := &Logfile{baseName: baseName, frequency: frequency, maxSize: maxSize, maxFiles: maxFiles}
	err = self.Rotate()
	go self.LogRotator()
	return self, err
}

func (self *Logfile) Rotate() error {
	format := "20060102.150405"
	t := time.Now()
	newName := fmt.Sprintf("%s.%s", self.baseName, t.Format(format))
	if newName == self.curName {
		return nil
	}
	handle, err := os.Create(newName)
	if err != nil {
		return err
	}
	self.curName = newName
	self.switchHandle(handle)
	os.Remove(self.baseName)
	os.Symlink(path.Base(newName), self.baseName)
	go self.LogPurge()
	return nil
}

func (self *Logfile) Close() error {
	return self.switchHandle(nil)
}

func (self *Logfile) switchHandle(newHandle *os.File) error {
	self.Lock()
	oldHandle := self.handle
	self.handle = newHandle
	self.curSize = 0
	self.Unlock()
	if oldHandle != nil {
		return oldHandle.Close()
	}
	return nil
}

func (self *Logfile) Write(b []byte) (n int, err error) {
	self.Lock()
	if self.handle == nil {
		self.Unlock()
		return 0, os.ErrInvalid
	}
	n, err = self.handle.Write(b)
	self.Unlock()
	self.curSize += int64(n)
	if self.maxSize > 0 && self.curSize > self.maxSize {
		self.Rotate()
	}
	return n, err
}

func (self *Logfile) LogRotator() {
	if self.frequency <= 0 {
		return
	}
	nanoFreq := self.frequency * 1e9
	for self.handle != nil {
		now := time.Now().UnixNano()
		next := (now/nanoFreq)*nanoFreq + nanoFreq
		<-time.After(time.Duration(next - now))
		self.Rotate()
	}
}

func (self *Logfile) LogPurge() {
	if self.maxFiles <= 0 {
		return
	}
	files, _ := filepath.Glob(fmt.Sprintf("%s.*", self.baseName))
	for i := 0; i < len(files)-int(self.maxFiles); i++ {
		os.Remove(files[i])
	}
}
