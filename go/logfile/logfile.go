/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package logfile

import (
	"fmt"
	"io"
	"os"
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
	os.Symlink(newName, self.baseName)
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
