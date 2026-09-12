/*
Copyright 2026 The Vitess Authors.

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

package logutil

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// WriterLogger is a Logger that writes log lines to an io.Writer.
// It is thread-safe.
type WriterLogger struct {
	mu sync.Mutex
	w  io.Writer
}

// NewWriterLogger returns a Logger that writes formatted log lines to w.
func NewWriterLogger(w io.Writer) *WriterLogger {
	return &WriterLogger{w: w}
}

func (wl *WriterLogger) write(level, s string) {
	ts := time.Now().UTC().Format("2006-01-02 15:04:05.000000")
	wl.mu.Lock()
	defer wl.mu.Unlock()
	fmt.Fprintf(wl.w, "%s %s %s\n", level, ts, s)
}

func (wl *WriterLogger) InfoDepth(_ int, s string) {
	wl.write("I", s)
}

func (wl *WriterLogger) WarningDepth(_ int, s string) {
	wl.write("W", s)
}

func (wl *WriterLogger) ErrorDepth(_ int, s string) {
	wl.write("E", s)
}

func (wl *WriterLogger) Infof(format string, v ...any) {
	wl.write("I", fmt.Sprintf(format, v...))
}

func (wl *WriterLogger) Warningf(format string, v ...any) {
	wl.write("W", fmt.Sprintf(format, v...))
}

func (wl *WriterLogger) Errorf(format string, v ...any) {
	wl.write("E", fmt.Sprintf(format, v...))
}

func (wl *WriterLogger) Errorf2(err error, format string, v ...any) {
	wl.write("E", fmt.Sprintf(format+": %+v", append(v, err)))
}

func (wl *WriterLogger) Error(err error) {
	wl.write("E", fmt.Sprintf("%+v", err))
}

func (wl *WriterLogger) Printf(format string, v ...any) {
	wl.mu.Lock()
	defer wl.mu.Unlock()
	fmt.Fprintf(wl.w, format, v...)
}
