/*
Copyright 2022 The Vitess Authors.

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

package ioutil

import (
	"io"
	"time"
)

// TimedWriteCloser tracks how much time is spent on Write calls.
type TimedWriteCloser interface {
	io.WriteCloser
	// Duration reports the total duration of time spent on Write calls.
	Duration() time.Duration
}

// TimedWriter tracks how much time is spent on Write calls.
type TimedWriter interface {
	io.Writer
	// Duration reports the total duration of time spent on Writer calls.
	Duration() time.Duration
}

type timedWriteCloser struct {
	io.WriteCloser
	fs    []func(delta time.Duration)
	total time.Duration
}

type timedWriter struct {
	io.Writer
	fs    []func(delta time.Duration)
	total time.Duration
}

// NewTimedWriteCloser creates a TimedWriteCloser which tracks the amount of
// time spent on Write calls to the provided inner WriteCloser. Optional
// callbacks will be called with the time spent on each Write call.
func NewTimedWriteCloser(wc io.WriteCloser, fns ...func(delta time.Duration)) TimedWriteCloser {
	return &timedWriteCloser{wc, fns, 0}
}

// Duration reports the total time spend on Write calls so far.
func (twc *timedWriteCloser) Duration() time.Duration {
	return twc.total
}

// Write calls the inner WriteCloser, increments the total Duration, and calls
// any registered callbacks with the amount of time spent on this Write call.
func (twc *timedWriteCloser) Write(p []byte) (n int, err error) {
	t := time.Now()
	n, err = twc.WriteCloser.Write(p)
	delta := time.Since(t)
	twc.total = twc.total + delta
	for _, f := range twc.fs {
		f(delta)
	}
	return n, err
}

// NewTimedWriter creates a TimedWriter which tracks the amount of time spent
// on Write calls to the provided inner Writer. Optional callbacks will be
// called with the time spent on each Write call.
func NewTimedWriter(tw io.Writer, fns ...func(delta time.Duration)) TimedWriter {
	return &timedWriter{tw, fns, 0}
}

// Duration reports the total time spend on Write calls so far.
func (tw *timedWriter) Duration() time.Duration {
	return tw.total
}

// Write calls the inner Writer, increments the total Duration, and calls
// any registered callbacks with the amount of time spent on this Write call.
func (tw *timedWriter) Write(p []byte) (n int, err error) {
	t := time.Now()
	n, err = tw.Writer.Write(p)
	delta := time.Since(t)
	tw.total = tw.total + delta
	for _, f := range tw.fs {
		f(delta)
	}
	return n, err
}
