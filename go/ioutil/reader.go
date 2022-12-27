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

/*
This file contains TimedReadCloser and TimedReader, which are, respectively,
time-keeping wrappers around ReadCloser and Reader.
*/

package ioutil

import (
	"io"
	"time"
)

// TimedReadCloser tracks how much time is spent on Read calls.
type TimedReadCloser interface {
	io.ReadCloser
	// Duration reports the total duration of time spent on Read calls.
	Duration() time.Duration
}

// TimedReader tracks how much time is spent on Read calls.
type TimedReader interface {
	io.Reader
	// Duration reports the total duration of time spent on Read calls.
	Duration() time.Duration
}

type timedReadCloser struct {
	io.ReadCloser
	*timer
}

type timedReader struct {
	io.Reader
	*timer
}

// NewTimedReadCloser creates a TimedReadCloser which tracks the amount of time
// spent on Read calls to the provided inner ReadCloser. Optional callbacks
// will be called with the time spent on each Read call.
func NewTimedReadCloser(rc io.ReadCloser, fns ...func(delta time.Duration)) TimedReadCloser {
	return &timedReadCloser{
		ReadCloser: rc,
		timer:      &timer{fns, 0},
	}
}

// Read calls the inner ReadCloser, increments the total Duration, and calls
// any registered callbacks with the amount of time spent on this Read call.
func (trc *timedReadCloser) Read(p []byte) (n int, err error) {
	return trc.timer.time(trc.ReadCloser.Read, p)
}

// NewTimedReader creates a TimedReader which tracks the amount of time spent
// on Read calls to the provided inner Reader. Optional callbacks will be
// called with the time spent on each Read call.
func NewTimedReader(r io.Reader, fns ...func(delta time.Duration)) TimedReader {
	return &timedReader{
		Reader: r,
		timer:  &timer{fns, 0},
	}
}

// Duration reports the total time spend on Read calls so far.
func (tr *timedReader) Duration() time.Duration {
	return tr.total
}

// Read calls the inner Reader, increments the total Duration, and calls
// any registered callbacks with the amount of time spent on this Read call.
func (tr *timedReader) Read(p []byte) (int, error) {
	return tr.time(tr.Reader.Read, p)
}
