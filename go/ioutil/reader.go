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
MeteredReadCloser and MeteredReader are time-and-byte-tracking wrappers around
ReadCloser and Reader.
*/

package ioutil

import (
	"io"
	"time"
)

// MeteredReadCloser tracks how much time is spent and bytes are read in Read
// calls.
type MeteredReadCloser interface {
	io.ReadCloser
	// Bytes reports the total number of bytes read in Read calls.
	Bytes() int64
	// Duration reports the total duration of time spent on Read calls.
	Duration() time.Duration
}

// MeteredReader tracks how much time is spent and bytes are read in Read
// calls.
type MeteredReader interface {
	io.Reader
	// Bytes reports the total number of bytes read in Read calls.
	Bytes() int64
	// Duration reports the total duration of time spent on Read calls.
	Duration() time.Duration
}

type meteredReadCloser struct {
	io.ReadCloser
	*meter
}

type meteredReader struct {
	io.Reader
	*meter
}

// NewMeteredReadCloser creates a MeteredReadCloser which tracks the amount of
// time spent and bytes read in Read calls to the provided inner ReadCloser.
// Optional callbacks will be called with the time spent and bytes read in each
// Read call.
func NewMeteredReadCloser(rc io.ReadCloser, fns ...func(int, time.Duration)) MeteredReadCloser {
	return &meteredReadCloser{
		ReadCloser: rc,
		meter:      &meter{fns, 0, 0},
	}
}

// Read calls the inner ReadCloser, increments the total Duration and Bytes,
// and calls any registered callbacks with the amount of time spent and bytes
// read in this Read call.
func (trc *meteredReadCloser) Read(p []byte) (n int, err error) {
	return trc.meter.measure(trc.ReadCloser.Read, p)
}

// NewMeteredReader creates a MeteredReader which tracks the amount of time spent
// and bytes read in Read calls to the provided inner Reader. Optional
// callbacks will be called with the time spent and bytes read in each Read
// call.
func NewMeteredReader(r io.Reader, fns ...func(int, time.Duration)) MeteredReader {
	return &meteredReader{
		Reader: r,
		meter:  &meter{fns, 0, 0},
	}
}

// Duration reports the total time spend on Read calls so far.
func (tr *meteredReader) Duration() time.Duration {
	return tr.duration
}

// Read calls the inner Reader, increments the total Duration and Bytes, and
// calls any registered callbacks with the amount of time spent and bytes read
// in this Read call.
func (tr *meteredReader) Read(p []byte) (int, error) {
	return tr.measure(tr.Reader.Read, p)
}
