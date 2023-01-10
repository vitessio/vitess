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
MeteredWriteCloser and MeteredWriter are respectively, time-and-byte-tracking
wrappers around WriteCloser and Writer.
*/

package ioutil

import (
	"io"
	"time"
)

// MeteredWriteCloser tracks how much time is spent and bytes are written in
// Write calls.
type MeteredWriteCloser interface {
	io.WriteCloser
	// Duration reports the total duration of time spent on Write calls.
	Duration() time.Duration
}

// MeteredWriter tracks how much time is spent and bytes are written in Write
// calls.
type MeteredWriter interface {
	io.Writer
	// Duration reports the total duration of time spent on Writer calls.
	Duration() time.Duration
}

type meteredWriteCloser struct {
	io.WriteCloser
	*meter
}

type meteredWriter struct {
	io.Writer
	*meter
}

// NewMeteredWriteCloser creates a MeteredWriteCloser which tracks the amount of
// time spent and bytes writtein in Write calls to the provided inner
// WriteCloser. Optional callbacks will be called with the time spent and bytes
// written in each Write call.
func NewMeteredWriteCloser(wc io.WriteCloser, fns ...func(int, time.Duration)) MeteredWriteCloser {
	return &meteredWriteCloser{
		WriteCloser: wc,
		meter:       &meter{fns, 0, 0},
	}
}

// Write calls the inner WriteCloser, increments the total Duration and Bytes,
// and calls any registered callbacks with the amount of time spent and bytes
// written in this Write call.
func (twc *meteredWriteCloser) Write(p []byte) (int, error) {
	return twc.meter.measure(twc.WriteCloser.Write, p)
}

// NewMeteredWriter creates a MeteredWriter which tracks the amount of time spent
// and bytes written in Write calls to the provided inner Writer. Optional
// callbacks will be called with the time spent and bytes written in each Write
// call.
func NewMeteredWriter(tw io.Writer, fns ...func(int, time.Duration)) MeteredWriter {
	return &meteredWriter{
		Writer: tw,
		meter:  &meter{fns, 0, 0},
	}
}

// Write calls the inner Writer, increments the total Duration and Bytes, and
// calls any registered callbacks with the amount of time spent and bytes
// written in this Write call.
func (tw *meteredWriter) Write(p []byte) (int, error) {
	return tw.meter.measure(tw.Writer.Write, p)
}
