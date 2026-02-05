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
The meter struct contains time-and-byte-tracking functionality used by
MeteredReader, MeteredReadCloser, MeteredWriter, MeteredWriteCloser.
*/

package ioutil

import "time"

// meter contains time-and-byte-tracking functionality.
type meter struct {
	fs       []func(b int, d time.Duration)
	bytes    int64
	duration time.Duration
}

// Bytes reports the total bytes processed in calls to measure().
func (mtr *meter) Bytes() int64 {
	return mtr.bytes
}

// Duration reports the total time spent in calls to measure().
func (mtr *meter) Duration() time.Duration {
	return mtr.duration
}

// measure tracks the time spent and bytes processed by f. Time is accumulated into total,
// and reported to callback fns.
func (mtr *meter) measure(f func(p []byte) (int, error), p []byte) (b int, err error) {
	s := time.Now()
	b, err = f(p)
	d := time.Since(s)

	mtr.bytes += int64(b)
	mtr.duration += d

	for _, cb := range mtr.fs {
		cb(b, d)
	}

	return
}
