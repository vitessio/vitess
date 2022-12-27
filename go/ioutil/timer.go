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
This file contains the timer struct, which contains contains time-keeping
functionality used by TimedReader, TimedReadCloser, TimedWriter,
TimedWriteCloser.
*/

package ioutil

import "time"

// timer contains time-keeping functionality used by TimedReader,
// TimedReadCloser, TimedWriter, TimedWriteCloser.
type timer struct {
	fs    []func(delta time.Duration)
	total time.Duration
}

// Duration reports the total time spend on Read calls so far.
func (t *timer) Duration() time.Duration {
	return t.total
}

// time tracks the time it takes to execute f. Time is accumulated into total,
// and reported to callback fns.
func (t *timer) time(f func(p []byte) (int, error), p []byte) (n int, err error) {
	s := time.Now()
	n, err = f(p)
	delta := time.Since(s)
	t.total += delta
	for _, cb := range t.fs {
		cb(delta)
	}
	return
}
