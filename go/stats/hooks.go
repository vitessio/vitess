/*
Copyright 2019 The Vitess Authors.

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

package stats

type statsdHook struct {
	timerHook     func(string, string, int64, *Timings)
	histogramHook func(string, int64)
}

var defaultStatsdHook = statsdHook{}

// RegisterTimerHook registers timer hook
func RegisterTimerHook(hook func(string, string, int64, *Timings)) {
	defaultStatsdHook.timerHook = hook
}

// RegisterHistogramHook registers timer hook
func RegisterHistogramHook(hook func(string, int64)) {
	defaultStatsdHook.histogramHook = hook
}
