/*
Copyright 2017 Google Inc.

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
	"github.com/youtube/vitess/go/event"
)

var (
	onFlushHooks event.Hooks
)

// OnFlush registers a function to be called when Flush() is invoked.
func OnFlush(fn func()) {
	onFlushHooks.Add(fn)
}

// Flush calls the functions registered through OnFlush() and waits for them.
//
// Programs that use servenv.Run*() will invoke Flush() automatically at
// shutdown. Other programs should defer logutil.Flush() at the beginning of
// main().
//
// Concurrent calls to Flush are serialized.
func Flush() {
	onFlushHooks.Fire()
}
