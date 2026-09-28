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

package servenv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/event"
)

func TestFireOnTermSyncHooksFinished(t *testing.T) {
	onTermSyncHooks = event.Hooks{}

	triggered1 := false
	OnTermSync(func() {
		triggered1 = true
	})
	triggered2 := false
	OnTermSync(func() {
		triggered2 = true
	})

	finished, want := fireOnTermSyncHooks(1*time.Second), true
	assert.Equalf(t, want, finished, "finished = %v, want %v", finished, want)
	want1 := true
	assert.Equalf(t, want1, triggered1, "triggered1 = %v, want %v", triggered1, want1)
	want2 := true
	assert.Equalf(t, want2, triggered2, "triggered1 = %v, want %v", triggered2, want2)
}

func TestFireOnTermSyncHooksTimeout(t *testing.T) {
	onTermSyncHooks = event.Hooks{}

	OnTermSync(func() {
		time.Sleep(1 * time.Second)
	})

	finished, want := fireOnTermSyncHooks(1*time.Nanosecond), false
	assert.Equalf(t, want, finished, "finished = %v, want %v", finished, want)
}

func TestFireOnCloseHooksTimeout(t *testing.T) {
	onCloseHooks = event.Hooks{}

	OnClose(func() {
		time.Sleep(1 * time.Second)
	})

	finished, want := fireOnCloseHooks(1*time.Nanosecond), false
	assert.Equalf(t, want, finished, "finished = %v, want %v", finished, want)
}
