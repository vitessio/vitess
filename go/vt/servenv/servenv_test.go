// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/event"
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

	if finished, want := fireOnTermSyncHooks(1*time.Second), true; finished != want {
		t.Errorf("finished = %v, want %v", finished, want)
	}
	if want := true; triggered1 != want {
		t.Errorf("triggered1 = %v, want %v", triggered1, want)
	}
	if want := true; triggered2 != want {
		t.Errorf("triggered1 = %v, want %v", triggered2, want)
	}
}

func TestFireOnTermSyncHooksTimeout(t *testing.T) {
	onTermSyncHooks = event.Hooks{}

	OnTermSync(func() {
		time.Sleep(1 * time.Second)
	})

	if finished, want := fireOnTermSyncHooks(1*time.Nanosecond), false; finished != want {
		t.Errorf("finished = %v, want %v", finished, want)
	}
}
