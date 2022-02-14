package servenv

import (
	"testing"
	"time"

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

func TestFireOnCloseHooksTimeout(t *testing.T) {
	onCloseHooks = event.Hooks{}

	OnClose(func() {
		time.Sleep(1 * time.Second)
	})

	// we deliberatly test the flag to make sure it's not accidently set to a
	// high value.
	if finished, want := fireOnCloseHooks(*onCloseTimeout), false; finished != want {
		t.Errorf("finished = %v, want %v", finished, want)
	}
}
