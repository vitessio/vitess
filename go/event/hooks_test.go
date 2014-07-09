package event

import (
	"testing"
)

// TestHooks checks that hooks get triggered.
func TestHooks(t *testing.T) {
	triggered1 := false
	triggered2 := false

	var hooks Hooks
	hooks.Add(func() { triggered1 = true })
	hooks.Add(func() { triggered2 = true })

	hooks.Fire()

	if !triggered1 || !triggered2 {
		t.Errorf("registered hook functions failed to trigger on Fire()")
	}
}
