package logutil

import (
	"vitess.io/vitess/go/event"
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
