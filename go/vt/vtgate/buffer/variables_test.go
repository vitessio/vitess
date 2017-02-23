package buffer

import (
	"flag"
	"testing"
)

func TestVariables(t *testing.T) {
	flag.Set("buffer_size", "23")
	defer resetFlagsForTesting()

	// Create new buffer which will the flags.
	New()

	if got, want := bufferSize.Get(), int64(23); got != want {
		t.Fatalf("BufferSize variable not set during initilization: got = %v, want = %v", got, want)
	}
}
