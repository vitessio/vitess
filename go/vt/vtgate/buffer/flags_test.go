package buffer

import (
	"flag"
	"strings"
	"testing"
)

func TestVerifyFlags(t *testing.T) {
	// Verify that the non-allowed (non-trivial) flag combinations are caught.
	defer resetFlagsForTesting()

	flag.Set("buffer_keyspace_shards", "ks1/0")
	if err := verifyFlags(); err == nil || !strings.Contains(err.Error(), "also requires that") {
		t.Fatalf("List of shards requires --enable_buffer. err: %v", err)
	}

	resetFlagsForTesting()
	flag.Set("enable_buffer", "true")
	flag.Set("enable_buffer_dry_run", "true")
	if err := verifyFlags(); err == nil || !strings.Contains(err.Error(), "To avoid ambiguity") {
		t.Fatalf("Dry-run and non-dry-run mode together require an explicit list of shards for actual buffering. err: %v", err)
	}

	resetFlagsForTesting()
	flag.Set("enable_buffer", "true")
	flag.Set("buffer_keyspace_shards", "ks1//0")
	if err := verifyFlags(); err == nil || !strings.Contains(err.Error(), "Invalid shard path") {
		t.Fatalf("Invalid shard names are not allowed. err: %v", err)
	}

	resetFlagsForTesting()
	flag.Set("enable_buffer", "true")
	flag.Set("buffer_keyspace_shards", "ks1,ks1/0")
	if err := verifyFlags(); err == nil || !strings.Contains(err.Error(), "has overlapping entries") {
		t.Fatalf("Listed keyspaces and shards must not overlap. err: %v", err)
	}
}
