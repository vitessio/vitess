package clustertest

import (
	"testing"
)

func TestVtctldProcess(t *testing.T) {
	testURL(t, "http://localhost:15000/api/keyspaces/", "keyspace url")
}
