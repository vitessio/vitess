package clustertest

import (
	"fmt"
	"testing"
)

func TestVtctldProcess(t *testing.T) {
	url := fmt.Sprintf("http://localhost:%d/api/keyspaces/", ClusterInstance.VtctldHTTPPort)
	testURL(t, url, "keyspace url")
}
