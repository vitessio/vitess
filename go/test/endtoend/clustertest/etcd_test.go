package clustertest

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestEtcdServer(t *testing.T) {
	defer cluster.PanicHandler(t)
	etcdURL := fmt.Sprintf("http://%s:%d/v2/keys", clusterInstance.Hostname, clusterInstance.TopoPort)
	testURL(t, etcdURL, "generic etcd url")
	testURL(t, etcdURL+"/vitess/global", "vitess global key")
	testURL(t, etcdURL+"/vitess/zone1", "vitess zone1 key")
}
