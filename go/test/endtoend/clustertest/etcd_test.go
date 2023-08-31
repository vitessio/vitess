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

package clustertest

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestEtcdServer(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Confirm the basic etcd cluster health.
	etcdHealthURL := fmt.Sprintf("http://%s:%d/health", clusterInstance.Hostname, clusterInstance.TopoPort)
	testURL(t, etcdHealthURL, "generic etcd health url")

	// Confirm that we have a working topo server by looking for some
	// expected keys.
	etcdClientOptions := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly(),
		clientv3.WithLimit(1),
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{net.JoinHostPort(clusterInstance.TopoProcess.Host, fmt.Sprintf("%d", clusterInstance.TopoProcess.Port))},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer cli.Close()
	keyPrefixes := []string{
		// At a minimum, this prefix confirms that we have a functioning
		// global topo server with a valid cell from the test env.
		fmt.Sprintf("/vitess/global/cells/%s", cell),
	}
	for _, keyPrefix := range keyPrefixes {
		res, err := cli.Get(cli.Ctx(), keyPrefix, etcdClientOptions...)
		require.NoError(t, err)
		require.NotNil(t, res)
		// Confirm that we have at least one key matching the prefix.
		require.Greaterf(t, len(res.Kvs), 0, "no keys found matching prefix: %s", keyPrefix)
	}
}
