/*
Copyright 2026 The Vitess Authors.

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

package vitesst

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"vitess.io/vitess/go/vt/vterrors"
)

// startEtcd starts the etcd topology server container.
func (c *Cluster) startEtcd(ctx context.Context) error {
	etcd := &component{
		name:     "etcd",
		httpPort: fmt.Sprintf("%d/tcp", etcdClientPort),
		cluster:  c,
	}

	ctr, err := testcontainers.Run(ctx, etcdImage,
		testcontainers.WithCmd(
			"etcd",
			"--listen-client-urls", fmt.Sprintf("http://0.0.0.0:%d", etcdClientPort),
			"--advertise-client-urls", c.topoServerAddress(),
		),
		testcontainers.WithExposedPorts(etcd.httpPort),
		network.WithNetwork([]string{etcd.name}, c.network),
		testcontainers.WithTmpfs(map[string]string{"/var/lib/etcd": ""}),
		testcontainers.WithLogConsumers(c.newLogConsumer(etcd.name)),
		testcontainers.WithWaitStrategyAndDeadline(defaultStartupTimeout,
			wait.ForHTTP("/health").
				WithPort(etcd.httpPort).
				WithStartupTimeout(defaultStartupTimeout).
				WithPollInterval(defaultPollInterval),
		),
	)
	if err != nil {
		return vterrors.Wrapf(err, "starting etcd")
	}

	etcd.setContainer(ctr)
	c.etcd = etcd
	return nil
}

// topoServerAddress is the etcd client address as seen from inside the
// cluster network.
func (c *Cluster) topoServerAddress() string {
	return fmt.Sprintf("http://etcd:%d", etcdClientPort)
}

// topoFlags returns the topo flags shared by every Vitess component.
func (c *Cluster) topoFlags() []string {
	return []string{
		"--topo-implementation", defaultTopoImplementation,
		"--topo-global-server-address", fmt.Sprintf("etcd:%d", etcdClientPort),
		"--topo-global-root", topoGlobalRoot,
	}
}
