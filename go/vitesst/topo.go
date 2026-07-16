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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Topology server images. The versions match the binaries pinned by build.env.
const (
	etcdImage   = "quay.io/coreos/etcd:v3.6.7"
	consulImage = "hashicorp/consul:2.0.1"
	zkImage     = "zookeeper:3.9.5"

	consulClientPort = 8500
	zkClientPort     = 2181
)

// Topo returns the cluster's topology server component.
func (c *Cluster) Topo() *component {
	return c.topo
}

// EtcdAddr returns the host-reachable "host:port" of the topology server's
// etcd client port. It errors for non-etcd topo flavors.
func (c *Cluster) EtcdAddr(ctx context.Context) (string, error) {
	if c.opts.topoFlavor != "etcd2" {
		return "", fmt.Errorf("topo flavor is %s, not etcd2", c.opts.topoFlavor)
	}
	if c.topo == nil {
		return "", errors.New("topo server is not running")
	}
	return c.topo.HTTPAddr(ctx)
}

// startTopo starts the topology server container for the configured flavor.
func (c *Cluster) startTopo(t testing.TB, ctx context.Context) error {
	switch c.opts.topoFlavor {
	case "etcd2":
		return c.startEtcd(t, ctx)
	case "consul":
		return c.startConsul(t, ctx)
	case "zk2":
		return c.startZookeeper(t, ctx)
	default:
		return fmt.Errorf("unsupported topo flavor %q", c.opts.topoFlavor)
	}
}

// startEtcd starts the etcd topology server container.
func (c *Cluster) startEtcd(t testing.TB, ctx context.Context) error {
	topo := &component{
		name:     c.name("etcd"),
		httpPort: fmt.Sprintf("%d/tcp", etcdClientPort),
		cluster:  c,
	}

	ctr, err := testcontainers.Run(
		ctx, etcdImage,
		testcontainers.WithCmd(
			"etcd",
			"--listen-client-urls", fmt.Sprintf("http://0.0.0.0:%d", etcdClientPort),
			"--advertise-client-urls", fmt.Sprintf("http://%s:%d", topo.name, etcdClientPort),
		),
		testcontainers.WithExposedPorts(topo.httpPort),
		network.WithNetwork([]string{topo.name}, c.network),
		testcontainers.WithLogConsumers(c.newFileLogConsumer(t, topo.name)),
		testcontainers.WithWaitStrategyAndDeadline(
			defaultStartupTimeout,
			wait.ForHTTP("/health").
				WithPort(topo.httpPort).
				WithStartupTimeout(defaultStartupTimeout).
				WithPollInterval(defaultPollInterval),
		),
	)
	if err != nil {
		return fmt.Errorf("starting etcd: %w", err)
	}

	topo.setContainer(ctr)
	c.topo = topo
	return nil
}

// startConsul starts the consul topology server container.
func (c *Cluster) startConsul(t testing.TB, ctx context.Context) error {
	topo := &component{
		name:     c.name("consul"),
		httpPort: fmt.Sprintf("%d/tcp", consulClientPort),
		cluster:  c,
	}

	ctr, err := testcontainers.Run(
		ctx, consulImage,
		testcontainers.WithCmd(
			"agent", "-server", "-bootstrap-expect", "1",
			"-bind", "0.0.0.0", "-client", "0.0.0.0",
		),
		testcontainers.WithExposedPorts(topo.httpPort),
		network.WithNetwork([]string{topo.name}, c.network),
		testcontainers.WithLogConsumers(c.newFileLogConsumer(t, topo.name)),
		testcontainers.WithWaitStrategyAndDeadline(
			defaultStartupTimeout,
			wait.ForHTTP("/v1/kv/?keys").
				WithPort(topo.httpPort).
				WithStartupTimeout(defaultStartupTimeout).
				WithPollInterval(defaultPollInterval),
		),
	)
	if err != nil {
		return fmt.Errorf("starting consul: %w", err)
	}

	topo.setContainer(ctr)
	c.topo = topo
	return nil
}

// startZookeeper starts the zookeeper topology server container.
func (c *Cluster) startZookeeper(t testing.TB, ctx context.Context) error {
	topo := &component{
		name:     c.name("zk"),
		httpPort: fmt.Sprintf("%d/tcp", zkClientPort),
		cluster:  c,
	}

	// The image's own entrypoint generates /conf/zoo.cfg only when it wraps
	// zkServer.sh directly, so the supervisor writes an equivalent standalone
	// config before its loop.
	script := `if [[ ! -f /conf/zoo.cfg ]]; then
  printf 'dataDir=/data\ndataLogDir=/datalog\nclientPort=2181\ntickTime=2000\nadmin.enableServer=false\n4lw.commands.whitelist=*\n' > /conf/zoo.cfg
fi
touch /tmp/zk.run
while true; do
  if [[ -f /tmp/zk.run ]]; then
    zkServer.sh start-foreground
  fi
  sleep 0.5
done`

	ctr, err := testcontainers.Run(
		ctx, zkImage,
		testcontainers.WithEntrypoint("bash", "-c", script),
		testcontainers.WithExposedPorts(topo.httpPort),
		network.WithNetwork([]string{topo.name}, c.network),
		testcontainers.WithLogConsumers(c.newFileLogConsumer(t, topo.name)),
		testcontainers.WithWaitStrategyAndDeadline(
			defaultStartupTimeout,
			wait.ForListeningPort(topo.httpPort).
				WithStartupTimeout(defaultStartupTimeout).
				WithPollInterval(defaultPollInterval),
		),
	)
	if err != nil {
		return fmt.Errorf("starting zookeeper: %w", err)
	}

	topo.setContainer(ctr)
	c.topo = topo
	return nil
}

// TopoAddress returns the topology server's client address as seen from
// inside the cluster network, e.g. "ext-etcd:2379". Another cluster on the
// same network reaches this cluster's topology server at that address.
func (c *Cluster) TopoAddress() string {
	switch c.opts.topoFlavor {
	case "consul":
		return fmt.Sprintf("%s:%d", c.name("consul"), consulClientPort)
	case "zk2":
		return fmt.Sprintf("%s:%d", c.name("zk"), zkClientPort)
	default:
		return fmt.Sprintf("%s:%d", c.name("etcd"), etcdClientPort)
	}
}

// topoGlobalRoot is the global root path for the configured topo flavor.
func (c *Cluster) topoGlobalRoot() string {
	if c.opts.topoFlavor == "consul" {
		return "global"
	}
	return topoGlobalRoot
}

// topoCellRoot is the cell root path passed to AddCellInfo.
func (c *Cluster) topoCellRoot(cell string) string {
	if c.opts.topoFlavor == "consul" {
		return cell
	}
	return "/vitess/" + cell
}

// TopoFlags returns the topo flags shared by every Vitess component. A test
// that runs a vtctldclient command against the topology server directly, with
// "--server internal", passes them so the command reaches the same topology
// server as the cluster, whatever its flavor.
func (c *Cluster) TopoFlags() []string {
	return []string{
		"--topo-implementation", c.opts.topoFlavor,
		"--topo-global-server-address", c.TopoAddress(),
		"--topo-global-root", c.topoGlobalRoot(),
	}
}

// StopTopoProcess stops the topology server process while keeping its
// container and network alias alive where the flavor allows it, so clients
// see refused connections like a process kill. For zk2 the zookeeper process
// stops inside the running container; etcd2 and consul fall back to stopping
// the container.
func (c *Cluster) StopTopoProcess(ctx context.Context) error {
	if c.opts.topoFlavor != "zk2" {
		return c.topo.StopContainer(ctx, 10*time.Second)
	}

	// The character class stops pkill -f from matching this exec's own argv.
	script := `rm -f /tmp/zk.run && pkill -f '[Q]uorumPeerMain' || pkill '[j]ava' || true`
	if _, err := mustExec(ctx, c.topo.container(), []string{"bash", "-c", script}); err != nil {
		return fmt.Errorf("stopping zookeeper process: %w", err)
	}
	return nil
}

// StartTopoProcess starts the topology server process again after
// StopTopoProcess and blocks until it accepts sessions.
func (c *Cluster) StartTopoProcess(ctx context.Context) error {
	if c.opts.topoFlavor != "zk2" {
		return c.topo.StartContainer(ctx)
	}

	if _, err := mustExec(ctx, c.topo.container(), []string{"bash", "-c", "touch /tmp/zk.run"}); err != nil {
		return fmt.Errorf("starting zookeeper process: %w", err)
	}

	// Wait for the four-letter ruok probe to answer imok, so callers see a
	// serving zookeeper when this returns.
	probe := `exec 3<>/dev/tcp/127.0.0.1/2181 && printf ruok >&3 && head -c 4 <&3`
	waitCtx, cancel := context.WithTimeout(ctx, defaultStartupTimeout)
	defer cancel()
	for {
		exitCode, output, err := containerExec(waitCtx, c.topo.container(), []string{"bash", "-c", probe})
		if err == nil && exitCode == 0 && output == "imok" {
			return nil
		}

		select {
		case <-waitCtx.Done():
			return fmt.Errorf("zookeeper did not accept sessions after restart: %w", waitCtx.Err())
		case <-time.After(defaultPollInterval):
		}
	}
}
