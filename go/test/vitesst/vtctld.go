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
	"strconv"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"vitess.io/vitess/go/vt/vterrors"
)

// startVtctld starts the vtctld container.
func (c *Cluster) startVtctld(ctx context.Context) error {
	vtctld := &component{
		name:     "vtctld",
		httpPort: fmt.Sprintf("%d/tcp", vtctldHTTPPort),
		cluster:  c,
	}

	args := []string{"vtctld"}
	args = append(args, c.topoFlags()...)
	args = append(args,
		"--cell", c.cells[0],
		"--service-map", "grpc-vtctl,grpc-vtctld",
		"--backup-storage-implementation", "file",
		"--file-backup-storage-root", vtDataRoot+"/backups",
		"--port", strconv.Itoa(vtctldHTTPPort),
		"--grpc-port", strconv.Itoa(vtctldGRPCPort),
		"--log-format", "text",
	)
	args = append(args, c.opts.vtctldArgs...)

	ctr, err := testcontainers.Run(ctx, c.image,
		testcontainers.WithCmd(args...),
		testcontainers.WithExposedPorts(
			vtctld.httpPort,
			fmt.Sprintf("%d/tcp", vtctldGRPCPort),
		),
		network.WithNetwork([]string{vtctld.name}, c.network),
		testcontainers.WithTmpfs(map[string]string{vtDataRoot: "uid=999,gid=999"}),
		testcontainers.WithEnv(map[string]string{"VTTEST": "endtoend"}),
		testcontainers.WithLogConsumers(c.newLogConsumer(vtctld.name)),
		testcontainers.WithWaitStrategyAndDeadline(defaultStartupTimeout,
			wait.ForHTTP("/debug/vars").
				WithPort(vtctld.httpPort).
				WithStartupTimeout(defaultStartupTimeout).
				WithPollInterval(defaultPollInterval),
		),
	)
	if err != nil {
		return vterrors.Wrapf(err, "starting vtctld")
	}

	vtctld.setContainer(ctr)
	c.vtctld = vtctld
	return nil
}

// addCellInfo registers a cell in the topology server.
func (c *Cluster) addCellInfo(ctx context.Context, cell string) error {
	_, err := c.vtctldClient.executeCommand(ctx,
		"AddCellInfo",
		"--root", "/vitess/"+cell,
		"--server-address", fmt.Sprintf("etcd:%d", etcdClientPort),
		cell,
	)
	return vterrors.Wrapf(err, "adding cell %s", cell)
}
