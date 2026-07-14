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

type (
	// VTOrc is the runtime handle for the cluster's VTOrc container, present
	// only when the cluster was created with WithVTOrc.
	VTOrc struct {
		component
	}
)

// VTOrc returns the cluster's VTOrc, or nil when it was not enabled.
func (c *Cluster) VTOrc() *VTOrc {
	return c.vtorc
}

// startVTOrc starts the VTOrc container.
func (c *Cluster) startVTOrc(ctx context.Context) error {
	vtorc := &VTOrc{
		component: component{
			name:     "vtorc",
			httpPort: fmt.Sprintf("%d/tcp", vtorcHTTPPort),
			cluster:  c,
		},
	}

	args := []string{"vtorc"}
	args = append(args, c.topoFlags()...)
	args = append(args,
		"--cell", c.cells[0],
		"--port", strconv.Itoa(vtorcHTTPPort),
		"--instance-poll-time", "1s",
		"--topo-information-refresh-duration", "3s",
		"--log-format", "text",
		"--alsologtostderr",
	)
	args = append(args, c.opts.vtorcArgs...)

	ctr, err := testcontainers.Run(ctx, c.image,
		testcontainers.WithCmd(args...),
		testcontainers.WithExposedPorts(vtorc.httpPort),
		network.WithNetwork([]string{vtorc.name}, c.network),
		testcontainers.WithEnv(map[string]string{"VTTEST": "endtoend"}),
		testcontainers.WithLogConsumers(c.newLogConsumer(vtorc.name)),
		testcontainers.WithWaitStrategyAndDeadline(defaultStartupTimeout,
			wait.ForHTTP("/debug/health").
				WithPort(vtorc.httpPort).
				WithStartupTimeout(defaultStartupTimeout).
				WithPollInterval(defaultPollInterval),
		),
	)
	if err != nil {
		return vterrors.Wrapf(err, "starting vtorc")
	}

	vtorc.setContainer(ctr)
	c.vtorc = vtorc
	return nil
}
