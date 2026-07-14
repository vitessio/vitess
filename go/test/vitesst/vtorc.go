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
	"sync"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// vtorcConfigPath is the VTOrc config file inside its container. The vtorc
// watches it, so WriteConfig changes apply to the running process.
const vtorcConfigPath = containerFilesDir + "/vtorc.json"

type (
	// VTOrc is the runtime handle for one VTOrc container. Restart swaps the
	// container behind the handle.
	VTOrc struct {
		component

		argsMu    sync.Mutex
		cell      string
		extraArgs []string
	}
)

// VTOrc returns the cluster's first VTOrc, or nil when none is running.
func (c *Cluster) VTOrc() *VTOrc {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.vtorcs) == 0 {
		return nil
	}
	return c.vtorcs[0]
}

// VTOrcs returns all of the cluster's VTOrc instances.
func (c *Cluster) VTOrcs() []*VTOrc {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*VTOrc, len(c.vtorcs))
	copy(out, c.vtorcs)
	return out
}

// AddVTOrc starts a VTOrc instance on the running cluster in the given cell
// ("" means the first cell), with optional extra arguments.
func (c *Cluster) AddVTOrc(ctx context.Context, cell string, extraArgs ...string) (*VTOrc, error) {
	if cell == "" {
		cell = c.cells[0]
	}

	c.mu.Lock()
	index := c.vtorcSeq
	c.vtorcSeq++
	c.mu.Unlock()

	name := "vtorc"
	if index > 0 {
		name = fmt.Sprintf("vtorc-%d", index+1)
	}

	v := &VTOrc{
		component: component{
			name:     name,
			httpPort: fmt.Sprintf("%d/tcp", vtorcHTTPPort),
			cluster:  c,
		},
		cell:      cell,
		extraArgs: extraArgs,
	}

	ctr, err := c.runVTOrcContainer(ctx, name, cell, extraArgs)
	if err != nil {
		return nil, vterrors.Wrapf(err, "starting %s", name)
	}
	v.setContainer(ctr)

	c.mu.Lock()
	c.vtorcs = append(c.vtorcs, v)
	c.mu.Unlock()
	return v, nil
}

// WriteConfig replaces the VTOrc's watched config file, so the running vtorc
// hot-reloads the new values. Poll /debug/config through MakeAPICall to
// observe the reload.
func (v *VTOrc) WriteConfig(ctx context.Context, content string) error {
	ctr := v.container()
	if ctr == nil {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s has no container", v.name)
	}
	return writeContainerFile(ctx, ctr, vtorcConfigPath, content)
}

// Restart recreates the VTOrc container behind this handle with the same
// network alias and a fresh config file. When extraArgs are given they
// replace the previous extra args.
func (v *VTOrc) Restart(ctx context.Context, extraArgs ...string) error {
	v.argsMu.Lock()
	if len(extraArgs) > 0 {
		v.extraArgs = extraArgs
	}
	args := v.extraArgs
	cell := v.cell
	v.argsMu.Unlock()

	old := v.setContainer(nil)
	if old != nil {
		if err := testcontainers.TerminateContainer(old, testcontainers.StopContext(ctx), testcontainers.StopTimeout(0)); err != nil {
			return vterrors.Wrapf(err, "terminating %s for restart", v.name)
		}
	}

	ctr, err := v.cluster.runVTOrcContainer(ctx, v.name, cell, args)
	if err != nil {
		return vterrors.Wrapf(err, "restarting %s", v.name)
	}
	v.setContainer(ctr)
	return nil
}

// runVTOrcContainer starts one VTOrc container with the given network alias,
// cell, and extra args.
func (c *Cluster) runVTOrcContainer(ctx context.Context, name, cell string, extraArgs []string) (testcontainers.Container, error) {
	args := []string{"vtorc"}
	args = append(args, c.topoFlags()...)
	args = append(args,
		"--cell", cell,
		"--config-file", vtorcConfigPath,
		"--port", strconv.Itoa(vtorcHTTPPort),
		"--log-format", "text",
		"--alsologtostderr",
	)
	if !argsContain(extraArgs, "instance-poll-time") {
		args = append(args, "--instance-poll-time", "1s")
	}
	if !argsContain(extraArgs, "topo-information-refresh-duration") {
		args = append(args, "--topo-information-refresh-duration", "3s")
	}
	args = append(args, extraArgs...)

	// The config file is staged world-writable: files are copied in as root,
	// and WriteConfig overwrites this path by exec as the vitess user.
	filesOpt, err := withContainerFiles([]ContainerFile{
		{Content: []byte("{}\n"), ContainerPath: vtorcConfigPath, Mode: 0o666},
	})
	if err != nil {
		return nil, vterrors.Wrapf(err, "preparing files for %s", name)
	}

	return testcontainers.Run(ctx, c.image,
		testcontainers.WithCmd(args...),
		testcontainers.WithExposedPorts(fmt.Sprintf("%d/tcp", vtorcHTTPPort)),
		network.WithNetwork([]string{name}, c.network),
		testcontainers.WithEnv(map[string]string{"VTTEST": "endtoend"}),
		filesOpt,
		testcontainers.WithLogConsumers(c.newLogConsumer(name)),
		testcontainers.WithWaitStrategyAndDeadline(defaultStartupTimeout,
			wait.ForHTTP("/debug/health").
				WithPort(fmt.Sprintf("%d/tcp", vtorcHTTPPort)).
				WithStartupTimeout(defaultStartupTimeout).
				WithPollInterval(defaultPollInterval),
		),
	)
}
