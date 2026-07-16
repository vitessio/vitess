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
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
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
func (c *Cluster) AddVTOrc(t testing.TB, ctx context.Context, cell string, extraArgs ...string) (*VTOrc, error) {
	if cell == "" {
		cell = c.firstCell()
	}

	c.mu.Lock()
	index := c.vtorcSeq
	c.vtorcSeq++
	c.mu.Unlock()

	name := c.name("vtorc")
	if index > 0 {
		name = c.name(fmt.Sprintf("vtorc-%d", index+1))
	}

	args := withVTOrcPollArgs(extraArgs)
	v := &VTOrc{
		component: component{
			name:     name,
			httpPort: fmt.Sprintf("%d/tcp", vtorcHTTPPort),
			cluster:  c,
		},
		cell:      cell,
		extraArgs: args,
	}

	ctr, err := c.runVTOrcContainer(t, ctx, name, cell, args)
	if err != nil {
		return nil, fmt.Errorf("starting %s: %w", name, err)
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
		return fmt.Errorf("%s has no container", v.name)
	}
	return writeContainerFile(ctx, ctr, vtorcConfigPath, content)
}

// DisableGlobalRecoveries stops this VTOrc from running recoveries, so tests
// can change the topology without VTOrc reacting to the intermediate states.
func (v *VTOrc) DisableGlobalRecoveries(ctx context.Context) error {
	return v.callRecoveriesAPI(ctx, "/api/disable-global-recoveries")
}

// EnableGlobalRecoveries lets this VTOrc run recoveries again.
func (v *VTOrc) EnableGlobalRecoveries(ctx context.Context) error {
	return v.callRecoveriesAPI(ctx, "/api/enable-global-recoveries")
}

// callRecoveriesAPI calls one of the global-recovery endpoints and checks that
// VTOrc accepted the change.
func (v *VTOrc) callRecoveriesAPI(ctx context.Context, path string) error {
	status, body, err := v.MakeAPICall(ctx, path)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("%s %s returned status %d: %s", v.name, path, status, body)
	}
	return nil
}

// Restart recreates the VTOrc container behind this handle with the same
// network alias and a fresh config file. When extraArgs are given they
// replace the previous extra args.
func (v *VTOrc) Restart(t testing.TB, ctx context.Context, extraArgs ...string) error {
	v.argsMu.Lock()
	if len(extraArgs) > 0 {
		v.extraArgs = withVTOrcPollArgs(extraArgs)
	}
	args := v.extraArgs
	v.argsMu.Unlock()
	return v.restart(t, ctx, args)
}

// RestartWithBuiltinConfig recreates the VTOrc container with no command-line
// flags beyond the ones VTOrc needs to reach the cluster, so that every
// configuration value reported by /api/config is VTOrc's own default.
func (v *VTOrc) RestartWithBuiltinConfig(t testing.TB, ctx context.Context) error {
	v.argsMu.Lock()
	v.extraArgs = nil
	v.argsMu.Unlock()
	return v.restart(t, ctx, nil)
}

// restart terminates the current container and runs a new one with the given
// extra args.
func (v *VTOrc) restart(t testing.TB, ctx context.Context, args []string) error {
	v.argsMu.Lock()
	cell := v.cell
	v.argsMu.Unlock()

	old := v.setContainer(nil)
	if old != nil {
		if err := testcontainers.TerminateContainer(old, testcontainers.StopContext(ctx), testcontainers.StopTimeout(0)); err != nil {
			return fmt.Errorf("terminating %s for restart: %w", v.name, err)
		}
	}

	ctr, err := v.cluster.runVTOrcContainer(t, ctx, v.name, cell, args)
	if err != nil {
		return fmt.Errorf("restarting %s: %w", v.name, err)
	}
	v.setContainer(ctr)
	return nil
}

// withVTOrcPollArgs returns the extra args with the poll intervals a test
// cluster wants, tightened from VTOrc's defaults so recoveries happen quickly.
// Args the caller already set are left alone.
func withVTOrcPollArgs(extraArgs []string) []string {
	args := make([]string, 0, len(extraArgs)+4)
	if !argsContain(extraArgs, "instance-poll-time") {
		args = append(args, "--instance-poll-time", "1s")
	}
	if !argsContain(extraArgs, "topo-information-refresh-duration") {
		args = append(args, "--topo-information-refresh-duration", "3s")
	}
	return append(args, extraArgs...)
}

// runVTOrcContainer starts one VTOrc container with the given network alias,
// cell, and extra args.
func (c *Cluster) runVTOrcContainer(t testing.TB, ctx context.Context, name, cell string, extraArgs []string) (testcontainers.Container, error) {
	args := []string{"vtorc"}
	args = append(args, c.TopoFlags()...)
	args = append(
		args,
		"--cell", cell,
		"--config-file", vtorcConfigPath,
		"--port", strconv.Itoa(vtorcHTTPPort),
		"--log-format", "text",
		"--alsologtostderr",
	)
	args = append(args, extraArgs...)

	// The config file is staged world-writable: files are copied in as root,
	// and WriteConfig overwrites this path by exec as the vitess user.
	filesOpt, err := withContainerFiles([]ContainerFile{
		{Content: []byte("{}\n"), ContainerPath: vtorcConfigPath, Mode: 0o666},
	})
	if err != nil {
		return nil, fmt.Errorf("preparing files for %s: %w", name, err)
	}

	return testcontainers.Run(
		ctx, c.image,
		testcontainers.WithCmd(args...),
		testcontainers.WithExposedPorts(fmt.Sprintf("%d/tcp", vtorcHTTPPort)),
		network.WithNetwork([]string{name}, c.network),
		testcontainers.WithEnv(map[string]string{"VTTEST": "endtoend"}),
		filesOpt,
		testcontainers.WithLogConsumers(c.newFileLogConsumer(t, name)),
		testcontainers.WithWaitStrategyAndDeadline(
			defaultStartupTimeout,
			wait.ForHTTP("/debug/health").
				WithPort(fmt.Sprintf("%d/tcp", vtorcHTTPPort)).
				WithStartupTimeout(defaultStartupTimeout).
				WithPollInterval(defaultPollInterval),
		),
	)
}
