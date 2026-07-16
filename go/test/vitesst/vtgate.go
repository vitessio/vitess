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
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

type (
	// VTGate is the runtime handle for one vtgate container. Restart swaps
	// the container behind the handle; address accessors re-resolve mapped
	// ports on every call.
	VTGate struct {
		component

		specMu sync.Mutex
		spec   VTGateSpec
	}

	// VTGateSpec describes a vtgate to start. An empty Cell places it in the
	// cluster's first cell, and an empty CellsToWatch makes it watch every
	// cell of the cluster. CellsToWatch also accepts a cell alias.
	VTGateSpec struct {
		Cell         string
		CellsToWatch []string
		ExtraArgs    []string
	}
)

// MySQLAddr returns the host-reachable "host:port" of the vtgate MySQL
// listener.
func (g *VTGate) MySQLAddr(ctx context.Context) (string, error) {
	return g.hostAddr(ctx, fmt.Sprintf("%d/tcp", vtgateMySQLPort))
}

// GRPCAddr returns the host-reachable "host:port" of the vtgate gRPC port.
func (g *VTGate) GRPCAddr(ctx context.Context) (string, error) {
	return g.hostAddr(ctx, fmt.Sprintf("%d/tcp", vtgateGRPCPort))
}

// DialVTGate returns a vtgateconn connected to this vtgate over gRPC.
func (g *VTGate) DialVTGate(ctx context.Context) (*vtgateconn.VTGateConn, error) {
	addr, err := g.GRPCAddr(ctx)
	if err != nil {
		return nil, err
	}
	return vtgateconn.DialProtocol(ctx, "grpc", addr)
}

// dialerSeq hands out a unique protocol name for each credentialed dialer, so
// concurrent DialVTGateAs calls with different credentials never share a
// registry entry.
var dialerSeq atomic.Uint64

// DialVTGateAs returns a vtgateconn connected to this vtgate over gRPC,
// authenticating as the given static-auth user. An empty username and password
// dials without credentials, so the vtgate rejects the unauthenticated client.
func (g *VTGate) DialVTGateAs(ctx context.Context, username, password string) (*vtgateconn.VTGateConn, error) {
	addr, err := g.GRPCAddr(ctx)
	if err != nil {
		return nil, err
	}

	creds := grpc.WithPerRPCCredentials(&grpcclient.StaticAuthClientCreds{Username: username, Password: password})
	protocol := fmt.Sprintf("grpc-auth-%d", dialerSeq.Add(1))
	vtgateconn.RegisterDialer(protocol, grpcvtgateconn.Dial(creds))
	return vtgateconn.DialProtocol(ctx, protocol, addr)
}

// ReadVSchema fetches and decodes the vtgate's /debug/vschema.
func (g *VTGate) ReadVSchema(ctx context.Context) (*any, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	status, body, err := g.MakeAPICall(ctx, "/debug/vschema")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("%s /debug/vschema returned status %d", g.name, status)
	}

	var results any
	if err := json.Unmarshal([]byte(body), &results); err != nil {
		return nil, fmt.Errorf("decoding %s /debug/vschema: %w", g.name, err)
	}
	return &results, nil
}

// WriteConfig replaces the vtgate's watched config file, so the running
// vtgate hot-reloads the new values. Poll /debug/config through MakeAPICall
// to observe the reload.
func (g *VTGate) WriteConfig(ctx context.Context, content string) error {
	ctr := g.container()
	if ctr == nil {
		return fmt.Errorf("%s has no container", g.name)
	}
	return writeContainerFile(ctx, ctr, vtgateConfigPath, content)
}

// QueryLog returns the vtgate's query log content so far.
func (g *VTGate) QueryLog(ctx context.Context) (string, error) {
	ctr := g.container()
	if ctr == nil {
		return "", fmt.Errorf("%s has no container", g.name)
	}
	_, output, err := containerExec(ctx, ctr, []string{"cat", vtgateQueryLogPath})
	if err != nil {
		return "", fmt.Errorf("reading %s query log: %w", g.name, err)
	}
	return output, nil
}

// Restart recreates the vtgate container behind this handle with the same
// network alias. When extraArgs are given they replace the vtgate's previous
// extra args, so tests can restart vtgate with new flags. Mapped host ports
// change across a restart; use the address accessors to re-resolve them.
func (g *VTGate) Restart(ctx context.Context, extraArgs ...string) error {
	g.specMu.Lock()
	if len(extraArgs) > 0 {
		g.spec.ExtraArgs = extraArgs
	}
	spec := g.spec
	g.specMu.Unlock()

	old := g.setContainer(nil)
	if old != nil {
		if err := testcontainers.TerminateContainer(old, testcontainers.StopContext(ctx), testcontainers.StopTimeout(0)); err != nil {
			return fmt.Errorf("terminating %s for restart: %w", g.name, err)
		}
	}

	ctr, err := g.cluster.runVTGateContainer(ctx, g.name, spec)
	if err != nil {
		return fmt.Errorf("restarting %s: %w", g.name, err)
	}
	g.setContainer(ctr)
	return nil
}

// VTGate returns the cluster's first vtgate.
func (c *Cluster) VTGate() *VTGate {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.vtgates) == 0 {
		return nil
	}
	return c.vtgates[0]
}

// AddVTGate starts an additional vtgate with its own network alias for
// multi-vtgate tests, in the cluster's first cell and watching every cell. The
// given extraArgs apply to it in place of the cluster-wide vtgate args.
func (c *Cluster) AddVTGate(ctx context.Context, extraArgs ...string) (*VTGate, error) {
	return c.AddVTGateSpec(ctx, VTGateSpec{ExtraArgs: extraArgs})
}

// AddVTGateSpec starts an additional vtgate with its own network alias, placed
// in the spec's cell and watching the spec's cells.
func (c *Cluster) AddVTGateSpec(ctx context.Context, spec VTGateSpec) (*VTGate, error) {
	if spec.Cell == "" {
		spec.Cell = c.firstCell()
	}
	if len(spec.CellsToWatch) == 0 {
		spec.CellsToWatch = c.cellNames()
	}
	if len(spec.ExtraArgs) == 0 {
		spec.ExtraArgs = c.opts.vtgateArgs
	}

	c.mu.Lock()
	index := c.vtgateSeq
	c.vtgateSeq++
	c.mu.Unlock()

	name := c.name("vtgate")
	if index > 0 {
		name = c.name(fmt.Sprintf("vtgate-%d", index+1))
	}

	g := &VTGate{
		component: component{
			name:     name,
			httpPort: fmt.Sprintf("%d/tcp", vtgateHTTPPort),
			cluster:  c,
		},
		spec: spec,
	}

	ctr, err := c.runVTGateContainer(ctx, name, spec)
	if err != nil {
		return nil, fmt.Errorf("starting %s: %w", name, err)
	}
	g.setContainer(ctr)

	c.mu.Lock()
	c.vtgates = append(c.vtgates, g)
	c.mu.Unlock()
	return g, nil
}

// runVTGateContainer starts one vtgate container with the given network alias,
// from a spec whose cell and watched cells are already resolved.
func (c *Cluster) runVTGateContainer(ctx context.Context, name string, spec VTGateSpec) (testcontainers.Container, error) {
	args := []string{"vtgate"}
	args = append(args, c.TopoFlags()...)
	args = append(
		args,
		"--config-file", vtgateConfigPath,
		"--log-queries-to-file", vtgateQueryLogPath,
		"--cell", spec.Cell,
		"--cells-to-watch", strings.Join(spec.CellsToWatch, ","),
		"--port", strconv.Itoa(vtgateHTTPPort),
		"--grpc-port", strconv.Itoa(vtgateGRPCPort),
		"--mysql-server-port", strconv.Itoa(vtgateMySQLPort),
		"--mysql-auth-server-impl", "none",
		"--tablet-types-to-wait", "PRIMARY",
		"--service-map", "grpc-tabletmanager,grpc-throttler,grpc-queryservice,grpc-updatestream,grpc-vtctl,grpc-vtgateservice",
		"--log-format", "text",
		"--alsologtostderr",
	)
	if c.mysqlVersion != "" && !argsContain(spec.ExtraArgs, "mysql-server-version") {
		args = append(args, "--mysql-server-version", c.mysqlVersion+"-vitess")
	}
	args = append(args, spec.ExtraArgs...)

	// The config file is staged world-writable: files are copied in as root,
	// and WriteConfig overwrites this path by exec as the vitess user.
	files := append([]ContainerFile{{Content: []byte("{}\n"), ContainerPath: vtgateConfigPath, Mode: 0o666}}, c.opts.vtgateFiles...)
	filesOpt, err := withContainerFiles(files)
	if err != nil {
		return nil, fmt.Errorf("preparing files for %s: %w", name, err)
	}

	return testcontainers.Run(
		ctx, c.vtgateImage(),
		testcontainers.WithCmd(args...),
		testcontainers.WithExposedPorts(
			fmt.Sprintf("%d/tcp", vtgateHTTPPort),
			fmt.Sprintf("%d/tcp", vtgateGRPCPort),
			fmt.Sprintf("%d/tcp", vtgateMySQLPort),
		),
		network.WithNetwork([]string{name}, c.network),
		testcontainers.WithEnv(mergeEnv(map[string]string{"VTTEST": "endtoend"}, c.opts.vtgateEnv)),
		filesOpt,
		testcontainers.WithLogConsumers(c.newLogConsumer(name)),
		testcontainers.WithWaitStrategyAndDeadline(
			defaultStartupTimeout,
			wait.ForHTTP("/debug/vars").
				WithPort(fmt.Sprintf("%d/tcp", vtgateHTTPPort)).
				WithStartupTimeout(defaultStartupTimeout).
				WithPollInterval(defaultPollInterval),
		),
	)
}

// argsContain reports whether any arg mentions the given flag name.
func argsContain(args []string, flag string) bool {
	for _, arg := range args {
		if strings.Contains(arg, flag) {
			return true
		}
	}
	return false
}
