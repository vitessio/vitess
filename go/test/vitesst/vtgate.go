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
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	// Imported for its side effect of registering the grpc vtgateconn dialer,
	// so DialVTGate works from any test package.
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

type (
	// VTGate is the runtime handle for one vtgate container. Restart swaps
	// the container behind the handle; address accessors re-resolve mapped
	// ports on every call.
	VTGate struct {
		component

		argsMu    sync.Mutex
		extraArgs []string
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

// ReadVSchema fetches and decodes the vtgate's /debug/vschema.
func (g *VTGate) ReadVSchema() (*any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	status, body, err := g.MakeAPICall(ctx, "/debug/vschema")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s /debug/vschema returned status %d", g.name, status)
	}

	var results any
	if err := json.Unmarshal([]byte(body), &results); err != nil {
		return nil, vterrors.Wrapf(err, "decoding %s /debug/vschema", g.name)
	}
	return &results, nil
}

// Restart recreates the vtgate container behind this handle with the same
// network alias. When extraArgs are given they replace the vtgate's previous
// extra args, so tests can restart vtgate with new flags. Mapped host ports
// change across a restart; use the address accessors to re-resolve them.
func (g *VTGate) Restart(ctx context.Context, extraArgs ...string) error {
	g.argsMu.Lock()
	if len(extraArgs) > 0 {
		g.extraArgs = extraArgs
	}
	args := g.extraArgs
	g.argsMu.Unlock()

	old := g.setContainer(nil)
	if old != nil {
		if err := testcontainers.TerminateContainer(old, testcontainers.StopContext(ctx), testcontainers.StopTimeout(0)); err != nil {
			return vterrors.Wrapf(err, "terminating %s for restart", g.name)
		}
	}

	ctr, err := g.cluster.runVTGateContainer(ctx, g.name, args)
	if err != nil {
		return vterrors.Wrapf(err, "restarting %s", g.name)
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

// VTGates returns all of the cluster's vtgates.
func (c *Cluster) VTGates() []*VTGate {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*VTGate, len(c.vtgates))
	copy(out, c.vtgates)
	return out
}

// AddVTGate starts an additional vtgate with its own network alias for
// multi-vtgate tests. The given extraArgs apply to it in place of the
// cluster-wide vtgate args.
func (c *Cluster) AddVTGate(ctx context.Context, extraArgs ...string) (*VTGate, error) {
	c.mu.Lock()
	index := c.vtgateSeq
	c.vtgateSeq++
	c.mu.Unlock()

	name := "vtgate"
	if index > 0 {
		name = fmt.Sprintf("vtgate-%d", index+1)
	}

	args := c.opts.vtgateArgs
	if len(extraArgs) > 0 {
		args = extraArgs
	}

	g := &VTGate{
		component: component{
			name:     name,
			httpPort: fmt.Sprintf("%d/tcp", vtgateHTTPPort),
			cluster:  c,
		},
		extraArgs: args,
	}

	ctr, err := c.runVTGateContainer(ctx, name, args)
	if err != nil {
		return nil, vterrors.Wrapf(err, "starting %s", name)
	}
	g.setContainer(ctr)

	c.mu.Lock()
	c.vtgates = append(c.vtgates, g)
	c.mu.Unlock()
	return g, nil
}

// runVTGateContainer starts one vtgate container with the given network alias
// and extra args.
func (c *Cluster) runVTGateContainer(ctx context.Context, name string, extraArgs []string) (testcontainers.Container, error) {
	args := []string{"vtgate"}
	args = append(args, c.topoFlags()...)
	args = append(args,
		"--cell", c.cells[0],
		"--cells-to-watch", strings.Join(c.cells, ","),
		"--port", strconv.Itoa(vtgateHTTPPort),
		"--grpc-port", strconv.Itoa(vtgateGRPCPort),
		"--mysql-server-port", strconv.Itoa(vtgateMySQLPort),
		"--mysql-auth-server-impl", "none",
		"--tablet-types-to-wait", "PRIMARY",
		"--service-map", "grpc-tabletmanager,grpc-throttler,grpc-queryservice,grpc-updatestream,grpc-vtctl,grpc-vtgateservice",
		"--log-format", "text",
	)
	if c.mysqlVersion != "" && !argsContain(extraArgs, "mysql-server-version") {
		args = append(args, "--mysql-server-version", c.mysqlVersion+"-vitess")
	}
	args = append(args, extraArgs...)

	filesOpt, err := withContainerFiles(c.opts.vtgateFiles)
	if err != nil {
		return nil, vterrors.Wrapf(err, "preparing files for %s", name)
	}

	return testcontainers.Run(ctx, c.image,
		testcontainers.WithCmd(args...),
		testcontainers.WithExposedPorts(
			fmt.Sprintf("%d/tcp", vtgateHTTPPort),
			fmt.Sprintf("%d/tcp", vtgateGRPCPort),
			fmt.Sprintf("%d/tcp", vtgateMySQLPort),
		),
		network.WithNetwork([]string{name}, c.network),
		testcontainers.WithEnv(map[string]string{"VTTEST": "endtoend"}),
		filesOpt,
		testcontainers.WithLogConsumers(c.newLogConsumer(name)),
		testcontainers.WithWaitStrategyAndDeadline(defaultStartupTimeout,
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
