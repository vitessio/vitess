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
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Supervisor control paths inside tablet containers. The tablet entrypoint is
// a small supervisor loop so tests can stop, start and kill vttablet and
// mysqld inside a live container without losing its data or address.
const (
	supervisorDir         = vtDataRoot + "/supervisor"
	supervisorDesiredFile = supervisorDir + "/vttablet.desired"
	supervisorArgsFile    = supervisorDir + "/vttablet.args"
	supervisorPidFile     = supervisorDir + "/vttablet.pid"
	supervisorScriptPath  = containerFilesDir + "/supervisor.sh"
	tabletInitDBPath      = containerFilesDir + "/init_db.sql"
)

type (
	// TabletSpec describes one tablet before its container starts. A
	// WithTabletSpec customizer may mutate ExtraArgs and Files; the identity
	// fields are informational.
	TabletSpec struct {
		// UID is the numeric tablet id used in the tablet alias.
		UID int

		// Cell is the topology cell that owns the tablet alias.
		Cell string

		// Keyspace is the keyspace served by the tablet.
		Keyspace string

		// Shard is the shard served by the tablet.
		Shard string

		// Type is the initial tablet kind: "primary", "replica" or "rdonly".
		// Primaries start as replicas and are elected afterwards.
		Type string

		// ExtraArgs are appended to this tablet's vttablet command line after
		// cluster-wide and keyspace-wide extra args.
		ExtraArgs []string

		// Files are placed into this tablet's container before it starts, in
		// addition to cluster-wide WithTabletFiles.
		Files []ContainerFile
	}

	// Tablet is the runtime handle for one tablet container, holding both the
	// vttablet and its mysqld.
	Tablet struct {
		component

		// UID is the numeric tablet id used in the tablet alias.
		UID int

		// Cell is the topology cell that owns the tablet alias.
		Cell string

		// Keyspace is the keyspace served by the tablet.
		Keyspace string

		// Shard is the shard served by the tablet.
		Shard string

		typ string
	}
)

// Alias returns the tablet alias, e.g. "zone1-100".
func (t *Tablet) Alias() string {
	return fmt.Sprintf("%s-%d", t.Cell, t.UID)
}

// Type returns the tablet kind the framework started this tablet as:
// "primary", "replica" or "rdonly". It reflects the initial election, not
// later reparents.
func (t *Tablet) Type() string {
	return t.typ
}

// tabletDir is the tablet's data directory inside the container.
func (t *Tablet) tabletDir() string {
	return tabletDirForUID(t.UID)
}

func tabletDirForUID(uid int) string {
	return fmt.Sprintf("%s/vt_%010d", vtDataRoot, uid)
}

// MySQLAddr returns the host-reachable "host:port" of the tablet's mysqld.
func (t *Tablet) MySQLAddr(ctx context.Context) (string, error) {
	return t.hostAddr(ctx, fmt.Sprintf("%d/tcp", tabletMySQLPort))
}

// GRPCAddr returns the host-reachable "host:port" of the tablet's gRPC port.
func (t *Tablet) GRPCAddr(ctx context.Context) (string, error) {
	return t.hostAddr(ctx, fmt.Sprintf("%d/tcp", tabletGRPCPort))
}

// DBAConnParams returns connection parameters for a dba-level connection to
// the tablet's mysqld from the host.
func (t *Tablet) DBAConnParams(ctx context.Context, dbName string) (mysql.ConnParams, error) {
	addr, err := t.MySQLAddr(ctx)
	if err != nil {
		return mysql.ConnParams{}, err
	}
	return dbaConnParams(addr, dbName)
}

// dbaConnParams builds dba-level connection parameters for a host-reachable
// mysqld address.
func dbaConnParams(addr, dbName string) (mysql.ConnParams, error) {
	host, portStr, ok := strings.Cut(addr, ":")
	if !ok {
		return mysql.ConnParams{}, fmt.Errorf("malformed mysqld address %q", addr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return mysql.ConnParams{}, fmt.Errorf("malformed mysqld port in %q: %w", addr, err)
	}

	return mysql.ConnParams{
		Host:   host,
		Port:   port,
		Uname:  dbaUser,
		DbName: dbName,
	}, nil
}

// QueryTablet executes a query directly on the tablet's mysqld against the
// tablet's vt_<keyspace> database, bypassing vtgate and vttablet.
func (t *Tablet) QueryTablet(ctx context.Context, query string) (*sqltypes.Result, error) {
	return t.QueryTabletWithDB(ctx, query, "vt_"+t.Keyspace)
}

// QueryTabletWithDB executes a query against a specific database on the
// tablet's mysqld. Use dbName "" to run without selecting a database.
func (t *Tablet) QueryTabletWithDB(ctx context.Context, query, dbName string) (*sqltypes.Result, error) {
	params, err := t.DBAConnParams(ctx, dbName)
	if err != nil {
		return nil, err
	}

	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		return nil, fmt.Errorf("connecting to tablet %s mysqld: %w", t.Alias(), err)
	}
	defer conn.Close()

	return conn.ExecuteFetch(query, 10000, true)
}

// TabletProto returns the tablet's topology record with Hostname and the
// grpc/vt ports rewritten to host-reachable values, so tmclient calls from
// the test process work unchanged.
func (t *Tablet) TabletProto(ctx context.Context) (*topodatapb.Tablet, error) {
	record, err := t.cluster.Vtctld().getTablet(ctx, t.Alias())
	if err != nil {
		return nil, err
	}

	grpcAddr, err := t.GRPCAddr(ctx)
	if err != nil {
		return nil, err
	}
	httpAddr, err := t.HTTPAddr(ctx)
	if err != nil {
		return nil, err
	}

	host, grpcPort, ok := strings.Cut(grpcAddr, ":")
	if !ok {
		return nil, fmt.Errorf("malformed grpc address %q", grpcAddr)
	}
	_, httpPort, _ := strings.Cut(httpAddr, ":")

	record.Hostname = host
	record.PortMap["grpc"], err = parsePort(grpcPort)
	if err != nil {
		return nil, err
	}
	record.PortMap["vt"], err = parsePort(httpPort)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func parsePort(s string) (int32, error) {
	port, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parsing port %q: %w", s, err)
	}
	return int32(port), nil
}

// KillContainer stops the tablet container immediately with SIGKILL. The
// data directory is on a tmpfs, so a later StartContainer boots from a fresh
// mysqlctl init.
func (t *Tablet) KillContainer(ctx context.Context) error {
	timeout := time.Duration(0)
	return t.container().Stop(ctx, &timeout)
}

// TabletDir is the tablet's data directory inside its container.
func (t *Tablet) TabletDir() string {
	return tabletDirForUID(t.UID)
}

// MySQLSocket is the path to the tablet's mysqld socket inside its container.
func (t *Tablet) MySQLSocket() string {
	return t.TabletDir() + "/mysql.sock"
}

// MysqlctldGRPCAddr returns the host-reachable "host:port" of the tablet's
// mysqlctld gRPC endpoint. It is available only on clusters started with
// WithMysqlctld.
func (t *Tablet) MysqlctldGRPCAddr(ctx context.Context) (string, error) {
	if !t.cluster.opts.mysqlctld {
		return "", fmt.Errorf("tablet %s has no mysqlctld, use WithMysqlctld", t.Alias())
	}
	return t.hostAddr(ctx, fmt.Sprintf("%d/tcp", tabletMysqlctldGRPCPort))
}

// Remove shuts the tablet's vttablet down, terminates its container and
// removes the tablet from the cluster's and its shard's bookkeeping. The
// topology record is untouched; tests delete it through vtctldclient when
// needed.
//
// vttablet gets a chance to shut down gracefully because that is when it
// prunes the hostname and ports from its own topology record. A record that
// keeps them after the container is gone still advertises an address that
// nothing answers, and the next tablet to start in the shard blocks
// indefinitely initializing replication against it.
func (t *Tablet) Remove(ctx context.Context) error {
	c := t.cluster

	if t.IsRunning() {
		if err := t.StopVttablet(ctx); err != nil {
			c.logf("shutting vttablet %s down before removing it: %v", t.Alias(), err)
		}
	}

	c.mu.Lock()
	c.tablets = slices.DeleteFunc(c.tablets, func(other *Tablet) bool { return other == t })
	c.mu.Unlock()

	if ks := c.Keyspace(t.Keyspace); ks != nil {
		if sh := ks.Shard(t.Shard); sh != nil {
			sh.mu.Lock()
			sh.replicas = slices.DeleteFunc(sh.replicas, func(other *Tablet) bool { return other == t })
			sh.rdonly = slices.DeleteFunc(sh.rdonly, func(other *Tablet) bool { return other == t })
			if sh.primary == t {
				sh.primary = nil
			}
			sh.mu.Unlock()
		}
	}

	return t.terminate(ctx)
}

// StopVttablet gracefully stops the vttablet process inside the live
// container, leaving mysqld and the container running.
func (t *Tablet) StopVttablet(ctx context.Context) error {
	return t.signalVttablet(ctx, "TERM")
}

// KillVttablet kills the vttablet process inside the live container with
// SIGKILL, leaving mysqld and the container running.
func (t *Tablet) KillVttablet(ctx context.Context) error {
	return t.signalVttablet(ctx, "KILL")
}

func (t *Tablet) signalVttablet(ctx context.Context, signal string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultOperationTimeout)
	defer cancel()

	// Setting the desired state first stops the supervisor from restarting
	// the process after it exits.
	if err := t.writeControlFile(ctx, supervisorDesiredFile, "stop\n"); err != nil {
		return err
	}

	// The kill may race the supervisor reaping a vttablet that already
	// exited on its own; the desired end state holds either way, and
	// waitVttabletGone confirms it.
	script := fmt.Sprintf(`if [[ -f %[1]s ]]; then kill -%[2]s "$(cat %[1]s)" 2>/dev/null || true; fi`, supervisorPidFile, signal)
	if _, err := mustExec(ctx, t.container(), []string{"bash", "-c", script}); err != nil {
		return fmt.Errorf("signaling vttablet on %s: %w", t.Alias(), err)
	}

	return t.waitVttabletGone(ctx)
}

// waitVttabletGone polls until the supervisor reaps the vttablet process.
func (t *Tablet) waitVttabletGone(ctx context.Context) error {
	for {
		exitCode, _, err := containerExec(ctx, t.container(), []string{"test", "!", "-f", supervisorPidFile})
		if err != nil {
			return fmt.Errorf("waiting for vttablet on %s to exit: %w", t.Alias(), err)
		}
		if exitCode == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("vttablet on %s did not exit: %w", t.Alias(), ctx.Err())
		case <-time.After(defaultPollInterval):
		}
	}
}

// StartVttablet starts the vttablet process inside the live container after a
// StopVttablet or KillVttablet, with the data directory intact. When
// extraArgs are given they replace the tablet's previous extra args, so
// tests can restart a tablet with new flags. It blocks until the tablet
// reports SERVING or NOT_SERVING.
func (t *Tablet) StartVttablet(ctx context.Context, extraArgs ...string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultOperationTimeout)
	defer cancel()

	if len(extraArgs) > 0 {
		if err := t.writeControlFile(ctx, supervisorArgsFile, strings.Join(extraArgs, "\n")+"\n"); err != nil {
			return err
		}
	}
	if err := t.writeControlFile(ctx, supervisorDesiredFile, "run\n"); err != nil {
		return err
	}

	return t.WaitForTabletStatus(ctx, defaultOperationTimeout, "SERVING", "NOT_SERVING")
}

func (t *Tablet) writeControlFile(ctx context.Context, path, content string) error {
	if err := writeContainerFile(ctx, t.container(), path, content); err != nil {
		return fmt.Errorf("writing %s on %s: %w", path, t.Alias(), err)
	}
	return nil
}

// FreezeVttablet pauses the vttablet process with SIGSTOP, so it stays alive
// but stops responding, without the supervisor restarting it.
func (t *Tablet) FreezeVttablet(ctx context.Context) error {
	script := fmt.Sprintf(`kill -STOP "$(cat %s)"`, supervisorPidFile)
	if _, err := mustExec(ctx, t.container(), []string{"bash", "-c", script}); err != nil {
		return fmt.Errorf("freezing vttablet on %s: %w", t.Alias(), err)
	}
	return nil
}

// UnfreezeVttablet resumes a vttablet paused by FreezeVttablet.
func (t *Tablet) UnfreezeVttablet(ctx context.Context) error {
	script := fmt.Sprintf(`kill -CONT "$(cat %s)"`, supervisorPidFile)
	if _, err := mustExec(ctx, t.container(), []string{"bash", "-c", script}); err != nil {
		return fmt.Errorf("unfreezing vttablet on %s: %w", t.Alias(), err)
	}
	return nil
}

// DisconnectNetwork detaches the tablet's container from the cluster network,
// simulating a network partition. The container and its processes keep
// running.
func (t *Tablet) DisconnectNetwork(ctx context.Context) error {
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return fmt.Errorf("creating docker client: %w", err)
	}
	defer cli.Close()

	_, err = cli.NetworkDisconnect(ctx, t.cluster.network.ID, mobyclient.NetworkDisconnectOptions{
		Container: t.container().GetContainerID(),
		Force:     true,
	})
	if err != nil {
		return fmt.Errorf("disconnecting %s from the cluster network: %w", t.Alias(), err)
	}
	return nil
}

// ReconnectNetwork reattaches the tablet's container to the cluster network
// with its original alias.
func (t *Tablet) ReconnectNetwork(ctx context.Context) error {
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return fmt.Errorf("creating docker client: %w", err)
	}
	defer cli.Close()

	_, err = cli.NetworkConnect(ctx, t.cluster.network.ID, mobyclient.NetworkConnectOptions{
		Container: t.container().GetContainerID(),
		EndpointConfig: &mobynetwork.EndpointSettings{
			Aliases: []string{t.name},
		},
	})
	if err != nil {
		return fmt.Errorf("reconnecting %s to the cluster network: %w", t.Alias(), err)
	}
	return nil
}

// StopMySQL shuts the tablet's mysqld down gracefully, leaving vttablet and
// the container running.
func (t *Tablet) StopMySQL(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, defaultOperationTimeout)
	defer cancel()

	cmd := []string{
		"mysqlctl",
		"--tablet-uid", strconv.Itoa(t.UID),
		"--mysql-port", strconv.Itoa(tabletMySQLPort),
		"--log-format", "text",
		"shutdown",
	}
	if _, err := mustExec(ctx, t.container(), cmd); err != nil {
		return fmt.Errorf("stopping mysqld on %s: %w", t.Alias(), err)
	}
	return nil
}

// StartMySQL starts the tablet's mysqld again after StopMySQL or KillMySQL.
// When the data directory is still present it starts mysqld from it, and when
// the data directory is absent it re-initializes mysqld from the init SQL, so
// a test can wipe the data directory and bring mysqld back cleanly. This
// matches the tablet supervisor's own boot behavior.
func (t *Tablet) StartMySQL(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, defaultOperationTimeout)
	defer cancel()

	script := fmt.Sprintf(`if [[ -d %[1]s ]]; then
  mysqlctl --tablet-uid %[2]d --mysql-port %[3]d --log-format text start
else
  mysqlctl --tablet-uid %[2]d --mysql-port %[3]d --log-format text init --init-db-sql-file %[4]s
fi`, t.tabletDir(), t.UID, tabletMySQLPort, tabletInitDBPath)

	if _, err := mustExec(ctx, t.container(), []string{"bash", "-c", script}); err != nil {
		return fmt.Errorf("starting mysqld on %s: %w", t.Alias(), err)
	}
	return nil
}

// KillMySQL kills the tablet's mysqld with SIGKILL for crash tests, leaving
// vttablet and the container running.
func (t *Tablet) KillMySQL(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, defaultOperationTimeout)
	defer cancel()

	script := fmt.Sprintf(`kill -9 "$(cat %s/mysql.pid)"`, t.tabletDir())
	if _, err := mustExec(ctx, t.container(), []string{"bash", "-c", script}); err != nil {
		return fmt.Errorf("killing mysqld on %s: %w", t.Alias(), err)
	}
	return nil
}

// WaitForTabletStatus polls the tablet's /debug/vars until TabletStateName is
// one of the wanted states.
func (t *Tablet) WaitForTabletStatus(ctx context.Context, timeout time.Duration, states ...string) error {
	_, _, err := t.MakeAPICallRetry(ctx, "/debug/vars", timeout, func(status int, body string) bool {
		return status == 200 && matchesDebugVar(body, "TabletStateName", states)
	})
	if err != nil {
		return fmt.Errorf("tablet %s did not reach state %v: %w", t.Alias(), states, err)
	}
	return nil
}

// WaitForTabletType polls the tablet's /debug/vars until TabletType is one of
// the wanted types (e.g. "primary", "replica", "rdonly").
func (t *Tablet) WaitForTabletType(ctx context.Context, timeout time.Duration, types ...string) error {
	_, _, err := t.MakeAPICallRetry(ctx, "/debug/vars", timeout, func(status int, body string) bool {
		return status == 200 && matchesDebugVar(body, "TabletType", types)
	})
	if err != nil {
		return fmt.Errorf("tablet %s did not reach type %v: %w", t.Alias(), types, err)
	}
	return nil
}

// matchesDebugVar decodes a /debug/vars body and reports whether the named
// string variable equals one of the wanted values.
func matchesDebugVar(body, name string, wanted []string) bool {
	var vars map[string]any
	if err := json.Unmarshal([]byte(body), &vars); err != nil {
		return false
	}
	value, ok := vars[name].(string)
	if !ok {
		return false
	}
	return slices.Contains(wanted, value)
}

// startTablet starts one tablet container and waits for the tablet to report
// SERVING or NOT_SERVING.
func (c *Cluster) startTablet(ctx context.Context, spec *TabletSpec) (*Tablet, error) {
	alias := c.name(fmt.Sprintf("vttablet-%d", spec.UID))

	t := &Tablet{
		component: component{
			name:     alias,
			httpPort: fmt.Sprintf("%d/tcp", tabletHTTPPort),
			cluster:  c,
		},
		UID:      spec.UID,
		Cell:     spec.Cell,
		Keyspace: spec.Keyspace,
		Shard:    spec.Shard,
		typ:      spec.Type,
	}

	initDBSQL, err := c.tabletInitDBSQL(spec.Keyspace)
	if err != nil {
		return nil, fmt.Errorf("assembling init_db.sql for tablet %s: %w", t.Alias(), err)
	}

	files := []ContainerFile{
		{Content: []byte(initDBSQL), ContainerPath: tabletInitDBPath},
		{Content: []byte(c.supervisorScript(spec, alias)), ContainerPath: supervisorScriptPath, Mode: 0o755},
	}
	files = append(files, c.opts.tabletFiles...)
	files = append(files, spec.Files...)

	filesOpt, err := withContainerFiles(files)
	if err != nil {
		return nil, fmt.Errorf("preparing files for tablet %s: %w", t.Alias(), err)
	}

	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithEntrypoint("bash", supervisorScriptPath),
		testcontainers.WithExposedPorts(
			fmt.Sprintf("%d/tcp", tabletHTTPPort),
			fmt.Sprintf("%d/tcp", tabletGRPCPort),
			fmt.Sprintf("%d/tcp", tabletMySQLPort),
		),
		network.WithNetwork([]string{alias}, c.network),
		testcontainers.WithTmpfs(map[string]string{vtDataRoot: "uid=999,gid=999"}),
		testcontainers.WithEnv(mergeEnv(map[string]string{"VTTEST": "endtoend"}, c.opts.tabletEnv)),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			// An init process reaps the mysqld and vttablet processes the
			// supervisor loop leaves behind when tests kill them.
			initTrue := true
			hc.Init = &initTrue
		}),
		filesOpt,
		testcontainers.WithLogConsumers(c.newLogConsumer(alias)),
		testcontainers.WithWaitStrategyAndDeadline(
			tabletStartupTimeout,
			wait.ForHTTP("/debug/vars").
				WithPort(t.httpPort).
				WithStartupTimeout(tabletStartupTimeout).
				WithPollInterval(defaultPollInterval).
				WithResponseMatcher(func(body io.Reader) bool {
					data, err := io.ReadAll(body)
					if err != nil {
						return false
					}
					return matchesDebugVar(string(data), "TabletStateName", []string{"SERVING", "NOT_SERVING"})
				}),
		),
	}
	if c.opts.backupStorage {
		opts = append(opts, c.backupMount())
	}
	if c.opts.mysqlctld {
		opts = append(opts, testcontainers.WithExposedPorts(fmt.Sprintf("%d/tcp", tabletMysqlctldGRPCPort)))
	}

	ctr, err := testcontainers.Run(ctx, c.vttabletImage(spec.Keyspace), opts...)
	if err != nil {
		return nil, fmt.Errorf("starting tablet %s: %w", t.Alias(), err)
	}

	t.setContainer(ctr)
	return t, nil
}

// vttabletBaseArgs builds the vttablet command line the supervisor runs.
func (c *Cluster) vttabletBaseArgs(spec *TabletSpec, alias string) []string {
	initTabletType := "replica"
	if spec.Type == "rdonly" {
		initTabletType = "rdonly"
	}

	args := c.TopoFlags()
	args = append(
		args,
		"--tablet-path", fmt.Sprintf("%s-%d", spec.Cell, spec.UID),
		"--port", strconv.Itoa(tabletHTTPPort),
		"--grpc-port", strconv.Itoa(tabletGRPCPort),
		"--init-keyspace", spec.Keyspace,
		"--init-shard", spec.Shard,
		"--init-tablet-type", initTabletType,
		"--tablet-hostname", alias,
		"--health-check-interval", "5s",
		"--enable-replication-reporter",
		"--service-map", "grpc-queryservice,grpc-tabletmanager,grpc-updatestream,grpc-throttler",
		"--db-charset", "utf8mb4",
		"--log-format", "text",
		"--alsologtostderr",
	)

	// A tablet only restores when the cluster has backup storage: without it
	// vttablet fails to start, having no backup storage implementation to
	// restore from.
	if c.opts.backupStorage {
		args = append(args, "--restore-from-backup")
		args = append(args, c.backupFlags()...)
	}
	return args
}

// supervisorScript renders the tablet container's entrypoint: init or start
// mysqld, then keep vttablet running whenever the desired-state file says
// "run". Tests drive it through the control files in /vt/vtdataroot/supervisor.
func (c *Cluster) supervisorScript(spec *TabletSpec, alias string) string {
	baseArgs := shellQuoteAll(c.vttabletBaseArgs(spec, alias))

	var extra []string
	extra = append(extra, c.opts.vttabletArgs...)
	extra = append(extra, c.keyspaceTabletArgs(spec.Keyspace)...)
	extra = append(extra, spec.ExtraArgs...)
	extraArgs := shellQuoteAll(extra)

	// mysqlctld manages mysqld as a daemon, so tests can stop and start it
	// through its socket instead of running mysqlctl per operation.
	mysqldStart := fmt.Sprintf(`if [[ -d %[1]s ]]; then
  mysqlctl --tablet-uid %[2]d --mysql-port %[3]d --log-format text start
else
  mysqlctl --tablet-uid %[2]d --mysql-port %[3]d --log-format text init --init-db-sql-file %[4]s
fi`, tabletDirForUID(spec.UID), spec.UID, tabletMySQLPort, tabletInitDBPath)

	// mysqld answers the socket before mysqlctld has applied the init SQL, so the
	// gate is a query as vt_dba: the user the init SQL creates and vttablet
	// connects as.
	if c.opts.mysqlctld {
		mysqldStart = fmt.Sprintf(`mysqlctld --tablet-uid %[1]d --mysql-port %[2]d --grpc-port %[5]d --log-format text --init-db-sql-file %[3]s &
until mysql --socket %[4]s --user vt_dba --execute 'select 1' >/dev/null 2>&1; do sleep 0.2; done`,
			spec.UID, tabletMySQLPort, tabletInitDBPath, tabletDirForUID(spec.UID)+"/mysql.sock", tabletMysqlctldGRPCPort)
	}

	return fmt.Sprintf(
		`#!/bin/bash
# Tablet supervisor for %[1]s, generated by the vitesst framework.
set -u

# The umask a packaged MySQL server runs under: mysqld_safe sets it before it
# starts mysqld. Every process below the supervisor inherits it, so the files
# that mysqld, mysqlctl and the backup tools create in the data directory are
# not world readable.
umask 0007

mkdir -p %[2]s
if [[ ! -f %[3]s ]]; then
  echo run > %[3]s
fi

%[12]s

base_args=(%[8]s)
initial_extra_args=(%[9]s)

while true; do
  desired=$(cat %[3]s 2>/dev/null || echo run)
  if [[ "$desired" == run && ! -f %[10]s ]]; then
    args=("${base_args[@]}")
    if [[ -f %[11]s ]]; then
      readarray -t override_args < %[11]s
      args+=("${override_args[@]}")
    else
      args+=("${initial_extra_args[@]}")
    fi
    echo "vitesst-supervisor: starting vttablet ${args[*]}"
    vttablet "${args[@]}" &
    echo $! > %[10]s
    wait $! || true
    echo "vitesst-supervisor: vttablet exited"
    rm -f %[10]s
  fi
  sleep 0.2
done
`,
		alias,
		supervisorDir,
		supervisorDesiredFile,
		tabletDirForUID(spec.UID),
		spec.UID,
		tabletMySQLPort,
		tabletInitDBPath,
		strings.Join(baseArgs, " "),
		strings.Join(extraArgs, " "),
		supervisorPidFile,
		supervisorArgsFile,
		mysqldStart,
	)
}

// vttabletImage returns the Docker image a keyspace's tablets run: the one the
// keyspace was configured with, then the VITESST_VTTABLET_IMAGE override, then
// the cluster image. This image also carries mysqlctl and mysqlctld.
func (c *Cluster) vttabletImage(keyspace string) string {
	if kc := c.keyspaceConfig(keyspace); kc != nil && kc.image != "" {
		return kc.image
	}
	if image := os.Getenv("VITESST_VTTABLET_IMAGE"); image != "" {
		return image
	}
	return c.image
}

// keyspaceTabletArgs returns the configured per-keyspace tablet args.
func (c *Cluster) keyspaceTabletArgs(keyspace string) []string {
	for i := range c.opts.keyspaces {
		if c.opts.keyspaces[i].name == keyspace {
			return c.opts.keyspaces[i].tabletArgs
		}
	}
	return nil
}

// shellQuote single-quotes a string for safe embedding in a bash script.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// shellQuoteAll single-quotes every argument for safe embedding in a bash
// script.
func shellQuoteAll(args []string) []string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, shellQuote(arg))
	}
	return quoted
}
