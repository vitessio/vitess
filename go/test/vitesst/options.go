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
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/testcontainers/testcontainers-go"
)

type (
	// clusterOptions holds all validated cluster configuration.
	clusterOptions struct {
		// keyspaces lists the keyspaces StartCluster creates.
		keyspaces []keyspaceConfig

		// cells lists the topology cells; tablets are distributed across them
		// round-robin.
		cells []string

		// vtgateArgs are appended to the vtgate command line.
		vtgateArgs []string

		// vttabletArgs are appended to every vttablet command line.
		vttabletArgs []string

		// vtctldArgs are appended to the vtctld command line.
		vtctldArgs []string

		// vtorcEnabled records whether the test requested a VTOrc container.
		vtorcEnabled bool

		// vtorcArgs are appended to the VTOrc command line.
		vtorcArgs []string

		// vtadminEnabled records whether the test requested a vtadmin container.
		vtadminEnabled bool

		// vtadminArgs are appended to the vtadmin command line.
		vtadminArgs []string

		// vtadminClusterID overrides vtadmin's cluster identifier.
		vtadminClusterID string

		// mysqlVersion selects the Docker image tag used for Vitess components.
		mysqlVersion string

		// topoFlavor selects the topology server: etcd2, consul, or zk2.
		topoFlavor string

		// tabletEnv is merged into every tablet container's environment.
		tabletEnv map[string]string

		// vtgateEnv is merged into every vtgate container's environment.
		vtgateEnv map[string]string

		// vtctldFiles are placed into the vtctld container before start.
		vtctldFiles []ContainerFile

		// withoutVTGate skips starting a vtgate during cluster start.
		withoutVTGate bool

		// backupStorage gives the cluster a shared file backup storage volume.
		backupStorage bool

		// mysqlctld runs each tablet's mysqld under the mysqlctld daemon.
		mysqlctld bool

		// borrowedNetwork, when set, is used instead of creating a network,
		// and is not removed at teardown.
		borrowedNetwork *testcontainers.DockerNetwork

		// namePrefix is prepended to every component's network alias.
		namePrefix string

		// followLogs lists component name prefixes whose container logs are
		// streamed to the cluster log as they arrive.
		followLogs []string

		// initDBSQL, when set, fully replaces the init_db.sql content.
		initDBSQL string

		// initDBSQLExtra is spliced into the default init_db.sql at its
		// custom-SQL marker.
		initDBSQLExtra string

		// tabletFiles are placed into every tablet container before start.
		tabletFiles []ContainerFile

		// vtgateFiles are placed into every vtgate container before start.
		vtgateFiles []ContainerFile
	}

	// ClusterOption configures the cluster.
	ClusterOption interface {
		apply(*clusterOptions)
	}

	cellsOption []string

	vtgateArgsOption []string

	vttabletArgsOption []string

	vtctldArgsOption []string

	vtorcOption []string

	mysqlVersionOption string

	topoFlavorOption string

	tabletEnvOption map[string]string

	vtgateEnvOption map[string]string

	vtctldFilesOption []ContainerFile

	withoutVTGateOption struct{}

	networkOption struct{ nw *testcontainers.DockerNetwork }

	namePrefixOption string
)

func (o cellsOption) apply(opts *clusterOptions) {
	opts.cells = o
}

// WithCells sets the cell names for the cluster. The default is a single cell
// named "zone1". Tablets are distributed across cells round-robin.
func WithCells(cells ...string) ClusterOption {
	return cellsOption(cells)
}

func (o vtgateArgsOption) apply(opts *clusterOptions) {
	opts.vtgateArgs = append(opts.vtgateArgs, o...)
}

// WithVTGateArgs adds extra arguments to vtgate.
func WithVTGateArgs(args ...string) ClusterOption {
	return vtgateArgsOption(args)
}

func (o vttabletArgsOption) apply(opts *clusterOptions) {
	opts.vttabletArgs = append(opts.vttabletArgs, o...)
}

// WithVTTabletArgs adds extra arguments to every vttablet in the cluster.
func WithVTTabletArgs(args ...string) ClusterOption {
	return vttabletArgsOption(args)
}

func (o vtctldArgsOption) apply(opts *clusterOptions) {
	opts.vtctldArgs = append(opts.vtctldArgs, o...)
}

// WithVTCtldArgs adds extra arguments to vtctld.
func WithVTCtldArgs(args ...string) ClusterOption {
	return vtctldArgsOption(args)
}

func (o vtorcOption) apply(opts *clusterOptions) {
	opts.vtorcEnabled = true
	opts.vtorcArgs = append(opts.vtorcArgs, o...)
}

// WithVTOrc enables a VTOrc container for the cluster, with optional extra
// arguments.
func WithVTOrc(args ...string) ClusterOption {
	return vtorcOption(args)
}

func (o mysqlVersionOption) apply(opts *clusterOptions) {
	opts.mysqlVersion = string(o)
}

// WithMySQLVersion sets the MySQL version for the cluster, selecting the
// Docker image tag (e.g. "vitesst:mysql84"). Valid values are "8.0" and
// "8.4"; the default is "8.4".
func WithMySQLVersion(version string) ClusterOption {
	return mysqlVersionOption(version)
}

func (o topoFlavorOption) apply(opts *clusterOptions) {
	opts.topoFlavor = string(o)
}

// WithTopo selects the topology server flavor: "etcd2" (the default),
// "consul", or "zk2".
func WithTopo(flavor string) ClusterOption {
	return topoFlavorOption(flavor)
}

func (o tabletEnvOption) apply(opts *clusterOptions) {
	if opts.tabletEnv == nil {
		opts.tabletEnv = map[string]string{}
	}
	maps.Copy(opts.tabletEnv, o)
}

// WithTabletEnv merges environment variables into every tablet container,
// e.g. EXTRA_MY_CNF for mysqld configuration snippets shipped through
// WithTabletFiles.
func WithTabletEnv(env map[string]string) ClusterOption {
	return tabletEnvOption(env)
}

func (o vtgateEnvOption) apply(opts *clusterOptions) {
	if opts.vtgateEnv == nil {
		opts.vtgateEnv = map[string]string{}
	}
	maps.Copy(opts.vtgateEnv, o)
}

// WithVTGateEnv merges environment variables into every vtgate container.
func WithVTGateEnv(env map[string]string) ClusterOption {
	return vtgateEnvOption(env)
}

func (o vtctldFilesOption) apply(opts *clusterOptions) {
	opts.vtctldFiles = append(opts.vtctldFiles, o...)
}

// WithVTCtldFiles places files into the vtctld container before it starts,
// e.g. client certificates for talking to mTLS-required tablets.
func WithVTCtldFiles(files ...ContainerFile) ClusterOption {
	return vtctldFilesOption(files)
}

func (withoutVTGateOption) apply(opts *clusterOptions) {
	opts.withoutVTGate = true
}

// WithoutVTGate skips starting a vtgate; tests that only exercise the control
// plane or tablets use it. AddVTGate can still start one later.
func WithoutVTGate() ClusterOption {
	return withoutVTGateOption{}
}

func (o networkOption) apply(opts *clusterOptions) {
	opts.borrowedNetwork = o.nw
}

// WithNetwork makes the cluster join a caller-created Docker network instead
// of creating its own, so tests can attach sidecar containers the components
// must reach. The caller owns the network's lifecycle.
func WithNetwork(nw *testcontainers.DockerNetwork) ClusterOption {
	return networkOption{nw: nw}
}

func (o namePrefixOption) apply(opts *clusterOptions) {
	opts.namePrefix = string(o)
}

// WithNamePrefix prepends a prefix to the network alias of every component of
// the cluster, so that two clusters sharing one network through WithNetwork do
// not collide. A cluster created with WithNamePrefix("ext-") reaches its
// topology server at "ext-etcd:2379", which Cluster.TopoAddress returns.
func WithNamePrefix(prefix string) ClusterOption {
	return namePrefixOption(prefix)
}

// newClusterOptions applies the options over the defaults.
func newClusterOptions(opts []ClusterOption) *clusterOptions {
	config := &clusterOptions{
		cells:        []string{defaultCell},
		mysqlVersion: defaultMySQLVersion,
		topoFlavor:   defaultTopoImplementation,
	}
	for _, opt := range opts {
		opt.apply(config)
	}
	return config
}

// validate checks the applied configuration.
func (config *clusterOptions) validate() error {
	if len(config.keyspaces) == 0 {
		return errors.New("at least one keyspace is required")
	}
	if len(config.cells) == 0 {
		return errors.New("at least one cell is required")
	}
	if slices.Contains(config.cells, "") {
		return errors.New("cell names must not be empty")
	}

	seen := make(map[string]bool, len(config.keyspaces))
	for i := range config.keyspaces {
		ks := &config.keyspaces[i]
		if err := ks.validate(); err != nil {
			return err
		}
		if seen[ks.name] {
			return fmt.Errorf("keyspace %s configured twice", ks.name)
		}
		seen[ks.name] = true
	}

	switch config.mysqlVersion {
	case "8.0", "8.4":
	default:
		return fmt.Errorf("unsupported MySQL version %q, supported versions are 8.0 and 8.4", config.mysqlVersion)
	}

	switch config.topoFlavor {
	case "etcd2", "consul", "zk2":
	default:
		return fmt.Errorf("unsupported topo flavor %q, supported flavors are etcd2, consul and zk2", config.topoFlavor)
	}

	return nil
}
