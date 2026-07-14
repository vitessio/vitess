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
	"slices"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
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

// newClusterOptions applies the options over the defaults.
func newClusterOptions(opts []ClusterOption) *clusterOptions {
	config := &clusterOptions{
		cells:        []string{defaultCell},
		mysqlVersion: defaultMySQLVersion,
	}
	for _, opt := range opts {
		opt.apply(config)
	}
	return config
}

// validate checks the applied configuration.
func (config *clusterOptions) validate() error {
	if len(config.keyspaces) == 0 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "at least one keyspace is required")
	}
	if len(config.cells) == 0 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "at least one cell is required")
	}
	if slices.Contains(config.cells, "") {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cell names must not be empty")
	}

	seen := make(map[string]bool, len(config.keyspaces))
	for i := range config.keyspaces {
		ks := &config.keyspaces[i]
		if err := ks.validate(); err != nil {
			return err
		}
		if seen[ks.name] {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "keyspace %s configured twice", ks.name)
		}
		seen[ks.name] = true
	}

	switch config.mysqlVersion {
	case "8.0", "8.4":
	default:
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "unsupported MySQL version %q, supported versions are 8.0 and 8.4", config.mysqlVersion)
	}

	return nil
}
