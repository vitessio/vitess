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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/vt/key"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// Cluster represents a running Vitess cluster where every component runs
	// in its own container on a per-cluster Docker network.
	Cluster struct {
		// opts keeps the applied cluster configuration; Start validates it.
		opts *clusterOptions

		// network isolates this cluster's containers from parallel tests and
		// gives them stable aliases for intra-cluster communication.
		network *testcontainers.DockerNetwork

		// cells lists the topology cells.
		cells []string

		// image is the resolved Docker image for Vitess components.
		image string

		// mysqlVersion is the mysqld version detected from the image, e.g.
		// "8.4.8".
		mysqlVersion string

		// initDBSQL is the assembled init_db.sql content shipped to tablets.
		initDBSQL string

		// logf receives framework progress messages; NewCluster wires it to
		// t.Logf.
		logf func(format string, args ...any)

		// etcd is the cluster's topology server.
		etcd *component

		// vtctld is the cluster's control plane.
		vtctld *Vtctld

		// vtorc is present only when the test enabled VTOrc.
		vtorc *VTOrc

		// mu guards the mutable collections below.
		mu           sync.Mutex
		vtgates      []*VTGate
		vtgateSeq    int
		keyspaces    []*Keyspace
		tablets      []*Tablet
		logConsumers []*ringLogConsumer
	}
)

// NewCluster creates a Vitess cluster from the given options, validating
// them. Nothing runs until Start is called. Requires at least one keyspace to
// be configured.
func NewCluster(opts ...ClusterOption) (*Cluster, error) {
	config := newClusterOptions(opts)
	if err := config.validate(); err != nil {
		return nil, err
	}

	logf := func(format string, args ...any) {}
	if os.Getenv("VITESST_DEBUG") != "" {
		logf = func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "vitesst: "+format+"\n", args...)
		}
	}

	c := &Cluster{
		opts:  config,
		cells: config.cells,
		image: vitesstImage(config.mysqlVersion),
		logf:  logf,
	}
	c.vtctld = &Vtctld{component: component{
		name:     "vtctld",
		httpPort: fmt.Sprintf("%d/tcp", vtctldHTTPPort),
		cluster:  c,
	}}
	return c, nil
}

// Start brings the whole cluster up. The returned cleanup tears everything
// down again; it is safe to call more than once, and bounds itself when the
// given context has no deadline. It is non-nil even on error, so cleanup
// registration and error handling can come in either order. On error,
// everything already started has been torn down.
func (c *Cluster) Start(ctx context.Context) (cleanup func(context.Context) error, err error) {
	cleanup = c.terminate

	if c.network != nil {
		return cleanup, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cluster is already started")
	}

	// Tear down whatever came up when bootstrap fails part-way, so failed
	// tests do not leak containers beyond what ryuk would reap later.
	defer func() {
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminateTimeout)
			defer cancel()
			if cleanupErr := c.terminate(cleanupCtx); cleanupErr != nil {
				c.logf("cleaning up after failed bootstrap: %v", cleanupErr)
			}
		}
	}()

	c.logf("creating cluster network")
	c.network, err = network.New(ctx, network.WithDriver("bridge"))
	if err != nil {
		return cleanup, vterrors.Wrapf(err, "creating cluster network")
	}

	c.logf("starting etcd")
	if err = c.startEtcd(ctx); err != nil {
		return cleanup, err
	}

	c.logf("starting vtctld")
	if err = c.startVtctld(ctx); err != nil {
		return cleanup, err
	}

	if err = c.detectMySQLVersion(ctx); err != nil {
		return cleanup, err
	}
	if err = c.assembleInitDBSQL(ctx); err != nil {
		return cleanup, err
	}

	for _, cell := range c.cells {
		c.logf("adding cell %s", cell)
		if err = c.addCellInfo(ctx, cell); err != nil {
			return cleanup, err
		}
	}

	for i := range c.opts.keyspaces {
		if err = c.startKeyspace(ctx, &c.opts.keyspaces[i]); err != nil {
			return cleanup, err
		}
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		c.logf("starting vtgate")
		_, err := c.AddVTGate(groupCtx)
		return err
	})
	if c.opts.vtorcEnabled {
		group.Go(func() error {
			c.logf("starting vtorc")
			return c.startVTOrc(groupCtx)
		})
	}
	if err = group.Wait(); err != nil {
		return cleanup, err
	}

	c.logf("cluster is ready")
	return cleanup, nil
}

// startKeyspace creates one keyspace in topology, starts its tablets, elects
// primaries, applies the schema, and then applies the vschema.
func (c *Cluster) startKeyspace(ctx context.Context, kc *keyspaceConfig) error {
	c.logf("creating keyspace %s", kc.name)
	if err := c.vtctld.createKeyspace(ctx, kc.name, kc.durabilityPolicy); err != nil {
		return err
	}

	shardNames, err := key.GenerateShardRanges(kc.shards, 0)
	if err != nil {
		return vterrors.Wrapf(err, "generating %d shard ranges for keyspace %s", kc.shards, kc.name)
	}

	ks := &Keyspace{Name: kc.name}
	c.mu.Lock()
	c.keyspaces = append(c.keyspaces, ks)
	nextUID := firstTabletUID + len(c.tablets)
	c.mu.Unlock()

	group, groupCtx := errgroup.WithContext(ctx)
	cellIndex := 0
	for _, shardName := range shardNames {
		shard := &Shard{Name: shardName, Keyspace: ks}
		ks.mu.Lock()
		ks.shards = append(ks.shards, shard)
		ks.mu.Unlock()

		specs := make([]*TabletSpec, 0, kc.tabletsPerShard())
		for i := range kc.tabletsPerShard() {
			typ := "replica"
			switch {
			case i == 0:
				typ = "primary"
			case i > kc.replicas:
				typ = "rdonly"
			}

			spec := &TabletSpec{
				UID:      nextUID,
				Cell:     c.cells[cellIndex%len(c.cells)],
				Keyspace: kc.name,
				Shard:    shardName,
				Type:     typ,
			}
			nextUID++
			cellIndex++
			if kc.tabletSpec != nil {
				kc.tabletSpec(spec)
			}
			specs = append(specs, spec)
		}

		group.Go(func() error {
			return c.startShard(groupCtx, shard, specs)
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}

	if kc.schema != "" {
		c.logf("applying schema to keyspace %s", kc.name)
		if err := c.vtctld.applySchema(ctx, kc.name, kc.schema); err != nil {
			return err
		}
	}
	if kc.vschema != "" {
		c.logf("applying vschema to keyspace %s", kc.name)
		if err := c.vtctld.applyVSchema(ctx, kc.name, kc.vschema); err != nil {
			return err
		}
	}
	return nil
}

// startShard starts all of a shard's tablets concurrently, records them on
// the Shard, and elects the first tablet as primary.
func (c *Cluster) startShard(ctx context.Context, shard *Shard, specs []*TabletSpec) error {
	tablets := make([]*Tablet, len(specs))

	group, groupCtx := errgroup.WithContext(ctx)
	for i, spec := range specs {
		group.Go(func() error {
			c.logf("starting tablet %s-%d (%s) for %s", spec.Cell, spec.UID, spec.Type, shard.Ref())
			tablet, err := c.startTablet(groupCtx, spec)
			if err != nil {
				return err
			}
			tablets[i] = tablet

			// Record the tablet immediately so Terminate reaches its
			// container even when a sibling tablet fails to start and this
			// shard's bring-up is abandoned.
			c.mu.Lock()
			c.tablets = append(c.tablets, tablet)
			c.mu.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}

	shard.mu.Lock()
	for i, tablet := range tablets {
		switch {
		case i == 0:
			shard.primary = tablet
		case tablet.typ == "rdonly":
			shard.rdonly = append(shard.rdonly, tablet)
		default:
			shard.replicas = append(shard.replicas, tablet)
		}
	}
	shard.mu.Unlock()

	c.logf("electing %s as primary of %s", tablets[0].Alias(), shard.Ref())
	if err := c.vtctld.initializeShard(ctx, shard.Keyspace.Name, shard.Name, tablets[0].Alias()); err != nil {
		return vterrors.Wrapf(err, "electing primary for shard %s", shard.Ref())
	}
	return nil
}

// detectMySQLVersion asks the image's mysqld for its version so vtgate can
// advertise a matching --mysql-server-version.
func (c *Cluster) detectMySQLVersion(ctx context.Context) error {
	output, err := mustExec(ctx, c.vtctld.container(), []string{"bash", "-c", "mysqld --version || /usr/sbin/mysqld --version"})
	if err != nil {
		return vterrors.Wrapf(err, "detecting mysqld version")
	}

	match := regexp.MustCompile(`(\d+)\.(\d+)\.(\d+)`).FindString(output)
	if match == "" {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "cannot parse mysqld version from %q", output)
	}
	c.mysqlVersion = match
	return nil
}

// assembleInitDBSQL builds the init_db.sql content shipped to every tablet:
// the image's copy of config/init_db.sql with host access for vt_dba and any
// WithInitDBSQLExtra content spliced in at the custom-SQL marker.
func (c *Cluster) assembleInitDBSQL(ctx context.Context) error {
	if c.opts.initDBSQL != "" {
		c.initDBSQL = c.opts.initDBSQL
		return nil
	}

	base, err := mustExec(ctx, c.vtctld.container(), []string{"cat", imageInitDBPath})
	if err != nil {
		return vterrors.Wrapf(err, "reading %s from image", imageInitDBPath)
	}

	c.initDBSQL, err = spliceInitDBSQL(base, c.opts.initDBSQLExtra)
	return err
}

// spliceInitDBSQL splices the framework's vt_dba host-access grants and any
// extra SQL into an init_db.sql document at its custom-SQL marker.
func spliceInitDBSQL(base, extraSQL string) (string, error) {
	custom := fmt.Sprintf(`CREATE USER '%[1]s'@'%%';
GRANT ALL ON *.* TO '%[1]s'@'%%';
GRANT GRANT OPTION ON *.* TO '%[1]s'@'%%';
GRANT PROXY ON ''@'' TO '%[1]s'@'%%' WITH GRANT OPTION;
`, dbaUser)
	if extraSQL != "" {
		custom += extraSQL + "\n"
	}

	pieces := strings.SplitN(base, customSQLMarker, 2)
	if len(pieces) != 2 {
		return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "missing %q marker in init_db.sql", customSQLMarker)
	}

	return pieces[0] + custom + customSQLMarker + pieces[1], nil
}

// Keyspace returns the runtime handle for a keyspace, or nil.
func (c *Cluster) Keyspace(name string) *Keyspace {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ks := range c.keyspaces {
		if ks.Name == name {
			return ks
		}
	}
	return nil
}

// Keyspaces returns all keyspaces.
func (c *Cluster) Keyspaces() []*Keyspace {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Keyspace, len(c.keyspaces))
	copy(out, c.keyspaces)
	return out
}

// Tablets returns all tablets in the cluster.
func (c *Cluster) Tablets() []*Tablet {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Tablet, len(c.tablets))
	copy(out, c.tablets)
	return out
}

// Cells returns the cluster's cell names.
func (c *Cluster) Cells() []string {
	out := make([]string, len(c.cells))
	copy(out, c.cells)
	return out
}

// MySQLVersion returns the mysqld version detected from the image, e.g.
// "8.4.8".
func (c *Cluster) MySQLVersion() string {
	return c.mysqlVersion
}

// DumpDiagnostics writes the tail of every component's logs through logf,
// and, when the VITESST_ARTIFACTS environment variable names a directory,
// writes full logs and container state there for CI upload.
func (c *Cluster) DumpDiagnostics(ctx context.Context, logf func(format string, args ...any)) {
	c.dumpLogs(logf)

	dir := os.Getenv("VITESST_ARTIFACTS")
	if dir == "" {
		return
	}
	if err := c.writeArtifacts(ctx, dir); err != nil {
		logf("writing artifacts to %s: %v", dir, err)
	}
}

// writeArtifacts writes full component logs and mysqld error logs to dir.
func (c *Cluster) writeArtifacts(ctx context.Context, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	c.mu.Lock()
	consumers := make([]*ringLogConsumer, len(c.logConsumers))
	copy(consumers, c.logConsumers)
	tablets := make([]*Tablet, len(c.tablets))
	copy(tablets, c.tablets)
	c.mu.Unlock()

	for _, rc := range consumers {
		content := strings.Join(rc.snapshot(), "\n") + "\n"
		if err := os.WriteFile(filepath.Join(dir, rc.name+".log"), []byte(content), 0o644); err != nil {
			return err
		}
	}

	for _, tablet := range tablets {
		if !tablet.IsRunning() {
			continue
		}
		_, output, err := containerExec(ctx, tablet.container(), []string{"cat", tablet.tabletDir() + "/error.log"})
		if err != nil {
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, tablet.name+"-mysqld-error.log"), []byte(output), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// terminate tears down all of the cluster's containers and its network. It
// is safe to call more than once. When the given context has no deadline,
// teardown is bounded so a wedged Docker daemon cannot hang the test binary.
func (c *Cluster) terminate(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, terminateTimeout)
		defer cancel()
	}

	c.mu.Lock()
	vtgates := c.vtgates
	tablets := c.tablets
	c.vtgates = nil
	c.tablets = nil
	c.mu.Unlock()

	var group errgroup.Group
	for _, g := range vtgates {
		group.Go(func() error { return g.terminate(ctx) })
	}
	if c.vtorc != nil {
		vtorc := c.vtorc
		c.vtorc = nil
		group.Go(func() error { return vtorc.terminate(ctx) })
	}
	for _, tablet := range tablets {
		group.Go(func() error { return tablet.terminate(ctx) })
	}
	if c.vtctld != nil {
		vtctld := c.vtctld
		c.vtctld = nil
		group.Go(func() error { return vtctld.terminate(ctx) })
	}
	if c.etcd != nil {
		etcd := c.etcd
		c.etcd = nil
		group.Go(func() error { return etcd.terminate(ctx) })
	}
	err := group.Wait()

	if c.network != nil {
		nw := c.network
		c.network = nil
		if removeErr := nw.Remove(ctx); removeErr != nil && err == nil {
			err = vterrors.Wrapf(removeErr, "removing cluster network")
		}
	}
	return err
}
