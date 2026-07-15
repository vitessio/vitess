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
	"slices"
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

		// backupVolume is the Docker volume backing the shared backup storage.
		backupVolume string

		// logf receives framework progress messages; NewCluster wires it to
		// t.Logf.
		logf func(format string, args ...any)

		// topo is the cluster's topology server.
		topo *component

		// vtctld is the cluster's control plane.
		vtctld *Vtctld

		// vtadmin is present only when the test enabled vtadmin.
		vtadmin *VTAdmin

		// mu guards the mutable collections below.
		mu        sync.Mutex
		vtgates   []*VTGate
		vtgateSeq int
		vtorcs    []*VTOrc
		vtorcSeq  int
		vtbackups []*Vtbackup

		// tabletSeq hands out tablet UIDs. It never goes backwards, so a
		// removed tablet's UID is not reused by a later one, whose alias and
		// data directory would otherwise collide with a live tablet's.
		tabletSeq    int
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
		name:     c.name("vtctld"),
		httpPort: fmt.Sprintf("%d/tcp", vtctldHTTPPort),
		cluster:  c,
	}}
	return c, nil
}

// name returns a component's network alias, applying the cluster's name
// prefix. Components address each other by these aliases.
func (c *Cluster) name(base string) string {
	return c.opts.namePrefix + base
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

	if c.opts.borrowedNetwork != nil {
		c.network = c.opts.borrowedNetwork
	} else {
		c.logf("creating cluster network")
		c.network, err = network.New(ctx, network.WithDriver("bridge"))
		if err != nil {
			return cleanup, vterrors.Wrapf(err, "creating cluster network")
		}
	}

	if c.opts.backupStorage {
		c.logf("creating backup volume")
		if err = c.createBackupVolume(ctx); err != nil {
			return cleanup, err
		}
	}

	c.logf("starting topo server")
	if err = c.startTopo(ctx); err != nil {
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
	if !c.opts.withoutVTGate {
		group.Go(func() error {
			c.logf("starting vtgate")
			_, err := c.AddVTGate(groupCtx)
			return err
		})
	}
	if c.opts.vtorcEnabled {
		group.Go(func() error {
			c.logf("starting vtorc")
			_, err := c.AddVTOrc(groupCtx, "", c.opts.vtorcArgs...)
			return err
		})
	}
	if err = group.Wait(); err != nil {
		return cleanup, err
	}

	if c.opts.vtadminEnabled {
		c.logf("starting vtadmin")
		if err = c.startVTAdmin(ctx); err != nil {
			return cleanup, err
		}
	}

	c.logf("cluster is ready")
	return cleanup, nil
}

// startKeyspace creates one keyspace in topology, starts its tablets, elects
// primaries, applies the schema, and then applies the vschema.
func (c *Cluster) startKeyspace(ctx context.Context, kc *keyspaceConfig) error {
	c.logf("creating keyspace %s", kc.name)
	if err := c.vtctld.createKeyspace(ctx, kc); err != nil {
		return err
	}

	shardNames := kc.shardNames
	if len(shardNames) == 0 {
		var err error
		shardNames, err = key.GenerateShardRanges(kc.shards, 0)
		if err != nil {
			return vterrors.Wrapf(err, "generating %d shard ranges for keyspace %s", kc.shards, kc.name)
		}
	}

	ks := &Keyspace{Name: kc.name}
	c.mu.Lock()
	c.keyspaces = append(c.keyspaces, ks)
	c.mu.Unlock()

	group, groupCtx := errgroup.WithContext(ctx)
	cells := c.Cells()
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
				UID:      c.nextTabletUID(),
				Cell:     cells[cellIndex%len(cells)],
				Keyspace: kc.name,
				Shard:    shardName,
				Type:     typ,
			}
			cellIndex++
			if kc.tabletSpec != nil {
				kc.tabletSpec(spec)
			}
			specs = append(specs, spec)
		}

		group.Go(func() error {
			return c.startShard(groupCtx, shard, specs, kc.withoutPrimaryElection)
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
func (c *Cluster) startShard(ctx context.Context, shard *Shard, specs []*TabletSpec, withoutPrimaryElection bool) error {
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
		case i == 0 && !withoutPrimaryElection:
			shard.primary = tablet
		case tablet.typ == "rdonly":
			shard.rdonly = append(shard.rdonly, tablet)
		default:
			shard.replicas = append(shard.replicas, tablet)
		}
	}
	shard.mu.Unlock()

	if withoutPrimaryElection {
		return nil
	}

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

// AddKeyspace creates and starts an additional keyspace on the running
// cluster: topology record, tablets, primary election, schema, and vschema.
func (c *Cluster) AddKeyspace(ctx context.Context, kb *keyspaceBuilder) (*Keyspace, error) {
	kc := kb.config
	if err := kc.validate(); err != nil {
		return nil, err
	}
	if c.Keyspace(kc.name) != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "keyspace %s already exists", kc.name)
	}

	// Register the configuration so the keyspace's tablet args and schema are
	// available to later AddShard and AddTablet calls.
	c.mu.Lock()
	c.opts.keyspaces = append(c.opts.keyspaces, kc)
	config := &c.opts.keyspaces[len(c.opts.keyspaces)-1]
	c.mu.Unlock()

	if err := c.startKeyspace(ctx, config); err != nil {
		return nil, err
	}
	return c.Keyspace(kc.name), nil
}

// AddShard starts a new shard on an existing keyspace of the running cluster:
// its tablets (one primary plus the given replicas and rdonly tablets) and its
// primary election. Resharding tests use it to bring up the target shards; the
// schema reaches them through the reshard itself or a later ApplySchema, and
// the vschema and routing rules stay with the test.
func (c *Cluster) AddShard(ctx context.Context, keyspace, shardName string, replicas, rdonly int) (*Shard, error) {
	ks := c.Keyspace(keyspace)
	if ks == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "keyspace %s does not exist", keyspace)
	}
	if ks.Shard(shardName) != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "shard %s/%s already exists", keyspace, shardName)
	}

	kc := c.keyspaceConfig(keyspace)
	if kc == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "keyspace %s has no configuration", keyspace)
	}

	shard := &Shard{Name: shardName, Keyspace: ks}
	ks.mu.Lock()
	ks.shards = append(ks.shards, shard)
	ks.mu.Unlock()

	cells := c.Cells()
	specs := make([]*TabletSpec, 0, 1+replicas+rdonly)
	for i := range 1 + replicas + rdonly {
		typ := "replica"
		switch {
		case i == 0:
			typ = "primary"
		case i > replicas:
			typ = "rdonly"
		}

		spec := &TabletSpec{
			UID:      c.nextTabletUID(),
			Cell:     cells[i%len(cells)],
			Keyspace: keyspace,
			Shard:    shardName,
			Type:     typ,
		}
		if kc.tabletSpec != nil {
			kc.tabletSpec(spec)
		}
		specs = append(specs, spec)
	}

	c.logf("starting shard %s/%s", keyspace, shardName)
	if err := c.startShard(ctx, shard, specs, false); err != nil {
		return nil, err
	}
	return shard, nil
}

// nextTabletUID hands out the next tablet UID.
func (c *Cluster) nextTabletUID() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	uid := firstTabletUID + c.tabletSeq
	c.tabletSeq++
	return uid
}

// RemoveShard terminates a shard's remaining tablets and drops it from the
// cluster's bookkeeping, so a later AddShard can create the same shard again.
// The topology record is untouched; tests delete it through vtctldclient.
func (c *Cluster) RemoveShard(ctx context.Context, keyspace, shardName string) error {
	ks := c.Keyspace(keyspace)
	if ks == nil {
		return nil
	}
	shard := ks.Shard(shardName)
	if shard == nil {
		return nil
	}

	for _, tablet := range shard.Tablets() {
		if err := tablet.Remove(ctx); err != nil {
			return vterrors.Wrapf(err, "removing tablet %s", tablet.Alias())
		}
	}

	ks.mu.Lock()
	ks.shards = slices.DeleteFunc(ks.shards, func(other *Shard) bool { return other == shard })
	ks.mu.Unlock()
	return nil
}

// tabletInitDBSQL returns the init_db.sql content mysqlctl init applies on a
// keyspace's tablets: the keyspace's own init SQL when it sets one, spliced
// with the framework's vt_dba host-access grants, otherwise the cluster
// init_db.sql.
func (c *Cluster) tabletInitDBSQL(keyspace string) (string, error) {
	if kc := c.keyspaceConfig(keyspace); kc != nil && kc.initDBSQL != "" {
		return spliceInitDBSQL(kc.initDBSQL, c.opts.initDBSQLExtra)
	}
	return c.initDBSQL, nil
}

// keyspaceConfig returns the configuration a keyspace was created with.
func (c *Cluster) keyspaceConfig(name string) *keyspaceConfig {
	for i := range c.opts.keyspaces {
		if c.opts.keyspaces[i].name == name {
			return &c.opts.keyspaces[i]
		}
	}
	return nil
}

// AddTablet starts one more tablet of the given kind ("replica" or "rdonly")
// serving an existing shard on the running cluster, in the given cell (""
// means the first cell). It joins as a replica of the shard's primary and is
// recorded on the Shard.
func (c *Cluster) AddTablet(ctx context.Context, cell, keyspace, shard, tabletType string) (*Tablet, error) {
	ks := c.Keyspace(keyspace)
	if ks == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "keyspace %s does not exist", keyspace)
	}
	sh := ks.Shard(shard)
	if sh == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "shard %s/%s does not exist", keyspace, shard)
	}

	if cell == "" {
		cell = c.firstCell()
	}
	spec := &TabletSpec{
		UID:      c.nextTabletUID(),
		Cell:     cell,
		Keyspace: keyspace,
		Shard:    shard,
		Type:     tabletType,
	}
	tablet, err := c.startTablet(ctx, spec)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.tablets = append(c.tablets, tablet)
	c.mu.Unlock()

	sh.mu.Lock()
	if tabletType == "rdonly" {
		sh.rdonly = append(sh.rdonly, tablet)
	} else {
		sh.replicas = append(sh.replicas, tablet)
	}
	sh.mu.Unlock()
	return tablet, nil
}

// RemoveKeyspace terminates any remaining tablets of a keyspace and drops it
// from the cluster's bookkeeping, so AddKeyspace can provision the same name
// again. The topology record is untouched; tests delete it through
// vtctldclient when needed.
func (c *Cluster) RemoveKeyspace(ctx context.Context, name string) error {
	ks := c.Keyspace(name)
	if ks == nil {
		return nil
	}

	for _, tablet := range ks.Tablets() {
		if err := tablet.Remove(ctx); err != nil {
			return vterrors.Wrapf(err, "removing tablet %s", tablet.Alias())
		}
	}

	c.mu.Lock()
	c.keyspaces = slices.DeleteFunc(c.keyspaces, func(other *Keyspace) bool { return other == ks })
	c.mu.Unlock()
	return nil
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
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.cells))
	copy(out, c.cells)
	return out
}

// firstCell returns the cell that components default to.
func (c *Cluster) firstCell() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cells[0]
}

// AddCell registers a new cell in the topology of the running cluster and adds
// it to the cluster's cells, so later tablets round-robin over it too. A vtgate
// only routes to the new cell's tablets when it watches the cell, which
// AddVTGateSpec configures.
func (c *Cluster) AddCell(ctx context.Context, cell string) error {
	if cell == "" {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cell name must not be empty")
	}
	if slices.Contains(c.Cells(), cell) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cell %s already exists", cell)
	}

	c.logf("adding cell %s", cell)
	if err := c.addCellInfo(ctx, cell); err != nil {
		return err
	}

	c.mu.Lock()
	c.cells = append(c.cells, cell)
	c.mu.Unlock()
	return nil
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
	vtorcs := c.vtorcs
	vtbackups := c.vtbackups
	tablets := c.tablets
	c.vtgates = nil
	c.vtorcs = nil
	c.vtbackups = nil
	c.tablets = nil
	c.mu.Unlock()

	var group errgroup.Group
	for _, g := range vtgates {
		group.Go(func() error { return g.terminate(ctx) })
	}
	for _, vtorc := range vtorcs {
		group.Go(func() error { return vtorc.terminate(ctx) })
	}
	for _, vtbackup := range vtbackups {
		group.Go(func() error { return vtbackup.terminate(ctx) })
	}
	if c.vtadmin != nil {
		vtadmin := c.vtadmin
		c.vtadmin = nil
		group.Go(func() error { return vtadmin.terminate(ctx) })
	}
	for _, tablet := range tablets {
		group.Go(func() error { return tablet.terminate(ctx) })
	}
	if c.vtctld != nil {
		vtctld := c.vtctld
		c.vtctld = nil
		group.Go(func() error { return vtctld.terminate(ctx) })
	}
	if c.topo != nil {
		topo := c.topo
		c.topo = nil
		group.Go(func() error { return topo.terminate(ctx) })
	}
	err := group.Wait()

	if removeErr := c.removeBackupVolume(ctx); removeErr != nil && err == nil {
		err = removeErr
	}

	if c.network != nil {
		nw := c.network
		c.network = nil
		if c.opts.borrowedNetwork == nil {
			if removeErr := nw.Remove(ctx); removeErr != nil && err == nil {
				err = vterrors.Wrapf(removeErr, "removing cluster network")
			}
		}
	}
	return err
}
