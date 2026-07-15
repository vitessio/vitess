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
	"slices"
	"sync"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// keyspaceConfig holds validated keyspace configuration.
	keyspaceConfig struct {
		// name is the keyspace name stored in topology.
		name string

		// schema is applied through ApplySchema after the shards have primaries.
		schema string

		// vschema is applied through ApplyVSchema after the schema.
		vschema string

		// shards controls how many shard ranges are generated.
		shards int

		// shardNames, when set, are the explicit shard range names to use
		// instead of generated uniform ranges.
		shardNames []string

		// replicas is the number of replica tablets per shard, excluding the
		// primary and any rdonly tablets.
		replicas int

		// rdonly is the number of rdonly tablets per shard.
		rdonly int

		// durabilityPolicy is passed to CreateKeyspace.
		durabilityPolicy string

		// sidecarDBName overrides the keyspace's sidecar database name.
		sidecarDBName string

		// baseKeyspace and snapshotTime make this a snapshot keyspace of
		// another keyspace at a point in time.
		baseKeyspace string
		snapshotTime string

		// image, when set, is the Docker image this keyspace's tablets run,
		// instead of the cluster image.
		image string

		// initDBSQL, when set, is the init_db.sql content mysqlctl init applies
		// on this keyspace's tablets, instead of the cluster init_db.sql. A
		// MariaDB keyspace needs a variant without super_read_only. The
		// framework's vt_dba host-access grants are spliced in the same way as
		// the cluster init_db.sql.
		initDBSQL string

		// tabletArgs are appended to this keyspace's vttablet command lines,
		// after cluster-wide vttablet args.
		tabletArgs []string

		// tabletSpec, when set, customizes each tablet's spec before start.
		tabletSpec func(*TabletSpec)

		// withoutPrimaryElection skips the automatic primary election, for
		// tests that drive the election themselves.
		withoutPrimaryElection bool
	}

	// keyspaceBuilder provides a fluent API for configuring a keyspace.
	// Create one with WithKeyspace and chain methods to configure it:
	//
	//	WithKeyspace("ks").
	//	    WithShards(2).
	//	    WithReplicas(1).
	//	    WithSchema(`CREATE TABLE t1 (id BIGINT PRIMARY KEY)`).
	//	    WithVSchema(vschemaJSON)
	keyspaceBuilder struct {
		config keyspaceConfig
	}

	// Keyspace is the runtime handle for a started keyspace.
	Keyspace struct {
		// Name is the keyspace name.
		Name string

		mu     sync.Mutex
		shards []*Shard
	}

	// Shard is the runtime handle for a started shard.
	Shard struct {
		// Name is the shard name, e.g. "-" or "-80".
		Name string

		// Keyspace is the keyspace this shard belongs to.
		Keyspace *Keyspace

		cluster  *Cluster
		mu       sync.Mutex
		primary  *Tablet
		replicas []*Tablet
		rdonly   []*Tablet
	}
)

// WithKeyspace creates a keyspace builder with the given name. The default
// keyspace is unsharded with a single primary tablet and no replicas.
func WithKeyspace(name string) *keyspaceBuilder {
	return &keyspaceBuilder{
		config: keyspaceConfig{
			name:             name,
			shards:           defaultShards,
			durabilityPolicy: defaultDurabilityPolicy,
		},
	}
}

// apply implements ClusterOption.
func (kb *keyspaceBuilder) apply(opts *clusterOptions) {
	opts.keyspaces = append(opts.keyspaces, kb.config)
}

// WithSchema sets the SQL schema applied to the keyspace.
func (kb *keyspaceBuilder) WithSchema(sql string) *keyspaceBuilder {
	kb.config.schema = sql
	return kb
}

// WithVSchema sets the VSchema JSON applied to the keyspace.
func (kb *keyspaceBuilder) WithVSchema(json string) *keyspaceBuilder {
	kb.config.vschema = json
	return kb
}

// WithShards sets the number of shards; ranges are generated automatically
// (1 shard means the unsharded range "-").
func (kb *keyspaceBuilder) WithShards(n int) *keyspaceBuilder {
	kb.config.shards = n
	return kb
}

// WithShardNames sets explicit shard range names (e.g. "-41", "41-4180",
// "4180-"), for keyspaces whose tests depend on exact shard boundaries.
func (kb *keyspaceBuilder) WithShardNames(names ...string) *keyspaceBuilder {
	kb.config.shardNames = names
	return kb
}

// WithReplicas sets the number of replica tablets per shard, in addition to
// the primary. WithReplicas(1) gives each shard a primary and one replica.
func (kb *keyspaceBuilder) WithReplicas(n int) *keyspaceBuilder {
	kb.config.replicas = n
	return kb
}

// WithRDOnly sets the number of rdonly tablets per shard.
func (kb *keyspaceBuilder) WithRDOnly(n int) *keyspaceBuilder {
	kb.config.rdonly = n
	return kb
}

// WithDurabilityPolicy sets the durability policy passed to CreateKeyspace.
func (kb *keyspaceBuilder) WithDurabilityPolicy(policy string) *keyspaceBuilder {
	kb.config.durabilityPolicy = policy
	return kb
}

// WithSidecarDBName overrides the keyspace's sidecar database name; the
// default is "_vt".
func (kb *keyspaceBuilder) WithSidecarDBName(name string) *keyspaceBuilder {
	kb.config.sidecarDBName = name
	return kb
}

// WithSnapshotOf makes this a snapshot keyspace of another keyspace at the
// given time (RFC 3339), for recovery tests. Its tablets restore from the base
// keyspace's backups.
func (kb *keyspaceBuilder) WithSnapshotOf(baseKeyspace, snapshotTime string) *keyspaceBuilder {
	kb.config.baseKeyspace = baseKeyspace
	kb.config.snapshotTime = snapshotTime
	return kb
}

// WithImage runs this keyspace's tablets on the given Docker image instead of
// the cluster image, e.g. Image("8.0") or "vitesst:mariadb" for a workflow
// whose source keyspace runs an older MySQL or MariaDB than its target.
func (kb *keyspaceBuilder) WithImage(image string) *keyspaceBuilder {
	kb.config.image = image
	return kb
}

// WithInitDBSQL sets the init_db.sql content mysqlctl init applies on this
// keyspace's tablets, instead of the cluster init_db.sql. The framework splices
// its vt_dba host-access grants into it at the custom-SQL marker, so the SQL
// passed here is a standard init_db.sql document. A MariaDB source keyspace
// uses it to supply a variant that sets read_only instead of the unsupported
// super_read_only.
func (kb *keyspaceBuilder) WithInitDBSQL(sql string) *keyspaceBuilder {
	kb.config.initDBSQL = sql
	return kb
}

// WithTabletArgs adds extra arguments to this keyspace's vttablets, appended
// after any cluster-wide WithVTTabletArgs.
func (kb *keyspaceBuilder) WithTabletArgs(args ...string) *keyspaceBuilder {
	kb.config.tabletArgs = append(kb.config.tabletArgs, args...)
	return kb
}

// WithTabletSpec registers a customizer invoked with every tablet's spec
// before its container starts, for per-tablet extra args or files.
func (kb *keyspaceBuilder) WithTabletSpec(fn func(*TabletSpec)) *keyspaceBuilder {
	kb.config.tabletSpec = fn
	return kb
}

// WithoutPrimaryElection skips the automatic primary election for this
// keyspace's shards; Shard.Primary stays nil and the test drives the election
// itself.
func (kb *keyspaceBuilder) WithoutPrimaryElection() *keyspaceBuilder {
	kb.config.withoutPrimaryElection = true
	return kb
}

// validate checks the keyspace configuration.
func (kc *keyspaceConfig) validate() error {
	switch {
	case kc.name == "":
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "keyspace name cannot be empty")
	case len(kc.shardNames) > 0 && kc.shards != defaultShards:
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "keyspace %s sets both WithShards and WithShardNames", kc.name)
	case slices.Contains(kc.shardNames, ""):
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "keyspace %s shard names must not be empty", kc.name)
	case kc.shards < 1:
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "keyspace %s needs at least one shard", kc.name)
	case kc.replicas < 0:
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "keyspace %s replica count cannot be negative", kc.name)
	case kc.rdonly < 0:
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "keyspace %s rdonly count cannot be negative", kc.name)
	}
	return nil
}

// tabletsPerShard returns the total tablet count per shard: one primary plus
// replicas plus rdonly tablets.
func (kc *keyspaceConfig) tabletsPerShard() int {
	return 1 + kc.replicas + kc.rdonly
}

// Shards returns the keyspace's shards in range order.
func (k *Keyspace) Shards() []*Shard {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]*Shard, len(k.shards))
	copy(out, k.shards)
	return out
}

// Shard returns the shard with the given name, or nil.
func (k *Keyspace) Shard(name string) *Shard {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, s := range k.shards {
		if s.Name == name {
			return s
		}
	}
	return nil
}

// Tablets returns all tablets in the keyspace.
func (k *Keyspace) Tablets() []*Tablet {
	var tablets []*Tablet
	for _, s := range k.Shards() {
		tablets = append(tablets, s.Tablets()...)
	}
	return tablets
}

// Ref returns the "keyspace/shard" reference used by vtctldclient commands.
func (s *Shard) Ref() string {
	return s.Keyspace.Name + "/" + s.Name
}

// Primary returns the shard's primary tablet as recorded by the framework at
// the initial election. It does not track reparents a test performs later.
func (s *Shard) Primary() *Tablet {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.primary
}

// Replicas returns the shard's replica tablets, excluding the primary and
// rdonly tablets.
func (s *Shard) Replicas() []*Tablet {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Tablet, len(s.replicas))
	copy(out, s.replicas)
	return out
}

// RDOnly returns the shard's rdonly tablets.
func (s *Shard) RDOnly() []*Tablet {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Tablet, len(s.rdonly))
	copy(out, s.rdonly)
	return out
}

// Tablets returns all of the shard's tablets: primary first, then replicas,
// then rdonly tablets.
func (s *Shard) Tablets() []*Tablet {
	s.mu.Lock()
	defer s.mu.Unlock()

	tablets := make([]*Tablet, 0, 1+len(s.replicas)+len(s.rdonly))
	if s.primary != nil {
		tablets = append(tablets, s.primary)
	}
	tablets = append(tablets, s.replicas...)
	tablets = append(tablets, s.rdonly...)
	return tablets
}

// CurrentPrimary reads the shard's topology record and returns the tablet the
// topology currently names as primary, or nil if the shard has no primary.
// Unlike Primary, it reflects the live topology after reparents.
func (s *Shard) CurrentPrimary(ctx context.Context) (*Tablet, error) {
	record, err := s.cluster.Vtctld().Shard(ctx, s.Keyspace.Name, s.Name)
	if err != nil {
		return nil, err
	}

	alias := record.GetShard().GetPrimaryAlias()
	if alias.GetUid() == 0 {
		return nil, nil
	}

	for _, t := range s.Tablets() {
		if t.Cell == alias.GetCell() && t.UID == int(alias.GetUid()) {
			return t, nil
		}
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "shard %s/%s primary %s-%d is not a tracked tablet", s.Keyspace.Name, s.Name, alias.GetCell(), alias.GetUid())
}

// WaitForPrimary polls the shard's topology record until it names a primary,
// then returns that tablet.
func (s *Shard) WaitForPrimary(ctx context.Context, timeout time.Duration) (*Tablet, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastErr error
	for {
		primary, err := s.CurrentPrimary(ctx)
		lastErr = err
		if err == nil && primary != nil {
			return primary, nil
		}

		select {
		case <-ctx.Done():
			return nil, vterrors.Wrapf(errFirst(lastErr, ctx.Err()), "shard %s/%s did not get a primary within %s", s.Keyspace.Name, s.Name, timeout)
		case <-time.After(healthyShardPollInterval):
		}
	}
}
