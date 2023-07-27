/*
Copyright 2020 The Vitess Authors.

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

package cluster

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sets"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtadmin/cache"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
	"vitess.io/vitess/go/vt/vtadmin/cluster/internal/caches/schemacache"
	"vitess.io/vitess/go/vt/vtadmin/debug"
	"vitess.io/vitess/go/vt/vtadmin/errors"
	"vitess.io/vitess/go/vt/vtadmin/vtadminproto"
	"vitess.io/vitess/go/vt/vtadmin/vtctldclient"
	"vitess.io/vitess/go/vt/vtadmin/vtsql"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// Cluster is the self-contained unit of services required for vtadmin to talk
// to a vitess cluster. This consists of a discovery service, a database
// connection, and a vtctl client.
type Cluster struct {
	ID        string
	Name      string
	Discovery discovery.Discovery

	DB     vtsql.DB
	Vtctld vtctldclient.Proxy

	// These fields are kept to power debug endpoints.
	// (TODO|@amason): Figure out if these are needed or if there's a way to
	// push down to the credentials / vtsql.
	// vtgateCredentialsPath string

	// Fields for generating FQDNs for tablets
	TabletFQDNTmpl *template.Template

	backupReadPool   *pools.RPCPool
	schemaReadPool   *pools.RPCPool
	topoRWPool       *pools.RPCPool
	topoReadPool     *pools.RPCPool
	workflowReadPool *pools.RPCPool

	emergencyFailoverPool *pools.RPCPool // ERS-only
	failoverPool          *pools.RPCPool // PRS-only

	// schemaCache caches schema(s) for different GetSchema(s) requests.
	//
	// - if we call GetSchema, then getSchemaCacheRequest.Keyspace will be
	// non-empty and the cached schemas slice will contain exactly one element,
	// namely for that keyspace's schema.
	// - if we call GetSchemas, then getSchemaCacheRequest == "", and the cached
	// schemas slice will contain one element per keyspace* in the cluster
	// 	*: at the time it was cached; if keyspaces were created/destroyed in
	//  the interim, we won't pick that up until something refreshes the cache.
	schemaCache *cache.Cache[schemacache.Key, []*vtadminpb.Schema]

	cfg Config
}

// New creates a new Cluster from a Config.
func New(ctx context.Context, cfg Config) (*Cluster, error) {
	cluster := &Cluster{
		ID:   cfg.ID,
		Name: cfg.Name,
		cfg:  cfg,
	}

	discoargs := buildPFlagSlice(cfg.DiscoveryFlagsByImpl[cfg.DiscoveryImpl])

	disco, err := discovery.New(cfg.DiscoveryImpl, cluster.ToProto(), discoargs)
	if err != nil {
		return nil, fmt.Errorf("error creating discovery impl (%s): %w", cfg.DiscoveryImpl, err)
	}

	cluster.Discovery = disco

	protocluster := cluster.ToProto()

	vtsqlargs := buildPFlagSlice(cfg.VtSQLFlags)

	vtsqlCfg, err := vtsql.Parse(protocluster, disco, vtsqlargs)
	if err != nil {
		return nil, fmt.Errorf("error creating vtsql connection config: %w", err)
	}

	for _, opt := range cfg.vtsqlConfigOpts {
		vtsqlCfg = opt(vtsqlCfg)
	}

	vtctldargs := buildPFlagSlice(cfg.VtctldFlags)

	vtctldCfg, err := vtctldclient.Parse(protocluster, disco, vtctldargs)
	if err != nil {
		return nil, fmt.Errorf("error creating vtctldclient proxy config: %w", err)
	}

	for _, opt := range cfg.vtctldConfigOpts {
		vtctldCfg = opt(vtctldCfg)
	}

	cluster.DB, err = vtsql.New(ctx, vtsqlCfg)
	if err != nil {
		return nil, fmt.Errorf("error creating vtsql proxy: %w", err)
	}

	cluster.Vtctld, err = vtctldclient.New(ctx, vtctldCfg)
	if err != nil {
		return nil, fmt.Errorf("error creating vtctldclient: %w", err)
	}

	if cfg.TabletFQDNTmplStr != "" {
		cluster.TabletFQDNTmpl, err = template.New(cluster.ID + "-tablet-fqdn").Parse(cfg.TabletFQDNTmplStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse tablet fqdn template %s: %w", cfg.TabletFQDNTmplStr, err)
		}
	}

	cluster.backupReadPool = cfg.BackupReadPoolConfig.NewReadPool()
	cluster.schemaReadPool = cfg.SchemaReadPoolConfig.NewReadPool()
	cluster.topoRWPool = cfg.TopoRWPoolConfig.NewRWPool()
	cluster.topoReadPool = cfg.TopoReadPoolConfig.NewReadPool()
	cluster.workflowReadPool = cfg.WorkflowReadPoolConfig.NewReadPool()

	cluster.emergencyFailoverPool = cfg.EmergencyFailoverPoolConfig.NewRWPool()
	cluster.failoverPool = cfg.FailoverPoolConfig.NewRWPool()

	if cluster.cfg.SchemaCacheConfig == nil {
		cluster.cfg.SchemaCacheConfig = &cache.Config{}
	}
	cluster.schemaCache = cache.New(func(ctx context.Context, key schemacache.Key) ([]*vtadminpb.Schema, error) {
		// TODO: make a private method to separate the fetching bits from the cache bits
		if key.Keyspace == "" {
			return cluster.GetSchemas(ctx, GetSchemaOptions{
				BaseRequest: &vtctldatapb.GetSchemaRequest{
					IncludeViews: true,
				},
				TableSizeOptions: &vtadminpb.GetSchemaTableSizeOptions{
					AggregateSizes:          true,
					IncludeNonServingShards: key.IncludeNonServingShards,
				},
				isBackfill: true,
			})
		}

		schema, err := cluster.GetSchema(ctx, key.Keyspace, GetSchemaOptions{
			BaseRequest: &vtctldatapb.GetSchemaRequest{
				IncludeViews: true,
			},
			TableSizeOptions: &vtadminpb.GetSchemaTableSizeOptions{
				AggregateSizes:          true,
				IncludeNonServingShards: key.IncludeNonServingShards,
			},
			isBackfill: true,
		})
		if err != nil {
			return nil, err
		}

		return []*vtadminpb.Schema{schema}, nil
	}, *cluster.cfg.SchemaCacheConfig)

	return cluster, nil
}

// Close closes a cluster, gracefully closing any open proxy connections to
// Vtctld(s) or VTGate(s) in the cluster, as well as gracefully shutting-down
// any background cache goroutines.
//
// Its primary functions are to avoid leaking connections and other resources
// when dynamic clusters are evicted from an API using dynamic clusters, and
// to avoid data races in tests (the latter of these is caused by the cache
// goroutines).
//
// Sub-components of the cluster are `Close`-d concurrently, caches first, then
// proxy connections.
func (c *Cluster) Close() error {
	var (
		wg  sync.WaitGroup
		rec concurrency.AllErrorRecorder
	)

	// First, close any caches, which may have connections to DB or Vtctld
	// (N.B. (andrew) when we have multiple caches, we can close them
	// concurrently, like we do with the proxies).
	rec.RecordError(c.schemaCache.Close())

	for _, closer := range []io.Closer{c.DB, c.Vtctld} {
		wg.Add(1)
		go func(closer io.Closer) {
			defer wg.Done()
			rec.RecordError(closer.Close())
		}(closer)
	}

	if rec.HasErrors() {
		return fmt.Errorf("failed to cleanly close cluster (id=%s): %w", c.ID, rec.Error())
	}

	return nil
}

// ToProto returns a value-copy protobuf equivalent of the cluster.
func (c Cluster) ToProto() *vtadminpb.Cluster {
	return &vtadminpb.Cluster{
		Id:   c.ID,
		Name: c.Name,
	}
}

func buildPFlagSlice(flags map[string]string) []string {
	args := make([]string, 0, len(flags))
	for k, v := range flags {
		// The k=v syntax is needed to account for negating boolean flags.
		args = append(args, "--"+k+"="+v)
	}

	return args
}

// parseTablets converts a set of *sql.Rows into a slice of Tablets, for the
// given cluster.
func (c *Cluster) parseTablets(rows *sql.Rows) ([]*vtadminpb.Tablet, error) {
	var tablets []*vtadminpb.Tablet

	for rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}

		tablet, err := c.parseTablet(rows)
		if err != nil {
			return nil, err
		}

		tablets = append(tablets, tablet)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tablets, nil
}

// Fields are:
// Cell | Keyspace | Shard | TabletType (string) | ServingState (string) | Alias | Hostname | PrimaryTermStartTime.
func (c *Cluster) parseTablet(rows *sql.Rows) (*vtadminpb.Tablet, error) {
	var (
		cell            string
		tabletTypeStr   string
		servingStateStr string
		aliasStr        string
		mtstStr         string
		topotablet      topodatapb.Tablet

		err error
	)

	if err := rows.Scan(
		&cell,
		&topotablet.Keyspace,
		&topotablet.Shard,
		&tabletTypeStr,
		&servingStateStr,
		&aliasStr,
		&topotablet.Hostname,
		&mtstStr,
	); err != nil {
		return nil, err
	}

	tablet := &vtadminpb.Tablet{
		Cluster: &vtadminpb.Cluster{
			Id:   c.ID,
			Name: c.Name,
		},
		Tablet: &topotablet,
	}

	topotablet.Type, err = topoproto.ParseTabletType(tabletTypeStr)
	if err != nil {
		return nil, err
	}

	tablet.State = vtadminproto.ParseTabletServingState(servingStateStr)

	topotablet.Alias, err = topoproto.ParseTabletAlias(aliasStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse tablet_alias %s: %w", aliasStr, err)
	}

	if topotablet.Alias.Cell != cell {
		// (TODO:@amason) ???
		log.Warningf("tablet cell %s does not match alias %s. ignoring for now", cell, topoproto.TabletAliasString(topotablet.Alias))
	}

	if mtstStr != "" {
		timeTime, err := time.Parse(time.RFC3339, mtstStr)
		if err != nil {
			return nil, fmt.Errorf("failed parsing primary_term_start_time %s: %w", mtstStr, err)
		}

		topotablet.PrimaryTermStartTime = logutil.TimeToProto(timeTime)
	}

	if c.TabletFQDNTmpl != nil {
		tablet.FQDN, err = textutil.ExecuteTemplate(c.TabletFQDNTmpl, tablet)
		if err != nil {
			return nil, fmt.Errorf("failed to execute tablet FQDN template for %+v: %w", tablet, err)
		}
	}

	return tablet, nil
}

// CreateKeyspace creates a keyspace in the given cluster, proxying a
// CreateKeyspaceRequest to a vtctld in that cluster.
func (c *Cluster) CreateKeyspace(ctx context.Context, req *vtctldatapb.CreateKeyspaceRequest) (*vtadminpb.Keyspace, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.CreateKeyspace")
	defer span.Finish()

	AnnotateSpan(c, span)

	if req == nil {
		return nil, fmt.Errorf("%w: request cannot be nil", errors.ErrInvalidRequest)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: keyspace name is required", errors.ErrInvalidRequest)
	}

	span.Annotate("keyspace", req.Name)

	if err := c.topoRWPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("CreateKeyspace(%+v) failed to acquire topoRWPool: %w", req, err)
	}
	defer c.topoRWPool.Release()

	resp, err := c.Vtctld.CreateKeyspace(ctx, req)
	if err != nil {
		return nil, err
	}

	return &vtadminpb.Keyspace{
		Cluster:  c.ToProto(),
		Keyspace: resp.Keyspace,
		Shards:   map[string]*vtctldatapb.Shard{},
	}, nil
}

// CreateShard creates a shard in the given cluster, proxying a
// CreateShardRequest to a vtctld in that cluster.
func (c *Cluster) CreateShard(ctx context.Context, req *vtctldatapb.CreateShardRequest) (*vtctldatapb.CreateShardResponse, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.CreateShard")
	defer span.Finish()

	AnnotateSpan(c, span)

	if req == nil {
		return nil, fmt.Errorf("%w: request cannot be nil", errors.ErrInvalidRequest)
	}

	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.ShardName)
	span.Annotate("force", req.Force)
	span.Annotate("include_parent", req.IncludeParent)

	if req.Keyspace == "" {
		return nil, fmt.Errorf("%w: keyspace name is required", errors.ErrInvalidRequest)
	}

	if req.ShardName == "" {
		return nil, fmt.Errorf("%w: shard name is required", errors.ErrInvalidRequest)
	}

	if err := c.topoRWPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("CreateShard(%+v) failed to acquire topoRWPool: %w", req, err)
	}
	defer c.topoRWPool.Release()

	return c.Vtctld.CreateShard(ctx, req)
}

// DeleteKeyspace deletes a keyspace in the given cluster, proxying a
// DeleteKeyspaceRequest to a vtctld in that cluster.
func (c *Cluster) DeleteKeyspace(ctx context.Context, req *vtctldatapb.DeleteKeyspaceRequest) (*vtctldatapb.DeleteKeyspaceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.DeleteKeyspace")
	defer span.Finish()

	AnnotateSpan(c, span)

	if req == nil {
		return nil, fmt.Errorf("%w: request cannot be nil", errors.ErrInvalidRequest)
	}

	if req.Keyspace == "" {
		return nil, fmt.Errorf("%w: keyspace name is required", errors.ErrInvalidRequest)
	}

	span.Annotate("keyspace", req.Keyspace)

	if err := c.topoRWPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("DeleteKeyspace(%+v) failed to acquire topoRWPool: %w", req, err)
	}
	defer c.topoRWPool.Release()

	return c.Vtctld.DeleteKeyspace(ctx, req)
}

// DeleteShards deletes one or more shards in the given cluster, proxying a
// single DeleteShardsRequest to a vtctld in that cluster.
func (c *Cluster) DeleteShards(ctx context.Context, req *vtctldatapb.DeleteShardsRequest) (*vtctldatapb.DeleteShardsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.DeleteShards")
	defer span.Finish()

	AnnotateSpan(c, span)

	if req == nil {
		return nil, fmt.Errorf("%w: request cannot be nil", errors.ErrInvalidRequest)
	}

	shards := make([]string, len(req.Shards))
	for i, shard := range req.Shards {
		shards[i] = fmt.Sprintf("%s/%s", shard.Keyspace, shard.Name)
	}

	sort.Strings(shards)

	span.Annotate("num_shards", len(shards))
	span.Annotate("shards", strings.Join(shards, ", "))
	span.Annotate("recursive", req.Recursive)
	span.Annotate("even_if_serving", req.EvenIfServing)

	if err := c.topoRWPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("DeleteShards(%+v) failed to acquire topoRWPool: %w", req, err)
	}
	defer c.topoRWPool.Release()

	return c.Vtctld.DeleteShards(ctx, req)
}

// DeleteTablets deletes one or more tablets in the given cluster.
func (c *Cluster) DeleteTablets(ctx context.Context, req *vtctldatapb.DeleteTabletsRequest) (*vtctldatapb.DeleteTabletsResponse, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.DeleteTablets")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("tablet_aliases", strings.Join(topoproto.TabletAliasList(req.TabletAliases).ToStringSlice(), ","))

	if err := c.topoRWPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("DeleteTablets(%+v) failed to acquire topoRWPool: %w", req, err)
	}
	defer c.topoRWPool.Release()

	return c.Vtctld.DeleteTablets(ctx, req)
}

// EmergencyFailoverShard fails over a shard to a new primary. It assumes the
// old primary is dead or otherwise not responding.
func (c *Cluster) EmergencyFailoverShard(ctx context.Context, req *vtctldatapb.EmergencyReparentShardRequest) (*vtadminpb.EmergencyFailoverShardResponse, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.EmergencyFailoverShard")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("new_primary", topoproto.TabletAliasString(req.NewPrimary))
	span.Annotate("ignore_replicas", strings.Join(topoproto.TabletAliasList(req.IgnoreReplicas).ToStringSlice(), ","))
	span.Annotate("prevent_cross_cell_promotion", req.PreventCrossCellPromotion)
	span.Annotate("wait_for_all_tablets", req.WaitForAllTablets)

	if d, ok, err := protoutil.DurationFromProto(req.WaitReplicasTimeout); ok && err == nil {
		span.Annotate("wait_replicas_timeout", d.String())
	}

	if err := c.emergencyFailoverPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("EmergencyFailoverShard(%s/%s) failed to acquire emergencyFailoverPool: %w", req.Keyspace, req.Shard, err)
	}
	defer c.emergencyFailoverPool.Release()

	resp, err := c.Vtctld.EmergencyReparentShard(ctx, req)
	if err != nil {
		return nil, err
	}

	return &vtadminpb.EmergencyFailoverShardResponse{
		Cluster:         c.ToProto(),
		Keyspace:        resp.Keyspace,
		Shard:           resp.Shard,
		PromotedPrimary: resp.PromotedPrimary,
		Events:          resp.Events,
	}, nil
}

// FindAllShardsInKeyspaceOptions modify the behavior of a cluster's
// FindAllShardsInKeyspace method.
type FindAllShardsInKeyspaceOptions struct {
	// skipPool indicates that the caller has already made a successful call to
	// Acquire on the topoReadPool. It is not exported, because the cluster
	// pools are not exported, so it's not possible to manually Acquire from
	// outside this package.
	skipPool bool
}

// FindAllShardsInKeyspace proxies a FindAllShardsInKeyspace RPC to a cluster's
// vtctld, unpacking the response struct.
//
// It can also optionally ensure the vtctldclient has a valid connection before
// making the RPC call.
func (c *Cluster) FindAllShardsInKeyspace(ctx context.Context, keyspace string, opts FindAllShardsInKeyspaceOptions) (map[string]*vtctldatapb.Shard, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.FindAllShardsInKeyspace")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("keyspace", keyspace)

	if !opts.skipPool {
		if err := c.topoReadPool.Acquire(ctx); err != nil {
			return nil, fmt.Errorf("FindAllShardsInKeyspace(%s) failed to acquire topoReadPool: %w", keyspace, err)
		}
		defer c.topoReadPool.Release()
	}

	resp, err := c.Vtctld.FindAllShardsInKeyspace(ctx, &vtctldatapb.FindAllShardsInKeyspaceRequest{
		Keyspace: keyspace,
	})
	if err != nil {
		return nil, fmt.Errorf("FindAllShardsInKeyspace(cluster = %s, keyspace = %s) failed: %w", c.ID, keyspace, err)
	}

	return resp.Shards, nil
}

// FindTablet returns the first tablet in a given cluster that satisfies the filter function.
func (c *Cluster) FindTablet(ctx context.Context, filter func(*vtadminpb.Tablet) bool) (*vtadminpb.Tablet, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.FindTablet")
	defer span.Finish()

	AnnotateSpan(c, span)

	tablets, err := c.findTablets(ctx, filter, 1)
	if err != nil {
		return nil, err
	}

	if len(tablets) != 1 {
		return nil, errors.ErrNoTablet
	}

	return tablets[0], nil
}

// FindTablets returns the first N tablets in the given cluster that satisfy
// the filter function. If N = -1, then all matching tablets are returned.
// Ordering is not guaranteed, and callers should write their filter functions accordingly.
func (c *Cluster) FindTablets(ctx context.Context, filter func(*vtadminpb.Tablet) bool, n int) ([]*vtadminpb.Tablet, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.FindTablets")
	defer span.Finish()

	AnnotateSpan(c, span)

	return c.findTablets(ctx, filter, n)
}

func (c *Cluster) findTablets(ctx context.Context, filter func(*vtadminpb.Tablet) bool, n int) ([]*vtadminpb.Tablet, error) {
	span, _ := trace.FromContext(ctx)

	tablets, err := c.GetTablets(ctx)
	if err != nil {
		return nil, err
	}

	if n == -1 {
		n = len(tablets)
	}

	if span != nil {
		span.Annotate("max_result_length", n) // this is a bad name; I didn't want just "n", but it's more like, "requested result length".
	}

	return vtadminproto.FilterTablets(filter, tablets, n), nil
}

// FindWorkflowsOptions is the set of options for FindWorkflows requests.
type FindWorkflowsOptions struct {
	ActiveOnly      bool
	IgnoreKeyspaces sets.Set[string]
	Filter          func(workflow *vtadminpb.Workflow) bool
}

// FindWorkflows returns a list of Workflows in this cluster, across the given
// keyspaces and filtering according to the options passed in.
//
// If the list of keyspaces to check is empty, then FindWorkflows will use the
// result of GetKeyspaces to search all keyspaces in the cluster. In this case,
// opts.IgnoreKeyspaces is respected.
//
// Callers should use this function when they want more fine-grained filtering,
// and GetWorkflows when they just want to filter on keyspace name.
//
// Note that if only a subset of keyspaces error on their vtctld GetWorkflows
// rpc, this is treated as a partial success, and the ClusterWorkflows response
// will include any errors in the Warnings slice. If all keyspaces fail, or if
// non-(Vtctld.GetWorkflows) calls fail, this is treated as an error by this
// function.
func (c *Cluster) FindWorkflows(ctx context.Context, keyspaces []string, opts FindWorkflowsOptions) (*vtadminpb.ClusterWorkflows, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.FindWorkflows")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("active_only", opts.ActiveOnly)

	return c.findWorkflows(ctx, keyspaces, opts)
}

func (c *Cluster) findWorkflows(ctx context.Context, keyspaces []string, opts FindWorkflowsOptions) (*vtadminpb.ClusterWorkflows, error) {
	if opts.Filter == nil {
		opts.Filter = func(_ *vtadminpb.Workflow) bool { return true }
	}

	if opts.IgnoreKeyspaces == nil {
		opts.IgnoreKeyspaces = sets.New[string]()
	}

	if len(keyspaces) == 0 {
		span, ctx := trace.NewSpan(ctx, "Cluster.GetKeyspaces")
		AnnotateSpan(c, span)

		if err := c.topoReadPool.Acquire(ctx); err != nil {
			span.Finish()
			return nil, fmt.Errorf("findWorkflows(keyspaces = %v, opts = %+v) failed to acquire topoReadPool: %w", keyspaces, opts, err)
		}

		resp, err := c.Vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		c.topoReadPool.Release()

		if err != nil {
			span.Finish()
			return nil, fmt.Errorf("GetKeyspaces(cluster = %s) failed: %w", c.ID, err)
		}

		for _, ks := range resp.Keyspaces {
			keyspaces = append(keyspaces, ks.Name)
		}

		span.Finish()
	} else if opts.IgnoreKeyspaces.Len() > 0 {
		log.Warningf("Cluster.findWorkflows: IgnoreKeyspaces was set, but Keyspaces was not empty; ignoring IgnoreKeyspaces in favor of explicitly checking everything in Keyspaces: (%s)", strings.Join(keyspaces, ", "))
		opts.IgnoreKeyspaces = sets.New[string]()
	}

	// Annotate the parent span with some additional information about the call.
	if span, _ := trace.FromContext(ctx); span != nil {
		span.Annotate("num_keyspaces", len(keyspaces))
		span.Annotate("keyspaces", strings.Join(keyspaces, ","))
		span.Annotate("num_ignore_keyspaces", opts.IgnoreKeyspaces.Len())
		span.Annotate("ignore_keyspaces", strings.Join(sets.List(opts.IgnoreKeyspaces), ","))
	}

	clusterpb := c.ToProto()

	var (
		m       sync.Mutex
		wg      sync.WaitGroup
		rec     concurrency.AllErrorRecorder
		results []*vtadminpb.Workflow
	)

	for _, ks := range keyspaces {
		if opts.IgnoreKeyspaces.Has(ks) {
			log.Infof("Cluster.findWorkflows: ignoring keyspace %s", ks)

			continue
		}

		wg.Add(1)

		go func(ks string) {
			defer wg.Done()

			span, ctx := trace.NewSpan(ctx, "Cluster.GetWorkflowsForKeyspace")
			defer span.Finish()

			AnnotateSpan(c, span)
			span.Annotate("keyspace", ks)
			span.Annotate("active_only", opts.ActiveOnly)

			if err := c.workflowReadPool.Acquire(ctx); err != nil {
				err = fmt.Errorf("GetWorkflows(keyspace = %s, active_only = %v) failed to acquire workflowReadPool: %w", ks, opts.ActiveOnly, err)
				rec.RecordError(err)

				return
			}

			resp, err := c.Vtctld.GetWorkflows(ctx, &vtctldatapb.GetWorkflowsRequest{
				Keyspace:   ks,
				ActiveOnly: opts.ActiveOnly,
			})
			c.workflowReadPool.Release()

			if err != nil {
				err = fmt.Errorf("GetWorkflows(keyspace = %s, active_only = %v) failed: %w", ks, opts.ActiveOnly, err)
				rec.RecordError(err)

				return
			}

			workflows := make([]*vtadminpb.Workflow, 0, len(resp.Workflows))
			for _, wf := range resp.Workflows {
				workflow := &vtadminpb.Workflow{
					Cluster:  clusterpb,
					Keyspace: ks,
					Workflow: wf,
				}

				if opts.Filter(workflow) {
					workflows = append(workflows, workflow)
				}
			}

			m.Lock()
			results = append(results, workflows...)
			m.Unlock()
		}(ks)
	}

	wg.Wait()

	// If every keyspace failed, treat this as an error.
	if rec.HasErrors() && len(rec.Errors) == len(keyspaces) {
		return nil, rec.Error()
	}

	// Otherwise, append any failures into the warnings slice, and return what
	// results we have.
	return &vtadminpb.ClusterWorkflows{
		Workflows: results,
		Warnings:  rec.ErrorStrings(),
	}, nil
}

// GetBackups returns a ClusterBackups object for all backups in the cluster.
func (c *Cluster) GetBackups(ctx context.Context, req *vtadminpb.GetBackupsRequest) ([]*vtadminpb.ClusterBackup, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetBackups")
	defer span.Finish()

	AnnotateSpan(c, span)

	shardsByKeyspace, err := c.getShardSets(ctx, req.Keyspaces, req.KeyspaceShards)
	if err != nil {
		return nil, err
	}

	var (
		m            sync.Mutex
		wg           sync.WaitGroup
		rec          concurrency.AllErrorRecorder
		backups      []*vtadminpb.ClusterBackup
		clusterProto = c.ToProto()
	)

	for ks, shardSet := range shardsByKeyspace {
		for _, shard := range sets.List(shardSet) {
			wg.Add(1)

			go func(keyspace, shard string) {
				defer wg.Done()

				span, ctx := trace.NewSpan(ctx, "Cluster.getBackupsForShard")
				defer span.Finish()

				AnnotateSpan(c, span)
				span.Annotate("keyspace", keyspace)
				span.Annotate("shard", shard)

				if err := c.backupReadPool.Acquire(ctx); err != nil {
					rec.RecordError(fmt.Errorf("GetBackups(%s/%s) failed to acquire backupReadPool: %w", keyspace, shard, err))
					return
				}

				resp, err := c.Vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
					Keyspace:      keyspace,
					Shard:         shard,
					Limit:         req.RequestOptions.Limit,
					Detailed:      req.RequestOptions.Detailed,
					DetailedLimit: req.RequestOptions.DetailedLimit,
				})
				c.backupReadPool.Release()

				if err != nil {
					rec.RecordError(fmt.Errorf("GetBackups(%s/%s): %w", keyspace, shard, err))
					return
				}

				shardBackups := make([]*vtadminpb.ClusterBackup, len(resp.Backups))
				for i, backup := range resp.Backups {
					shardBackups[i] = &vtadminpb.ClusterBackup{
						Cluster: clusterProto,
						Backup:  backup,
					}
				}

				m.Lock()
				defer m.Unlock()

				backups = append(backups, shardBackups...)
			}(ks, shard)
		}
	}

	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return backups, nil
}

func (c *Cluster) getShardSets(ctx context.Context, keyspaces []string, keyspaceShards []string) (map[string]sets.Set[string], error) {
	shardsByKeyspace := map[string]sets.Set[string]{}

	if len(keyspaces) == 0 && len(keyspaceShards) == 0 {
		// Special case: if nothing was explicitly passed, get all shards in
		// all keyspaces.
		kss, err := c.GetKeyspaces(ctx)
		if err != nil {
			return nil, err
		}

		for _, ks := range kss {
			shardsByKeyspace[ks.Keyspace.Name] = sets.New[string]()
			for _, shard := range ks.Shards {
				shardsByKeyspace[ks.Keyspace.Name].Insert(shard.Name)
			}
		}

		return shardsByKeyspace, nil
	}

	for _, ksShard := range keyspaceShards {
		ks, shard, err := topoproto.ParseKeyspaceShard(ksShard)
		if err != nil {
			return nil, err
		}

		if _, ok := shardsByKeyspace[ks]; !ok {
			shardsByKeyspace[ks] = sets.New[string](shard)
			continue
		}

		shardsByKeyspace[ks].Insert(shard)
	}

	for _, ks := range keyspaces {
		// For each keyspace specified, if it was also one of the keyspaceShards,
		// we added the set in the above loop, so nothing to do. If not, add an
		// empty set to indicate we should take all shards in the GetKeyspace
		// section below.
		if _, ok := shardsByKeyspace[ks]; !ok {
			shardsByKeyspace[ks] = sets.New[string]()
		}
	}

	var (
		m   sync.Mutex
		wg  sync.WaitGroup
		rec concurrency.AllErrorRecorder
	)

	m.Lock() // lock the map while we're iterating over it

	for ksName, shardSet := range shardsByKeyspace {
		wg.Add(1)

		go func(ksName string, shardSet sets.Set[string]) {
			defer wg.Done()

			keyspace, err := c.GetKeyspace(ctx, ksName)
			if err != nil {
				if strings.Contains(err.Error(), "node doesn't exist") {
					// (TODO:@ajm188) Make better use of error codes on the
					// vtctld side, and we can do better checking here.
					// Since this is on the client-side of an RPC we can't
					// even use topo.IsErrType(topo.NoNode) :(
					log.Warningf("getShardSets(): keyspace %s does not exist in cluster %s", ksName, c.ID)
					m.Lock()
					defer m.Unlock()

					delete(shardsByKeyspace, ksName)
					return
				}

				rec.RecordError(err)
				return
			}

			fullShardSet := sets.New[string]()
			for _, shard := range keyspace.Shards {
				fullShardSet.Insert(shard.Name)
			}

			if shardSet.Len() == 0 {
				m.Lock()
				defer m.Unlock()

				shardsByKeyspace[ksName] = fullShardSet
				return
			}

			overlap := shardSet.Intersection(fullShardSet)
			if overlap.Len() != shardSet.Len() {
				log.Warningf("getShardSets(): keyspace %s is missing specified shards in cluster %s: %v", ksName, c.ID, sets.List(shardSet.Difference(overlap)))
			}

			m.Lock()
			defer m.Unlock()

			shardsByKeyspace[ksName] = overlap
		}(ksName, shardSet)
	}

	m.Unlock()
	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return shardsByKeyspace, nil
}

// GetCellInfos returns a list of ClusterCellInfo objects for cells in the
// given cluster.
//
// If req.Cells is set, cells are restricted only to cells with those names.
// Note: specifying a cell name that does not exist in the cluster fails the
// overall request.
//
// If req.NamesOnly is set, each ClusterCellInfo will only contain the Cluster
// and Name fields. req.Cells takes precedence over this option.
func (c *Cluster) GetCellInfos(ctx context.Context, req *vtadminpb.GetCellInfosRequest) ([]*vtadminpb.ClusterCellInfo, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetCellInfos")
	defer span.Finish()

	names := req.Cells
	if len(names) == 0 {
		if err := c.topoReadPool.Acquire(ctx); err != nil {
			return nil, fmt.Errorf("GetCellInfoNames() failed to acquire topoReadPool: %w", err)
		}
		resp, err := c.Vtctld.GetCellInfoNames(ctx, &vtctldatapb.GetCellInfoNamesRequest{})
		c.topoReadPool.Release()

		if err != nil {
			return nil, fmt.Errorf("failed to GetCellInfoNames: %w", err)
		}

		names = resp.Names
	}

	namesOnly := req.NamesOnly
	if namesOnly && len(req.Cells) > 0 {
		log.Warning("Cluster.GetCellInfos: req.Cells and req.NamesOnly set, ignoring NamesOnly")
		namesOnly = false
	}

	span.Annotate("names_only", namesOnly)
	span.Annotate("cells", req.Cells) // deliberately not the cellnames we (maybe) fetched above

	cpb := c.ToProto()
	infos := make([]*vtadminpb.ClusterCellInfo, 0, len(names))
	if namesOnly {
		for _, name := range names {
			infos = append(infos, &vtadminpb.ClusterCellInfo{
				Cluster: cpb,
				Name:    name,
			})
		}

		return infos, nil
	}

	var (
		m   sync.Mutex
		wg  sync.WaitGroup
		rec concurrency.AllErrorRecorder
	)

	for _, name := range names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()

			if err := c.topoReadPool.Acquire(ctx); err != nil {
				rec.RecordError(fmt.Errorf("GetCellInfo(%s) failed to acquire topoReadPool: %w", name, err))
				return
			}
			resp, err := c.Vtctld.GetCellInfo(ctx, &vtctldatapb.GetCellInfoRequest{
				Cell: name,
			})
			c.topoReadPool.Release()

			if err != nil {
				rec.RecordError(fmt.Errorf("GetCellInfo(%s) failed: %w", name, err))
				return
			}

			m.Lock()
			defer m.Unlock()
			infos = append(infos, &vtadminpb.ClusterCellInfo{
				Cluster:  cpb,
				Name:     name,
				CellInfo: resp.CellInfo,
			})
		}(name)
	}

	wg.Wait()
	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return infos, nil
}

// GetCellsAliases returns all CellsAliases in the cluster.
func (c *Cluster) GetCellsAliases(ctx context.Context) (*vtadminpb.ClusterCellsAliases, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetCellsAliases")
	defer span.Finish()

	if err := c.topoReadPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("GetCellsAliases() failed to acquire topoReadPool: %w", err)
	}
	defer c.topoReadPool.Release()

	resp, err := c.Vtctld.GetCellsAliases(ctx, &vtctldatapb.GetCellsAliasesRequest{})
	if err != nil {
		return nil, err
	}

	return &vtadminpb.ClusterCellsAliases{
		Cluster: c.ToProto(),
		Aliases: resp.Aliases,
	}, nil
}

// GetGates returns the list of all VTGates in the cluster.
func (c *Cluster) GetGates(ctx context.Context) ([]*vtadminpb.VTGate, error) {
	// (TODO|@ajm188) Support tags in the vtadmin RPC request and pass them
	// through here.
	gates, err := c.Discovery.DiscoverVTGates(ctx, []string{})
	if err != nil {
		return nil, fmt.Errorf("DiscoverVTGates(cluster = %s): %w", c.ID, err)
	}

	// This overwrites any Cluster field populated by a particular discovery
	// implementation.
	cpb := c.ToProto()

	for _, g := range gates {
		g.Cluster = cpb
	}

	return gates, nil
}

// GetKeyspace returns a single keyspace in the cluster.
func (c *Cluster) GetKeyspace(ctx context.Context, name string) (*vtadminpb.Keyspace, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetKeyspace")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("keyspace", name)

	if err := c.topoReadPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("GetKeyspace(%s) failed to acquire topoReadPool: %w", name, err)
	}
	defer c.topoReadPool.Release()

	resp, err := c.Vtctld.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{
		Keyspace: name,
	})
	if err != nil {
		return nil, err
	}

	shards, err := c.FindAllShardsInKeyspace(ctx, name, FindAllShardsInKeyspaceOptions{
		skipPool: true, // we already acquired before making the GetKeyspace call
	})
	if err != nil {
		return nil, err
	}

	return &vtadminpb.Keyspace{
		Cluster:  c.ToProto(),
		Keyspace: resp.Keyspace,
		Shards:   shards,
	}, nil
}

// GetKeyspaces returns all keyspaces, with their shard maps, in the cluster.
func (c *Cluster) GetKeyspaces(ctx context.Context) ([]*vtadminpb.Keyspace, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetKeyspaces")
	defer span.Finish()

	AnnotateSpan(c, span)

	if err := c.topoReadPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("GetKeyspaces() failed to acquire topoReadPool: %w", err)
	}

	resp, err := c.Vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	c.topoReadPool.Release()

	if err != nil {
		return nil, err
	}

	var (
		m         sync.Mutex
		wg        sync.WaitGroup
		rec       concurrency.AllErrorRecorder
		keyspaces = make([]*vtadminpb.Keyspace, len(resp.Keyspaces))
	)

	for i, ks := range resp.Keyspaces {
		wg.Add(1)
		go func(i int, ks *vtctldatapb.Keyspace) {
			defer wg.Done()

			shards, err := c.FindAllShardsInKeyspace(ctx, ks.Name, FindAllShardsInKeyspaceOptions{})
			if err != nil {
				rec.RecordError(err)
				return
			}

			keyspace := &vtadminpb.Keyspace{
				Cluster:  c.ToProto(),
				Keyspace: ks,
				Shards:   shards,
			}

			m.Lock()
			defer m.Unlock()
			keyspaces[i] = keyspace
		}(i, ks)
	}

	wg.Wait()
	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return keyspaces, nil
}

// GetSrvKeyspaces returns all SrvKeyspaces for all keyspaces in a cluster.
func (c *Cluster) GetSrvKeyspaces(ctx context.Context, cells []string) (map[string]*vtctldatapb.GetSrvKeyspacesResponse, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetKeyspaces")
	AnnotateSpan(c, span)

	defer span.Finish()
	keyspaces, err := c.Vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	if err != nil {
		return nil, fmt.Errorf("GetKeyspaces(cluster = %s): %w", c.ID, err)
	}

	var (
		m            sync.Mutex
		wg           sync.WaitGroup
		rec          concurrency.AllErrorRecorder
		srvKeyspaces = make(map[string]*vtctldatapb.GetSrvKeyspacesResponse, len(keyspaces.Keyspaces))
	)

	for _, keyspace := range keyspaces.Keyspaces {
		wg.Add(1)

		go func(keyspace *vtctldatapb.Keyspace) {
			defer wg.Done()
			srv_keyspaces, err := c.Vtctld.GetSrvKeyspaces(ctx, &vtctldatapb.GetSrvKeyspacesRequest{Keyspace: keyspace.Name, Cells: cells})
			if err != nil {
				rec.RecordError(fmt.Errorf("GetSrvKeyspaces(keyspace = %s): %w", keyspace.Name, err))
				return
			}

			m.Lock()
			srvKeyspaces[keyspace.Name] = srv_keyspaces
			m.Unlock()
		}(keyspace)
	}

	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return srvKeyspaces, nil
}

// GetTablets returns all tablets in the cluster.
func (c *Cluster) GetTablets(ctx context.Context) ([]*vtadminpb.Tablet, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetTablets")
	defer span.Finish()

	AnnotateSpan(c, span)

	return c.getTablets(ctx)
}

func (c *Cluster) getTablets(ctx context.Context) ([]*vtadminpb.Tablet, error) {
	rows, err := c.DB.ShowTablets(ctx)
	if err != nil {
		return nil, err
	}

	return c.parseTablets(rows)
}

// GetSchemaOptions contains the options that modify the behavior of the
// (*Cluster).GetSchema method.
type GetSchemaOptions struct {
	// BaseRequest is used to share some common parameters to use for the
	// individual tablet GetSchema RPCs made by (*Cluster).GetSchema, which
	// takes a copy of this request in order to makeb certain overrides as
	// needed, so these mutations are transparent to the caller.
	//
	// The TabletAlias field is ignored completely by (*Cluster).GetSchema, as
	// it is overwritten for each tablet RPC that method makes.
	//
	// The TableSizesOnly field is overwritten only in certain tablet RPCs when
	// SizeOpts.AggregateSizes is true. In order to move minimal bytes over the
	// wire, we assume that schema definitions match across all shards, so we
	// can get the full schema from just one tablet, and then just the table
	// size information from the other N-1 tablets.
	//
	// The TableNamesOnly field is untouched by (*Cluster).GetSchema when not
	// doing size aggregation. However, when doing size aggregation, if
	// TableNamesOnly is true, we log a warning and override it. This is because
	// TableNamesOnly is mutually exclusive with TableSizesOnly, and size
	// aggregation requires setting TableSizesOnly in the cases described above.
	BaseRequest *vtctldatapb.GetSchemaRequest
	// TableSizeOptions control whether the (*Cluster).GetSchema method performs
	// cross-shard table size aggregation (via the AggregateSizes field).
	//
	// If the AggregateSizes field is false, the rest of this struct is ignored,
	// no size aggregation is done, and (*Cluster).GetSchema will make exactly
	// one GetSchema RPC to a SERVING tablet in the keyspace.
	//
	// If the AggregateSizes field is true, (*Cluster).GetSchema will make a
	// FindAllShardsInKeyspace vtctld RPC, and then filter the given Tablets
	// (described above) to find one SERVING tablet for each shard in the
	// keyspace, skipping any non-serving shards in the keyspace.
	TableSizeOptions *vtadminpb.GetSchemaTableSizeOptions

	isBackfill bool
}

// GetSchema returns the schema for a given keyspace. GetSchema has a few
// different behaviors depending on the GetSchemaOptions provided, as follows:
//
// (1) If opts.SizeOpts.AggregateSizes is true, we will also make a call to
// FindAllShardsInKeyspace, in order to fan out GetSchema RPCs to a tablet in
// each shard. If this option is false, we make exactly one GetSchema request to
// a single, randomly-chosen, tablet in the keyspace.
//
// (1.1) If, in size aggregation mode, opts.SizeOpts.IncludeNonServingShards is
// false (the default), then we will filter out any shards for which
// IsPrimaryServing is false in the topo, and make GetSchema RPCs to one tablet
// in every _serving_ shard. Otherwise we will make a GetSchema RPC to one
// tablet in _every_ shard.
//
// (2) Irrespective of whether we're including nonserving shards, or whether
// we're doing size aggregation at all, we will only make GetSchema RPCs to
// tablets that are in SERVING state; we don't want to use a tablet that might
// be in a bad state as the source of truth for a schema. Therefore if we can't
// find a SERVING tablet for the keyspace (in non-aggregation mode) or for a
// shard in that keyspace (in aggregation mode), then we will return an error
// back to the caller.
func (c *Cluster) GetSchema(ctx context.Context, keyspace string, opts GetSchemaOptions) (*vtadminpb.Schema, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetSchema")
	defer span.Finish()

	if opts.TableSizeOptions == nil {
		opts.TableSizeOptions = &vtadminpb.GetSchemaTableSizeOptions{
			AggregateSizes:          false,
			IncludeNonServingShards: false,
		}
	}

	if opts.BaseRequest == nil {
		opts.BaseRequest = &vtctldatapb.GetSchemaRequest{}
	}

	if opts.TableSizeOptions.AggregateSizes && opts.BaseRequest.TableNamesOnly {
		log.Warningf("GetSchema(cluster = %s) size aggregation is incompatible with TableNamesOnly, ignoring the latter in favor of aggregating sizes", c.ID)
		opts.BaseRequest.TableNamesOnly = false
	}

	AnnotateSpan(c, span)
	span.Annotate("keyspace", keyspace)
	annotateGetSchemaRequest(opts.BaseRequest, span)
	vtadminproto.AnnotateSpanWithGetSchemaTableSizeOptions(opts.TableSizeOptions, span)
	span.Annotate("is_backfill", opts.isBackfill)

	key := schemacache.Key{
		ClusterID:               c.ID,
		Keyspace:                keyspace,
		IncludeNonServingShards: opts.TableSizeOptions.IncludeNonServingShards,
	}
	if !(opts.isBackfill || cache.ShouldRefreshFromIncomingContext(ctx)) {
		schema, ok, err := schemacache.LoadOne(c.schemaCache, key, schemacache.LoadOptions{
			BaseRequest:    opts.BaseRequest,
			AggregateSizes: opts.TableSizeOptions.AggregateSizes,
		})

		span.Annotate("cache_hit", ok)
		if ok {
			return schema, err
		}
	}

	// Fetch all tablets for the keyspace.
	tablets, err := c.FindTablets(ctx, func(tablet *vtadminpb.Tablet) bool {
		return tablet.Tablet.Keyspace == keyspace
	}, -1)
	if err != nil {
		return nil, fmt.Errorf("%w for keyspace %s", errors.ErrNoTablet, keyspace)
	}

	tabletsToQuery, err := c.getTabletsToQueryForSchemas(ctx, keyspace, tablets, opts)
	if err != nil {
		return nil, err
	}

	schema, err := c.getSchemaFromTablets(ctx, keyspace, tabletsToQuery, opts)
	if err != nil {
		return nil, err
	}

	go schemacache.AddOrBackfill(c.schemaCache, []*vtadminpb.Schema{schema}, key, cache.DefaultExpiration, schemacache.LoadOptions{
		BaseRequest:    opts.BaseRequest,
		AggregateSizes: opts.TableSizeOptions.AggregateSizes,
	})

	return schema, nil
}

// GetSchemas returns all of the schemas across all keyspaces in the cluster.
func (c *Cluster) GetSchemas(ctx context.Context, opts GetSchemaOptions) ([]*vtadminpb.Schema, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetSchemas")
	defer span.Finish()

	if opts.TableSizeOptions == nil {
		opts.TableSizeOptions = &vtadminpb.GetSchemaTableSizeOptions{
			AggregateSizes:          false,
			IncludeNonServingShards: false,
		}
	}

	if opts.BaseRequest == nil {
		opts.BaseRequest = &vtctldatapb.GetSchemaRequest{}
	}

	if opts.TableSizeOptions.AggregateSizes && opts.BaseRequest.TableNamesOnly {
		log.Warningf("GetSchemas(cluster = %s) size aggregation is incompatible with TableNamesOnly, ignoring the latter in favor of aggregating sizes", c.ID)
		opts.BaseRequest.TableNamesOnly = false
	}

	AnnotateSpan(c, span)
	annotateGetSchemaRequest(opts.BaseRequest, span)
	vtadminproto.AnnotateSpanWithGetSchemaTableSizeOptions(opts.TableSizeOptions, span)
	span.Annotate("is_backfill", opts.isBackfill)

	key := schemacache.Key{
		ClusterID:               c.ID,
		Keyspace:                "",
		IncludeNonServingShards: opts.TableSizeOptions.IncludeNonServingShards,
	}
	if !(opts.isBackfill || cache.ShouldRefreshFromIncomingContext(ctx)) {
		schemas, ok, err := schemacache.LoadAll(c.schemaCache, key, schemacache.LoadOptions{
			BaseRequest:    opts.BaseRequest,
			AggregateSizes: opts.TableSizeOptions.AggregateSizes,
		})

		span.Annotate("cache_hit", ok)
		if ok {
			return schemas, err
		}
	}

	var (
		m   sync.Mutex
		wg  sync.WaitGroup
		rec concurrency.AllErrorRecorder

		tablets   []*vtadminpb.Tablet
		keyspaces []*vtadminpb.Keyspace

		schemas []*vtadminpb.Schema
	)

	// Start by collecting the tablets and keyspace names concurrently.
	wg.Add(1)
	go func() {
		defer wg.Done()

		var err error
		tablets, err = c.GetTablets(ctx)
		if err != nil {
			rec.RecordError(err)
			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		// TODO: (ajm188) we can't use c.GetKeyspaces because it also makes a
		// FindAllShardsInKeyspace call for each keyspace, which we may or may
		// not need. Refactor that method so we can get better code reuse.
		span, ctx := trace.NewSpan(ctx, "Cluster.GetKeyspaces")
		defer span.Finish()

		if err := c.topoReadPool.Acquire(ctx); err != nil {
			rec.RecordError(fmt.Errorf("GetKeyspaces() failed to acquire topoReadPool: %w", err))
			return
		}

		resp, err := c.Vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		c.topoReadPool.Release()

		if err != nil {
			rec.RecordError(err)
			return
		}

		keyspaces = make([]*vtadminpb.Keyspace, len(resp.Keyspaces))
		for i, ks := range resp.Keyspaces {
			keyspaces[i] = &vtadminpb.Keyspace{
				Cluster:  c.ToProto(),
				Keyspace: ks,
			}
		}
	}()

	wg.Wait()
	if rec.HasErrors() {
		return nil, rec.Error()
	}

	// Now, fan out to collect the schemas.
	for _, ks := range keyspaces {
		wg.Add(1)
		go func(ctx context.Context, ks *vtadminpb.Keyspace) {
			defer wg.Done()

			tablets, err := c.getTabletsToQueryForSchemas(ctx, ks.Keyspace.Name, tablets, opts)
			if err != nil {
				// Ignore keyspaces without any serving tablets.
				if stderrors.Is(err, errors.ErrNoServingTablet) {
					log.Infof(err.Error())
					return
				}

				rec.RecordError(fmt.Errorf("opts %+v, err: %w", opts, err))
				return
			}

			schema, err := c.getSchemaFromTablets(ctx, ks.Keyspace.Name, tablets, opts)
			if err != nil {
				rec.RecordError(err)
				return
			}

			// Ignore keyspaces without schemas
			if schema == nil {
				log.Infof("No schemas for %s", ks.Keyspace.Name)
				return
			}

			if len(schema.TableDefinitions) == 0 {
				log.Infof("No tables in schema for %s", ks.Keyspace.Name)
				return
			}

			m.Lock()
			schemas = append(schemas, schema)
			m.Unlock()
		}(ctx, ks)
	}

	wg.Wait()
	if rec.HasErrors() {
		return nil, rec.Error()
	}

	go schemacache.AddOrBackfill(c.schemaCache, schemas, key, cache.DefaultExpiration, schemacache.LoadOptions{
		BaseRequest:    opts.BaseRequest,
		AggregateSizes: opts.TableSizeOptions.AggregateSizes,
	})

	return schemas, nil
}

// Note that for this function we use the tablets parameter, ignoring the
// opts.Tablets value completely.
func (c *Cluster) getSchemaFromTablets(ctx context.Context, keyspace string, tablets []*vtadminpb.Tablet, opts GetSchemaOptions) (*vtadminpb.Schema, error) {
	var (
		m      sync.Mutex
		wg     sync.WaitGroup
		rec    concurrency.AllErrorRecorder
		schema = &vtadminpb.Schema{
			Cluster:    c.ToProto(),
			Keyspace:   keyspace,
			TableSizes: map[string]*vtadminpb.Schema_TableSize{},
		}
		// Instead of starting at false, we start with whatever the base request
		// specified. If we have exactly one tablet to query (i.e. we're not
		// doing multi-shard aggregation), it's possible the request was to
		// literally just get the table sizes; we shouldn't assume. If we have
		// more than one tablet to query, then we are doing size aggregation,
		// and we'll flip this to true after spawning the first GetSchema rpc.
		sizesOnly = opts.BaseRequest.TableSizesOnly
	)

	for _, tablet := range tablets {
		wg.Add(1)

		go func(tablet *vtadminpb.Tablet, sizesOnly bool) {
			defer wg.Done()

			span, ctx := trace.NewSpan(ctx, "Vtctld.GetSchema")
			defer span.Finish()

			req := proto.Clone(opts.BaseRequest).(*vtctldatapb.GetSchemaRequest)
			req.TableSizesOnly = sizesOnly
			req.TabletAlias = tablet.Tablet.Alias

			AnnotateSpan(c, span)
			annotateGetSchemaRequest(req, span)
			span.Annotate("keyspace", keyspace)
			span.Annotate("shard", tablet.Tablet.Shard)

			if err := c.schemaReadPool.Acquire(ctx); err != nil {
				err = fmt.Errorf("GetSchema(cluster = %s, keyspace = %s, tablet = %s) failed to acquire schemaReadPool: %w", c.ID, keyspace, tablet.Tablet.Alias, err)
				rec.RecordError(err)
				return
			}

			resp, err := c.Vtctld.GetSchema(ctx, req)
			c.schemaReadPool.Release()

			if err != nil {
				err = fmt.Errorf("GetSchema(cluster = %s, keyspace = %s, tablet = %s) failed: %w", c.ID, keyspace, tablet.Tablet.Alias, err)
				rec.RecordError(err)

				return
			}

			if resp == nil || resp.Schema == nil {
				return
			}

			m.Lock()
			defer m.Unlock()

			if !sizesOnly {
				schema.TableDefinitions = resp.Schema.TableDefinitions
			}

			if !opts.TableSizeOptions.AggregateSizes {
				return
			}

			for _, td := range resp.Schema.TableDefinitions {
				tableSize, ok := schema.TableSizes[td.Name]
				if !ok {
					tableSize = &vtadminpb.Schema_TableSize{
						ByShard: map[string]*vtadminpb.Schema_ShardTableSize{},
					}
					schema.TableSizes[td.Name] = tableSize
				}

				if _, ok = tableSize.ByShard[tablet.Tablet.Shard]; ok {
					err := fmt.Errorf("duplicate shard queries for table %s on shard %s/%s", td.Name, keyspace, tablet.Tablet.Shard)
					log.Warningf("Impossible: %s", err)
					rec.RecordError(err)

					return
				}

				tableSize.RowCount += td.RowCount
				tableSize.DataLength += td.DataLength

				tableSize.ByShard[tablet.Tablet.Shard] = &vtadminpb.Schema_ShardTableSize{
					RowCount:   td.RowCount,
					DataLength: td.DataLength,
				}
			}
		}(tablet, sizesOnly)

		// If we have more than one tablet to query, we definitely don't want to
		// get more than the sizes twice, so invariably set this to true for
		// subsequent iterations
		sizesOnly = true
	}

	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return schema, nil
}

func (c *Cluster) getTabletsToQueryForSchemas(ctx context.Context, keyspace string, tablets []*vtadminpb.Tablet, opts GetSchemaOptions) ([]*vtadminpb.Tablet, error) {
	if opts.TableSizeOptions.AggregateSizes {
		shards, err := c.FindAllShardsInKeyspace(ctx, keyspace, FindAllShardsInKeyspaceOptions{})
		if err != nil {
			return nil, err
		}

		tabletsToQuery := make([]*vtadminpb.Tablet, 0, len(shards))

		for _, shard := range shards {
			// In certain setups, empty but "serving" shards may required to
			// provide a contiguous keyspace so that certain keyspace-level
			// operations will work. In our case, we care about whether the
			// shard is truly serving, which we define as also having a known
			// primary (via PrimaryAlias) in addition to the IsPrimaryServing bit.
			if !shard.Shard.IsPrimaryServing || shard.Shard.PrimaryAlias == nil {
				if !opts.TableSizeOptions.IncludeNonServingShards {
					log.Infof("%s/%s is not serving; ignoring because IncludeNonServingShards = false", keyspace, shard.Name)
					continue
				}
			}

			shardTablets := vtadminproto.FilterTablets(func(tablet *vtadminpb.Tablet) bool {
				return tablet.Tablet.Keyspace == keyspace && tablet.Tablet.Shard == shard.Name && tablet.State == vtadminpb.Tablet_SERVING
			}, tablets, len(tablets))

			if len(shardTablets) == 0 {
				return nil, fmt.Errorf("%w for shard %s/%s", errors.ErrNoServingTablet, shard.Keyspace, shard.Name)
			}

			randomServingTablet := shardTablets[rand.Intn(len(shardTablets))]
			tabletsToQuery = append(tabletsToQuery, randomServingTablet)
		}

		return tabletsToQuery, nil
	}

	keyspaceTablets := vtadminproto.FilterTablets(func(tablet *vtadminpb.Tablet) bool {
		return tablet.Tablet.Keyspace == keyspace && tablet.State == vtadminpb.Tablet_SERVING
	}, tablets, len(tablets))

	if len(keyspaceTablets) == 0 {
		err := fmt.Errorf("%w for keyspace %s", errors.ErrNoServingTablet, keyspace)
		log.Warningf("%s. Searched tablets: %v", err, vtadminproto.Tablets(tablets).AliasStringList())
		return nil, err
	}

	randomServingTablet := keyspaceTablets[rand.Intn(len(keyspaceTablets))]
	return []*vtadminpb.Tablet{randomServingTablet}, nil
}

// GetShardReplicationPositions returns a ClusterShardReplicationPosition object
// for each keyspace/shard in the cluster.
func (c *Cluster) GetShardReplicationPositions(ctx context.Context, req *vtadminpb.GetShardReplicationPositionsRequest) ([]*vtadminpb.ClusterShardReplicationPosition, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetShardReplicationPositions")
	defer span.Finish()

	AnnotateSpan(c, span)

	shardsByKeyspace, err := c.getShardSets(ctx, req.Keyspaces, req.KeyspaceShards)
	if err != nil {
		return nil, err
	}

	var (
		m            sync.Mutex
		wg           sync.WaitGroup
		rec          concurrency.AllErrorRecorder
		positions    []*vtadminpb.ClusterShardReplicationPosition
		clusterProto = c.ToProto()
	)

	for ks, shardSet := range shardsByKeyspace {
		for _, shard := range sets.List(shardSet) {
			wg.Add(1)

			go func(keyspace, shard string) {
				defer wg.Done()

				span, ctx := trace.NewSpan(ctx, "Cluster.getShardReplicationPositionsForShard")
				defer span.Finish()

				AnnotateSpan(c, span)
				span.Annotate("keyspace", keyspace)
				span.Annotate("shard", shard)

				if err := c.topoReadPool.Acquire(ctx); err != nil {
					rec.RecordError(fmt.Errorf("ShardReplicationPositions(%s/%s) failed to acquire topoReadPool: %w", keyspace, shard, err))
					return
				}

				resp, err := c.Vtctld.ShardReplicationPositions(ctx, &vtctldatapb.ShardReplicationPositionsRequest{
					Keyspace: keyspace,
					Shard:    shard,
				})
				c.topoReadPool.Release()

				if err != nil {
					rec.RecordError(fmt.Errorf("ShardReplicationPositions(%s/%s): %w", keyspace, shard, err))
					return
				}

				m.Lock()
				defer m.Unlock()

				positions = append(positions, &vtadminpb.ClusterShardReplicationPosition{
					Cluster:      clusterProto,
					Keyspace:     keyspace,
					Shard:        shard,
					PositionInfo: resp,
				})
			}(ks, shard)
		}
	}

	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return positions, nil
}

// GetSrvVSchema returns the SrvVSchema for a given cell in the cluster.
func (c *Cluster) GetSrvVSchema(ctx context.Context, cell string) (*vtadminpb.SrvVSchema, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetVSchema")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("cell", cell)

	if err := c.topoReadPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("GetSrvVSchema(%s) failed to acquire topoReadPool: %w", cell, err)
	}
	defer c.topoReadPool.Release()

	sv, err := c.Vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{
		Cell: cell,
	})

	if err != nil {
		return nil, err
	}

	return &vtadminpb.SrvVSchema{
		Cell:       cell,
		Cluster:    c.ToProto(),
		SrvVSchema: sv.SrvVSchema,
	}, nil
}

// GetSrvVSchemas returns the SrvVSchema for all cells in the cluster,
// optionally filtered by cell.
func (c *Cluster) GetSrvVSchemas(ctx context.Context, cells []string) ([]*vtadminpb.SrvVSchema, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetVSchemas")
	defer span.Finish()

	AnnotateSpan(c, span)

	if err := c.topoReadPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("GetSrvVSchema(cluster = %s, cells = %v) failed to acquire topoReadPool: %w", c.ID, cells, err)
	}
	resp, err := c.Vtctld.GetSrvVSchemas(ctx, &vtctldatapb.GetSrvVSchemasRequest{
		Cells: cells,
	})
	c.topoReadPool.Release()

	if err != nil {
		return nil, err
	}

	svs := make([]*vtadminpb.SrvVSchema, 0, len(resp.SrvVSchemas))

	for cell, s := range resp.SrvVSchemas {
		svs = append(svs, &vtadminpb.SrvVSchema{
			Cell:       cell,
			Cluster:    c.ToProto(),
			SrvVSchema: s,
		})
	}

	return svs, nil
}

// GetVSchema returns the vschema for a given keyspace in this cluster. The
// caller is responsible for making at least one call to c.Vtctld.Dial prior to
// calling this function.
func (c *Cluster) GetVSchema(ctx context.Context, keyspace string) (*vtadminpb.VSchema, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetVSchema")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("keyspace", keyspace)

	if err := c.topoReadPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("GetVSchema(%s) failed to acquire topoReadPool: %w", keyspace, err)
	}
	defer c.topoReadPool.Release()

	vschema, err := c.Vtctld.GetVSchema(ctx, &vtctldatapb.GetVSchemaRequest{
		Keyspace: keyspace,
	})

	if err != nil {
		return nil, err
	}

	return &vtadminpb.VSchema{
		Cluster: c.ToProto(),
		Name:    keyspace,
		VSchema: vschema.VSchema,
	}, nil
}

// GetVtctlds returns a list of all Vtctlds in the cluster.
func (c *Cluster) GetVtctlds(ctx context.Context) ([]*vtadminpb.Vtctld, error) {
	vtctlds, err := c.Discovery.DiscoverVtctlds(ctx, []string{})
	if err != nil {
		return nil, fmt.Errorf("DiscoverVtctlds(cluster = %s): %w", c.ID, err)
	}

	// This overwrites any Cluster field populated by a particular discovery
	// implementation.
	cpb := c.ToProto()

	for _, v := range vtctlds {
		v.Cluster = cpb
	}

	return vtctlds, nil
}

// GetWorkflowOptions is the set of filtering options for GetWorkflow requests.
type GetWorkflowOptions struct {
	ActiveOnly bool
}

// GetWorkflow returns the single Workflow in this cluster for the given
// keyspace and workflow name. It returns an error if either no workflows or
// multiple workflows are found.
func (c *Cluster) GetWorkflow(ctx context.Context, keyspace string, name string, opts GetWorkflowOptions) (*vtadminpb.Workflow, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetWorkflow")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("active_only", opts.ActiveOnly)
	span.Annotate("keyspace", keyspace)
	span.Annotate("workflow_name", name)

	workflows, err := c.findWorkflows(ctx, []string{keyspace}, FindWorkflowsOptions{
		ActiveOnly: opts.ActiveOnly,
		Filter: func(workflow *vtadminpb.Workflow) bool {
			return workflow.Workflow.Name == name
		},
	})
	if err != nil {
		return nil, err
	}

	switch len(workflows.Workflows) {
	case 0:
		msg := "%w for keyspace %s and name %s (active_only = %v)"
		if len(workflows.Warnings) > 0 {
			return nil, fmt.Errorf(msg+"; warnings: %v", errors.ErrNoWorkflow, keyspace, name, opts.ActiveOnly, workflows.Warnings)
		}

		return nil, fmt.Errorf(msg, errors.ErrNoWorkflow, keyspace, name, opts.ActiveOnly)
	case 1:
		return workflows.Workflows[0], nil
	default:
		return nil, fmt.Errorf("%w: found %d workflows in keyspace %s with name %s (active_only = %v); this should be impossible", errors.ErrAmbiguousWorkflow, len(workflows.Workflows), keyspace, name, opts.ActiveOnly)
	}
}

// GetWorkflowsOptions is the set of filtering options for GetWorkflows
// requests.
type GetWorkflowsOptions struct {
	ActiveOnly      bool
	IgnoreKeyspaces sets.Set[string]
}

// GetWorkflows returns a list of Workflows in this cluster, across the given
// keyspaces and filtering according to the options passed in.
//
// If the list of keyspaces to check is empty, then GetWorkflows will use the
// result of GetKeyspaces to search all keyspaces in the cluster. In this case,
// opts.IgnoreKeyspaces is respected.
func (c *Cluster) GetWorkflows(ctx context.Context, keyspaces []string, opts GetWorkflowsOptions) (*vtadminpb.ClusterWorkflows, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetWorkflows")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("active_only", opts.ActiveOnly)

	return c.findWorkflows(ctx, keyspaces, FindWorkflowsOptions{
		ActiveOnly:      opts.ActiveOnly,
		IgnoreKeyspaces: opts.IgnoreKeyspaces,
		Filter:          func(_ *vtadminpb.Workflow) bool { return true },
	})
}

// PlannedFailoverShard fails over the shard either to a new primary or away
// from an old primary. Both the current and candidate primaries must be
// reachable and running.
func (c *Cluster) PlannedFailoverShard(ctx context.Context, req *vtctldatapb.PlannedReparentShardRequest) (*vtadminpb.PlannedFailoverShardResponse, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.PlannedFailoverShard")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("keyspace", req.Keyspace)
	span.Annotate("shard", req.Shard)
	span.Annotate("new_primary", topoproto.TabletAliasString(req.NewPrimary))
	span.Annotate("avoid_primary", topoproto.TabletAliasString(req.AvoidPrimary))

	if d, ok, err := protoutil.DurationFromProto(req.WaitReplicasTimeout); ok && err == nil {
		span.Annotate("wait_replicas_timeout", d.String())
	}

	if err := c.failoverPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("PlannedFailoverShard(%s/%s): failed to acquire failoverPool: %w", req.Keyspace, req.Shard, err)
	}
	defer c.failoverPool.Release()

	resp, err := c.Vtctld.PlannedReparentShard(ctx, req)
	if err != nil {
		return nil, err
	}

	return &vtadminpb.PlannedFailoverShardResponse{
		Cluster:         c.ToProto(),
		Keyspace:        resp.Keyspace,
		Shard:           resp.Shard,
		PromotedPrimary: resp.PromotedPrimary,
		Events:          resp.Events,
	}, nil
}

// RefreshState reloads the tablet record from a cluster's topo on a tablet.
func (c *Cluster) RefreshState(ctx context.Context, tablet *vtadminpb.Tablet) error {
	span, ctx := trace.NewSpan(ctx, "Cluster.RefreshState")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("tablet_alias", topoproto.TabletAliasString(tablet.Tablet.Alias))

	if err := c.topoReadPool.Acquire(ctx); err != nil {
		return fmt.Errorf("RefreshState(%v) failed to acquire topoReadPool: %w", topoproto.TabletAliasString(tablet.Tablet.Alias), err)
	}
	defer c.topoReadPool.Release()

	_, err := c.Vtctld.RefreshState(ctx, &vtctldatapb.RefreshStateRequest{
		TabletAlias: tablet.Tablet.Alias,
	})
	return err
}

// RefreshTabletReplicationSource performs a `CHANGE REPLICATION SOURCE TO` on
// a tablet to replicate from the current primary in the shard.
func (c *Cluster) RefreshTabletReplicationSource(ctx context.Context, tablet *vtadminpb.Tablet) (*vtadminpb.RefreshTabletReplicationSourceResponse, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.RefreshTabletReplicationSource")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("tablet_alias", topoproto.TabletAliasString(tablet.Tablet.Alias))

	if err := c.topoRWPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("RefreshTabletReplicationSource(%v) failed to acquire topoRWPool: %w", topoproto.TabletAliasString(tablet.Tablet.Alias), err)
	}
	defer c.topoRWPool.Release()

	resp, err := c.Vtctld.ReparentTablet(ctx, &vtctldatapb.ReparentTabletRequest{Tablet: tablet.Tablet.Alias})
	if err != nil {
		return nil, err
	}

	return &vtadminpb.RefreshTabletReplicationSourceResponse{
		Keyspace: resp.Keyspace,
		Shard:    resp.Shard,
		Primary:  resp.Primary,
		Cluster:  c.ToProto(),
	}, nil
}

// ReloadSchemas reloads schemas in one or more keyspaces, shards, or tablets
// in the cluster, depending on the request parameters.
func (c *Cluster) ReloadSchemas(ctx context.Context, req *vtadminpb.ReloadSchemasRequest) (*vtadminpb.ReloadSchemasResponse, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.ReloadSchemas")
	defer span.Finish()

	AnnotateSpan(c, span)

	var (
		resp vtadminpb.ReloadSchemasResponse
		err  error
	)
	switch {
	case len(req.Tablets) > 0:
		resp.TabletResults, err = c.reloadTabletSchemas(ctx, req)
	case len(req.KeyspaceShards) > 0:
		resp.ShardResults, err = c.reloadShardSchemas(ctx, req)
	default:
		resp.KeyspaceResults, err = c.reloadKeyspaceSchemas(ctx, req)
	}

	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// reloadKeyspaceSchemas reloads schemas in one or more keyspaces in the
// cluster.
func (c *Cluster) reloadKeyspaceSchemas(ctx context.Context, req *vtadminpb.ReloadSchemasRequest) ([]*vtadminpb.ReloadSchemasResponse_KeyspaceResult, error) {
	keyspaces, err := func() (keyspaces []*vtctldatapb.Keyspace, err error) {
		span, ctx := trace.NewSpan(ctx, "Cluster.GetKeyspaces")
		defer span.Finish()

		if err := c.topoReadPool.Acquire(ctx); err != nil {
			return nil, fmt.Errorf("ReloadSchemas: failed to acquire topoReadPool: %w", err)
		}

		// Load all keyspaces up front so we don't have to make one-trip per
		// keyspace to check its existence.
		resp, err := c.Vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
		if err != nil {
			return nil, err
		}

		// The request specified no keyspace names, so default to all of them.
		if len(req.Keyspaces) == 0 {
			return resp.Keyspaces, nil
		}

		keyspaceNames := sets.New[string](req.Keyspaces...)

		for _, ks := range resp.Keyspaces {
			if keyspaceNames.Has(ks.Name) {
				keyspaces = append(keyspaces, ks)
			}
		}

		return keyspaces, nil
	}()
	if err != nil {
		return nil, err
	}

	var (
		m   sync.Mutex
		wg  sync.WaitGroup
		rec concurrency.AllErrorRecorder

		cpb     = c.ToProto()
		results = make([]*vtadminpb.ReloadSchemasResponse_KeyspaceResult, 0, len(keyspaces))
	)

	for _, ks := range keyspaces {
		wg.Add(1)
		go func(ks *vtctldatapb.Keyspace) {
			defer wg.Done()

			span, ctx := trace.NewSpan(ctx, "Cluster.ReloadSchemaKeyspace")
			defer span.Finish()

			AnnotateSpan(c, span)
			span.Annotate("keyspace", ks.Name)
			span.Annotate("concurrency", req.Concurrency)
			span.Annotate("include_primary", req.IncludePrimary)
			span.Annotate("wait_position", req.WaitPosition)

			resp, err := c.Vtctld.ReloadSchemaKeyspace(ctx, &vtctldatapb.ReloadSchemaKeyspaceRequest{
				Keyspace:       ks.Name,
				Concurrency:    req.Concurrency,
				IncludePrimary: req.IncludePrimary,
				WaitPosition:   req.WaitPosition,
			})
			if err != nil {
				rec.RecordError(fmt.Errorf("ReloadSchemaKeyspace(%s) failed: %w", ks.Name, err))
				return
			}

			m.Lock()
			defer m.Unlock()
			results = append(results, &vtadminpb.ReloadSchemasResponse_KeyspaceResult{
				Keyspace: &vtadminpb.Keyspace{
					Cluster:  cpb,
					Keyspace: ks,
				},
				Events: resp.Events,
			})
		}(ks)
	}

	wg.Wait()
	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return results, nil
}

// reloadShardSchemas reloads schemas in one or more shards in the cluster.
func (c *Cluster) reloadShardSchemas(ctx context.Context, req *vtadminpb.ReloadSchemasRequest) ([]*vtadminpb.ReloadSchemasResponse_ShardResult, error) {
	shardSets, err := c.getShardSets(ctx, nil, req.KeyspaceShards)
	if err != nil {
		return nil, err
	}

	var (
		m   sync.Mutex
		wg  sync.WaitGroup
		rec concurrency.AllErrorRecorder

		cpb     = c.ToProto()
		results = make([]*vtadminpb.ReloadSchemasResponse_ShardResult, 0, len(shardSets))
	)

	for ks, shards := range shardSets {
		for _, shard := range sets.List(shards) {
			wg.Add(1)
			go func(keyspace, shard string) {
				defer wg.Done()

				span, ctx := trace.NewSpan(ctx, "Cluster.reloadShardSchema")
				defer span.Finish()

				AnnotateSpan(c, span)
				span.Annotate("keyspace", keyspace)
				span.Annotate("shard", shard)
				span.Annotate("concurrency", req.Concurrency)
				span.Annotate("include_primary", req.IncludePrimary)
				span.Annotate("wait_position", req.WaitPosition)

				resp, err := c.Vtctld.ReloadSchemaShard(ctx, &vtctldatapb.ReloadSchemaShardRequest{
					Keyspace:       keyspace,
					Shard:          shard,
					Concurrency:    req.Concurrency,
					IncludePrimary: req.IncludePrimary,
					WaitPosition:   req.WaitPosition,
				})
				if err != nil {
					rec.RecordError(fmt.Errorf("ReloadSchemaShard(%s/%s) failed: %w", keyspace, shard, err))
					return
				}

				m.Lock()
				defer m.Unlock()
				results = append(results, &vtadminpb.ReloadSchemasResponse_ShardResult{
					Shard: &vtadminpb.Shard{
						Cluster: cpb,
						Shard: &vtctldatapb.Shard{
							Keyspace: keyspace,
							Name:     shard,
						},
					},
					Events: resp.Events,
				})
			}(ks, shard)
		}
	}

	wg.Wait()
	if rec.HasErrors() {
		return nil, rec.Error()
	}

	return results, nil
}

// reloadTabletSchemas reloads schemas in one or more tablets in the cluster.
func (c *Cluster) reloadTabletSchemas(ctx context.Context, req *vtadminpb.ReloadSchemasRequest) ([]*vtadminpb.ReloadSchemasResponse_TabletResult, error) {
	aliasSet := sets.New[string]()
	for _, alias := range req.Tablets {
		aliasSet.Insert(topoproto.TabletAliasString(alias))
	}

	tablets, err := c.FindTablets(ctx, func(t *vtadminpb.Tablet) bool {
		return aliasSet.Has(topoproto.TabletAliasString(t.Tablet.Alias))
	}, -1)
	if err != nil {
		return nil, err
	}

	var (
		m           sync.Mutex
		wg          sync.WaitGroup
		ch          = make(chan *vtadminpb.Tablet)
		concurrency = int(req.Concurrency)

		results = make([]*vtadminpb.ReloadSchemasResponse_TabletResult, 0, len(tablets))
	)

	if concurrency < 1 {
		concurrency = len(tablets)
	}

	reloadTablet := func(t *vtadminpb.Tablet) *vtadminpb.ReloadSchemasResponse_TabletResult {
		span, ctx := trace.NewSpan(ctx, "Cluster.reloadTabletSchema")
		defer span.Finish()

		AnnotateSpan(c, span)
		span.Annotate("tablet_alias", topoproto.TabletAliasString(t.Tablet.Alias))

		result := &vtadminpb.ReloadSchemasResponse_TabletResult{
			Tablet: t,
			Result: "ok",
		}
		_, err := c.Vtctld.ReloadSchema(ctx, &vtctldatapb.ReloadSchemaRequest{
			TabletAlias: t.Tablet.Alias,
		})
		if err != nil {
			result.Result = err.Error()
		}

		return result
	}

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for tablet := range ch {
				result := reloadTablet(tablet)

				m.Lock()
				results = append(results, result)
				m.Unlock()
			}
		}()
	}

	for _, t := range tablets {
		ch <- t
	}

	close(ch)
	wg.Wait()

	return results, nil
}

// SetWritable toggles the writability of a tablet, setting it to either
// read-write or read-only.
func (c *Cluster) SetWritable(ctx context.Context, req *vtctldatapb.SetWritableRequest) error {
	span, ctx := trace.NewSpan(ctx, "Cluster.SetWritable")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("tablet_alias", topoproto.TabletAliasString(req.TabletAlias))
	span.Annotate("writable", req.Writable)

	_, err := c.Vtctld.SetWritable(ctx, req)
	return err
}

// TabletExternallyPromoted updates the topo record for a shard to reflect a
// tablet that was promoted to primary external to Vitess (e.g. orchestrator).
func (c *Cluster) TabletExternallyPromoted(ctx context.Context, tablet *vtadminpb.Tablet) (*vtadminpb.TabletExternallyPromotedResponse, error) {
	span, ctx := trace.NewSpan(ctx, "API.TabletExternallyPromoted")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("tablet_alias", topoproto.TabletAliasString(tablet.Tablet.Alias))

	if err := c.topoRWPool.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("TabletExternallyPromoted(%s): failed to acquire topoRWPool: %w", topoproto.TabletAliasString(tablet.Tablet.Alias), err)
	}
	defer c.topoRWPool.Release()

	resp, err := c.Vtctld.TabletExternallyReparented(ctx, &vtctldatapb.TabletExternallyReparentedRequest{
		Tablet: tablet.Tablet.Alias,
	})
	if err != nil {
		return nil, err
	}

	return &vtadminpb.TabletExternallyPromotedResponse{
		Cluster:    c.ToProto(),
		Keyspace:   resp.Keyspace,
		Shard:      resp.Shard,
		NewPrimary: resp.NewPrimary,
		OldPrimary: resp.OldPrimary,
	}, nil
}

// ToggleTabletReplication either starts or stops replication on the specified
// tablet.
func (c *Cluster) ToggleTabletReplication(ctx context.Context, tablet *vtadminpb.Tablet, start bool) (err error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.ToggleTabletReplication")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("tablet_alias", topoproto.TabletAliasString(tablet.Tablet.Alias))
	span.Annotate("start", start)
	span.Annotate("stop", !start)

	if start {
		_, err = c.Vtctld.StartReplication(ctx, &vtctldatapb.StartReplicationRequest{
			TabletAlias: tablet.Tablet.Alias,
		})
	} else {
		_, err = c.Vtctld.StopReplication(ctx, &vtctldatapb.StopReplicationRequest{
			TabletAlias: tablet.Tablet.Alias,
		})
	}

	return err
}

// Debug returns a map of debug information for a cluster.
func (c *Cluster) Debug() map[string]any {
	m := map[string]any{
		"cluster": c.ToProto(),
		"config":  c.cfg,
		"pools": map[string]json.RawMessage{
			"backup_read_pool":        json.RawMessage(c.backupReadPool.StatsJSON()),
			"schema_read_pool":        json.RawMessage(c.schemaReadPool.StatsJSON()),
			"topo_read_pool":          json.RawMessage(c.topoReadPool.StatsJSON()),
			"topo_rw_pool":            json.RawMessage(c.topoRWPool.StatsJSON()),
			"workflow_read_pool":      json.RawMessage(c.workflowReadPool.StatsJSON()),
			"emergency_failover_pool": json.RawMessage(c.emergencyFailoverPool.StatsJSON()),
			"failover_pool":           json.RawMessage(c.failoverPool.StatsJSON()),
		},
		"caches": map[string]any{
			"schemas": c.schemaCache.Debug(),
		},
	}

	if vtsql, ok := c.DB.(debug.Debuggable); ok {
		m["vtsql"] = vtsql.Debug()
	}

	if vtctld, ok := c.Vtctld.(debug.Debuggable); ok {
		m["vtctld"] = vtctld.Debug()
	}

	return m
}

// Equal compares the vtctld and vtgate addresses of the clusters for equality
func (c *Cluster) Equal(otherCluster *Cluster) (bool, error) {
	ctx := context.Background()
	vtgateAddresses, err := c.Discovery.DiscoverVTGateAddrs(ctx, []string{})
	if err != nil {
		return false, err
	}
	otherVtgateAddresses, err := otherCluster.Discovery.DiscoverVTGateAddrs(ctx, []string{})
	if err != nil {
		return false, err
	}

	vtctldAddresses, err := c.Discovery.DiscoverVtctldAddrs(ctx, []string{})
	if err != nil {
		return false, err
	}

	otherVtctldAddresses, err := otherCluster.Discovery.DiscoverVtctldAddrs(ctx, []string{})
	if err != nil {
		return false, err
	}

	return equalAddresses(vtgateAddresses, otherVtgateAddresses) && equalAddresses(vtctldAddresses, otherVtctldAddresses), nil
}

func equalAddresses(list1 []string, list2 []string) bool {
	if len(list1) != len(list2) {
		return false
	}

	sort.Strings(list1)
	sort.Strings(list2)
	for i, e1 := range list1 {
		e2 := list2[i]
		if e1 != e2 {
			return false
		}
	}

	return true
}
