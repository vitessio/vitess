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
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
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
}

// New creates a new Cluster from a Config.
func New(cfg Config) (*Cluster, error) {
	cluster := &Cluster{
		ID:   cfg.ID,
		Name: cfg.Name,
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

	vtctldargs := buildPFlagSlice(cfg.VtctldFlags)

	vtctldCfg, err := vtctldclient.Parse(protocluster, disco, vtctldargs)
	if err != nil {
		return nil, fmt.Errorf("error creating vtctldclient proxy config: %w", err)
	}

	cluster.DB = vtsql.New(vtsqlCfg)
	cluster.Vtctld = vtctldclient.New(vtctldCfg)

	return cluster, nil
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
// Cell | Keyspace | Shard | TabletType (string) | ServingState (string) | Alias | Hostname | MasterTermStartTime.
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
			return nil, fmt.Errorf("failed parsing master_term_start_time %s: %w", mtstStr, err)
		}

		topotablet.MasterTermStartTime = logutil.TimeToProto(timeTime)
	}

	return tablet, nil
}

// GetTablets returns all tablets in the cluster.
func (c *Cluster) GetTablets(ctx context.Context) ([]*vtadminpb.Tablet, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetTablets")
	defer span.Finish()

	AnnotateSpan(c, span)

	return c.getTablets(ctx)
}

func (c *Cluster) getTablets(ctx context.Context) ([]*vtadminpb.Tablet, error) {
	if err := c.DB.Dial(ctx, ""); err != nil {
		return nil, err
	}

	rows, err := c.DB.ShowTablets(ctx)
	if err != nil {
		return nil, err
	}

	return c.parseTablets(rows)
}

// GetSchema returns the schema for a GetSchemaRequest on the given tablet. The
// caller is responsible for making at least one call to c.Vtctld.Dial prior to
// calling this function.
//
// Note that the request's TabletAlias field will be ignored, using the provided
// tablet's Alias instead. This override is done on a copy of the request, so it
// is transparent to the caller.
//
// This function takes both the request argument and tablet argument to
// (a) set the Keyspace field on the resulting Schema object, which comes from
// the provided tablet; and, (b) allow a caller, like vtadmin.API to collect a
// bunch of tablets once and make a series of GetSchema calls without Cluster
// refetching the tablet list each time.
func (c *Cluster) GetSchema(ctx context.Context, req *vtctldatapb.GetSchemaRequest, tablet *vtadminpb.Tablet) (*vtadminpb.Schema, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetSchema")
	defer span.Finish()

	AnnotateSpan(c, span)

	// Copy the request to not mutate the caller's request object.
	r := *req
	r.TabletAlias = tablet.Tablet.Alias

	span.Annotate("tablet_alias", topoproto.TabletAliasString(r.TabletAlias))
	span.Annotate("exclude_tables", strings.Join(r.ExcludeTables, ","))
	span.Annotate("tables", strings.Join(r.Tables, ","))
	span.Annotate("include_views", r.IncludeViews)
	span.Annotate("table_names_only", r.TableNamesOnly)
	span.Annotate("table_sizes_only", r.TableSizesOnly)

	schema, err := c.Vtctld.GetSchema(ctx, &r)
	if err != nil {
		return nil, err
	}

	if schema == nil || schema.Schema == nil {
		return nil, nil
	}

	return &vtadminpb.Schema{
		Cluster:          c.ToProto(),
		Keyspace:         tablet.Tablet.Keyspace,
		TableDefinitions: schema.Schema.TableDefinitions,
	}, nil
}

// GetVSchema returns the vschema for a given keyspace in this cluster. The
// caller is responsible for making at least one call to c.Vtctld.Dial prior to
// calling this function.
func (c *Cluster) GetVSchema(ctx context.Context, keyspace string) (*vtadminpb.VSchema, error) {
	span, ctx := trace.NewSpan(ctx, "Cluster.GetVSchema")
	defer span.Finish()

	AnnotateSpan(c, span)
	span.Annotate("keyspace", keyspace)

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

	results := make([]*vtadminpb.Tablet, 0, n)
	for _, t := range tablets {
		if len(results) >= n {
			break
		}

		if filter(t) {
			results = append(results, t)
		}
	}

	return results, nil
}
