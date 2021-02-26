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
	"time"

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
		return nil, fmt.Errorf("error while creating discovery impl (%s): %w", cfg.DiscoveryImpl, err)
	}

	cluster.Discovery = disco

	protocluster := cluster.ToProto()

	vtsqlargs := buildPFlagSlice(cfg.VtSQLFlags)

	vtsqlCfg, err := vtsql.Parse(protocluster, disco, vtsqlargs)
	if err != nil {
		return nil, fmt.Errorf("error while creating vtsql connection config: %w", err)
	}

	vtctldargs := buildPFlagSlice(cfg.VtctldFlags)

	vtctldCfg, err := vtctldclient.Parse(protocluster, disco, vtctldargs)
	if err != nil {
		return nil, fmt.Errorf("error while creating vtctldclient proxy config: %w", err)
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
		return nil, err
	}

	if topotablet.Alias.Cell != cell {
		// (TODO:@amason) ???
		log.Warningf("tablet cell %s does not match alias %s. ignoring for now", cell, topoproto.TabletAliasString(topotablet.Alias))
	}

	if mtstStr != "" {
		timeTime, err := time.Parse(time.RFC3339, mtstStr)
		if err != nil {
			return nil, err
		}

		topotablet.MasterTermStartTime = logutil.TimeToProto(timeTime)
	}

	return tablet, nil
}

// GetTablets returns all tablets in the cluster.
func (c *Cluster) GetTablets(ctx context.Context) ([]*vtadminpb.Tablet, error) {
	if err := c.DB.Dial(ctx, ""); err != nil {
		return nil, err
	}

	rows, err := c.DB.ShowTablets(ctx)
	if err != nil {
		return nil, err
	}

	return c.parseTablets(rows)
}

// FindTablet returns the first tablet in a given cluster that satisfies the filter function.
func (c *Cluster) FindTablet(ctx context.Context, filter func(*vtadminpb.Tablet) bool) (*vtadminpb.Tablet, error) {
	tablets, err := c.FindTablets(ctx, filter, 1)
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
	tablets, err := c.GetTablets(ctx)
	if err != nil {
		return nil, err
	}

	if n == -1 {
		n = len(tablets)
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
