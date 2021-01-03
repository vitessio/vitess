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

package discovery

import (
	"bytes"
	"context"
	"math/rand"
	"strings"
	"text/template"
	"time"

	consul "github.com/hashicorp/consul/api"
	"github.com/spf13/pflag"
	"vitess.io/vitess/go/trace"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// ConsulDiscovery implements the Discovery interface for consul.
type ConsulDiscovery struct {
	cluster      *vtadminpb.Cluster
	client       ConsulClient
	queryOptions *consul.QueryOptions

	/* misc options */
	passingOnly bool

	/* vtgate options */
	vtgateDatacenter          string
	vtgateService             string
	vtgatePoolTag             string
	vtgateCellTag             string
	vtgateKeyspacesToWatchTag string
	vtgateAddrTmpl            *template.Template
}

// NewConsul returns a ConsulDiscovery for the given cluster. Args are a slice
// of command-line flags (e.g. "-key=value") that are parsed by a consul-
// specific flag set.
func NewConsul(cluster *vtadminpb.Cluster, flags *pflag.FlagSet, args []string) (Discovery, error) { // nolint:funlen
	c, err := consul.NewClient(consul.DefaultConfig())
	if err != nil {
		return nil, err
	}

	qopts := &consul.QueryOptions{
		AllowStale:        false,
		RequireConsistent: true,
		WaitIndex:         uint64(0),
		UseCache:          true,
	}

	disco := &ConsulDiscovery{
		cluster:      cluster,
		client:       &consulClient{c},
		queryOptions: qopts,
	}

	flags.DurationVar(&disco.queryOptions.MaxAge, "max-age", time.Second*30,
		"how old a cached value can be before consul queries stop using it")
	flags.StringVar(&disco.queryOptions.Token, "token", "", "consul ACL token to use for requests")
	flags.BoolVar(&disco.passingOnly, "passing-only", true, "whether to include only nodes passing healthchecks")

	flags.StringVar(&disco.vtgateService, "vtgate-service-name", "vtgate", "consul service name vtgates register as")
	flags.StringVar(&disco.vtgatePoolTag, "vtgate-pool-tag", "pool", "consul service tag to group vtgates by pool")
	flags.StringVar(&disco.vtgateCellTag, "vtgate-cell-tag", "cell", "consul service tag to group vtgates by cell")
	flags.StringVar(&disco.vtgateKeyspacesToWatchTag, "vtgate-keyspaces-to-watch-tag", "keyspaces",
		"consul service tag identifying -keyspaces_to_watch for vtgates")

	vtgateAddrTmplStr := flags.String("vtgate-addr-tmpl", "{{ .Hostname }}",
		"Go template string to produce a dialable address from a *vtadminpb.VTGate")
	vtgateDatacenterTmplStr := flags.String("vtgate-datacenter-tmpl", "",
		"Go template string to generate the datacenter for vtgate consul queries. "+
			"The meta information about the cluster is provided to the template via {{ .Cluster }}. "+
			"Used once during initialization.")

	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	if *vtgateDatacenterTmplStr != "" {
		tmpl, err := template.New("consul-vtgate-datacenter-" + cluster.Id).Parse(*vtgateDatacenterTmplStr)
		if err != nil {
			return nil, err
		}

		buf := bytes.NewBuffer(nil)
		err = tmpl.Execute(buf, &struct {
			Cluster *vtadminpb.Cluster
		}{
			Cluster: cluster,
		})

		if err != nil {
			return nil, err
		}

		disco.vtgateDatacenter = buf.String()
	}

	disco.vtgateAddrTmpl, err = template.New("consul-vtgate-address-template").Parse(*vtgateAddrTmplStr)
	if err != nil {
		return nil, err
	}

	return disco, nil
}

// DiscoverVTGate is part of the Discovery interface.
func (c *ConsulDiscovery) DiscoverVTGate(ctx context.Context, tags []string) (*vtadminpb.VTGate, error) {
	span, ctx := trace.NewSpan(ctx, "ConsulDiscovery.DiscoverVTGate")
	defer span.Finish()

	return c.discoverVTGate(ctx, tags)
}

func (c *ConsulDiscovery) discoverVTGate(ctx context.Context, tags []string) (*vtadminpb.VTGate, error) {
	vtgates, err := c.discoverVTGates(ctx, tags)
	if err != nil {
		return nil, err
	}

	if len(vtgates) == 0 {
		return nil, ErrNoVTGates
	}

	return vtgates[rand.Intn(len(vtgates))], nil
}

// DiscoverVTGateAddr is part of the Discovery interface.
func (c *ConsulDiscovery) DiscoverVTGateAddr(ctx context.Context, tags []string) (string, error) {
	span, ctx := trace.NewSpan(ctx, "ConsulDiscovery.DiscoverVTGateAddr")
	defer span.Finish()

	vtgate, err := c.discoverVTGate(ctx, tags)
	if err != nil {
		return "", err
	}

	buf := bytes.NewBuffer(nil)
	if err := c.vtgateAddrTmpl.Execute(buf, vtgate); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// DiscoverVTGates is part of the Discovery interface.
func (c *ConsulDiscovery) DiscoverVTGates(ctx context.Context, tags []string) ([]*vtadminpb.VTGate, error) {
	span, ctx := trace.NewSpan(ctx, "ConsulDiscovery.DiscoverVTGates")
	defer span.Finish()

	return c.discoverVTGates(ctx, tags)
}

func (c *ConsulDiscovery) discoverVTGates(_ context.Context, tags []string) ([]*vtadminpb.VTGate, error) {
	opts := c.getQueryOptions()
	opts.Datacenter = c.vtgateDatacenter

	entries, _, err := c.client.Health().ServiceMultipleTags(c.vtgateService, tags, c.passingOnly, &opts)
	if err != nil {
		return nil, err
	}

	vtgates := make([]*vtadminpb.VTGate, len(entries))

	for i, entry := range entries {
		vtgate := &vtadminpb.VTGate{
			Hostname: entry.Node.Node,
			Cluster: &vtadminpb.Cluster{
				Id:   c.cluster.Id,
				Name: c.cluster.Name,
			},
		}

		var cell, pool string
		for _, tag := range entry.Service.Tags {
			if pool != "" && cell != "" {
				break
			}

			parts := strings.Split(tag, ":")
			if len(parts) != 2 {
				continue
			}

			name, value := parts[0], parts[1]
			switch name {
			case c.vtgateCellTag:
				cell = value
			case c.vtgatePoolTag:
				pool = value
			}
		}

		vtgate.Cell = cell
		vtgate.Pool = pool

		if keyspaces, ok := entry.Service.Meta[c.vtgateKeyspacesToWatchTag]; ok {
			vtgate.Keyspaces = strings.Split(keyspaces, ",")
		}

		vtgates[i] = vtgate
	}

	return vtgates, nil
}

// getQueryOptions returns a shallow copy so we can swap in the vtgateDatacenter.
// If we were to set it directly, we'd need a mutex to guard against concurrent
// vtgate and (soon) vtctld queries.
func (c *ConsulDiscovery) getQueryOptions() consul.QueryOptions {
	if c.queryOptions == nil {
		return consul.QueryOptions{}
	}

	opts := *c.queryOptions

	return opts
}

// ConsulClient defines an interface for the subset of the consul API used by
// discovery, so we can swap in an implementation for testing.
type ConsulClient interface {
	Health() ConsulHealth
}

// ConsulHealth defines an interface for the subset of the (*consul.Health) struct
// used by discovery, so we can swap in an implementation for testing.
type ConsulHealth interface {
	ServiceMultipleTags(service string, tags []string, passingOnly bool, q *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error) // nolint:lll
}

// consulClient is our shim wrapper around the upstream consul client.
type consulClient struct {
	*consul.Client
}

func (c *consulClient) Health() ConsulHealth {
	return c.Client.Health()
}
