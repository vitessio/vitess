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
	"context"
	"fmt"
	"math/rand"
	"strings"
	"text/template"
	"time"

	consul "github.com/hashicorp/consul/api"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/textutil"
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
	vtgateFQDNTmpl            *template.Template

	/* vtctld options */
	vtctldDatacenter string
	vtctldService    string
	vtctldAddrTmpl   *template.Template
	vtctldFQDNTmpl   *template.Template
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

	/* vtgate discovery config options */
	flags.StringVar(&disco.vtgateService, "vtgate-service-name", "vtgate", "consul service name vtgates register as")
	flags.StringVar(&disco.vtgatePoolTag, "vtgate-pool-tag", "pool", "consul service tag to group vtgates by pool")
	flags.StringVar(&disco.vtgateCellTag, "vtgate-cell-tag", "cell", "consul service tag to group vtgates by cell")
	flags.StringVar(&disco.vtgateKeyspacesToWatchTag, "vtgate-keyspaces-to-watch-tag", "keyspaces",
		"consul service tag identifying -keyspaces_to_watch for vtgates")

	vtgateAddrTmplStr := flags.String("vtgate-addr-tmpl", "{{ .Hostname }}",
		"Go template string to produce a dialable address from a *vtadminpb.VTGate "+
			"NOTE: the .FQDN field will never be set in the addr template context.")
	vtgateDatacenterTmplStr := flags.String("vtgate-datacenter-tmpl", "",
		"Go template string to generate the datacenter for vtgate consul queries. "+
			"The meta information about the cluster is provided to the template via {{ .Cluster }}. "+
			"Used once during initialization.")
	vtgateFQDNTmplStr := flags.String("vtgate-fqdn-tmpl", "",
		"Optional Go template string to produce an FQDN to access the vtgate from a browser. "+
			"E.g. \"{{ .Hostname }}.example.com\".")

	/* vtctld discovery config options */
	flags.StringVar(&disco.vtctldService, "vtctld-service-name", "vtctld", "consul service name vtctlds register as")

	vtctldAddrTmplStr := flags.String("vtctld-addr-tmpl", "{{ .Hostname }}",
		"Go template string to produce a dialable address from a *vtadminpb.Vtctld "+
			"NOTE: the .FQDN field will never be set in the addr template context.")
	vtctldDatacenterTmplStr := flags.String("vtctld-datacenter-tmpl", "",
		"Go template string to generate the datacenter for vtgate consul queries. "+
			"The cluster name is provided to the template via {{ .Cluster }}. "+
			"Used once during initialization.")
	vtctldFQDNTmplStr := flags.String("vtctld-fqdn-tmpl", "",
		"Optional Go template string to produce an FQDN to access the vtctld from a browser. "+
			"E.g. \"{{ .Hostname }}.example.com\".")

	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	/* gates options */
	if *vtgateDatacenterTmplStr != "" {
		disco.vtgateDatacenter, err = generateConsulDatacenter("vtgate", cluster, *vtgateDatacenterTmplStr)
		if err != nil {
			return nil, fmt.Errorf("failed to generate vtgate consul datacenter from template: %w", err)
		}
	}

	if *vtgateFQDNTmplStr != "" {
		disco.vtgateFQDNTmpl, err = template.New("consul-vtgate-fqdn-template-" + cluster.Id).Parse(*vtgateFQDNTmplStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse vtgate FQDN template %s: %w", *vtgateFQDNTmplStr, err)
		}
	}

	disco.vtgateAddrTmpl, err = template.New("consul-vtgate-address-template-" + cluster.Id).Parse(*vtgateAddrTmplStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse vtgate host address template %s: %w", *vtgateAddrTmplStr, err)
	}

	/* vtctld options */
	if *vtctldDatacenterTmplStr != "" {
		disco.vtctldDatacenter, err = generateConsulDatacenter("vtctld", cluster, *vtctldDatacenterTmplStr)
		if err != nil {
			return nil, fmt.Errorf("failed to generate vtctld consul datacenter from template: %w", err)
		}
	}

	if *vtctldFQDNTmplStr != "" {
		disco.vtctldFQDNTmpl, err = template.New("consul-vtctld-fqdn-template-" + cluster.Id).Parse(*vtctldFQDNTmplStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse vtctld FQDN template %s: %w", *vtctldFQDNTmplStr, err)
		}
	}

	disco.vtctldAddrTmpl, err = template.New("consul-vtctld-address-template-" + cluster.Id).Parse(*vtctldAddrTmplStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse vtctld host address template %s: %w", *vtctldAddrTmplStr, err)
	}

	return disco, nil
}

func generateConsulDatacenter(component string, cluster *vtadminpb.Cluster, tmplStr string) (string, error) {
	tmpl, err := template.New("consul-" + component + "-datacenter-" + cluster.Id).Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("error parsing template %s: %w", tmplStr, err)
	}

	dc, err := textutil.ExecuteTemplate(tmpl, &struct {
		Cluster *vtadminpb.Cluster
	}{
		Cluster: cluster,
	})

	if err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return dc, nil
}

// DiscoverVTGate is part of the Discovery interface.
func (c *ConsulDiscovery) DiscoverVTGate(ctx context.Context, tags []string) (*vtadminpb.VTGate, error) {
	span, ctx := trace.NewSpan(ctx, "ConsulDiscovery.DiscoverVTGate")
	defer span.Finish()

	executeFQDNTemplate := true

	return c.discoverVTGate(ctx, tags, executeFQDNTemplate)
}

// discoverVTGate calls discoverVTGates and then returns a random VTGate from
// the result. see discoverVTGates for further documentation.
func (c *ConsulDiscovery) discoverVTGate(ctx context.Context, tags []string, executeFQDNTemplate bool) (*vtadminpb.VTGate, error) {
	vtgates, err := c.discoverVTGates(ctx, tags, executeFQDNTemplate)
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

	executeFQDNTemplate := false

	vtgate, err := c.discoverVTGate(ctx, tags, executeFQDNTemplate)
	if err != nil {
		return "", err
	}

	addr, err := textutil.ExecuteTemplate(c.vtgateAddrTmpl, vtgate)
	if err != nil {
		return "", fmt.Errorf("failed to execute vtgate address template for %v: %w", vtgate, err)
	}

	return addr, nil
}

// DiscoverVTGateAddrs is part of the Discovery interface.
func (c *ConsulDiscovery) DiscoverVTGateAddrs(ctx context.Context, tags []string) ([]string, error) {
	span, ctx := trace.NewSpan(ctx, "ConsulDiscovery.DiscoverVTGateAddrs")
	defer span.Finish()

	executeFQDNTemplate := false

	vtgates, err := c.discoverVTGates(ctx, tags, executeFQDNTemplate)
	if err != nil {
		return nil, err
	}

	addrs := make([]string, len(vtgates))
	for i, vtgate := range vtgates {
		addr, err := textutil.ExecuteTemplate(c.vtgateAddrTmpl, vtgate)
		if err != nil {
			return nil, fmt.Errorf("failed to execute vtgate address template for %v: %w", vtgate, err)
		}

		addrs[i] = addr
	}

	return addrs, nil
}

// DiscoverVTGates is part of the Discovery interface.
func (c *ConsulDiscovery) DiscoverVTGates(ctx context.Context, tags []string) ([]*vtadminpb.VTGate, error) {
	span, ctx := trace.NewSpan(ctx, "ConsulDiscovery.DiscoverVTGates")
	defer span.Finish()

	executeFQDNTemplate := true

	return c.discoverVTGates(ctx, tags, executeFQDNTemplate)
}

// discoverVTGates does the actual work of discovering VTGate hosts from a
// consul datacenter. executeFQDNTemplate is boolean to allow an optimization
// for DiscoverVTGateAddr (the only function that sets the boolean to false).
func (c *ConsulDiscovery) discoverVTGates(_ context.Context, tags []string, executeFQDNTemplate bool) ([]*vtadminpb.VTGate, error) {
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

		if executeFQDNTemplate {
			if c.vtgateFQDNTmpl != nil {
				vtgate.FQDN, err = textutil.ExecuteTemplate(c.vtgateFQDNTmpl, vtgate)
				if err != nil {
					return nil, fmt.Errorf("failed to execute vtgate fqdn template for %v: %w", vtgate, err)
				}
			}
		}

		vtgates[i] = vtgate
	}

	return vtgates, nil
}

// DiscoverVtctld is part of the Discovery interface.
func (c *ConsulDiscovery) DiscoverVtctld(ctx context.Context, tags []string) (*vtadminpb.Vtctld, error) {
	span, ctx := trace.NewSpan(ctx, "ConsulDiscovery.DiscoverVtctld")
	defer span.Finish()

	executeFQDNTemplate := true

	return c.discoverVtctld(ctx, tags, executeFQDNTemplate)
}

// discoverVtctld calls discoverVtctlds and then returns a random vtctld from
// the result. see discoverVtctlds for further documentation.
func (c *ConsulDiscovery) discoverVtctld(ctx context.Context, tags []string, executeFQDNTemplate bool) (*vtadminpb.Vtctld, error) {
	vtctlds, err := c.discoverVtctlds(ctx, tags, executeFQDNTemplate)
	if err != nil {
		return nil, err
	}

	if len(vtctlds) == 0 {
		return nil, ErrNoVtctlds
	}

	return vtctlds[rand.Intn(len(vtctlds))], nil
}

// DiscoverVtctldAddr is part of the Discovery interface.
func (c *ConsulDiscovery) DiscoverVtctldAddr(ctx context.Context, tags []string) (string, error) {
	span, ctx := trace.NewSpan(ctx, "ConsulDiscovery.DiscoverVtctldAddr")
	defer span.Finish()

	executeFQDNTemplate := false

	vtctld, err := c.discoverVtctld(ctx, tags, executeFQDNTemplate)
	if err != nil {
		return "", err
	}

	addr, err := textutil.ExecuteTemplate(c.vtctldAddrTmpl, vtctld)
	if err != nil {
		return "", fmt.Errorf("failed to execute vtctld address template for %v: %w", vtctld, err)
	}

	return addr, nil
}

// DiscoverVtctldAddrs is part of the Discovery interface.
func (c *ConsulDiscovery) DiscoverVtctldAddrs(ctx context.Context, tags []string) ([]string, error) {
	span, ctx := trace.NewSpan(ctx, "ConsulDiscovery.DiscoverVtctldAddrs")
	defer span.Finish()

	executeFQDNTemplate := false

	vtctlds, err := c.discoverVtctlds(ctx, tags, executeFQDNTemplate)
	if err != nil {
		return nil, err
	}

	addrs := make([]string, len(vtctlds))
	for i, vtctld := range vtctlds {
		addr, err := textutil.ExecuteTemplate(c.vtctldAddrTmpl, vtctld)
		if err != nil {
			return nil, fmt.Errorf("failed to execute vtctld address template for %v: %w", vtctld, err)
		}

		addrs[i] = addr
	}

	return addrs, nil
}

// DiscoverVtctlds is part of the Discovery interface.
func (c *ConsulDiscovery) DiscoverVtctlds(ctx context.Context, tags []string) ([]*vtadminpb.Vtctld, error) {
	span, ctx := trace.NewSpan(ctx, "ConsulDiscovery.DiscoverVtctlds")
	defer span.Finish()

	executeFQDNTemplate := true

	return c.discoverVtctlds(ctx, tags, executeFQDNTemplate)
}

// discoverVtctlds does the actual work of discovering Vtctld hosts from a
// consul datacenter. executeFQDNTemplate is boolean to allow an optimization
// for DiscoverVtctldAddr (the only function that sets the boolean to false).
func (c *ConsulDiscovery) discoverVtctlds(_ context.Context, tags []string, executeFQDNTemplate bool) ([]*vtadminpb.Vtctld, error) {
	opts := c.getQueryOptions()
	opts.Datacenter = c.vtctldDatacenter

	entries, _, err := c.client.Health().ServiceMultipleTags(c.vtctldService, tags, c.passingOnly, &opts)
	if err != nil {
		return nil, err
	}

	vtctlds := make([]*vtadminpb.Vtctld, len(entries))

	for i, entry := range entries {
		vtctld := &vtadminpb.Vtctld{
			Cluster: &vtadminpb.Cluster{
				Id:   c.cluster.Id,
				Name: c.cluster.Name,
			},
			Hostname: entry.Node.Node,
		}

		if executeFQDNTemplate {
			if c.vtctldFQDNTmpl != nil {
				vtctld.FQDN, err = textutil.ExecuteTemplate(c.vtctldFQDNTmpl, vtctld)
				if err != nil {
					return nil, fmt.Errorf("failed to execute vtctld fqdn template for %v: %w", vtctld, err)
				}
			}
		}

		vtctlds[i] = vtctld
	}

	return vtctlds, nil
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
