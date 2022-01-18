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
	"errors"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/trace"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// DynamicDiscovery implements the Discovery interface for "discovering"
// Vitess components based on parameters passed directly in

type DynamicDiscovery struct {
	cluster *vtadminpb.Cluster
	vtgate  *vtadminpb.VTGate
	vtctld  *vtadminpb.Vtctld
}

// NewStaticFile returns a DynamicDiscovery for the given cluster.
func NewDynamic(cluster *vtadminpb.Cluster, flags *pflag.FlagSet, args []string) (Discovery, error) {
	disco := &DynamicDiscovery{
		cluster: cluster,
	}

	vtctldHostname := flags.String("vtctld_hostname", "", "hostname of vtctld instance")
	vtctldFqdn := flags.String("vtctld_fqdn", "", "fqdn of vtctld instance")
	vtgateHostname := flags.String("vtgate_hostname", "", "hostname of vtgate instance")

	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	if vtctldHostname == nil || *vtctldHostname == "" {
		return nil, errors.New("must specify vtctld hostname")
	}

	if vtctldFqdn == nil || *vtctldFqdn == "" {
		return nil, errors.New("must specify vtctld fqdn")
	}

	if vtgateHostname == nil || *vtgateHostname == "" {
		return nil, errors.New("must specify vtgate hostname")
	}

	disco.vtctld = &vtadminpb.Vtctld{
		Hostname: *vtctldHostname,
		FQDN:     *vtctldFqdn,
	}

	disco.vtgate = &vtadminpb.VTGate{
		Hostname: *vtgateHostname,
	}

	return disco, nil
}

// DiscoverVTGate is part of the Discovery interface.
func (d *DynamicDiscovery) DiscoverVTGate(ctx context.Context, tags []string) (*vtadminpb.VTGate, error) {
	span, _ := trace.NewSpan(ctx, "DynamicDiscovery.DiscoverVTGate")
	defer span.Finish()

	return d.vtgate, nil
}

// DiscoverVTGateAddr is part of the Discovery interface.
func (d *DynamicDiscovery) DiscoverVTGateAddr(ctx context.Context, tags []string) (string, error) {
	span, _ := trace.NewSpan(ctx, "DynamicDiscovery.DiscoverVTGateAddr")
	defer span.Finish()

	return d.vtgate.Hostname, nil
}

// DiscoverVTGates is part of the Discovery interface.
func (d *DynamicDiscovery) DiscoverVTGates(ctx context.Context, tags []string) ([]*vtadminpb.VTGate, error) {
	span, _ := trace.NewSpan(ctx, "DynamicDiscovery.DiscoverVTGates")
	defer span.Finish()

	return []*vtadminpb.VTGate{d.vtgate}, nil
}

// DiscoverVtctld is part of the Discovery interface.
func (d *DynamicDiscovery) DiscoverVtctld(ctx context.Context, tags []string) (*vtadminpb.Vtctld, error) {
	span, _ := trace.NewSpan(ctx, "DynamicDiscovery.DiscoverVtctld")
	defer span.Finish()

	return d.vtctld, nil
}

// DiscoverVtctldAddr is part of the Discovery interface.
func (d *DynamicDiscovery) DiscoverVtctldAddr(ctx context.Context, tags []string) (string, error) {
	span, _ := trace.NewSpan(ctx, "DynamicDiscovery.DiscoverVtctldAddr")
	defer span.Finish()

	return d.vtctld.Hostname, nil
}

// DiscoverVtctlds is part of the Discovery interface.
func (d *DynamicDiscovery) DiscoverVtctlds(ctx context.Context, tags []string) ([]*vtadminpb.Vtctld, error) {
	span, _ := trace.NewSpan(ctx, "DynamicDiscovery.DiscoverVtctlds")
	defer span.Finish()

	return []*vtadminpb.Vtctld{d.vtctld}, nil
}
