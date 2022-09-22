/*
Copyright 2022 The Vitess Authors.

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
	"encoding/json"
	"fmt"
	"math/rand"

	"vitess.io/vitess/go/trace"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// JSONDiscovery implements the Discovery interface for "discovering"
// Vitess components hardcoded in a json object.
//
// StaticFileDiscovery and DynamicDiscovery inherit from JSONDiscovery because
// they both read the same JSON object. They only differ in where the JSON object is stored.
//
// As an example, here's a minimal JSON file for a single Vitess cluster running locally
// (such as the one described in https://vitess.io/docs/get-started/local-docker):
//
//	{
//		"vtgates": [
//			{
//				"host": {
//					"hostname": "127.0.0.1:15991"
//				}
//			}
//		]
//	}
//
// For more examples of various static file configurations, see the unit tests.
type JSONDiscovery struct {
	cluster *vtadminpb.Cluster
	config  *JSONClusterConfig
	gates   struct {
		byName map[string]*vtadminpb.VTGate
		byTag  map[string][]*vtadminpb.VTGate
	}
	vtctlds struct {
		byName map[string]*vtadminpb.Vtctld
		byTag  map[string][]*vtadminpb.Vtctld
	}
}

// JSONClusterConfig configures Vitess components for a single cluster.
type JSONClusterConfig struct {
	VTGates []*JSONVTGateConfig `json:"vtgates,omitempty"`
	Vtctlds []*JSONVtctldConfig `json:"vtctlds,omitempty"`
}

// JSONVTGateConfig contains host and tag information for a single VTGate in a cluster.
type JSONVTGateConfig struct {
	Host *vtadminpb.VTGate `json:"host"`
	Tags []string          `json:"tags"`
}

// JSONVtctldConfig contains a host and tag information for a single
// Vtctld in a cluster.
type JSONVtctldConfig struct {
	Host *vtadminpb.Vtctld `json:"host"`
	Tags []string          `json:"tags"`
}

func (d *JSONDiscovery) parseConfig(bytes []byte) error {
	if err := json.Unmarshal(bytes, &d.config); err != nil {
		return fmt.Errorf("failed to unmarshal staticfile config from json: %w", err)
	}

	d.gates.byName = make(map[string]*vtadminpb.VTGate, len(d.config.VTGates))
	d.gates.byTag = make(map[string][]*vtadminpb.VTGate)

	// Index the gates by name and by tag for easier lookups
	for _, gate := range d.config.VTGates {
		d.gates.byName[gate.Host.Hostname] = gate.Host

		for _, tag := range gate.Tags {
			d.gates.byTag[tag] = append(d.gates.byTag[tag], gate.Host)
		}
	}

	d.vtctlds.byName = make(map[string]*vtadminpb.Vtctld, len(d.config.Vtctlds))
	d.vtctlds.byTag = make(map[string][]*vtadminpb.Vtctld)

	// Index the vtctlds by name and by tag for easier lookups
	for _, vtctld := range d.config.Vtctlds {
		d.vtctlds.byName[vtctld.Host.Hostname] = vtctld.Host

		for _, tag := range vtctld.Tags {
			d.vtctlds.byTag[tag] = append(d.vtctlds.byTag[tag], vtctld.Host)
		}
	}

	return nil
}

// DiscoverVTGate is part of the Discovery interface.
func (d *JSONDiscovery) DiscoverVTGate(ctx context.Context, tags []string) (*vtadminpb.VTGate, error) {
	span, ctx := trace.NewSpan(ctx, "JSONDiscovery.DiscoverVTGate")
	defer span.Finish()

	return d.discoverVTGate(ctx, tags)
}

func (d *JSONDiscovery) discoverVTGate(ctx context.Context, tags []string) (*vtadminpb.VTGate, error) {
	gates, err := d.discoverVTGates(ctx, tags)
	if err != nil {
		return nil, err
	}

	count := len(gates)
	if count == 0 {
		return nil, ErrNoVTGates
	}

	gate := gates[rand.Intn(len(gates))]
	return gate, nil
}

// DiscoverVTGateAddr is part of the Discovery interface.
func (d *JSONDiscovery) DiscoverVTGateAddr(ctx context.Context, tags []string) (string, error) {
	span, ctx := trace.NewSpan(ctx, "JSONDiscovery.DiscoverVTGateAddr")
	defer span.Finish()

	gate, err := d.DiscoverVTGate(ctx, tags)
	if err != nil {
		return "", err
	}

	return gate.Hostname, nil
}

// DiscoverVTGateAddrs is part of the Discovery interface.
func (d *JSONDiscovery) DiscoverVTGateAddrs(ctx context.Context, tags []string) ([]string, error) {
	span, ctx := trace.NewSpan(ctx, "JSONDiscovery.DiscoverVTGateAddrs")
	defer span.Finish()

	gates, err := d.discoverVTGates(ctx, tags)
	if err != nil {
		return nil, err
	}

	addrs := make([]string, len(gates))
	for i, gate := range gates {
		addrs[i] = gate.Hostname
	}

	return addrs, nil
}

// DiscoverVTGates is part of the Discovery interface.
func (d *JSONDiscovery) DiscoverVTGates(ctx context.Context, tags []string) ([]*vtadminpb.VTGate, error) {
	span, ctx := trace.NewSpan(ctx, "JSONDiscovery.DiscoverVTGates")
	defer span.Finish()

	return d.discoverVTGates(ctx, tags)
}

func (d *JSONDiscovery) discoverVTGates(ctx context.Context, tags []string) ([]*vtadminpb.VTGate, error) {
	if len(tags) == 0 {
		results := []*vtadminpb.VTGate{}
		for _, g := range d.gates.byName {
			results = append(results, g)
		}

		return results, nil
	}

	set := d.gates.byName

	for _, tag := range tags {
		intermediate := map[string]*vtadminpb.VTGate{}

		gates, ok := d.gates.byTag[tag]
		if !ok {
			return []*vtadminpb.VTGate{}, nil
		}

		for _, g := range gates {
			if _, ok := set[g.Hostname]; ok {
				intermediate[g.Hostname] = g
			}
		}

		set = intermediate
	}

	results := make([]*vtadminpb.VTGate, 0, len(set))

	for _, gate := range set {
		results = append(results, gate)
	}

	return results, nil
}

// DiscoverVtctld is part of the Discovery interface.
func (d *JSONDiscovery) DiscoverVtctld(ctx context.Context, tags []string) (*vtadminpb.Vtctld, error) {
	span, ctx := trace.NewSpan(ctx, "JSONDiscovery.DiscoverVtctld")
	defer span.Finish()

	return d.discoverVtctld(ctx, tags)
}

func (d *JSONDiscovery) discoverVtctld(ctx context.Context, tags []string) (*vtadminpb.Vtctld, error) {
	vtctlds, err := d.discoverVtctlds(ctx, tags)
	if err != nil {
		return nil, err
	}

	count := len(vtctlds)
	if count == 0 {
		return nil, ErrNoVtctlds
	}

	vtctld := vtctlds[rand.Intn(len(vtctlds))]
	return vtctld, nil
}

// DiscoverVtctldAddr is part of the Discovery interface.
func (d *JSONDiscovery) DiscoverVtctldAddr(ctx context.Context, tags []string) (string, error) {
	span, ctx := trace.NewSpan(ctx, "JSONDiscovery.DiscoverVtctldAddr")
	defer span.Finish()

	vtctld, err := d.discoverVtctld(ctx, tags)
	if err != nil {
		return "", err
	}

	return vtctld.Hostname, nil
}

// DiscoverVtctldAddrs is part of the Discovery interface.
func (d *JSONDiscovery) DiscoverVtctldAddrs(ctx context.Context, tags []string) ([]string, error) {
	span, ctx := trace.NewSpan(ctx, "JSONDiscovery.DiscoverVtctldAddrs")
	defer span.Finish()

	vtctlds, err := d.discoverVtctlds(ctx, tags)
	if err != nil {
		return nil, err
	}

	addrs := make([]string, len(vtctlds))
	for i, vtctld := range vtctlds {
		addrs[i] = vtctld.Hostname
	}

	return addrs, nil
}

// DiscoverVtctlds is part of the Discovery interface.
func (d *JSONDiscovery) DiscoverVtctlds(ctx context.Context, tags []string) ([]*vtadminpb.Vtctld, error) {
	span, ctx := trace.NewSpan(ctx, "JSONDiscovery.DiscoverVtctlds")
	defer span.Finish()

	return d.discoverVtctlds(ctx, tags)
}

func (d *JSONDiscovery) discoverVtctlds(ctx context.Context, tags []string) ([]*vtadminpb.Vtctld, error) {
	if len(tags) == 0 {
		results := []*vtadminpb.Vtctld{}
		for _, v := range d.vtctlds.byName {
			results = append(results, v)
		}

		return results, nil
	}

	set := d.vtctlds.byName

	for _, tag := range tags {
		intermediate := map[string]*vtadminpb.Vtctld{}

		vtctlds, ok := d.vtctlds.byTag[tag]
		if !ok {
			return []*vtadminpb.Vtctld{}, nil
		}

		for _, v := range vtctlds {
			if _, ok := set[v.Hostname]; ok {
				intermediate[v.Hostname] = v
			}
		}

		set = intermediate
	}

	results := make([]*vtadminpb.Vtctld, 0, len(set))

	for _, vtctld := range set {
		results = append(results, vtctld)
	}

	return results, nil
}
