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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/trace"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// StaticFileDiscovery implements the Discovery interface for "discovering"
// Vitess components hardcoded in a static JSON file.
//
// As an example, here's a minimal JSON file for a single Vitess cluster running locally
// (such as the one described in https://vitess.io/docs/get-started/local-docker):
//
// 		{
// 			"vtgates": [
// 				{
// 					"host": {
// 						"hostname": "127.0.0.1:15991"
// 					}
// 				}
// 			]
// 		}
//
// For more examples of various static file configurations, see the unit tests.
type StaticFileDiscovery struct {
	cluster *vtadminpb.Cluster
	config  *StaticFileClusterConfig
	gates   struct {
		byName map[string]*vtadminpb.VTGate
		byTag  map[string][]*vtadminpb.VTGate
	}
	vtctlds struct {
		byName map[string]*vtadminpb.Vtctld
		byTag  map[string][]*vtadminpb.Vtctld
	}
}

// StaticFileClusterConfig configures Vitess components for a single cluster.
type StaticFileClusterConfig struct {
	VTGates []*StaticFileVTGateConfig `json:"vtgates,omitempty"`
	Vtctlds []*StaticFileVtctldConfig `json:"vtctlds,omitempty"`
}

// StaticFileVTGateConfig contains host and tag information for a single VTGate in a cluster.
type StaticFileVTGateConfig struct {
	Host *vtadminpb.VTGate `json:"host"`
	Tags []string          `json:"tags"`
}

// StaticFileVtctldConfig contains a host and tag information for a single
// Vtctld in a cluster.
type StaticFileVtctldConfig struct {
	Host *vtadminpb.Vtctld `json:"host"`
	Tags []string          `json:"tags"`
}

// NewStaticFile returns a StaticFileDiscovery for the given cluster.
func NewStaticFile(cluster *vtadminpb.Cluster, flags *pflag.FlagSet, args []string) (Discovery, error) {
	disco := &StaticFileDiscovery{
		cluster: cluster,
	}

	filePath := flags.String("path", "", "path to the service discovery JSON config file")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	if filePath == nil || *filePath == "" {
		return nil, errors.New("must specify path to the service discovery JSON config file")
	}

	b, err := ioutil.ReadFile(*filePath)
	if err != nil {
		return nil, err
	}

	if err := disco.parseConfig(b); err != nil {
		return nil, err
	}

	return disco, nil
}

func (d *StaticFileDiscovery) parseConfig(bytes []byte) error {
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
func (d *StaticFileDiscovery) DiscoverVTGate(ctx context.Context, tags []string) (*vtadminpb.VTGate, error) {
	span, ctx := trace.NewSpan(ctx, "StaticFileDiscovery.DiscoverVTGate")
	defer span.Finish()

	return d.discoverVTGate(ctx, tags)
}

func (d *StaticFileDiscovery) discoverVTGate(ctx context.Context, tags []string) (*vtadminpb.VTGate, error) {
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
func (d *StaticFileDiscovery) DiscoverVTGateAddr(ctx context.Context, tags []string) (string, error) {
	span, ctx := trace.NewSpan(ctx, "StaticFileDiscovery.DiscoverVTGateAddr")
	defer span.Finish()

	gate, err := d.DiscoverVTGate(ctx, tags)
	if err != nil {
		return "", err
	}

	return gate.Hostname, nil
}

// DiscoverVTGates is part of the Discovery interface.
func (d *StaticFileDiscovery) DiscoverVTGates(ctx context.Context, tags []string) ([]*vtadminpb.VTGate, error) {
	span, ctx := trace.NewSpan(ctx, "StaticFileDiscovery.DiscoverVTGates")
	defer span.Finish()

	return d.discoverVTGates(ctx, tags)
}

func (d *StaticFileDiscovery) discoverVTGates(ctx context.Context, tags []string) ([]*vtadminpb.VTGate, error) {
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
func (d *StaticFileDiscovery) DiscoverVtctld(ctx context.Context, tags []string) (*vtadminpb.Vtctld, error) {
	span, ctx := trace.NewSpan(ctx, "StaticFileDiscovery.DiscoverVtctld")
	defer span.Finish()

	return d.discoverVtctld(ctx, tags)
}

func (d *StaticFileDiscovery) discoverVtctld(ctx context.Context, tags []string) (*vtadminpb.Vtctld, error) {
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
func (d *StaticFileDiscovery) DiscoverVtctldAddr(ctx context.Context, tags []string) (string, error) {
	span, ctx := trace.NewSpan(ctx, "StaticFileDiscovery.DiscoverVtctldAddr")
	defer span.Finish()

	vtctld, err := d.discoverVtctld(ctx, tags)
	if err != nil {
		return "", err
	}

	return vtctld.Hostname, nil
}

// DiscoverVtctlds is part of the Discovery interface.
func (d *StaticFileDiscovery) DiscoverVtctlds(ctx context.Context, tags []string) ([]*vtadminpb.Vtctld, error) {
	span, ctx := trace.NewSpan(ctx, "StaticFileDiscovery.DiscoverVtctlds")
	defer span.Finish()

	return d.discoverVtctlds(ctx, tags)
}

func (d *StaticFileDiscovery) discoverVtctlds(ctx context.Context, tags []string) ([]*vtadminpb.Vtctld, error) {
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
