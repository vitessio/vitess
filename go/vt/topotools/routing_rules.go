/*
Copyright 2021 The Vitess Authors.

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

package topotools

import (
	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// GetRoutingRules fetches routing rules from the topology server and returns a
// mapping of fromTable=>[]toTables.
func GetRoutingRules(ctx context.Context, ts *topo.Server) (map[string][]string, error) {
	rrs, err := ts.GetRoutingRules(ctx)
	if err != nil {
		return nil, err
	}

	rules := make(map[string][]string, len(rrs.Rules))
	for _, rr := range rrs.Rules {
		rules[rr.FromTable] = rr.ToTables
	}

	return rules, nil
}

// SaveRoutingRules converts a mapping of fromTable=>[]toTables into a
// vschemapb.RoutingRules protobuf message and saves it in the topology.
func SaveRoutingRules(ctx context.Context, ts *topo.Server, rules map[string][]string) error {
	log.Infof("Saving routing rules %v\n", rules)

	rrs := &vschemapb.RoutingRules{Rules: make([]*vschemapb.RoutingRule, 0, len(rules))}
	for from, to := range rules {
		rrs.Rules = append(rrs.Rules, &vschemapb.RoutingRule{
			FromTable: from,
			ToTables:  to,
		})
	}

	return ts.SaveRoutingRules(ctx, rrs)
}
