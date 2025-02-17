/*
Copyright 2024 The Vitess Authors.

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

func GetMirrorRulesMap(rules *vschemapb.MirrorRules) map[string]map[string]float32 {
	if rules == nil {
		return nil
	}
	rulesMap := make(map[string]map[string]float32)
	for _, mr := range rules.Rules {
		if _, ok := rulesMap[mr.FromTable]; !ok {
			rulesMap[mr.FromTable] = make(map[string]float32)
		}
		rulesMap[mr.FromTable][mr.ToTable] = mr.Percent
	}
	return rulesMap
}

// GetMirrorRules fetches mirror rules from the topology server and returns a
// mapping of fromTable=>toTable=>percent.
func GetMirrorRules(ctx context.Context, ts *topo.Server) (map[string]map[string]float32, error) {
	mrs, err := ts.GetMirrorRules(ctx)
	if err != nil {
		return nil, err
	}

	rules := GetMirrorRulesMap(mrs)

	return rules, nil
}

// SaveMirrorRules converts a mapping of fromTable=>[]toTables into a
// vschemapb.MirrorRules protobuf message and saves it in the topology.
func SaveMirrorRules(ctx context.Context, ts *topo.Server, rules map[string]map[string]float32) error {
	log.V(2).Infof("Saving mirror rules %v\n", rules)

	rrs := &vschemapb.MirrorRules{Rules: make([]*vschemapb.MirrorRule, 0)}
	for fromTable, mrs := range rules {
		for toTable, percent := range mrs {
			rrs.Rules = append(rrs.Rules, &vschemapb.MirrorRule{
				FromTable: fromTable,
				Percent:   percent,
				ToTable:   toTable,
			})
		}
	}

	return ts.SaveMirrorRules(ctx, rrs)
}
