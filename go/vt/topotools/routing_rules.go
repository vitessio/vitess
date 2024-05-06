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
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// region routing rules

func GetRoutingRulesMap(rules *vschemapb.RoutingRules) map[string][]string {
	if rules == nil {
		return nil
	}
	rulesMap := make(map[string][]string, len(rules.Rules))
	for _, rr := range rules.Rules {
		rulesMap[rr.FromTable] = rr.ToTables
	}
	return rulesMap
}

// GetRoutingRules fetches routing rules from the topology server and returns a
// mapping of fromTable=>[]toTables.
func GetRoutingRules(ctx context.Context, ts *topo.Server) (map[string][]string, error) {
	rrs, err := ts.GetRoutingRules(ctx)
	if err != nil {
		return nil, err
	}

	rules := GetRoutingRulesMap(rrs)

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

// endregion

// region shard routing rules

func GetShardRoutingRuleKey(fromKeyspace, shard string) string {
	return fmt.Sprintf("%s.%s", fromKeyspace, shard)
}
func ParseShardRoutingRuleKey(key string) (string, string) {
	arr := strings.Split(key, ".")
	return arr[0], arr[1]
}

func GetShardRoutingRulesMap(rules *vschemapb.ShardRoutingRules) map[string]string {
	if rules == nil {
		return nil
	}
	rulesMap := make(map[string]string, len(rules.Rules))
	for _, rr := range rules.Rules {
		rulesMap[GetShardRoutingRuleKey(rr.FromKeyspace, rr.Shard)] = rr.ToKeyspace
	}
	return rulesMap
}

// GetShardRoutingRules fetches shard routing rules from the topology server and returns a
// mapping of fromKeyspace.Shard=>toKeyspace.
func GetShardRoutingRules(ctx context.Context, ts *topo.Server) (map[string]string, error) {
	rrs, err := ts.GetShardRoutingRules(ctx)
	if err != nil {
		return nil, err
	}

	rules := GetShardRoutingRulesMap(rrs)

	return rules, nil
}

// SaveShardRoutingRules converts a mapping of fromKeyspace.Shard=>toKeyspace into a
// vschemapb.ShardRoutingRules protobuf message and saves it in the topology.
func SaveShardRoutingRules(ctx context.Context, ts *topo.Server, srr map[string]string) error {
	log.Infof("Saving shard routing rules %v\n", srr)

	srs := &vschemapb.ShardRoutingRules{Rules: make([]*vschemapb.ShardRoutingRule, 0, len(srr))}
	for from, to := range srr {
		fromKeyspace, shard := ParseShardRoutingRuleKey(from)
		srs.Rules = append(srs.Rules, &vschemapb.ShardRoutingRule{
			FromKeyspace: fromKeyspace,
			ToKeyspace:   to,
			Shard:        shard,
		})
	}

	return ts.SaveShardRoutingRules(ctx, srs)
}

// endregion

// region keyspace routing rules

// createKeyspaceRoutingRulesKey creates the keyspace routing rules key if it doesn't yet
// exist. Vitess expects a key to exist in order to lock it. So this is used when locking
// the keyspace routing rules.
func createKeyspaceRoutingRulesKey(ctx context.Context, ts *topo.Server) error {
	krr := &vschemapb.KeyspaceRoutingRules{}
	data, err := krr.MarshalVT()
	if err != nil {
		return err
	}
	_, err = ts.GetGlobalCell().Create(ctx, topo.KeyspaceRoutingRulesFile, data)
	if topo.IsErrType(err, topo.NodeExists) {
		// Another process created it, which is fine.
		return nil
	}
	if err != nil {
		log.Errorf("Failed to create keyspace empty routing rules key: %v", err)
	} else {
		log.Infof("Successfully created keyspace routing rules key %s", topo.KeyspaceRoutingRulesFile)
	}
	return err
}

func UpdateKeyspaceRoutingRulesLocked(ctx context.Context, ts *topo.Server, reason string, callback func(ctx context.Context) error) (err error) {
	var lock *topo.KeyspaceRoutingRulesLock
	lock, err = topo.NewKeyspaceRoutingRulesLock(ctx, ts, reason)
	if err != nil {
		return err
	}
	getLock := func() (context.Context, func(*error), error) {
		lockCtx, unlock, lockErr := lock.Lock(ctx)
		if lockErr != nil {
			return nil, nil, lockErr
		}
		return lockCtx, unlock, nil
	}
	lockCtx, unlock, lockErr := getLock()
	if topo.IsErrType(lockErr, topo.NoNode) {
		if err = createKeyspaceRoutingRulesKey(ctx, ts); err != nil {
			return err
		}
		lockCtx, unlock, lockErr = getLock()
	}
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)
	return callback(lockCtx)
}

// endregion
