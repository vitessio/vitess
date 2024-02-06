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
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

//region routing rules

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

//endregion

//region shard routing rules

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

//region keyspace routing rules

type KeyspaceRoutingRules struct {
	RulesHash string
	Rules     map[string]string
}

func GetKeyspaceRoutingRulesMap(rules *vschemapb.KeyspaceRoutingRules) map[string]string {
	if rules == nil {
		return nil
	}
	rulesMap := make(map[string]string, len(rules.Rules))
	for _, rr := range rules.Rules {
		rulesMap[rr.FromKeyspace] = rr.ToKeyspace
	}
	return rulesMap
}

func GetKeyspaceRoutingRulesMapFromCompressed(compressedRules string) (map[string]string, error) {
	rulesBytes, err := Decompress([]byte(compressedRules))
	if err != nil {
		return nil, err
	}

	var ksRules vschemapb.KeyspaceRoutingRules
	err = protojson.Unmarshal(rulesBytes, &ksRules)
	if err != nil {
		return nil, err
	}
	return GetKeyspaceRoutingRulesMap(&ksRules), nil
}

func GetKeyspaceRoutingRules(ctx context.Context, ts *topo.Server) (*KeyspaceRoutingRules, error) {
	var rules KeyspaceRoutingRules
	ks_rr_c, err := ts.GetKeyspaceRoutingRules(ctx)
	if err != nil {
		return nil, err
	}

	rules.RulesHash = ks_rr_c.RulesHash
	rules.Rules, err = GetKeyspaceRoutingRulesMapFromCompressed(ks_rr_c.CompressedRules)
	if err != nil {
		return nil, err
	}
	return &rules, nil
}

func SaveKeyspaceRoutingRules(ctx context.Context, ts *topo.Server, rules map[string]string) (string, error) {
	log.Infof("Saving keyspace routing rules %v\n", rules)

	ks_rr := &vschemapb.KeyspaceRoutingRules{Rules: make([]*vschemapb.KeyspaceRoutingRule, 0, len(rules))}
	for from, to := range rules {
		ks_rr.Rules = append(ks_rr.Rules, &vschemapb.KeyspaceRoutingRule{
			FromKeyspace: from,
			ToKeyspace:   to,
		})
	}

	ks_rr_c := &vschemapb.KeyspaceRoutingRulesCompressed{}
	ks_rr_bytes, err := protojson.Marshal(ks_rr)
	if err != nil {
		return "", err
	}
	compressedRules, err := Compress(ks_rr_bytes)
	if err != nil {
		return "", err
	}
	ks_rr_c.CompressedRules = string(compressedRules)
	ks_rr_c.RulesHash = ComputeStableMapHash(rules)

	err = ts.SaveKeyspaceRoutingRules(ctx, ks_rr_c)

	return ks_rr_c.RulesHash, err
}

//endregion

//region compression and hashing

// Compress compresses the given byte slice and returns a compressed byte slice.
func Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decompress decompresses the given byte slice and returns a decompressed byte slice.
func Decompress(data []byte) ([]byte, error) {
	b := bytes.NewReader(data)
	r, err := zlib.NewReader(b)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	decompressedData, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return decompressedData, nil
}

// ComputeStableMapHash computes a stable hash for a map with string keys and string values.
func ComputeStableMapHash(m map[string]string) string {
	// Create a slice to hold the map's keys.
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	// Sort the keys to ensure stable iteration order.
	sort.Strings(keys)

	// Create an xxhash hasher.
	hasher := xxhash.New()

	// Iterate over the map in the order of the sorted keys.
	for _, k := range keys {
		// Write the key and value to the hasher.
		// Adding a separator between the key and value to ensure unique combinations are hashed distinctly.
		// You might also consider how to handle potential collisions in key-value pairs, such as "ab"+"c" vs "a"+"bc".
		_, err := hasher.Write([]byte(k + ":" + m[k]))
		if err != nil {
			return ""
		}
	}

	// Compute the hash.
	hash := hasher.Sum64()

	return fmt.Sprintf("%016x", hash)
}

//endregion
