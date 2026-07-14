/*
Copyright 2026 The Vitess Authors.

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

package vitesst

import (
	"context"
	"encoding/json"
	"time"

	"vitess.io/vitess/go/vt/vterrors"
)

// healthyShardPollInterval is the poll interval for WaitForHealthyShard.
const healthyShardPollInterval = time.Second

// WaitForHealthyShard polls the shard's topology record until it has a
// primary. It checks the topo record only, not tablet health.
func (c *Cluster) WaitForHealthyShard(ctx context.Context, keyspace, shard string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastErr error
	for {
		output, err := c.Vtctld().getShard(ctx, keyspace, shard)
		lastErr = err
		if err == nil && shardHasPrimary(output) {
			return nil
		}

		select {
		case <-ctx.Done():
			return vterrors.Wrapf(errFirst(lastErr, ctx.Err()), "shard %s/%s did not get a primary within %s", keyspace, shard, timeout)
		case <-time.After(healthyShardPollInterval):
		}
	}
}

// shardHasPrimary parses a GetShard JSON response and reports whether the
// shard record names a primary tablet.
func shardHasPrimary(getShardJSON string) bool {
	var record struct {
		Shard struct {
			PrimaryAlias struct {
				Cell string `json:"cell"`
				UID  uint32 `json:"uid"`
			} `json:"primary_alias"`
		} `json:"shard"`
	}
	if err := json.Unmarshal([]byte(getShardJSON), &record); err != nil {
		return false
	}
	return record.Shard.PrimaryAlias.Cell != "" && record.Shard.PrimaryAlias.UID > 0
}
