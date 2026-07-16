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
	"fmt"
	"time"
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
		record, err := c.Vtctld().Shard(ctx, keyspace, shard)
		lastErr = err
		if err == nil && record.GetShard().GetPrimaryAlias().GetUid() != 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("shard %s/%s did not get a primary within %s: %w", keyspace, shard, timeout, errFirst(lastErr, ctx.Err()))
		case <-time.After(healthyShardPollInterval):
		}
	}
}
