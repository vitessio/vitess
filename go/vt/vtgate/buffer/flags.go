/*
Copyright 2019 The Vitess Authors.

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

package buffer

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	enabled       = flag.Bool("enable_buffer", false, "Enable buffering (stalling) of master traffic during failovers.")
	enabledDryRun = flag.Bool("enable_buffer_dry_run", false, "Detect and log failover events, but do not actually buffer requests.")

	window                  = flag.Duration("buffer_window", 10*time.Second, "Duration for how long a request should be buffered at most.")
	size                    = flag.Int("buffer_size", 10, "Maximum number of buffered requests in flight (across all ongoing failovers).")
	maxFailoverDuration     = flag.Duration("buffer_max_failover_duration", 20*time.Second, "Stop buffering completely if a failover takes longer than this duration.")
	minTimeBetweenFailovers = flag.Duration("buffer_min_time_between_failovers", 1*time.Minute, "Minimum time between the end of a failover and the start of the next one (tracked per shard). Faster consecutive failovers will not trigger buffering.")

	drainConcurrency = flag.Int("buffer_drain_concurrency", 1, "Maximum number of requests retried simultaneously. More concurrency will increase the load on the MASTER vttablet when draining the buffer.")

	shards = flag.String("buffer_keyspace_shards", "", "If not empty, limit buffering to these entries (comma separated). Entry format: keyspace or keyspace/shard. Requires --enable_buffer=true.")
)

func resetFlagsForTesting() {
	// Set all flags to their default value.
	flag.Set("enable_buffer", "false")
	flag.Set("enable_buffer_dry_run", "false")
	flag.Set("buffer_size", "10")
	flag.Set("buffer_window", "10s")
	flag.Set("buffer_keyspace_shards", "")
	flag.Set("buffer_max_failover_duration", "20s")
	flag.Set("buffer_min_time_between_failovers", "1m")
}

func verifyFlags() error {
	if *window < 1*time.Second {
		return fmt.Errorf("-buffer_window must be >= 1s (specified value: %v)", *window)
	}
	if *window > *maxFailoverDuration {
		return fmt.Errorf("-buffer_window must be <= -buffer_max_failover_duration: %v vs. %v", *window, *maxFailoverDuration)
	}
	if *size < 1 {
		return fmt.Errorf("-buffer_size must be >= 1 (specified value: %d)", *size)
	}
	if *minTimeBetweenFailovers < *maxFailoverDuration*time.Duration(2) {
		return fmt.Errorf("-buffer_min_time_between_failovers should be at least twice the length of -buffer_max_failover_duration: %v vs. %v", *minTimeBetweenFailovers, *maxFailoverDuration)
	}

	if *drainConcurrency < 1 {
		return fmt.Errorf("-buffer_drain_concurrency must be >= 1 (specified value: %d)", *drainConcurrency)
	}

	if *shards != "" && !*enabled {
		return fmt.Errorf("-buffer_keyspace_shards=%v also requires that -enable_buffer is set", *shards)
	}
	if *enabled && *enabledDryRun && *shards == "" {
		return errors.New("both the dry-run mode and actual buffering is enabled. To avoid ambiguity, keyspaces and shards for actual buffering must be explicitly listed in --buffer_keyspace_shards")
	}

	keyspaces, shards := keyspaceShardsToSets(*shards)
	for s := range shards {
		keyspace, _, err := topoproto.ParseKeyspaceShard(s)
		if err != nil {
			return err
		}
		if keyspaces[keyspace] {
			return fmt.Errorf("-buffer_keyspace_shards has overlapping entries (keyspace only vs. keyspace/shard): %v vs. %v Please remove one or the other", keyspace, s)
		}
	}

	return nil
}

// keyspaceShardsToSets converts a comma separated list of keyspace[/shard]
// entries to two sets: keyspaces (if the shard is not specified) and shards (if
// both keyspace and shard is specified).
func keyspaceShardsToSets(list string) (map[string]bool, map[string]bool) {
	keyspaces := make(map[string]bool)
	shards := make(map[string]bool)
	if list == "" {
		return keyspaces, shards
	}

	for _, item := range strings.Split(list, ",") {
		if strings.Contains(item, "/") {
			shards[item] = true
		} else {
			keyspaces[item] = true
		}
	}
	return keyspaces, shards
}

// setToString joins the set to a ", " separated string.
func setToString(set map[string]bool) string {
	result := ""
	for item := range set {
		if result != "" {
			result += ", "
		}
		result += item
	}
	return result
}
