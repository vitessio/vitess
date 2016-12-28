package buffer

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

var (
	enabled = flag.Bool("enable_vtgate_buffer", false, "Enable buffering (stalling) of master traffic during failovers.")

	window                  = flag.Duration("vtgate_buffer_window", 10*time.Second, "Duration for how long a request should be buffered at most.")
	size                    = flag.Int("vtgate_buffer_size", 10, "Maximum number of buffered requests in flight (across all ongoing failovers).")
	maxFailoverDuration     = flag.Duration("vtgate_buffer_max_failover_duration", 40*time.Second, "Stop buffering completely if a failover takes longer than this duration.")
	minTimeBetweenFailovers = flag.Duration("vtgate_buffer_min_time_between_failovers", 5*time.Minute, "Minimum time between the end of a failover and the start of the next one. Faster consecutive failovers will not trigger buffering.")

	drainConcurrency = flag.Int("vtgate_buffer_drain_concurrency", 1, "Maximum number of requests retried simultaneously.")

	shards       = flag.String("vtgate_buffer_keyspace_shards", "", "If not empty, limit buffering to these keyspace/shard entries. Requires --enable_vtgate_buffer=true.")
	shardsDryRun = flag.String("vtgate_buffer_keyspace_shards_dry_run", "", "If not empty, track failovers for these keyspace/shard entries but do not buffer requests (dry-run mode).")
)

func verifyFlags() error {
	if *window < 1*time.Second {
		return fmt.Errorf("-vtgate_buffer_window must be >= 1s (specified value: %v)", *window)
	}
	if *window > *maxFailoverDuration {
		return fmt.Errorf("-vtgate_buffer_window must be <= -vtgate_buffer_max_failover_duration: %v vs. %v", *window, *maxFailoverDuration)
	}
	if *size < 1 {
		return fmt.Errorf("-vtgate_buffer_size must be >= 1 (specified value: %d)", *size)
	}
	if *minTimeBetweenFailovers < *maxFailoverDuration*time.Duration(2) {
		return fmt.Errorf("-vtgate_buffer_min_time_between_failovers should be at least twice the length of -vtgate_buffer_max_failover_duration: %v vs. %v", *minTimeBetweenFailovers, *maxFailoverDuration)
	}

	if *drainConcurrency < 1 {
		return fmt.Errorf("-vtgate_buffer_drain_concurrency must be >= 1 (specified value: %d)", *drainConcurrency)
	}

	if *shards != "" && !*enabled {
		return fmt.Errorf("-vtgate_buffer_keyspace_shards=%v also requires that -enable_vtgate_buffer is set", *shards)
	}

	shards := listToSet(*shards)
	for s := range listToSet(*shardsDryRun) {
		if shards[s] {
			return fmt.Errorf("-vtgate_buffer_keyspace_shards and -vtgate_buffer_keyspace_shards_dry_run must not overlap. Common entry found: %v", s)
		}
	}

	return nil
}

// listToSet converts a comma separated list to a set.
func listToSet(list string) map[string]bool {
	set := make(map[string]bool)
	if list == "" {
		return set
	}

	for _, item := range strings.Split(list, ",") {
		set[item] = true
	}
	return set
}
