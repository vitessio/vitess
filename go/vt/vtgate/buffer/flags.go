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

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	f_enabled       = flag.Bool("enable_buffer", false, "Enable buffering (stalling) of primary traffic during failovers.")
	f_enabledDryRun = flag.Bool("enable_buffer_dry_run", false, "Detect and log failover events, but do not actually buffer requests.")

	f_window                  = flag.Duration("buffer_window", 10*time.Second, "Duration for how long a request should be buffered at most.")
	f_size                    = flag.Int("buffer_size", 10, "Maximum number of buffered requests in flight (across all ongoing failovers).")
	f_maxFailoverDuration     = flag.Duration("buffer_max_failover_duration", 20*time.Second, "Stop buffering completely if a failover takes longer than this duration.")
	f_minTimeBetweenFailovers = flag.Duration("buffer_min_time_between_failovers", 1*time.Minute, "Minimum time between the end of a failover and the start of the next one (tracked per shard). Faster consecutive failovers will not trigger buffering.")

	f_drainConcurrency = flag.Int("buffer_drain_concurrency", 1, "Maximum number of requests retried simultaneously. More concurrency will increase the load on the PRIMARY vttablet when draining the buffer.")

	f_shards = flag.String("buffer_keyspace_shards", "", "If not empty, limit buffering to these entries (comma separated). Entry format: keyspace or keyspace/shard. Requires --enable_buffer=true.")
)

func verifyFlags() error {
	if *f_window < 1*time.Second {
		return fmt.Errorf("-buffer_window must be >= 1s (specified value: %v)", *f_window)
	}
	if *f_window > *f_maxFailoverDuration {
		return fmt.Errorf("-buffer_window must be <= -buffer_max_failover_duration: %v vs. %v", *f_window, *f_maxFailoverDuration)
	}
	if *f_size < 1 {
		return fmt.Errorf("-buffer_size must be >= 1 (specified value: %d)", *f_size)
	}
	if *f_minTimeBetweenFailovers < *f_maxFailoverDuration*time.Duration(2) {
		return fmt.Errorf("-buffer_min_time_between_failovers should be at least twice the length of -buffer_max_failover_duration: %v vs. %v", *f_minTimeBetweenFailovers, *f_maxFailoverDuration)
	}

	if *f_drainConcurrency < 1 {
		return fmt.Errorf("-buffer_drain_concurrency must be >= 1 (specified value: %d)", *f_drainConcurrency)
	}

	if *f_shards != "" && !*f_enabled {
		return fmt.Errorf("-buffer_keyspace_shards=%v also requires that -enable_buffer is set", *f_shards)
	}
	if *f_enabled && *f_enabledDryRun && *f_shards == "" {
		return errors.New("both the dry-run mode and actual buffering is enabled. To avoid ambiguity, keyspaces and shards for actual buffering must be explicitly listed in --buffer_keyspace_shards")
	}

	keyspaces, shards := keyspaceShardsToSets(*f_shards)
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

type Config struct {
	Enabled bool
	DryRun  bool

	Window                  time.Duration
	Size                    int
	MaxFailoverDuration     time.Duration
	MinTimeBetweenFailovers time.Duration

	DrainConcurrency int

	// keyspaces has the same purpose as "shards" but applies to a whole keyspace.
	Keyspaces map[string]bool
	// shards is a set of keyspace/shard entries to which buffering is limited.
	// If empty (and *enabled==true), buffering is enabled for all shards.
	Shards map[string]bool

	// internal: used for testing
	now func() time.Time
}

func NewDefaultConfig() *Config {
	return &Config{
		Enabled:                 false,
		DryRun:                  false,
		Size:                    10,
		Window:                  10 * time.Second,
		MaxFailoverDuration:     20 * time.Second,
		MinTimeBetweenFailovers: 1 * time.Minute,
		DrainConcurrency:        1,
		now:                     time.Now,
	}
}

func NewConfigFromFlags() *Config {
	if err := verifyFlags(); err != nil {
		log.Fatalf("Invalid buffer configuration: %v", err)
	}
	bufferSize.Set(int64(*f_size))
	keyspaces, shards := keyspaceShardsToSets(*f_shards)

	if *f_enabledDryRun {
		log.Infof("vtgate buffer in dry-run mode enabled for all requests. Dry-run bufferings will log failovers but not buffer requests.")
	}

	if *f_enabled {
		log.Infof("vtgate buffer enabled. PRIMARY requests will be buffered during detected failovers.")

		// Log a second line if it's only enabled for some keyspaces or shards.
		header := "Buffering limited to configured "
		limited := ""
		if len(keyspaces) > 0 {
			limited += "keyspaces: " + setToString(keyspaces)
		}
		if len(shards) > 0 {
			if limited == "" {
				limited += " and "
			}
			limited += "shards: " + setToString(shards)
		}
		if limited != "" {
			limited = header + limited
			dryRunOverride := ""
			if *f_enabledDryRun {
				dryRunOverride = " Dry-run mode is overridden for these entries and actual buffering will take place."
			}
			log.Infof("%v.%v", limited, dryRunOverride)
		}
	}

	if !*f_enabledDryRun && !*f_enabled {
		log.Infof("vtgate buffer not enabled.")
	}

	return &Config{
		Enabled: *f_enabled,
		DryRun:  *f_enabledDryRun,

		Window:                  *f_window,
		Size:                    *f_size,
		MaxFailoverDuration:     *f_maxFailoverDuration,
		MinTimeBetweenFailovers: *f_minTimeBetweenFailovers,

		DrainConcurrency: *f_drainConcurrency,

		Keyspaces: keyspaces,
		Shards:    shards,

		now: time.Now,
	}
}

func (cfg *Config) bufferingMode(keyspace, shard string) bufferMode {
	// Actual buffering is enabled if
	// a) no keyspaces and shards were listed in particular,
	if cfg.Enabled && len(cfg.Keyspaces) == 0 && len(cfg.Shards) == 0 {
		// No explicit whitelist given i.e. all shards should be buffered.
		return bufferEnabled
	}
	// b) or this keyspace is listed,
	if cfg.Keyspaces[keyspace] {
		return bufferEnabled
	}
	// c) or this shard is listed.
	keyspaceShard := topoproto.KeyspaceShardString(keyspace, shard)
	if cfg.Shards[keyspaceShard] {
		return bufferEnabled
	}

	if cfg.DryRun {
		return bufferDryRun
	}

	return bufferDisabled
}
