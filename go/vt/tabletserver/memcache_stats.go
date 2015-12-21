// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/timer"
)

var mainStringMetrics = map[string]bool{
	"accepting_conns":       false,
	"auth_cmds":             false,
	"auth_errors":           false,
	"bytes_read":            false,
	"bytes_written":         false,
	"bytes":                 false,
	"cas_badval":            false,
	"cas_hits":              false,
	"cas_misses":            false,
	"cmd_flush":             false,
	"cmd_get":               false,
	"cmd_set":               false,
	"cmd_touch":             false,
	"conn_yields":           false,
	"connection_structures": false,
	"curr_connections":      false,
	"curr_items":            false,
	"decr_hits":             false,
	"decr_misses":           false,
	"delete_hits":           false,
	"delete_misses":         false,
	"evicted_unfetched":     false,
	"evictions":             false,
	"expired_unfetched":     false,
	"get_hits":              false,
	"get_misses":            false,
	"hash_bytes":            false,
	"hash_is_expanding":     false,
	"hash_power_level":      false,
	"incr_hits":             false,
	"incr_misses":           false,
	"libevent":              true,
	"limit_maxbytes":        false,
	"listen_disabled_num":   false,
	"pid":               false,
	"pointer_size":      false,
	"reclaimed":         false,
	"reserved_fds":      false,
	"rusage_system":     true,
	"rusage_user":       true,
	"threads":           false,
	"time":              false,
	"total_connections": false,
	"total_items":       false,
	"touch_hits":        false,
	"touch_misses":      false,
	"uptime":            false,
	"version":           true,
}

var slabsSingleMetrics = map[string]bool{
	"active_slabs":    true,
	"cas_badval":      false,
	"cas_hits":        false,
	"chunk_size":      false,
	"chunks_per_page": false,
	"cmd_set":         false,
	"decr_hits":       false,
	"delete_hits":     false,
	"free_chunks_end": false,
	"free_chunks":     false,
	"get_hits":        false,
	"incr_hits":       false,
	"mem_requested":   false,
	"total_chunks":    false,
	"total_malloced":  true,
	"total_pages":     false,
	"touch_hits":      false,
	"used_chunks":     false,
}

var itemsMetrics = []string{
	"age",
	"evicted",
	"evicted_nonzero",
	"evicted_time",
	"evicted_unfetched",
	"expired_unfetched",
	"number",
	"outofmemory",
	"reclaimed",
	"tailrepairs",
}

// RetrieveCacheStats returns current memcache stats.
type RetrieveCacheStats func(key string) string

// MemcacheStats exports the Memcache internal stats through stats package.
type MemcacheStats struct {
	ticks             *timer.Timer
	mu                sync.Mutex
	main              map[string]string
	slabs             map[string]map[string]int64
	items             map[string]map[string]int64
	statsPrefix       string
	statsFunc         RetrieveCacheStats
	queryServiceStats *QueryServiceStats
	flags             int64
}

const (
	enableMain = 1 << iota
	enableSlabs
	enableItems
)

// NewMemcacheStats creates a new MemcacheStats.
// main, slabs and items specify the categories of stats that need to be exported.
func NewMemcacheStats(
	statsPrefix string,
	refreshFreq time.Duration,
	flags int64,
	queryServiceStats *QueryServiceStats,
	statsFunc RetrieveCacheStats) *MemcacheStats {
	memstats := &MemcacheStats{
		ticks:             timer.NewTimer(refreshFreq),
		statsPrefix:       statsPrefix,
		statsFunc:         statsFunc,
		main:              make(map[string]string),
		slabs:             make(map[string]map[string]int64),
		items:             make(map[string]map[string]int64),
		queryServiceStats: queryServiceStats,
		flags:             flags,
	}
	if flags&enableMain > 0 {
		memstats.publishMainStats()
	}
	if flags&enableSlabs > 0 {
		memstats.publishSlabsStats()
	}
	if flags*enableItems > 0 {
		memstats.publishItemsStats()
	}
	return memstats
}

// Open starts exporting the stats.
func (memstats *MemcacheStats) Open() {
	memstats.ticks.Start(func() { memstats.update() })
}

// Close clears the variable values and stops exporting the stats.
func (memstats *MemcacheStats) Close() {
	memstats.ticks.Stop()

	memstats.mu.Lock()
	defer memstats.mu.Unlock()
	for key := range memstats.main {
		if mainStringMetrics[key] {
			memstats.main[key] = ""
		} else {
			memstats.main[key] = "0"
		}
	}
	for key := range memstats.slabs {
		memstats.slabs[key] = make(map[string]int64)
	}
	for key := range memstats.items {
		memstats.items[key] = make(map[string]int64)
	}
}

func (memstats *MemcacheStats) update() {
	if memstats.flags&enableMain > 0 {
		memstats.updateMainStats()
	}
	if memstats.flags&enableSlabs > 0 {
		memstats.updateSlabsStats()
	}
	if memstats.flags&enableItems > 0 {
		memstats.updateItemsStats()
	}
}

func (memstats *MemcacheStats) publishMainStats() {
	memstats.mu.Lock()
	defer memstats.mu.Unlock()
	for k, isstr := range mainStringMetrics {
		key := k
		if isstr {
			memstats.main[key] = ""
			stats.Publish(memstats.statsPrefix+"Memcache"+formatKey(key), stats.StringFunc(func() string {
				memstats.mu.Lock()
				defer memstats.mu.Unlock()
				return memstats.main[key]
			}))
		} else {
			memstats.main[key] = "0"
			stats.Publish(memstats.statsPrefix+"Memcache"+formatKey(key), stats.IntFunc(func() int64 {
				memstats.mu.Lock()
				defer memstats.mu.Unlock()
				ival, err := strconv.ParseInt(memstats.main[key], 10, 64)
				if err != nil {
					log.Errorf("value '%v' for key %v is not an int", memstats.main[key], key)
					memstats.queryServiceStats.InternalErrors.Add("MemcacheStats", 1)
					return -1
				}
				return ival
			}))
		}
	}
}

func (memstats *MemcacheStats) updateMainStats() {
	memstats.readStats("", func(sKey, sValue string) {
		memstats.main[sKey] = sValue
	})
}

func (memstats *MemcacheStats) publishSlabsStats() {
	memstats.mu.Lock()
	defer memstats.mu.Unlock()
	for key, isSingle := range slabsSingleMetrics {
		key := key
		memstats.slabs[key] = make(map[string]int64)
		if isSingle {
			stats.Publish(memstats.statsPrefix+"MemcacheSlabs"+formatKey(key), stats.IntFunc(func() int64 {
				memstats.mu.Lock()
				defer memstats.mu.Unlock()
				return memstats.slabs[key][""]
			}))
		} else {
			stats.Publish(memstats.statsPrefix+"MemcacheSlabs"+formatKey(key), stats.CountersFunc(func() map[string]int64 {
				memstats.mu.Lock()
				defer memstats.mu.Unlock()
				return copyMap(memstats.slabs[key])
			}))
		}
	}
}

func (memstats *MemcacheStats) updateSlabsStats() {
	memstats.readStats("slabs", func(sKey, sValue string) {
		ival, err := strconv.ParseInt(sValue, 10, 64)
		if err != nil {
			log.Error(err)
			memstats.queryServiceStats.InternalErrors.Add("MemcacheStats", 1)
			return
		}
		if slabsSingleMetrics[sKey] {
			m, ok := memstats.slabs[sKey]
			if !ok {
				log.Errorf("Unknown memcache slabs stats %v: %v", sKey, ival)
				memstats.queryServiceStats.InternalErrors.Add("MemcacheStats", 1)
				return
			}
			m[""] = ival
			return
		}
		subkey, slabid, err := parseSlabKey(sKey)
		if err != nil {
			log.Error(err)
			memstats.queryServiceStats.InternalErrors.Add("MemcacheStats", 1)
			return
		}
		m, ok := memstats.slabs[subkey]
		if !ok {
			log.Errorf("Unknown memcache slabs stats %v %v: %v", subkey, slabid, ival)
			memstats.queryServiceStats.InternalErrors.Add("MemcacheStats", 1)
			return
		}
		m[slabid] = ival
	})
}

func (memstats *MemcacheStats) publishItemsStats() {
	memstats.mu.Lock()
	defer memstats.mu.Unlock()
	for _, key := range itemsMetrics {
		key := key // create local var to keep current key
		memstats.items[key] = make(map[string]int64)
		stats.Publish(memstats.statsPrefix+"MemcacheItems"+formatKey(key), stats.CountersFunc(func() map[string]int64 {
			memstats.mu.Lock()
			defer memstats.mu.Unlock()
			return copyMap(memstats.items[key])
		}))
	}
}

func (memstats *MemcacheStats) updateItemsStats() {
	memstats.readStats("items", func(sKey, sValue string) {
		ival, err := strconv.ParseInt(sValue, 10, 64)
		if err != nil {
			log.Error(err)
			memstats.queryServiceStats.InternalErrors.Add("MemcacheStats", 1)
			return
		}
		subkey, slabid, err := parseItemKey(sKey)
		if err != nil {
			log.Error(err)
			memstats.queryServiceStats.InternalErrors.Add("MemcacheStats", 1)
			return
		}
		m, ok := memstats.items[subkey]
		if !ok {
			log.Errorf("Unknown memcache items stats %v %v: %v", subkey, slabid, ival)
			memstats.queryServiceStats.InternalErrors.Add("MemcacheStats", 1)
			return
		}
		m[slabid] = ival
	})
}

func (memstats *MemcacheStats) readStats(k string, proc func(key, value string)) {
	defer func() {
		if x := recover(); x != nil {
			_, ok := x.(*TabletError)
			if !ok {
				log.Errorf("Uncaught panic when reading memcache stats: %v", x)
			} else {
				log.Errorf("Could not read memcache stats: %v", x)
			}
			memstats.queryServiceStats.InternalErrors.Add("MemcacheStats", 1)
		}
	}()

	stats := memstats.statsFunc(k)
	if stats == "" {
		return
	}

	memstats.mu.Lock()
	defer memstats.mu.Unlock()
	lines := strings.Split(stats, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		items := strings.Split(line, " ")
		//if using apt-get, memcached info would be:STAT version 1.4.14 (Ubuntu)
		//so less then 3 would be compatible with original memcached
		if len(items) < 3 {
			log.Errorf("Unexpected stats: %v", line)
			memstats.queryServiceStats.InternalErrors.Add("MemcacheStats", 1)
			continue
		}
		proc(items[1], items[2])
	}
}

func formatKey(key string) string {
	key = regexp.MustCompile("^[a-z]").ReplaceAllStringFunc(key, func(item string) string {
		return strings.ToUpper(item)
	})
	key = regexp.MustCompile("_[a-z]").ReplaceAllStringFunc(key, func(item string) string {
		return strings.ToUpper(item[1:])
	})
	return key
}

// parseSlabKey splits a slab key into the subkey and slab id:
// "1:chunk_size" -> "chunk_size", 1
func parseSlabKey(key string) (subkey string, slabid string, err error) {
	tokens := strings.Split(key, ":")
	if len(tokens) != 2 {
		return "", "", fmt.Errorf("invalid slab key: %v", key)
	}
	return tokens[1], tokens[0], nil
}

// parseItemKey splits an item key into the subkey and slab id:
// "items:1:number" -> "number", 1
func parseItemKey(key string) (subkey string, slabid string, err error) {
	tokens := strings.Split(key, ":")
	if len(tokens) != 3 {
		return "", "", fmt.Errorf("invalid slab key: %v", key)
	}
	return tokens[2], tokens[1], nil
}

func copyMap(src map[string]int64) map[string]int64 {
	dst := make(map[string]int64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
