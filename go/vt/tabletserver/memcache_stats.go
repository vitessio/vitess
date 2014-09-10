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

var interval = 5 * time.Second

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

// MemcacheStats exports the Memcache internal stats through stats package.
type MemcacheStats struct {
	cachePool *CachePool
	ticks     *timer.Timer
	mu        sync.Mutex
	main      map[string]string
	slabs     map[string]map[string]int64
	items     map[string]map[string]int64
}

// NewMemcacheStats creates a new MemcacheStats based on given CachePool.
// main, slabs and items specify the categories of stats that need to be exported.
func NewMemcacheStats(cachePool *CachePool, main, slabs, items bool) *MemcacheStats {
	s := &MemcacheStats{
		cachePool: cachePool,
		ticks:     timer.NewTimer(10 * time.Second),
	}
	if main {
		s.publishMainStats()
	}
	if slabs {
		s.publishSlabsStats()
	}
	if items {
		s.publishItemsStats()
	}
	return s
}

// Open starts exporting the stats.
func (s *MemcacheStats) Open() {
	s.ticks.Start(func() {
		s.updateMainStats()
		s.updateSlabsStats()
		s.updateItemsStats()
	})
}

// Close clears the variable values and stops exporting the stats.
func (s *MemcacheStats) Close() {
	s.ticks.Stop()

	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.main {
		if mainStringMetrics[key] {
			s.main[key] = ""
		} else {
			s.main[key] = "0"
		}
	}
	for key := range s.slabs {
		s.slabs[key] = make(map[string]int64)
	}
	for key := range s.items {
		s.items[key] = make(map[string]int64)
	}
}

func (s *MemcacheStats) publishMainStats() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.main = make(map[string]string)
	for key, isstr := range mainStringMetrics {
		key := key
		if isstr {
			s.main[key] = ""
			stats.Publish(s.cachePool.name+"Memcache"+formatKey(key), stats.StringFunc(func() string {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.main[key]
			}))
		} else {
			s.main[key] = "0"
			stats.Publish(s.cachePool.name+"Memcache"+formatKey(key), stats.IntFunc(func() int64 {
				s.mu.Lock()
				defer s.mu.Unlock()
				ival, err := strconv.ParseInt(s.main[key], 10, 64)
				if err != nil {
					log.Errorf("value '%v' for key %v is not an int", s.main[key], key)
					internalErrors.Add("MemcacheStats", 1)
					return -1
				}
				return ival
			}))
		}
	}
}

func (s *MemcacheStats) updateMainStats() {
	if s.main == nil {
		return
	}
	s.readStats("", func(sKey, sValue string) {
		s.main[sKey] = sValue
	})
}

func (s *MemcacheStats) publishSlabsStats() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slabs = make(map[string]map[string]int64)
	for key, isSingle := range slabsSingleMetrics {
		key := key
		s.slabs[key] = make(map[string]int64)
		if isSingle {
			stats.Publish(s.cachePool.name+"MemcacheSlabs"+formatKey(key), stats.IntFunc(func() int64 {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.slabs[key][""]
			}))
		} else {
			stats.Publish(s.cachePool.name+"MemcacheSlabs"+formatKey(key), stats.CountersFunc(func() map[string]int64 {
				s.mu.Lock()
				defer s.mu.Unlock()
				return copyMap(s.slabs[key])
			}))
		}
	}
}

func (s *MemcacheStats) updateSlabsStats() {
	if s.slabs == nil {
		return
	}
	s.readStats("slabs", func(sKey, sValue string) {
		ival, err := strconv.ParseInt(sValue, 10, 64)
		if err != nil {
			log.Error(err)
			internalErrors.Add("MemcacheStats", 1)
			return
		}
		if slabsSingleMetrics[sKey] {
			m, ok := s.slabs[sKey]
			if !ok {
				log.Errorf("Unknown memcache slabs stats %v: %v", sKey, ival)
				internalErrors.Add("MemcacheStats", 1)
				return
			}
			m[""] = ival
			return
		}
		subkey, slabid, err := parseSlabKey(sKey)
		if err != nil {
			log.Error(err)
			internalErrors.Add("MemcacheStats", 1)
			return
		}
		m, ok := s.slabs[subkey]
		if !ok {
			log.Errorf("Unknown memcache slabs stats %v %v: %v", subkey, slabid, ival)
			internalErrors.Add("MemcacheStats", 1)
			return
		}
		m[slabid] = ival
	})
}

func (s *MemcacheStats) publishItemsStats() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = make(map[string]map[string]int64)
	for _, key := range itemsMetrics {
		key := key // create local var to keep current key
		s.items[key] = make(map[string]int64)
		stats.Publish(s.cachePool.name+"MemcacheItems"+formatKey(key), stats.CountersFunc(func() map[string]int64 {
			s.mu.Lock()
			defer s.mu.Unlock()
			return copyMap(s.items[key])
		}))
	}
}

func (s *MemcacheStats) updateItemsStats() {
	if s.items == nil {
		return
	}
	s.readStats("items", func(sKey, sValue string) {
		ival, err := strconv.ParseInt(sValue, 10, 64)
		if err != nil {
			log.Error(err)
			internalErrors.Add("MemcacheStats", 1)
			return
		}
		subkey, slabid, err := parseItemKey(sKey)
		if err != nil {
			log.Error(err)
			internalErrors.Add("MemcacheStats", 1)
			return
		}
		m, ok := s.items[subkey]
		if !ok {
			log.Errorf("Unknown memcache items stats %v %v: %v", subkey, slabid, ival)
			internalErrors.Add("MemcacheStats", 1)
			return
		}
		m[slabid] = ival
	})
}

func (s *MemcacheStats) readStats(k string, proc func(key, value string)) {
	defer func() {
		if x := recover(); x != nil {
			_, ok := x.(*TabletError)
			if !ok {
				log.Errorf("Uncaught panic when reading memcache stats: %v", x)
			} else {
				log.Errorf("Could not read memcache stats: %v", x)
			}
			internalErrors.Add("MemcacheStats", 1)
		}
	}()
	conn := s.cachePool.Get()
	// This is not the same as defer rc.cachePool.Put(conn)
	defer func() { s.cachePool.Put(conn) }()

	stats, err := conn.Stats(k)
	if err != nil {
		conn.Close()
		conn = nil
		log.Errorf("Cannot export memcache %v stats: %v", k, err)
		internalErrors.Add("MemcacheStats", 1)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	st := string(stats)
	lines := strings.Split(st, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		items := strings.Split(line, " ")
		if len(items) != 3 {
			log.Errorf("Unexpected stats: %v", line)
			internalErrors.Add("MemcacheStats", 1)
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
	if src == nil {
		return nil
	}
	dst := make(map[string]int64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
