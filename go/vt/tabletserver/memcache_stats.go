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

type mainStats struct {
	mu        sync.Mutex
	lastQuery time.Time
	stats     map[string]string
}

func (s *mainStats) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.stats {
		if mainStringMetrics[key] {
			s.stats[key] = ""
		} else {
			s.stats[key] = "0"
		}
	}
}

type slabsStats struct {
	mu        sync.Mutex
	lastQuery time.Time
	stats     map[string]map[string]int64
}

func (s *slabsStats) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k1 := range s.stats {
		s.stats[k1] = make(map[string]int64)
	}
}

type itemsStats struct {
	mu        sync.Mutex
	lastQuery time.Time
	stats     map[string]map[string]int64
}

func (s *itemsStats) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k1 := range s.stats {
		s.stats[k1] = make(map[string]int64)
	}
}

// MemcacheStats exports the Memcache internal stats through stats package.
type MemcacheStats struct {
	cachePool *CachePool
	main      mainStats
	slabs     slabsStats
	items     itemsStats
}

// NewMemcacheStats creates a new MemcacheStats based on given CachePool.
func NewMemcacheStats(cachePool *CachePool) *MemcacheStats {
	m := MemcacheStats{
		main:      mainStats{stats: make(map[string]string)},
		slabs:     slabsStats{stats: make(map[string]map[string]int64)},
		items:     itemsStats{stats: make(map[string]map[string]int64)},
		cachePool: cachePool,
	}
	m.publishMainStats()
	m.publishSlabsStats()
	m.publishItemsStats()
	return &m
}

// Open provides a common function API, and does nothing.
func (s *MemcacheStats) Open() {
	//noop
}

// Close clears the variable values.
func (s *MemcacheStats) Close() {
	s.main.clear()
	s.slabs.clear()
	s.items.clear()
}

func (s *MemcacheStats) updateMainStats() {
	if time.Now().Sub(s.main.lastQuery) <= interval {
		return
	}
	s.readStats("", func(sKey, sValue string) {
		s.main.stats[sKey] = sValue
	})
	s.main.lastQuery = time.Now()
}

func (s *MemcacheStats) publishMainStats() {
	s.main.mu.Lock()
	defer s.main.mu.Unlock()
	for key, isstr := range mainStringMetrics {
		key := key
		if isstr {
			s.main.stats[key] = ""
			f := func() string {
				s.main.mu.Lock()
				defer s.main.mu.Unlock()
				s.updateMainStats()
				return s.main.stats[key]
			}
			stats.Publish(s.cachePool.name+"Memcache"+formatKey(key), stats.StringFunc(f))
			continue
		}
		s.main.stats[key] = "0"
		f := func() int64 {
			s.main.mu.Lock()
			defer s.main.mu.Unlock()
			s.updateMainStats()
			ival, err := strconv.ParseInt(s.main.stats[key], 10, 64)
			if err != nil {
				log.Errorf("value '%v' for key %v is not an int", s.main.stats[key], key)
				return -1
			}
			return ival
		}
		stats.Publish(s.cachePool.name+"Memcache"+formatKey(key), stats.IntFunc(f))
	}
}

func (s *MemcacheStats) updateSlabsStats() {
	if time.Now().Sub(s.slabs.lastQuery) <= interval {
		return
	}
	s.readStats("slabs", func(sKey, sValue string) {
		ival, err := strconv.ParseInt(sValue, 10, 64)
		if err != nil {
			log.Error(err)
			return
		}
		if slabsSingleMetrics[sKey] {
			m, ok := s.slabs.stats[sKey]
			if !ok {
				log.Errorf("Unknown memcache slabs stats %v: %v", sKey, ival)
				return
			}
			m[""] = ival
			return
		}
		subkey, slabid, err := parseSlabKey(sKey)
		if err != nil {
			log.Error(err)
			return
		}
		m, ok := s.slabs.stats[subkey]
		if !ok {
			log.Errorf("Unknown memcache slabs stats %v %v: %v", subkey, slabid, ival)
			return
		}
		m[slabid] = ival
	})
	s.slabs.lastQuery = time.Now()
}

func (s *MemcacheStats) publishSlabsStats() {
	s.slabs.mu.Lock()
	defer s.slabs.mu.Unlock()
	for key, isSingle := range slabsSingleMetrics {
		key := key
		s.slabs.stats[key] = make(map[string]int64)
		if isSingle {
			f := func() int64 {
				s.slabs.mu.Lock()
				defer s.slabs.mu.Unlock()
				s.updateSlabsStats()
				return s.slabs.stats[key][""]
			}
			stats.Publish(s.cachePool.name+"MemcacheSlabs"+formatKey(key), stats.IntFunc(f))
			continue
		}
		f := func() map[string]int64 {
			s.slabs.mu.Lock()
			defer s.slabs.mu.Unlock()
			s.updateSlabsStats()
			return copyMap(s.slabs.stats[key])
		}
		stats.Publish(s.cachePool.name+"MemcacheSlabs"+formatKey(key), stats.CountersFunc(f))
	}
}

func (s *MemcacheStats) updateItemsStats() {
	if time.Now().Sub(s.items.lastQuery) <= interval {
		return
	}
	s.readStats("items", func(sKey, sValue string) {
		ival, err := strconv.ParseInt(sValue, 10, 64)
		if err != nil {
			log.Error(err)
			return
		}
		subkey, slabid, err := parseItemKey(sKey)
		if err != nil {
			log.Error(err)
			return
		}
		m, ok := s.items.stats[subkey]
		if !ok {
			log.Errorf("Unknown memcache items stats %v %v: %v", subkey, slabid, ival)
			return
		}
		m[slabid] = ival
	})
	s.items.lastQuery = time.Now()
}

func (s *MemcacheStats) publishItemsStats() {
	s.items.mu.Lock()
	defer s.items.mu.Unlock()
	for _, key := range itemsMetrics {
		key := key // create local var to keep current key
		s.items.stats[key] = make(map[string]int64)
		f := func() map[string]int64 {
			s.items.mu.Lock()
			defer s.items.mu.Unlock()
			s.updateItemsStats()
			return copyMap(s.items.stats[key])
		}
		stats.Publish(s.cachePool.name+"MemcacheItems"+formatKey(key), stats.CountersFunc(f))
	}
}

func (s *MemcacheStats) readStats(k string, proc func(key, value string)) {
	defer recover() // avoid race condition when cachepool stopped right before Get()
	if s.cachePool.IsClosed() {
		return
	}
	conn := s.cachePool.Get()
	defer conn.Recycle()
	stats, err := conn.Stats(k)
	if err != nil {
		log.Errorf("Cannot export memcache %v stats: %v", k, err)
		return
	}
	st := string(stats)
	lines := strings.Split(st, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		items := strings.Split(line, " ")
		if len(items) != 3 {
			log.Errorf("Unexpected stats: %v", line)
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
