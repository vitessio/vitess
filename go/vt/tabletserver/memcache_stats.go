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

var interval = time.Second * 5

type mainStats struct {
	mu        sync.Mutex
	lastQuery time.Time
	stats     map[string]string
}

type slabsStats struct {
	mu        sync.Mutex
	lastQuery time.Time
	stats     map[string]map[string]int64
}

type itemsStats struct {
	mu        sync.Mutex
	lastQuery time.Time
	stats     map[string]map[string]int64
}

// MemcacheStats exports the Memcache internal stats through stats package.
type MemcacheStats struct {
	started   bool
	cachePool *CachePool
	mu        sync.Mutex
	main      mainStats
	slabs     slabsStats
	items     itemsStats
}

// NewMemcacheStats creates a new MemcacheStats based on given CachePool.
func NewMemcacheStats(cachePool *CachePool) *MemcacheStats {
	s := MemcacheStats{
		main:      mainStats{stats: make(map[string]string)},
		slabs:     slabsStats{stats: make(map[string]map[string]int64)},
		items:     itemsStats{stats: make(map[string]map[string]int64)},
		cachePool: cachePool,
		started:   true,
	}
	s.exportMainStats()
	s.exportSlabsStats()
	s.exportItemsStats()
	return &s
}

// Start enables querying Memcache internal stats.
func (s *MemcacheStats) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = true
}

// Stop disables querying Memcache internal stats.
func (s *MemcacheStats) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = false
}

func (s *MemcacheStats) exportMainStats() {
	stringMetrics := map[string]bool{
		"version":       true,
		"libevent":      true,
		"rusage_user":   true,
		"rusage_system": true,
	}
	updater := func() {
		s.readStats("", func(sKey, sValue string) {
			s.main.stats[sKey] = sValue
		})
		s.main.lastQuery = time.Now()
	}

	s.main.mu.Lock()
	defer s.main.mu.Unlock()
	updater()
	for key := range s.main.stats {
		key := key // create local var to keep current key
		if stringMetrics[key] {
			f := func() string {
				s.main.mu.Lock()
				defer s.main.mu.Unlock()
				if time.Now().Sub(s.main.lastQuery) > interval {
					updater()
				}
				return s.main.stats[key]
			}
			stats.Publish(s.cachePool.name+"Memcache"+formatKey(key), stats.StringFunc(f))
			continue
		}
		f := func() int64 {
			s.main.mu.Lock()
			defer s.main.mu.Unlock()
			if time.Now().Sub(s.main.lastQuery) > interval {
				updater()
			}
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

func (s *MemcacheStats) exportSlabsStats() {
	singleMetrics := map[string]bool{
		"active_slabs":   true,
		"total_malloced": true,
	}
	updater := func() {
		s.readStats("slabs", func(sKey, sValue string) {
			ival, err := strconv.ParseInt(sValue, 10, 64)
			if err != nil {
				log.Error(err)
				return
			}
			if singleMetrics[sKey] {
				m, ok := s.slabs.stats[sKey]
				if !ok {
					m = make(map[string]int64)
					s.slabs.stats[sKey] = m
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
				m = make(map[string]int64)
				s.slabs.stats[subkey] = m
			}
			m[slabid] = ival
		})
		s.slabs.lastQuery = time.Now()
	}

	s.slabs.mu.Lock()
	defer s.slabs.mu.Unlock()
	updater()
	for key := range s.slabs.stats {
		key := key // create local var to keep current key
		if singleMetrics[key] {
			f := func() int64 {
				s.slabs.mu.Lock()
				defer s.slabs.mu.Unlock()
				if time.Now().Sub(s.slabs.lastQuery) > interval {
					updater()
				}
				return s.slabs.stats[key][""]
			}
			stats.Publish(s.cachePool.name+"MemcacheSlabs"+formatKey(key), stats.IntFunc(f))
			continue
		}
		f := func() map[string]int64 {
			s.slabs.mu.Lock()
			defer s.slabs.mu.Unlock()
			if time.Now().Sub(s.slabs.lastQuery) > interval {
				updater()
			}
			return copyMap(s.slabs.stats[key])
		}
		stats.Publish(s.cachePool.name+"MemcacheSlabs"+formatKey(key), stats.CounterFunc(f))
	}
}

func (s *MemcacheStats) exportItemsStats() {
	updater := func() {
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
				m = make(map[string]int64)
				s.items.stats[subkey] = m
			}
			m[slabid] = ival
		})
		s.items.lastQuery = time.Now()
	}

	s.items.mu.Lock()
	defer s.items.mu.Unlock()
	updater()
	for key := range s.items.stats {
		key := key // create local var to keep current key
		f := func() map[string]int64 {
			s.items.mu.Lock()
			defer s.items.mu.Unlock()
			if time.Now().Sub(s.items.lastQuery) > interval {
				updater()
			}
			return copyMap(s.items.stats[key])
		}
		stats.Publish(s.cachePool.name+"MemcacheItems"+formatKey(key), stats.CounterFunc(f))
	}
}

func (s *MemcacheStats) readStats(k string, proc func(key, value string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
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
