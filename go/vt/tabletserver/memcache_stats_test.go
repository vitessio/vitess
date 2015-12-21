// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"expvar"
	"fmt"
	"math/rand"
	"testing"
	"time"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestMemcacheStats(t *testing.T) {
	statsPrefix := newStatsPrefix()
	memcacheStats := NewMemcacheStats(
		statsPrefix, 1*time.Second, enableMain, NewQueryServiceStats("", false),
		func(key string) string {
			switch key {
			case "slabs":
				return ""
			case "items":
				return ""
			}
			return "STAT threads 1\n"
		},
	)
	memcacheStats.Open()
	defer memcacheStats.Close()
	memcacheStats.update()
	checkMemcacheExpvar(t, statsPrefix+"MemcacheThreads", "1")
}

func TestMemcacheStatsInvalidMainStatsValueType(t *testing.T) {
	statsPrefix := newStatsPrefix()
	memcacheStats := NewMemcacheStats(
		statsPrefix, 1*time.Second, enableMain, NewQueryServiceStats("", false),
		func(key string) string {
			switch key {
			case "slabs":
				return ""
			case "items":
				return ""
			}
			return "STAT threads invalid_val\n" +
				// incomplete stats
				"STAT threads"
		},
	)
	memcacheStats.Open()
	defer memcacheStats.Close()
	memcacheStats.update()
	checkMemcacheExpvar(t, statsPrefix+"MemcacheThreads", "-1")
}

func TestMemcacheStatsSlabsStats(t *testing.T) {
	statsPrefix := newStatsPrefix()
	memcacheStats := NewMemcacheStats(
		statsPrefix, 1*time.Second, enableSlabs, NewQueryServiceStats("", false),
		func(key string) string {
			switch key {
			case "slabs":
				return "STAT active_slabs 5\n" +
					"STAT 1:total_pages 1\n" +
					// invalid value
					"STAT 1:total_chunks invalid_val\n" +
					// invalid key format
					"STAT 1:used_chunks:invalid 10081\n" +
					// unknown slab metric
					"STAT 1:unknown_metrics 123\n" +
					"STAT 1:free_chunks 1\n" +
					"STAT 1:free_chunks_end 10079\n"
			case "items":
				return ""
			}
			return ""
		},
	)
	memcacheStats.Open()
	defer memcacheStats.Close()
	memcacheStats.update()
	checkMemcacheExpvar(t, statsPrefix+"MemcacheSlabsActiveSlabs", `5`)
	checkMemcacheExpvar(t, statsPrefix+"MemcacheSlabsTotalPages", `{"1": 1}`)
	checkMemcacheExpvar(t, statsPrefix+"MemcacheSlabsTotalChunks", `{}`)
	checkMemcacheExpvar(t, statsPrefix+"MemcacheSlabsUsedChunks", `{}`)
	checkMemcacheExpvar(t, statsPrefix+"MemcacheSlabsFreeChunks", `{"1": 1}`)
	checkMemcacheExpvar(t, statsPrefix+"MemcacheSlabsFreeChunksEnd", `{"1": 10079}`)

	if expvar.Get(statsPrefix+"MemcacheSlabsUnknownMetrics") != nil {
		t.Fatalf("%s should not be exported", statsPrefix+"MemcacheSlabsUnknownMetrics")
	}
}

func TestMemcacheStatsItemsStats(t *testing.T) {
	statsPrefix := newStatsPrefix()
	memcacheStats := NewMemcacheStats(
		statsPrefix, 1*time.Second, enableItems, NewQueryServiceStats("", false),
		func(key string) string {
			switch key {
			case "slabs":
				return ""
			case "items":
				return "STAT items:2:number 1\n" +
					// invalid item value
					"STAT items:2:age invalid_value\n" +
					// invalid item key format
					"STAT items:2:age:invalid 10\n" +
					// unknown item metric
					"STAT items:2:unknown_item 20\n" +
					"STAT items:2:evicted 4\n" +
					"STAT items:2:evicted_nonzero 5\n" +
					"STAT items:2:evicted_time 2\n" +
					"STAT items:2:outofmemory 7\n" +
					"STAT items:2:tailrepairs 11\n"
			}
			return ""
		},
	)
	memcacheStats.Open()
	defer memcacheStats.Close()
	memcacheStats.update()
	checkMemcacheExpvar(t, statsPrefix+"MemcacheItemsNumber", `{"2": 1}`)
	checkMemcacheExpvar(t, statsPrefix+"MemcacheItemsEvicted", `{"2": 4}`)
	checkMemcacheExpvar(t, statsPrefix+"MemcacheItemsEvictedNonzero", `{"2": 5}`)
	checkMemcacheExpvar(t, statsPrefix+"MemcacheItemsEvictedTime", `{"2": 2}`)
	checkMemcacheExpvar(t, statsPrefix+"MemcacheItemsOutofmemory", `{"2": 7}`)
	checkMemcacheExpvar(t, statsPrefix+"MemcacheItemsTailrepairs", `{"2": 11}`)

	if expvar.Get(statsPrefix+"MemcacheItemsUnknownItem") != nil {
		t.Fatalf("%s should not be exported", statsPrefix+"MemcacheItemsUnknownItem")
	}
}

func TestMemcacheStatsPanic(t *testing.T) {
	statsPrefix := newStatsPrefix()
	queryServiceStats := NewQueryServiceStats("", false)
	memcacheStats := NewMemcacheStats(
		statsPrefix, 100*time.Second, enableMain, queryServiceStats,
		func(key string) string {
			panic("unknown error")
		},
	)
	errCountBefore := queryServiceStats.InternalErrors.Counts()["MemcacheStats"]
	memcacheStats.Open()
	defer memcacheStats.Close()
	memcacheStats.update()
	errCountAfter := queryServiceStats.InternalErrors.Counts()["MemcacheStats"]
	if errCountAfter-errCountBefore != 1 {
		t.Fatalf("got unknown panic, MemcacheStats counter should increase by 1")
	}
}

func TestMemcacheStatsTabletError(t *testing.T) {
	statsPrefix := newStatsPrefix()
	queryServiceStats := NewQueryServiceStats("", false)
	memcacheStats := NewMemcacheStats(
		statsPrefix, 100*time.Second, enableMain, queryServiceStats,
		func(key string) string {
			panic(NewTabletError(ErrFail, vtrpcpb.ErrorCode_UNKNOWN_ERROR, "unknown tablet error"))
		},
	)
	errCountBefore := queryServiceStats.InternalErrors.Counts()["MemcacheStats"]
	memcacheStats.Open()
	defer memcacheStats.Close()
	memcacheStats.update()
	errCountAfter := queryServiceStats.InternalErrors.Counts()["MemcacheStats"]
	if errCountAfter-errCountBefore != 1 {
		t.Fatalf("got tablet error, MemcacheStats counter should increase by 1")
	}
}

func checkMemcacheExpvar(t *testing.T, name string, expectedVal string) {
	val := expvar.Get(name)
	if val == nil {
		t.Fatalf("cannot find exported variable: %s", name)
	}
	if val.String() != expectedVal {
		t.Fatalf("name: %s, expect to get %s, but got: %s", name, expectedVal, val.String())
	}
}

func newStatsPrefix() string {
	return fmt.Sprintf("TestMemcache-%d-", rand.Int63())
}
