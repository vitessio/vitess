package cache

import (
	"vitess.io/vitess/go/cache/ristretto"
)

// NewRistrettoCache returns a Cache implementation based on Ristretto
func NewRistrettoCache[I any](maxEntries, maxCost int64, cost func(I) int64) *ristretto.Cache[I] {
	// The TinyLFU paper recommends to allocate 10x times the max entries amount as counters
	// for the admission policy; since our caches are small and we're very interested on admission
	// accuracy, we're a bit more greedy than 10x
	const CounterRatio = 12

	config := ristretto.Config[I]{
		NumCounters: maxEntries * CounterRatio,
		MaxCost:     maxCost,
		BufferItems: 64,
		Metrics:     true,
		Cost:        cost,
	}
	cache, err := ristretto.NewCache[I](&config)
	if err != nil {
		panic(err)
	}
	return cache
}
