package cache

import (
	"vitess.io/vitess/go/cache/ristretto"
)

var _ Cache = &ristretto.Cache{}

// NewRistrettoCache returns a Cache implementation based on Ristretto
func NewRistrettoCache(maxEntries, maxCost int64, cost func(interface{}) int64) *ristretto.Cache {
	// The TinyLFU paper recommends to allocate 10x times the max entries amount as counters
	// for the admission policy; since our caches are small and we're very interested on admission
	// accuracy, we're a bit more greedy than 10x
	const CounterRatio = 12

	config := ristretto.Config{
		NumCounters: maxEntries * CounterRatio,
		MaxCost:     maxCost,
		BufferItems: 64,
		Metrics:     true,
		Cost:        cost,
	}
	cache, err := ristretto.NewCache(&config)
	if err != nil {
		panic(err)
	}
	return cache
}
