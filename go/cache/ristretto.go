package cache

import (
	"vitess.io/vitess/go/cache/ristretto"
)

var _ Cache = &ristretto.Cache{}

// NewRistrettoCache returns a Cache implementation based on Ristretto
func NewRistrettoCache(maxCost, averageItemSize int64, cost func(interface{}) int64) *ristretto.Cache {
	config := ristretto.Config{
		NumCounters: (maxCost / averageItemSize) * 10,
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
