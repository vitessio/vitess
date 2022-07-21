package cache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cache/ristretto"
)

func TestNewDefaultCacheImpl(t *testing.T) {
	assertNullCache := func(t *testing.T, cache Cache) {
		_, ok := cache.(*nullCache)
		require.True(t, ok)
	}

	assertLFUCache := func(t *testing.T, cache Cache) {
		_, ok := cache.(*ristretto.Cache)
		require.True(t, ok)
	}

	assertLRUCache := func(t *testing.T, cache Cache) {
		_, ok := cache.(*LRUCache)
		require.True(t, ok)
	}

	tests := []struct {
		cfg    *Config
		verify func(t *testing.T, cache Cache)
	}{
		{&Config{MaxEntries: 0, MaxMemoryUsage: 0, LFU: false}, assertNullCache},
		{&Config{MaxEntries: 0, MaxMemoryUsage: 0, LFU: true}, assertNullCache},
		{&Config{MaxEntries: 100, MaxMemoryUsage: 0, LFU: false}, assertLRUCache},
		{&Config{MaxEntries: 0, MaxMemoryUsage: 1000, LFU: false}, assertNullCache},
		{&Config{MaxEntries: 100, MaxMemoryUsage: 1000, LFU: false}, assertLRUCache},
		{&Config{MaxEntries: 100, MaxMemoryUsage: 0, LFU: true}, assertNullCache},
		{&Config{MaxEntries: 100, MaxMemoryUsage: 1000, LFU: true}, assertLFUCache},
		{&Config{MaxEntries: 0, MaxMemoryUsage: 1000, LFU: true}, assertNullCache},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d.%d.%v", tt.cfg.MaxEntries, tt.cfg.MaxMemoryUsage, tt.cfg.LFU), func(t *testing.T) {
			cache := NewDefaultCacheImpl(tt.cfg)
			tt.verify(t, cache)
		})
	}
}
