package cache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cache/ristretto"
)

type dummy uint32

func (dummy) CachedSize(bool) int64 {
	return 1
}

func TestNewDefaultCacheImpl(t *testing.T) {
	assertNullCache := func(t *testing.T, cache Cache[dummy]) {
		_, ok := cache.(*nullCache[dummy])
		require.True(t, ok)
	}

	assertLFUCache := func(t *testing.T, cache Cache[dummy]) {
		_, ok := cache.(*ristretto.Cache[dummy])
		require.True(t, ok)
	}

	assertLRUCache := func(t *testing.T, cache Cache[dummy]) {
		_, ok := cache.(*LRUCache[dummy])
		require.True(t, ok)
	}

	tests := []struct {
		cfg    *Config
		verify func(t *testing.T, cache Cache[dummy])
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
			cache := NewDefaultCacheImpl[dummy](tt.cfg)
			tt.verify(t, cache)
		})
	}
}
