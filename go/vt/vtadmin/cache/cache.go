/*
Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package cache provides a generic key/value cache with support for background
// filling.
package cache

import (
	"context"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"

	"vitess.io/vitess/go/vt/log"
)

// Keyer is the interface cache keys implement to turn themselves into string
// keys.
//
// Note: we define this type rather than using Stringer so users may implement
// that interface for different string representation needs, for example
// providing a human-friendly representation.
type Keyer interface{ Key() string }

const (
	// DefaultExpiration is used to instruct calls to Add to use the default
	// cache expiration.
	// See https://pkg.go.dev/github.com/patrickmn/go-cache@v2.1.0+incompatible#pkg-constants.
	DefaultExpiration = cache.DefaultExpiration
	// NoExpiration is used to create caches that do not expire items by
	// default.
	// See https://pkg.go.dev/github.com/patrickmn/go-cache@v2.1.0+incompatible#pkg-constants.
	NoExpiration = cache.NoExpiration

	// DefaultBackfillEnqueueWaitTime is the default value used for waiting to
	// enqueue backfill requests, if a config is passed with a non-positive
	// BackfillEnqueueWaitTime.
	DefaultBackfillEnqueueWaitTime = time.Millisecond * 50
	// DefaultBackfillRequestTTL is the default value used for how stale of
	// backfill requests to still process, if a config is passed with a
	// non-positive BackfillRequestTTL.
	DefaultBackfillRequestTTL = time.Millisecond * 100
)

// Config is the configuration for a cache.
type Config struct {
	// DefaultExpiration is how long to keep Values in the cache by default (the
	// duration passed to Add takes precedence). Use the sentinel NoExpiration
	// to make Values never expire by default.
	DefaultExpiration time.Duration `json:"default_expiration"`
	// CleanupInterval is how often to remove expired Values from the cache.
	CleanupInterval time.Duration `json:"cleanup_interval"`

	// BackfillRequestTTL is how long a backfill request is considered valid.
	// If the backfill goroutine encounters a request older than this, it is
	// discarded.
	BackfillRequestTTL time.Duration `json:"backfill_request_ttl"`
	// BackfillRequestDuplicateInterval is how much time must pass before the
	// backfill goroutine will re-backfill the same key. It is used to prevent
	// multiple callers queuing up too many requests for the same key, when one
	// backfill would satisfy all of them.
	BackfillRequestDuplicateInterval time.Duration `json:"backfill_request_duplicate_interval"`
	// BackfillQueueSize is how many outstanding backfill requests to permit.
	// If the queue is full, calls to EnqueueBackfill will return false and
	// those requests will be discarded.
	BackfillQueueSize int `json:"backfill_queue_size"`
	// BackfillEnqueueWaitTime is how long to wait when attempting to enqueue a
	// backfill request before giving up.
	BackfillEnqueueWaitTime time.Duration `json:"backfill_enqueue_wait_time"`
}

// Cache is a generic cache supporting background fills. To add things to the
// cache, call Add. To enqueue a background fill, call EnqueueBackfill with a
// Keyer implementation, which will be passed to the fill func provided to New.
//
// For example, to create a schema cache that can backfill full payloads (including
// size aggregation):
//
//	var c *cache.Cache[BackfillSchemaRequest, *vtadminpb.Schema]
//	c := cache.New(func(ctx context.Context, req BackfillSchemaRequest) (*vtadminpb.Schema, error) {
//		// Fetch schema based on fields in `req`.
//		// If err is nil, the backfilled schema will be added to the cache.
//		return cluster.fetchSchema(ctx, req)
//	})
type Cache[Key Keyer, Value any] struct {
	cache *cache.Cache

	m        sync.Mutex
	lastFill map[string]time.Time

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	backfills chan *backfillRequest[Key]

	fillFunc func(ctx context.Context, k Key) (Value, error)

	cfg Config
}

// New creates a new cache with the given backfill func. When a request is
// enqueued (via EnqueueBackfill), fillFunc will be called with that request.
func New[Key Keyer, Value any](fillFunc func(ctx context.Context, req Key) (Value, error), cfg Config) *Cache[Key, Value] {
	if cfg.BackfillEnqueueWaitTime <= 0 {
		log.Warningf("BackfillEnqueueWaitTime (%v) must be positive, defaulting to %v", cfg.BackfillEnqueueWaitTime, DefaultBackfillEnqueueWaitTime)
		cfg.BackfillEnqueueWaitTime = DefaultBackfillEnqueueWaitTime
	}

	if cfg.BackfillRequestTTL <= 0 {
		log.Warningf("BackfillRequestTTL (%v) must be positive, defaulting to %v", cfg.BackfillRequestTTL, DefaultBackfillRequestTTL)
		cfg.BackfillRequestTTL = DefaultBackfillRequestTTL
	}

	c := &Cache[Key, Value]{
		cache:     cache.New(cfg.DefaultExpiration, cfg.CleanupInterval),
		lastFill:  map[string]time.Time{},
		backfills: make(chan *backfillRequest[Key], cfg.BackfillQueueSize),
		fillFunc:  fillFunc,
		cfg:       cfg,
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())

	c.wg.Add(1)
	go c.backfill() // TODO: consider allowing N backfill threads to run, configurable

	return c
}

// Add adds a (key, value) to the cache directly, following the semantics of
// (github.com/patrickmn/go-cache).Cache.Add.
func (c *Cache[Key, Value]) Add(key Key, val Value, d time.Duration) error {
	return c.add(key.Key(), val, d)
}

func (c *Cache[Key, Value]) add(key string, val Value, d time.Duration) error {
	c.m.Lock()
	// Record the time we last cached this key, to check against
	c.lastFill[key] = time.Now().UTC()
	c.m.Unlock()

	// Then cache the actual value.
	return c.cache.Add(key, val, d)
}

// Get returns the Value stored for the key, if present in the cache. If the key
// is not cached, the zero value for the given type is returned, along with a
// boolean to indicated presence/absence.
func (c *Cache[Key, Value]) Get(key Key) (Value, bool) {
	v, exp, ok := c.cache.GetWithExpiration(key.Key())
	if !ok || (!exp.IsZero() && exp.Before(time.Now())) {
		var zero Value
		return zero, false
	}

	return v.(Value), ok
}

// EnqueueBackfill submits a request to the backfill queue.
func (c *Cache[Key, Value]) EnqueueBackfill(k Key) bool {
	req := &backfillRequest[Key]{
		k:           k,
		requestedAt: time.Now().UTC(),
	}

	select {
	case c.backfills <- req:
		return true
	case <-time.After(c.cfg.BackfillEnqueueWaitTime):
		return false
	}
}

// Close closes the backfill goroutine, effectively rendering this cache
// unusable for further background fills.
func (c *Cache[Key, Value]) Close() error {
	c.cancel()
	c.wg.Wait()
	return nil
}

type backfillRequest[Key Keyer] struct {
	k           Key
	requestedAt time.Time
}

func (c *Cache[Key, Value]) backfill() {
	defer c.wg.Done()

	for {
		var req *backfillRequest[Key]
		select {
		case <-c.ctx.Done():
			return
		case req = <-c.backfills:
		}

		if req.requestedAt.Add(c.cfg.BackfillRequestTTL).Before(time.Now()) {
			// We took too long to get to this request, per config options.
			log.Warningf("backfill for %s requested at %s; discarding due to exceeding TTL (%s)", req.k.Key(), req.requestedAt, c.cfg.BackfillRequestTTL)
			continue
		}

		key := req.k.Key()

		c.m.Lock()
		if t, ok := c.lastFill[key]; ok {
			if !t.IsZero() && t.Add(c.cfg.BackfillRequestDuplicateInterval).After(time.Now()) {
				// We recently added a value for this key to the cache, either via
				// another backfill request, or directly via a call to Add.
				log.Infof("filled cache for %s less than %s ago (at %s)", key, c.cfg.BackfillRequestDuplicateInterval, t.UTC())
				c.m.Unlock()
				continue
			}

			// NOTE: In the strictest sense, we would `delete(lastFill, key)`
			// here. However, we're about to fill that key again, so we'd be
			// immediately re-adding the key we just deleted from `lastFill`.
			//
			// We do *not* apply the same treatment to the actual Value cache,
			// because go-cache is running a background thread to periodically
			// clean up expired entries.
		}
		c.m.Unlock()

		val, err := c.fillFunc(c.ctx, req.k)
		if err != nil {
			log.Errorf("backfill failed for key %s: %s", key, err)
			// TODO: consider re-requesting with a retry-counter paired with a config to give up after N attempts
			continue
		}

		// Finally, store the value.
		if err := c.add(key, val, cache.DefaultExpiration); err != nil {
			log.Warningf("failed to add (%s, %+v) to cache: %s", key, val, err)
		}
	}
}

// Debug implements debug.Debuggable for Cache.
func (c *Cache[Key, Value]) Debug() map[string]any {
	return map[string]any{
		"size":                  c.cache.ItemCount(), // NOTE: this may include expired items that have not been evicted yet.
		"config":                c.cfg,
		"backfill_queue_length": len(c.backfills),
		"closed":                c.ctx.Err() != nil,
	}
}
