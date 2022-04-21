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
)

// Config is the configuration for a cache.
//
// TODO: provide a hook point for vtadmin cluster flags/config.
type Config struct {
	// DefaultExpiration is how long to keep Values in the cache by default (the
	// duration passed to Add takes precedence). Use the sentinel NoExpiration
	// to make Values never expire by default.
	DefaultExpiration time.Duration
	// CleanupInterval is how often to remove expired Values from the cache.
	CleanupInterval time.Duration

	// BackfillRequestTTL is how long a backfill request is considered valid.
	// If the backfill goroutine encounters a request older than this, it is
	// discarded.
	BackfillRequestTTL time.Duration
	// BackfillRequestDuplicateInterval is how much time must pass before the
	// backfill goroutine will re-backfill the same key. It is used to prevent
	// multiple callers queuing up too many requests for the same key, when one
	// backfill would satisfy all of them.
	BackfillRequestDuplicateInterval time.Duration
	// BackfillQueueSize is how many outstanding backfill requests to permit.
	// If the queue is full, calls to EnqueueBackfill will return false and
	// those requests will be discarded.
	BackfillQueueSize int
	// BackfillEnqueueWaitTime is how long to wait when attempting to enqueue a
	// backfill request before giving up.
	BackfillEnqueueWaitTime time.Duration
}

// Cache is a generic cache supporting background fills. To add things to the
// cache, call Add. To enqueue a background fill, call EnqueueBackfill with a
// Keyer implementation, which will be passed to the fill func provided to New.
//
// For example, to create a schema cache that can backfill full payloads (including
// size aggregation):
//
//		var c *cache.Cache[BackfillSchemaRequest, *vtadminpb.Schema]
//		c := cache.New(func(ctx context.Context, req BackfillSchemaRequest) (*vtadminpb.Schema, error) {
//			// Fetch schema based on fields in `req`.
//			// If err is nil, the backfilled schema will be added to the cache.
//			return cluster.fetchSchema(ctx, req)
//		})
//
type Cache[Key Keyer, Value any] struct {
	cache     *cache.Cache
	fillcache *cache.Cache

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
	c := &Cache[Key, Value]{
		cache:     cache.New(cfg.DefaultExpiration, cfg.CleanupInterval),
		fillcache: cache.New(cfg.BackfillRequestDuplicateInterval, time.Minute),
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
	// Record the time we last cached this key, to check against
	c.fillcache.Set(key, struct{}{}, cache.DefaultExpiration)
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

	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.BackfillEnqueueWaitTime)
	defer cancel()

	select {
	case c.backfills <- req:
		return true
	case <-ctx.Done():
		return false
	}
}

// Close closes the backfill goroutine, effectively rendering this cache
// unusable for further background fills.
func (c *Cache[Key, Value]) Close() {
	c.cancel()
	c.wg.Wait()
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
			select {
			case <-c.ctx.Done():
				return
			default:
			}
		}

		if req.requestedAt.Add(c.cfg.BackfillRequestTTL).Before(time.Now()) {
			// We took too long to get to this request, per config options.
			log.Warningf("backfill for %s requested at %s; discarding due to exceeding TTL (%s)", req.k.Key(), req.requestedAt, c.cfg.BackfillRequestTTL)
			continue
		}

		key := req.k.Key()
		if _, exp, ok := c.fillcache.GetWithExpiration(key); ok {
			if !exp.IsZero() && exp.Before(time.Now()) {
				// We recently added a value for this key to the cache, either via
				// another backfill request, or directly via a call to Add.
				log.Infof("filled cache for %s less than %s ago (at %s)", key, time.Duration(0) /* TODO: config */, exp.UTC())
				continue
			}
		}

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
