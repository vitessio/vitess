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
}

// New creates a new cache with the given backfill func. When a request is
// enqueued (via EnqueueBackfill), fillFunc will be called with that request.
func New[Key Keyer, Value any](fillFunc func(ctx context.Context, req Key) (Value, error)) *Cache[Key, Value] {
	c := &Cache[Key, Value]{
		cache:     cache.New(0, 0),                     // TODO: read timeouts from config
		fillcache: cache.New(0, 0),                     // TODO: also read from config (different values)
		backfills: make(chan *backfillRequest[Key], 0), // TODO: config buffer size
		fillFunc:  fillFunc,
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())

	c.wg.Add(1)
	go c.backfill() // TODO: consider allowing N backfill threads to run, configurable

	return c
}

// Add adds a (key, value) to the cache directly, following the semantics of
// (github.com/patrickmn/go-cache).Cache.Add.
func (c *Cache[Key, Value]) Add(key string, val Value, d time.Duration) error {
	// Record the time we last cached this key, to check against
	c.fillcache.Set(key, struct{}{}, cache.DefaultExpiration)
	// Then cache the actual value.
	return c.cache.Add(key, val, d)
}

// Get returns the Value stored for the key, if present in the cache. If the key
// is not cached, the zero value for the given type is returned, along with a
// boolean to indicated presence/absence.
func (c *Cache[Key, Value]) Get(key string) (Value, bool) {
	v, exp, ok := c.cache.GetWithExpiration(key)
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

	ctx, cancel := context.WithTimeout(c.ctx, 0 /* TODO: use config */)
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

		if req.requestedAt.Before(time.Now() /* TODO: time.Now() - backfill_request_expiration */) {
			// We took too long to get to this request, per config options.
			// TODO: log
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
		if err := c.Add(key, val, cache.DefaultExpiration); err != nil {
			log.Warningf("failed to add (%s, %+v) to cache: %s", key, val, err)
		}
	}
}
