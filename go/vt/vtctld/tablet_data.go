package vtctld

import (
	"flag"
	"io"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/tabletconn"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file maintains a tablet health cache. It establishes streaming
// connections with tablets, and updates its internal state with the
// result.

var (
	tabletHealthKeepAlive = flag.Duration("tablet_health_keep_alive", 5*time.Minute, "close streaming tablet health connection if there are no requests for this long")
)

type tabletHealth struct {
	mu sync.Mutex

	// result stores the most recent response.
	result *querypb.StreamHealthResponse
	// accessed stores the time of the most recent access.
	accessed time.Time

	// err stores the result of the stream attempt.
	err error
	// done is closed when the stream attempt ends.
	done chan struct{}
	// ready is closed when there is at least one result to read.
	ready chan struct{}
}

func newTabletHealth() *tabletHealth {
	return &tabletHealth{
		accessed: time.Now(),
		ready:    make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (th *tabletHealth) lastResult(ctx context.Context) (*querypb.StreamHealthResponse, error) {
	// Wait until at least the first result comes in, or the stream ends.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-th.ready:
	case <-th.done:
	}

	th.mu.Lock()
	defer th.mu.Unlock()

	th.accessed = time.Now()
	return th.result, th.err
}

func (th *tabletHealth) lastAccessed() time.Time {
	th.mu.Lock()
	defer th.mu.Unlock()

	return th.accessed
}

func (th *tabletHealth) stream(ctx context.Context, ts topo.Server, tabletAlias *topodatapb.TabletAlias) (err error) {
	defer func() {
		th.mu.Lock()
		th.err = err
		th.mu.Unlock()
		close(th.done)
	}()

	ti, err := ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}

	conn, err := tabletconn.GetDialer()(ti.Tablet, 30*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	first := true
	return conn.StreamHealth(ctx, func(shr *querypb.StreamHealthResponse) error {
		th.mu.Lock()
		th.result = shr
		th.mu.Unlock()

		if first {
			// We got the first result, so we're ready to be accessed.
			close(th.ready)
			first = false
		}
		if time.Since(th.lastAccessed()) >= *tabletHealthKeepAlive {
			return io.EOF
		}
		return nil
	})
}

type tabletHealthCache struct {
	ts topo.Server

	mu        sync.Mutex
	tabletMap map[topodatapb.TabletAlias]*tabletHealth
}

func newTabletHealthCache(ts topo.Server) *tabletHealthCache {
	return &tabletHealthCache{
		ts:        ts,
		tabletMap: make(map[topodatapb.TabletAlias]*tabletHealth),
	}
}

func (thc *tabletHealthCache) Get(ctx context.Context, tabletAlias *topodatapb.TabletAlias) (*querypb.StreamHealthResponse, error) {
	thc.mu.Lock()

	th, ok := thc.tabletMap[*tabletAlias]
	if !ok {
		// No existing stream, so start one.
		th = newTabletHealth()
		thc.tabletMap[*tabletAlias] = th

		go func() {
			log.Infof("starting health stream for tablet %v", tabletAlias)
			err := th.stream(context.Background(), thc.ts, tabletAlias)
			log.Infof("tablet %v health stream ended, error: %v", tabletAlias, err)
			thc.delete(tabletAlias)
		}()
	}

	thc.mu.Unlock()

	return th.lastResult(ctx)
}

func (thc *tabletHealthCache) delete(tabletAlias *topodatapb.TabletAlias) {
	thc.mu.Lock()
	delete(thc.tabletMap, *tabletAlias)
	thc.mu.Unlock()
}
