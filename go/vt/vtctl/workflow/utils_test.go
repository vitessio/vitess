package workflow

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
)

// TestUpdateKeyspaceRoutingRule confirms that the keyspace routing rules are updated correctly.
func TestUpdateKeyspaceRoutingRule(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()
	routes := make(map[string]string)
	for _, tabletType := range tabletTypeSuffixes {
		routes["from"+tabletType] = "to"
	}
	err := updateKeyspaceRoutingRule(ctx, ts, "ks", "test", routes)
	require.NoError(t, err)
	rules, err := topotools.GetKeyspaceRoutingRules(ctx, ts)
	require.NoError(t, err)
	require.EqualValues(t, routes, rules)
}

// TestConcurrentKeyspaceRoutingRulesUpdates runs multiple keyspace routing rules updates concurrently to test
// the locking mechanism.
func TestConcurrentKeyspaceRoutingRulesUpdates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	concurrency := 100
	duration := 3 * time.Second

	var wg sync.WaitGroup
	wg.Add(concurrency)

	stop := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					update(t, ts, id)
				}
			}
		}(i)
	}
	<-time.After(duration)
	close(stop)
	wg.Wait()
}

func update(t *testing.T, ts *topo.Server, id int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := fmt.Sprintf("%d_%d", id, rand.Intn(math.MaxInt))
	routes := make(map[string]string)
	for _, tabletType := range tabletTypeSuffixes {
		from := fmt.Sprintf("from%s%s", s, tabletType)
		routes[from] = s + tabletType
	}
	err := updateKeyspaceRoutingRule(ctx, ts, "ks", "test", routes)
	require.NoError(t, err)
	got, err := topotools.GetKeyspaceRoutingRules(ctx, ts)
	require.NoError(t, err)
	for _, tabletType := range tabletTypeSuffixes {
		from := fmt.Sprintf("from%s%s", s, tabletType)
		require.Equal(t, s+tabletType, got[from])
	}
}
