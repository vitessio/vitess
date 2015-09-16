package discovery

import (
	"fmt"
	"sync"
	"testing"
	"time"

	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test/faketopo"
	"golang.org/x/net/context"
)

func TestEndPointWatcher(t *testing.T) {
	ft := newFakeTopo()
	fhc := newFakeHealthCheck()
	t.Logf(`ft = FakeTopo(); fhc = FakeHealthCheck()`)
	epw := NewEndPointWatcher(topo.Server{Impl: ft}, fhc, []string{"aa"}, 10*time.Millisecond)
	t.Logf(`epw = EndPointWatcher(topo.Server{ft}, fhc, ["aa"], 10ms)`)

	ft.AddTablet("aa", 0, "host1", map[string]int32{"vt": 123})
	time.Sleep(100 * time.Millisecond)
	t.Logf(`ft.AddTablet("aa", 0, "host1", {"vt": 123}); Sleep(100ms)`)
	want := &pbt.EndPoint{
		Uid:     0,
		Host:    "host1",
		PortMap: map[string]int32{"vt": 123},
	}
	key := endPointToMapKey(want)
	if ep, ok := fhc.endPoints[key]; !ok || len(fhc.endPoints) != 1 {
		t.Errorf("fhc.endPoints[key] = %+v; want %+v", ep, want)
	}

	ft.AddTablet("aa", 0, "host1", map[string]int32{"vt": 456})
	time.Sleep(100 * time.Millisecond)
	t.Logf(`ft.AddTablet("aa", 0, "host1", {"vt": 456}); Sleep(100ms)`)
	want = &pbt.EndPoint{
		Uid:     0,
		Host:    "host1",
		PortMap: map[string]int32{"vt": 456},
	}
	key = endPointToMapKey(want)
	if ep, ok := fhc.endPoints[key]; !ok || len(fhc.endPoints) != 1 {
		t.Errorf("fhc.endPoints[key] = %+v; want %+v", ep, want)
	}

	epw.Stop()
}

func newFakeHealthCheck() *fakeHealthCheck {
	return &fakeHealthCheck{endPoints: make(map[string]*pbt.EndPoint)}
}

type fakeHealthCheck struct {
	endPoints map[string]*pbt.EndPoint
}

// AddEndPoint adds the endpoint, and starts health check.
func (fhc *fakeHealthCheck) AddEndPoint(cell string, endPoint *pbt.EndPoint) {
	key := endPointToMapKey(endPoint)
	fhc.endPoints[key] = endPoint
	fmt.Println("AddEndPoint: ", endPoint)
}

// RemoveEndPoint removes the endpoint, and stops the health check.
func (fhc *fakeHealthCheck) RemoveEndPoint(endPoint *pbt.EndPoint) {
	key := endPointToMapKey(endPoint)
	delete(fhc.endPoints, key)
}

// GetEndPointStatsFromKeyspaceShard returns all EndPointStats for the given keyspace/shard.
func (fhc *fakeHealthCheck) GetEndPointStatsFromKeyspaceShard(keyspace, shard string) []*EndPointStats {
	return nil
}

// GetEndPointStatsFromTarget returns all EndPointStats for the given target.
func (fhc *fakeHealthCheck) GetEndPointStatsFromTarget(keyspace, shard string, tabletType pbt.TabletType) []*EndPointStats {
	return nil
}

func newFakeTopo() *fakeTopo {
	return &fakeTopo{tablets: make(map[pbt.TabletAlias]*pbt.Tablet)}
}

type fakeTopo struct {
	faketopo.FakeTopo
	mu      sync.RWMutex
	tablets map[pbt.TabletAlias]*pbt.Tablet
}

func (ft *fakeTopo) AddTablet(cell string, uid uint32, host string, ports map[string]int32) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ta := pbt.TabletAlias{
		Cell: cell,
		Uid:  uid,
	}
	tablet := &pbt.Tablet{
		Alias:    &ta,
		Hostname: host,
		PortMap:  make(map[string]int32),
	}
	for name, port := range ports {
		tablet.PortMap[name] = port
	}
	ft.tablets[ta] = tablet
}

func (ft *fakeTopo) RemoveTablet(cell string, uid uint32) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ta := pbt.TabletAlias{
		Cell: cell,
		Uid:  uid,
	}
	delete(ft.tablets, ta)
}

func (ft *fakeTopo) GetTabletsByCell(ctx context.Context, cell string) ([]*pbt.TabletAlias, error) {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	res := make([]*pbt.TabletAlias, 0, 1)
	for alias, tablet := range ft.tablets {
		if tablet.Alias.Cell == cell {
			res = append(res, &alias)
		}
	}
	return res, nil
}

func (ft *fakeTopo) GetTablet(ctx context.Context, alias *pbt.TabletAlias) (*pbt.Tablet, int64, error) {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	return ft.tablets[*alias], 0, nil
}
