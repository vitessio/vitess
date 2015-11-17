package discovery

import (
	"sync"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
)

func newFakeHealthCheck() *fakeHealthCheck {
	return &fakeHealthCheck{endPoints: make(map[string]*topodatapb.EndPoint)}
}

type fakeHealthCheck struct {
	mu        sync.RWMutex
	endPoints map[string]*topodatapb.EndPoint
}

// SetListener sets the listener for healthcheck updates.
func (fhc *fakeHealthCheck) SetListener(listener HealthCheckStatsListener) {
}

// AddEndPoint adds the endpoint, and starts health check.
func (fhc *fakeHealthCheck) AddEndPoint(cell, name string, endPoint *topodatapb.EndPoint) {
	fhc.mu.Lock()
	defer fhc.mu.Unlock()
	key := EndPointToMapKey(endPoint)
	fhc.endPoints[key] = endPoint
}

// RemoveEndPoint removes the endpoint, and stops the health check.
func (fhc *fakeHealthCheck) RemoveEndPoint(endPoint *topodatapb.EndPoint) {
	fhc.mu.Lock()
	defer fhc.mu.Unlock()
	key := EndPointToMapKey(endPoint)
	delete(fhc.endPoints, key)
}

// GetEndPointStatsFromKeyspaceShard returns all EndPointStats for the given keyspace/shard.
func (fhc *fakeHealthCheck) GetEndPointStatsFromKeyspaceShard(keyspace, shard string) []*EndPointStats {
	return nil
}

// GetEndPointStatsFromTarget returns all EndPointStats for the given target.
func (fhc *fakeHealthCheck) GetEndPointStatsFromTarget(keyspace, shard string, tabletType topodatapb.TabletType) []*EndPointStats {
	return nil
}

// GetConnection returns the TabletConn of the given endpoint.
func (fhc *fakeHealthCheck) GetConnection(endPoint *topodatapb.EndPoint) tabletconn.TabletConn {
	return nil
}

// CacheStatus returns a displayable version of the cache.
func (fhc *fakeHealthCheck) CacheStatus() EndPointsCacheStatusList {
	return nil
}

func (fhc *fakeHealthCheck) GetAllEndPoints() map[string]*topodatapb.EndPoint {
	res := make(map[string]*topodatapb.EndPoint)
	fhc.mu.RLock()
	defer fhc.mu.RUnlock()
	for key, ep := range fhc.endPoints {
		res[key] = ep
	}
	return res
}
