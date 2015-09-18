package discovery

import pbt "github.com/youtube/vitess/go/vt/proto/topodata"

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

// CacheStatus returns a displayable version of the cache.
func (fhc *fakeHealthCheck) CacheStatus() EndPointsCacheStatusList {
	return nil
}
