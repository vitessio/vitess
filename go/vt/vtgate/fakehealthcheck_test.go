package vtgate

import (
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func newFakeHealthCheck() *fakeHealthCheck {
	return &fakeHealthCheck{items: make(map[string]*fhcItem)}
}

type fhcItem struct {
	eps  *discovery.EndPointStats
	conn tabletconn.TabletConn
}

// fakeHealthCheck implements discovery.HealthCheck.
type fakeHealthCheck struct {
	items map[string]*fhcItem

	// GetStatsFromTargetCounter counts GetEndpointStatsFromTarget() being called.
	GetStatsFromTargetCounter int
	// GetStatsFromKeyspaceShardCounter counts GetEndPointStatsFromKeyspaceShard() being called.
	GetStatsFromKeyspaceShardCounter int
}

// Reset cleans up the internal state.
func (fhc *fakeHealthCheck) Reset() {
	fhc.GetStatsFromTargetCounter = 0
	fhc.GetStatsFromKeyspaceShardCounter = 0
	fhc.items = make(map[string]*fhcItem)
}

// SetListener sets the listener for healthcheck updates.
func (fhc *fakeHealthCheck) SetListener(listener discovery.HealthCheckStatsListener) {
}

// AddEndPoint adds the endpoint.
func (fhc *fakeHealthCheck) AddEndPoint(cell, name string, endPoint *topodatapb.EndPoint) {
	key := discovery.EndPointToMapKey(endPoint)
	item := &fhcItem{
		eps: &discovery.EndPointStats{
			EndPoint: endPoint,
			Cell:     cell,
			Name:     name,
		},
	}
	fhc.items[key] = item
}

// RemoveEndPoint removes the endpoint.
func (fhc *fakeHealthCheck) RemoveEndPoint(endPoint *topodatapb.EndPoint) {
	key := discovery.EndPointToMapKey(endPoint)
	delete(fhc.items, key)
}

// GetEndPointStatsFromKeyspaceShard returns all EndPointStats for the given keyspace/shard.
func (fhc *fakeHealthCheck) GetEndPointStatsFromKeyspaceShard(keyspace, shard string) []*discovery.EndPointStats {
	fhc.GetStatsFromKeyspaceShardCounter++
	var res []*discovery.EndPointStats
	for _, item := range fhc.items {
		if item.eps.Target == nil {
			continue
		}
		if item.eps.Target.Keyspace == keyspace && item.eps.Target.Shard == shard {
			res = append(res, item.eps)
		}
	}
	return res
}

// GetEndPointStatsFromTarget returns all EndPointStats for the given target.
func (fhc *fakeHealthCheck) GetEndPointStatsFromTarget(keyspace, shard string, tabletType topodatapb.TabletType) []*discovery.EndPointStats {
	fhc.GetStatsFromTargetCounter++
	var res []*discovery.EndPointStats
	for _, item := range fhc.items {
		if item.eps.Target == nil {
			continue
		}
		if item.eps.Target.Keyspace == keyspace && item.eps.Target.Shard == shard && item.eps.Target.TabletType == tabletType {
			res = append(res, item.eps)
		}
	}
	return res
}

// GetConnection returns the TabletConn of the given endpoint.
func (fhc *fakeHealthCheck) GetConnection(endPoint *topodatapb.EndPoint) tabletconn.TabletConn {
	key := discovery.EndPointToMapKey(endPoint)
	if item := fhc.items[key]; item != nil {
		return item.conn
	}
	return nil
}

// CacheStatus returns a displayable version of the cache.
func (fhc *fakeHealthCheck) CacheStatus() discovery.EndPointsCacheStatusList {
	return nil
}

// Close stops the healthcheck.
func (fhc *fakeHealthCheck) Close() error {
	return nil
}

// addTestEndPoint inserts a fake entry into fakeHealthCheck.
func (fhc *fakeHealthCheck) addTestEndPoint(cell, host string, port int32, keyspace, shard string, tabletType topodatapb.TabletType, serving bool, reparentTS int64, err error, conn tabletconn.TabletConn) *topodatapb.EndPoint {
	if conn != nil {
		conn.SetTarget(keyspace, shard, tabletType)
	}
	ep := topo.NewEndPoint(0, host)
	ep.PortMap["vt"] = port
	key := discovery.EndPointToMapKey(ep)
	item := fhc.items[key]
	if item == nil {
		fhc.AddEndPoint(cell, "", ep)
		item = fhc.items[key]
	}
	item.eps.Target = &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: tabletType}
	item.eps.Serving = serving
	item.eps.TabletExternallyReparentedTimestamp = reparentTS
	item.eps.Stats = &querypb.RealtimeStats{}
	item.eps.LastError = err
	item.conn = conn
	return ep
}
