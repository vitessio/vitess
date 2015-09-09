package vtgate

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	pbq "github.com/youtube/vitess/go/vt/proto/query"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// NewHealthCheck creates a new HealthCheck object.
func NewHealthCheck(connTimeout time.Duration, retryDelay time.Duration) *HealthCheck {
	hc := &HealthCheck{
		addrToConns: make(map[string]*healthCheckConn),
		targetToEPs: make(map[string]map[string][]*pbt.EndPoint),
		connTimeout: connTimeout,
		retryDelay:  retryDelay,
	}
	return hc
}

// HealthCheck performs health checking and notifies downstream components about any changes.
type HealthCheck struct {
	mu          sync.RWMutex                          // mu protects the two maps, not the parameters.
	addrToConns map[string]*healthCheckConn           // addrToConns maps from address to the healthCheckConn object.
	targetToEPs map[string]map[string][]*pbt.EndPoint // targetToEPs maps from keyspace/shard to a list of endpoints.
	connTimeout time.Duration
	retryDelay  time.Duration
}

// healthCheckConn contains details about an endpoint.
type healthCheckConn struct {
	mu         sync.RWMutex
	target     *pbq.Target
	cancelFunc context.CancelFunc
	conn       tabletconn.TabletConn
	stats      *pbq.RealtimeStats
}

// checkConn performs health checking on the given endpoint.
func (hc *HealthCheck) checkConn(endPoint *pbt.EndPoint) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	hcc := &healthCheckConn{
		cancelFunc: cancelFunc,
	}

	// retry health check if it fails
	for {
		stream, errfunc, err := hcc.connect(ctx, hc, endPoint)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			log.Errorf("cannot connect to %+v: %v", endPoint, err)
			time.Sleep(hc.retryDelay)
			continue
		}
		for {
			err = hcc.processResponse(ctx, hc, endPoint, stream, errfunc)
			if err != nil {
				hcc.mu.Lock()
				hcc.conn.Close()
				hcc.conn = nil
				hcc.mu.Unlock()
				select {
				case <-ctx.Done():
					return
				default:
				}
				log.Errorf("error when streaming tablet health from %+v: %v", endPoint, err)
				time.Sleep(hc.retryDelay)
				break
			}
		}
	}
}

// func to connect to endpoint and start streaming.
// it makes sure we have a chance to signal the first connection attempt regardless whether it succeeds.
func (hcc *healthCheckConn) connect(ctx context.Context, hc *HealthCheck, endPoint *pbt.EndPoint) (<-chan *pbq.StreamHealthResponse, tabletconn.ErrFunc, error) {
	conn, err := tabletconn.GetDialer()(ctx, endPoint, "" /*keyspace*/, "" /*shard*/, pbt.TabletType_RDONLY, hc.connTimeout)
	if err != nil {
		return nil, nil, err
	}
	stream, errfunc, err := conn.StreamHealth(ctx)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	err = hcc.processResponse(ctx, hc, endPoint, stream, errfunc)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	hcc.mu.Lock()
	hcc.conn = conn
	hcc.mu.Unlock()
	return stream, errfunc, nil
}

// func to read one health check response, and notify downstream component
func (hcc *healthCheckConn) processResponse(ctx context.Context, hc *HealthCheck, endPoint *pbt.EndPoint, stream <-chan *pbq.StreamHealthResponse, errfunc tabletconn.ErrFunc) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case shr, ok := <-stream:
		if !ok {
			return errfunc()
		}
		if shr.Target == nil || shr.RealtimeStats == nil || shr.RealtimeStats.HealthError != "" {
			return fmt.Errorf("health stats is not valid: %v", shr)
		}

		if hcc.target == nil {
			// The first time we see response for the endpoint.
			hcc.mu.Lock()
			hcc.target = shr.Target
			hcc.stats = shr.RealtimeStats
			hcc.mu.Unlock()
			hc.mu.Lock()
			key := hc.endPointToMapKey(endPoint)
			hc.addrToConns[key] = hcc
			hc.addEndPointToTargetProtected(hcc.target.Keyspace, hcc.target.Shard, endPoint)
			// TODO: notify downstream component for endpoint going up
			hc.mu.Unlock()
		} else {
			hcc.mu.Lock()
			hcc.target = shr.Target
			hcc.stats = shr.RealtimeStats
			hcc.mu.Unlock()
		}
		// TODO: notify downstream for tablettype and realtimestats
		return nil
	}
}

// AddEndPoint adds the endpoint, and starts health check.
// It notifies downstram components for endpoint going up after the first health check succeeds.
func (hc *HealthCheck) AddEndPoint(endPoint *pbt.EndPoint) {
	go hc.checkConn(endPoint)
}

// RemoveEndPoint removes the endpoint, and cancels the health check.
// It also notifies downstream components for endpoint going down.
func (hc *HealthCheck) RemoveEndPoint(endPoint *pbt.EndPoint) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	key := hc.endPointToMapKey(endPoint)
	hcc, ok := hc.addrToConns[key]
	if !ok {
		return
	}
	hcc.cancelFunc()
	delete(hc.addrToConns, key)
	if hcc.target != nil {
		hc.deleteEndPointFromTargetProtected(hcc.target.Keyspace, hcc.target.Shard, endPoint)
	}
	// TODO: notify downstream component for endpoint going down
}

// GetEndPointFromTarget returns all endpoints for the given keyspace/shard.
func (hc *HealthCheck) GetEndPointFromTarget(keyspace, shard string) []*pbt.EndPoint {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	shardMap, ok := hc.targetToEPs[keyspace]
	if !ok {
		return nil
	}
	epList, ok := shardMap[shard]
	if !ok {
		return nil
	}
	res := make([]*pbt.EndPoint, 0, 1)
	return append(res, epList...)
}

// addEndPointToTargetProtected adds the endpoint to the given target.
// LOCK_REQUIRED hc.mu
func (hc *HealthCheck) addEndPointToTargetProtected(keyspace, shard string, endPoint *pbt.EndPoint) {
	shardMap, ok := hc.targetToEPs[keyspace]
	if !ok {
		shardMap = make(map[string][]*pbt.EndPoint)
		hc.targetToEPs[keyspace] = shardMap
	}
	epList, ok := shardMap[shard]
	if !ok {
		epList = make([]*pbt.EndPoint, 0, 1)
	}
	for _, ep := range epList {
		if topo.EndPointEquality(ep, endPoint) {
			log.Warningf("endpoint is already up: %+v", endPoint)
			return
		}
	}
	shardMap[shard] = append(epList, endPoint)
}

// deleteEndPointFromTargetProtected deletes the endpoint for the given target.
// LOCK_REQUIRED hc.mu
func (hc *HealthCheck) deleteEndPointFromTargetProtected(keyspace, shard string, endPoint *pbt.EndPoint) {
	shardMap, ok := hc.targetToEPs[keyspace]
	if !ok {
		return
	}
	epList, ok := shardMap[shard]
	if !ok {
		return
	}
	for i, ep := range epList {
		if topo.EndPointEquality(ep, endPoint) {
			epList = append(epList[:i], epList[i+1:]...)
			shardMap[shard] = epList
			return
		}
	}
}

// endPointToMapKey creates a key to the map from endpoint's host and ports.
func (hc *HealthCheck) endPointToMapKey(endPoint *pbt.EndPoint) string {
	parts := make([]string, 0, 1)
	for name, port := range endPoint.PortMap {
		parts = append(parts, name+":"+string(port))
	}
	sort.Strings(parts)
	parts = append([]string{endPoint.Host}, parts...)
	return strings.Join(parts, ":")
}
