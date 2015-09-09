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

// AddrUpdate updates about an endpoint and whether it is up.
type AddrUpdate struct {
	EndPoint *pbt.EndPoint
	IsUp     bool
}

// NewHealthCheck creates a new HealthCheck object.
func NewHealthCheck(endPoints []*pbt.EndPoint, addrUpdate <-chan *AddrUpdate, connTimeout time.Duration, retryDelay time.Duration) *HealthCheck {
	hc := &HealthCheck{
		addrToConns: make(map[string]*healthCheckConn),
		targetToEPs: make(map[string]map[string]map[pbt.TabletType][]*pbt.EndPoint),
		connTimeout: connTimeout,
		retryDelay:  retryDelay,
	}
	hc.initConns(endPoints)
	go hc.onAddrUpdate(addrUpdate)
	return hc
}

// HealthCheck performs health checking and notifies downstream components about any changes.
type HealthCheck struct {
	mu          sync.RWMutex
	addrToConns map[string]*healthCheckConn                              // addrToConns maps from address to the healthCheckConn object.
	targetToEPs map[string]map[string]map[pbt.TabletType][]*pbt.EndPoint // targetToEPs maps from keyspace/shard/tablettype to a list of endpoints.
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

// initConns starts health checking for the initial set of endpoints.
// It waits till all endpoints are tried to connect once.
func (hc *HealthCheck) initConns(endPoints []*pbt.EndPoint) {
	var wg sync.WaitGroup
	for _, endPoint := range endPoints {
		wg.Add(1)
		go hc.checkConn(endPoint, &wg)
	}
	wg.Wait()
}

// checkConn performs health checking on the given endpoint.
func (hc *HealthCheck) checkConn(endPoint *pbt.EndPoint, wg *sync.WaitGroup) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	hcc := &healthCheckConn{
		cancelFunc: cancelFunc,
	}
	addr := hc.endPointToString(endPoint)

	defer func() {
		// Clean up the healthCheckConn from maps, and notify addrwatcher
		hc.mu.Lock()
		// TODO: notify downstream component for endpoint going down
		if hcc.target != nil {
			hc.deleteEndPointFromTarget(hcc.target, endPoint)
			delete(hc.addrToConns, addr)
		}
		hc.mu.Unlock()
	}()

	var conn tabletconn.TabletConn
	var stream <-chan *pbq.StreamHealthResponse
	var errfunc tabletconn.ErrFunc
	var err error

	// func to read one health check response, and notify downstream component
	processResponse := func() error {
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
				// Add the healthCheckConn to maps, and notify downstream component for endpoint going up.
				hc.mu.Lock()
				hc.addrToConns[addr] = hcc
				hc.addEndPointToTarget(shr.Target, endPoint)
				// TODO: notify downstream component for endpoint going up
				hc.mu.Unlock()
			} else if hcc.target.TabletType != shr.Target.TabletType {
				// Update targetToConn when tablettype changes.
				hc.mu.Lock()
				hc.deleteEndPointFromTarget(hcc.target, endPoint)
				hc.addEndPointToTarget(shr.Target, endPoint)
				hc.mu.Unlock()
			}
			hcc.mu.Lock()
			hcc.target = shr.Target
			hcc.stats = shr.RealtimeStats
			// TODO: notify downstream for tablettype and realtimestats
			hcc.mu.Unlock()
			return nil
		}
	}

	// func to connect to endpoint and start streaming.
	// it makes sure we have a chance to signal the first connection attempt regardless whether it succeeds.
	connectFunc := func() error {
		defer func() {
			// signal that connection attempt is done for the first time.
			if wg != nil {
				wg.Done()
				wg = nil
			}
		}()

		conn, err = tabletconn.GetDialer()(ctx, endPoint, "" /*keyspace*/, "" /*shard*/, pbt.TabletType_RDONLY, hc.connTimeout)
		if err != nil {
			return err
		}
		stream, errfunc, err = conn.StreamHealth(ctx)
		if err != nil {
			conn.Close()
			return err
		}
		err = processResponse()
		if err != nil {
			conn.Close()
			return err
		}
		hcc.mu.Lock()
		hcc.conn = conn
		hcc.mu.Unlock()
		return nil
	}

	// main loop to retry health check if it fails
	for {
		err = connectFunc()
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
			err = processResponse()
			if err != nil {
				conn.Close()
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

// onAddrUpdate handles endpoint up/down events.
func (hc *HealthCheck) onAddrUpdate(updates <-chan *AddrUpdate) {
	for update := range updates {
		if update.IsUp {
			go hc.checkConn(update.EndPoint, nil)
			continue
		}
		addr := hc.endPointToString(update.EndPoint)
		hc.mu.RLock()
		hcc := hc.addrToConns[addr]
		hc.mu.RUnlock()
		hcc.cancelFunc()
	}
}

// endPointToString builds addr string from host and ports.
func (hc *HealthCheck) endPointToString(endPoint *pbt.EndPoint) string {
	parts := make([]string, 0, 1)
	for name, port := range endPoint.PortMap {
		parts = append(parts, name+":"+string(port))
	}
	sort.Strings(parts)
	parts = append([]string{endPoint.Host}, parts...)
	return strings.Join(parts, ":")
}

// addEndPointToTarget adds the endpoint to the given target.
func (hc *HealthCheck) addEndPointToTarget(target *pbq.Target, endPoint *pbt.EndPoint) {
	shardMap, ok := hc.targetToEPs[target.Keyspace]
	if !ok {
		shardMap = make(map[string]map[pbt.TabletType][]*pbt.EndPoint)
		hc.targetToEPs[target.Keyspace] = shardMap
	}
	ttMap, ok := shardMap[target.Shard]
	if !ok {
		ttMap = make(map[pbt.TabletType][]*pbt.EndPoint)
		shardMap[target.Shard] = ttMap
	}
	epList, ok := ttMap[target.TabletType]
	if !ok {
		epList = make([]*pbt.EndPoint, 0, 1)
		ttMap[target.TabletType] = epList
	}
	for _, ep := range epList {
		if topo.EndPointEquality(ep, endPoint) {
			log.Warningf("endpoint is already up: %+v", endPoint)
			return
		}
	}
	ttMap[target.TabletType] = append(epList, endPoint)
}

// getEndPointFromTarget returns all endpoints for the given keyspace/shard.
func (hc *HealthCheck) getEndPointFromTarget(keyspace, shard string) []*pbt.EndPoint {
	shardMap, ok := hc.targetToEPs[keyspace]
	if !ok {
		return nil
	}
	ttMap, ok := shardMap[shard]
	if !ok {
		return nil
	}
	res := make([]*pbt.EndPoint, 0, 1)
	for _, epList := range ttMap {
		res = append(res, epList...)
	}
	return res
}

// deleteEndPointFromTarget deletes the endpoint for the given target.
func (hc *HealthCheck) deleteEndPointFromTarget(target *pbq.Target, endPoint *pbt.EndPoint) {
	shardMap, ok := hc.targetToEPs[target.Keyspace]
	if !ok {
		return
	}
	ttMap, ok := shardMap[target.Shard]
	if !ok {
		return
	}
	epList, ok := ttMap[target.TabletType]
	if !ok {
		return
	}
	for i, ep := range epList {
		if topo.EndPointEquality(ep, endPoint) {
			epList = append(epList[:i], epList[i+1:]...)
			ttMap[target.TabletType] = epList
			return
		}
	}
}
