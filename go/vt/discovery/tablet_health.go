package discovery

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
)

// tabletHealthCheck maintains the health status of a tablet. A map of this
// structure is maintained in HealthCheckImpl.
type tabletHealthCheck struct {
	ctx context.Context
	// cancelFunc must be called before discarding tabletHealthCheck.
	// This will ensure that the associated checkConn goroutine will terminate.
	cancelFunc context.CancelFunc
	// Tablet is the tablet object that was sent to HealthCheck.AddTablet.
	Tablet *topodata.Tablet
	mu     sync.Mutex
	// Conn is the connection associated with the tablet.
	Conn queryservice.QueryService
	// Target is the current target as returned by the streaming
	// StreamHealth RPC.
	Target *query.Target
	// Serving describes if the tablet can be serving traffic.
	Serving bool
	// MasterTermStartTime is the last time at which
	// this tablet was either elected the master, or received
	// a TabletExternallyReparented event. It is set to 0 if the
	// tablet doesn't think it's a master.
	MasterTermStartTime int64
	// Stats is the current health status, as received by the
	// StreamHealth RPC (replication lag, ...).
	Stats *query.RealtimeStats
	// LastError is the error we last saw when trying to get the
	// tablet's healthcheck.
	LastError error
	// possibly delete both these
	loggedServingState    bool
	lastResponseTimestamp time.Time // timestamp of the last healthcheck response
}

// TabletHealth represents simple tablet health data that is returned to users of healthcheck.
// No synchronization is required because we always return a copy.
type TabletHealth struct {
	Conn                queryservice.QueryService
	Tablet              *topodata.Tablet
	Target              *query.Target
	Stats               *query.RealtimeStats
	MasterTermStartTime int64
	LastError           error
	Serving             bool
}

// DeepEqual compares two TabletHealth. Since we include protos, we
// need to use proto.Equal on these.
func (th *TabletHealth) DeepEqual(other *TabletHealth) bool {
	return proto.Equal(th.Tablet, other.Tablet) &&
		proto.Equal(th.Target, other.Target) &&
		th.Serving == other.Serving &&
		th.MasterTermStartTime == other.MasterTermStartTime &&
		proto.Equal(th.Stats, other.Stats) &&
		((th.LastError == nil && other.LastError == nil) ||
			(th.LastError != nil && other.LastError != nil && th.LastError.Error() == other.LastError.Error()))
}

// GetTabletHostPort formats a tablet host port address.
func (th *TabletHealth) GetTabletHostPort() string {
	hostname := th.Tablet.Hostname
	vtPort := th.Tablet.PortMap["vt"]
	return netutil.JoinHostPort(hostname, vtPort)
}

// GetHostNameLevel returns the specified hostname level. If the level does not exist it will pick the closest level.
// This seems unused but can be utilized by certain url formatting templates. See getTabletDebugURL for more details.
func (th *TabletHealth) GetHostNameLevel(level int) string {
	hostname := th.Tablet.Hostname
	chunkedHostname := strings.Split(hostname, ".")

	if level < 0 {
		return chunkedHostname[0]
	} else if level >= len(chunkedHostname) {
		return chunkedHostname[len(chunkedHostname)-1]
	} else {
		return chunkedHostname[level]
	}
}

// getTabletDebugURL formats a debug url to the tablet.
// It uses a format string that can be passed into the app to format
// the debug URL to accommodate different network setups. It applies
// the html/template string defined to a tabletHealthCheck object. The
// format string can refer to members and functions of tabletHealthCheck
// like a regular html/template string.
//
// For instance given a tablet with hostname:port of host.dc.domain:22
// could be configured as follows:
// http://{{.GetTabletHostPort}} -> http://host.dc.domain:22
// https://{{.Tablet.Hostname}} -> https://host.dc.domain
// https://{{.GetHostNameLevel 0}}.bastion.corp -> https://host.bastion.corp
func (th *TabletHealth) getTabletDebugURL() string {
	var buffer bytes.Buffer
	tabletURLTemplate.Execute(&buffer, th)
	return buffer.String()
}

// String is defined because we want to print a []*tabletHealthCheck array nicely.
func (th *tabletHealthCheck) String() string {
	th.mu.Lock()
	defer th.mu.Unlock()
	return fmt.Sprintf("tabletHealthCheck{Tablet: %v,Target: %v,Serving: %v, MasterTermStartTime: %v, Stats: %v, LastError: %v",
		th.Tablet, th.Target, th.Serving, th.MasterTermStartTime, *th.Stats, th.LastError)
}

// SimpleCopy returns a TabletHealth with all the necessary fields copied from tabletHealthCheck.
// Note that this is not a deep copy because we point to the same underlying RealtimeStats.
// That is fine because the RealtimeStats object is never changed after creation.
func (th *tabletHealthCheck) SimpleCopy() *TabletHealth {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.simpleCopyLocked()
}

func (th *tabletHealthCheck) simpleCopyLocked() *TabletHealth {
	return &TabletHealth{
		Conn:                th.Conn,
		Tablet:              th.Tablet,
		Target:              th.Target,
		Stats:               th.Stats,
		LastError:           th.LastError,
		MasterTermStartTime: th.MasterTermStartTime,
		Serving:             th.Serving,
	}
}

// DeepEqual compares two tabletHealthCheck. Since we include protos, we
// need to use proto.Equal on these.
func (th *tabletHealthCheck) DeepEqual(other *tabletHealthCheck) bool {
	return proto.Equal(th.Tablet, other.Tablet) &&
		proto.Equal(th.Target, other.Target) &&
		th.Serving == other.Serving &&
		th.MasterTermStartTime == other.MasterTermStartTime &&
		proto.Equal(th.Stats, other.Stats) &&
		((th.LastError == nil && other.LastError == nil) ||
			(th.LastError != nil && other.LastError != nil && th.LastError.Error() == other.LastError.Error()))
}

func (th *tabletHealthCheck) deleteConnLocked() {
	th.mu.Lock()
	th.Conn = nil
	th.mu.Unlock()
	th.cancelFunc()
}

func (th *tabletHealthCheck) isHealthy() bool {
	return th.Serving && th.LastError == nil && th.Stats != nil && !IsReplicationLagVeryHigh(th)
}

// setServingState sets the tablet state to the given value.
//
// If the state changes, it logs the change so that failures
// from the health check connection are logged the first time,
// but don't continue to log if the connection stays down.
//
// th.mu must be locked before calling this function
func (th *tabletHealthCheck) setServingState(serving bool, reason string) {
	if !th.loggedServingState || (serving != th.Serving) {
		// Emit the log from a separate goroutine to avoid holding
		// the th lock while logging is happening
		go log.Infof("HealthCheckUpdate(Serving State): tablet: %v serving => %v for %v/%v (%v) reason: %s",
			topotools.TabletIdent(th.Tablet),
			serving,
			th.Tablet.GetKeyspace(),
			th.Tablet.GetShard(),
			th.Target.GetTabletType(),
			reason,
		)
		th.loggedServingState = true
	}
	th.Serving = serving
}

// stream streams healthcheck responses to callback.
func (th *tabletHealthCheck) stream(ctx context.Context, callback func(*query.StreamHealthResponse) error) error {
	conn := th.getConnection()
	if conn == nil {
		// This signals the caller to retry
		return nil
	}
	err := conn.StreamHealth(ctx, callback)
	if err != nil {
		// Depending on the specific error the caller can take action
		th.closeConnection(ctx, err)
	}
	return err
}

func (th *tabletHealthCheck) getConnection() queryservice.QueryService {
	th.mu.Lock()
	defer th.mu.Unlock()
	if th.Conn == nil {
		conn, err := tabletconn.GetDialer()(th.Tablet, grpcclient.FailFast(true))
		if err != nil {
			th.LastError = err
			return nil
		}
		th.Conn = conn
		th.LastError = nil
	}
	return th.Conn
}

func (th *tabletHealthCheck) closeConnection(ctx context.Context, err error) {
	th.mu.Lock()
	defer th.mu.Unlock()
	log.Warningf("tablet %v healthcheck stream error: %v", th.Tablet.Alias, err)
	th.setServingState(false, err.Error())
	th.LastError = err
	_ = th.Conn.Close(ctx)
	th.Conn = nil
}

// processResponse reads one health check response, and updates health
func (th *tabletHealthCheck) processResponse(hc *HealthCheckImpl, shr *query.StreamHealthResponse) error {
	select {
	case <-th.ctx.Done():
		return th.ctx.Err()
	default:
	}

	// Check for invalid data, better than panicking.
	if shr.Target == nil || shr.RealtimeStats == nil {
		return fmt.Errorf("health stats is not valid: %v", shr)
	}

	// an app-level error from tablet, force serving state.
	var healthErr error
	serving := shr.Serving
	if shr.RealtimeStats.HealthError != "" {
		healthErr = fmt.Errorf("vttablet error: %v", shr.RealtimeStats.HealthError)
		serving = false
	}

	if shr.TabletAlias != nil && !proto.Equal(shr.TabletAlias, th.Tablet.Alias) {
		// TabletAlias change means that the host:port has been taken over by another tablet
		// We could cancel / exit the healthcheck for this tablet right away
		// However, we defer it until the next topo refresh informs us of the change because that is
		// the only way to discover the new host/port
		return vterrors.New(vtrpc.Code_FAILED_PRECONDITION, fmt.Sprintf("health stats mismatch, tablet %+v alias does not match response alias %v", th.Tablet, shr.TabletAlias))
	}

	th.mu.Lock()
	currentTablet := th.Tablet
	// check whether this is a trivial update so as to update healthy map
	trivialNonMasterUpdate := th.LastError == nil && th.Serving && shr.RealtimeStats.HealthError == "" && shr.Serving &&
		currentTablet.Type != topodata.TabletType_MASTER && currentTablet.Type == shr.Target.TabletType
	isMasterUpdate := currentTablet.Type == topodata.TabletType_MASTER && shr.Target.TabletType == topodata.TabletType_MASTER
	th.mu.Unlock()

	// hc.healthByAlias is authoritative, it should be updated
	hc.mu.Lock()
	tabletAlias := topoproto.TabletAliasString(th.Tablet.Alias)
	// this will only change the first time, but it's easiest to set it always rather than check and set
	hc.healthByAlias[tabletAlias] = th
	hc.mu.Unlock()

	hcErrorCounters.Add([]string{shr.Target.Keyspace, shr.Target.Shard, topoproto.TabletTypeLString(shr.Target.TabletType)}, 0)
	if currentTablet.Type != shr.Target.TabletType || currentTablet.Keyspace != shr.Target.Keyspace || currentTablet.Shard != shr.Target.Shard {
		// keyspace and shard are not expected to change, but just in case ...
		// hc still has this tabletHealthCheck in the wrong target (because tabletType changed)
		oldTargetKey := hc.keyFromTablet(currentTablet)
		newTargetKey := hc.keyFromTarget(shr.Target)
		tabletAlias := topoproto.TabletAliasString(currentTablet.Alias)
		hc.mu.Lock()
		delete(hc.healthData[oldTargetKey], tabletAlias)
		_, ok := hc.healthData[newTargetKey]
		if !ok {
			hc.healthData[newTargetKey] = make(map[string]*tabletHealthCheck)
		}
		hc.healthData[newTargetKey][tabletAlias] = th
		hc.mu.Unlock()
	}

	// Update our record
	th.mu.Lock()
	defer th.mu.Unlock()
	th.lastResponseTimestamp = time.Now()
	th.Target = shr.Target
	th.MasterTermStartTime = shr.TabletExternallyReparentedTimestamp
	th.Stats = shr.RealtimeStats
	th.LastError = healthErr
	reason := "healthCheck update"
	if healthErr != nil {
		reason = "healthCheck update error: " + healthErr.Error()
	}
	th.setServingState(serving, reason)

	targetKey := hc.keyFromTarget(shr.Target)
	if !trivialNonMasterUpdate {
		all := hc.healthData[targetKey]
		allArray := make([]*tabletHealthCheck, 0, len(all))
		for _, s := range all {
			allArray = append(allArray, s)
		}
		hc.healthy[targetKey] = FilterStatsByReplicationLag(allArray)
	}
	if isMasterUpdate {
		if len(hc.healthy[targetKey]) == 0 {
			hc.healthy[targetKey] = append(hc.healthy[targetKey], th)
		} else {
			// We already have one up server, see if we
			// need to replace it.
			if th.MasterTermStartTime < hc.healthy[targetKey][0].MasterTermStartTime {
				log.Warningf("not marking healthy master %s as Up for %s because its MasterTermStartTime is smaller than the highest known timestamp from previous MASTERs %s: %d < %d ",
					topoproto.TabletAliasString(currentTablet.Alias),
					topoproto.KeyspaceShardString(currentTablet.Keyspace, currentTablet.Shard),
					topoproto.TabletAliasString(hc.healthy[targetKey][0].Tablet.Alias),
					th.MasterTermStartTime,
					hc.healthy[targetKey][0].MasterTermStartTime)
			} else {
				// Just replace it.
				hc.healthy[targetKey][0] = th
			}
		}
	}

	result := th.simpleCopyLocked()
	hc.broadcast(result)
	// and notify downstream for master change
	if shr.Target.TabletType == topodata.TabletType_MASTER && hc.masterCallback != nil {
		hc.masterCallback(result)
	}

	return nil
}
