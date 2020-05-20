package discovery

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/sync2"

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

// tabletHealthCheck maintains the health status of a tablet. A map of this
// structure is maintained in HealthCheck.
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

// processResponse reads one health check response, and updates health
func (th *tabletHealthCheck) processResponse(hc *HealthCheck, shr *query.StreamHealthResponse) error {
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
	currentTarget := th.Target
	// check whether this is a trivial update so as to update healthy map
	trivialNonMasterUpdate := th.LastError == nil && th.Serving && shr.RealtimeStats.HealthError == "" && shr.Serving &&
		currentTarget.TabletType != topodata.TabletType_MASTER && currentTarget.TabletType == shr.Target.TabletType
	isMasterUpdate := shr.Target.TabletType == topodata.TabletType_MASTER
	isMasterChange := th.Target.TabletType != topodata.TabletType_MASTER && shr.Target.TabletType == topodata.TabletType_MASTER
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
	th.mu.Unlock()
	// notify downstream for master change
	hc.updateHealth(th, shr, currentTarget, trivialNonMasterUpdate, isMasterUpdate, isMasterChange)
	return nil
}

// checkConn performs health checking on the given tablet.
func (th *tabletHealthCheck) checkConn(hc *HealthCheck) {
	defer hc.connsWG.Done()
	defer th.finalizeConn()

	retryDelay := hc.retryDelay
	for {
		streamCtx, streamCancel := context.WithCancel(th.ctx)

		// Setup a watcher that restarts the timer every time an update is received.
		// If a timeout occurs for a serving tablet, we make it non-serving and send
		// a status update. The stream is also terminated so it can be retried.
		// servingStatus feeds into the serving var, which keeps track of the serving
		// status transmitted by the tablet.
		servingStatus := make(chan bool, 1)
		// timedout is accessed atomically because there could be a race
		// between the goroutine that sets it and the check for its value
		// later.
		timedout := sync2.NewAtomicBool(false)
		go func() {
			for {
				select {
				case <-servingStatus:
					continue
				case <-time.After(hc.healthCheckTimeout):
					timedout.Set(true)
					streamCancel()
					return
				case <-streamCtx.Done():
					// If the stream is done, stop watching.
					return
				}
			}
		}()

		// Read stream health responses.
		err := th.stream(streamCtx, func(shr *query.StreamHealthResponse) error {
			// We received a message. Reset the back-off.
			retryDelay = hc.retryDelay
			// Don't block on send to avoid deadlocks.
			select {
			case servingStatus <- shr.Serving:
			default:
			}
			return th.processResponse(hc, shr)
		})

		// streamCancel to make sure the watcher goroutine terminates.
		streamCancel()

		if err != nil {
			if strings.Contains(err.Error(), "health stats mismatch") {
				hc.deleteTablet(th.Tablet)
				return
			}
			res := th.SimpleCopy()
			hc.broadcast(res)
		}
		// If there was a timeout send an error. We do this after stream has returned.
		// This will ensure that this update prevails over any previous message that
		// stream could have sent.
		if timedout.Get() {
			th.mu.Lock()
			th.LastError = fmt.Errorf("healthcheck timed out (latest %v)", th.lastResponseTimestamp)
			th.setServingState(false, th.LastError.Error())
			hcErrorCounters.Add([]string{th.Target.Keyspace, th.Target.Shard, topoproto.TabletTypeLString(th.Target.TabletType)}, 1)
			res := th.simpleCopyLocked()
			th.mu.Unlock()
			hc.broadcast(res)
		}

		// Streaming RPC failed e.g. because vttablet was restarted or took too long.
		// Sleep until the next retry is up or the context is done/canceled.
		select {
		case <-th.ctx.Done():
			return
		case <-time.After(retryDelay):
			// Exponentially back-off to prevent tight-loop.
			retryDelay *= 2
			// Limit the retry delay backoff to the health check timeout
			if retryDelay > hc.healthCheckTimeout {
				retryDelay = hc.healthCheckTimeout
			}
		}
	}
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

// finalizeConn closes the health checking connection.
// To be called only on exit from checkConn().
func (th *tabletHealthCheck) finalizeConn() {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.setServingState(false, "finalizeConn closing connection")
	// Note: checkConn() exits only when th.ctx.Done() is closed. Thus it's
	// safe to simply get Err() value here and assign to LastError.
	th.LastError = th.ctx.Err()
	if th.Conn != nil {
		// Don't use th.ctx because it's already closed.
		// Use a separate context, and add a timeout to prevent unbounded waits.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = th.Conn.Close(ctx)
		th.Conn = nil
	}
}
