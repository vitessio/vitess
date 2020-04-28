package discovery

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
)

// TabletHealth maintains the health status of a tablet. A map of this
// structure is maintained in HealthCheckImpl.
type tabletHealth struct {
	mu sync.Mutex
	// cancelFunc must be called before discarding TabletHealth.
	// This will ensure that the associated checkConn goroutine will terminate.
	cancelFunc context.CancelFunc
	// conn is the connection associated with the tablet.
	conn queryservice.QueryService
	// Tablet is the tablet object that was sent to HealthCheck.AddTablet.
	Tablet *topodata.Tablet
	// Target is the current target as returned by the streaming
	// StreamHealth RPC.
	Target *query.Target
	// Up describes whether the tablet is added or removed.
	Up bool
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
}

// String is defined because we want to print a []*tabletHealth array nicely.
func (th *tabletHealth) String() string {
	return fmt.Sprintf("TabletHealth{Tablet: %v,Target: %v,Up: %v,Serving: %v, MasterTermStartTime: %v, Stats: %v, LastError: %v",
		th.Tablet, th.Target, th.Up, th.Serving, th.MasterTermStartTime, *th.Stats, th.LastError)
}

// DeepEqual compares two tabletHealth. Since we include protos, we
// need to use proto.Equal on these.
func (th *tabletHealth) DeepEqual(other *tabletHealth) bool {
	return proto.Equal(th.Tablet, other.Tablet) &&
		proto.Equal(th.Target, other.Target) &&
		th.Up == other.Up &&
		th.Serving == other.Serving &&
		th.MasterTermStartTime == other.MasterTermStartTime &&
		proto.Equal(th.Stats, other.Stats) &&
		((th.LastError == nil && other.LastError == nil) ||
			(th.LastError != nil && other.LastError != nil && th.LastError.Error() == other.LastError.Error()))
}

// GetTabletHostPort formats a tablet host port address.
func (th *tabletHealth) GetTabletHostPort() string {
	vtPort := th.Tablet.PortMap["vt"]
	return netutil.JoinHostPort(th.Tablet.Hostname, vtPort)
}

// GetHostNameLevel returns the specified hostname level. If the level does not exist it will pick the closest level.
// This seems unused but can be utilized by certain url formatting templates. See getTabletDebugURL for more details.
func (th *tabletHealth) GetHostNameLevel(level int) string {
	chunkedHostname := strings.Split(th.Tablet.Hostname, ".")

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
// the html/template string defined to a tabletHealth object. The
// format string can refer to members and functions of tabletHealth
// like a regular html/template string.
//
// For instance given a tablet with hostname:port of host.dc.domain:22
// could be configured as follows:
// http://{{.GetTabletHostPort}} -> http://host.dc.domain:22
// https://{{.Tablet.Hostname}} -> https://host.dc.domain
// https://{{.GetHostNameLevel 0}}.bastion.corp -> https://host.bastion.corp
func (th *tabletHealth) getTabletDebugURL() string {
	var buffer bytes.Buffer
	tabletURLTemplate.Execute(&buffer, th)
	return buffer.String()
}

// TrivialStatsUpdate returns true iff the old and new tabletHealth
// haven't changed enough to warrant re-calling FilterLegacyStatsByReplicationLag.
func (th *tabletHealth) TrivialStatsUpdate(n *tabletHealth) bool {
	// Skip replag filter when replag remains in the low rep lag range,
	// which should be the case majority of the time.
	lowRepLag := lowReplicationLag.Seconds()
	oldRepLag := float64(th.Stats.SecondsBehindMaster)
	newRepLag := float64(n.Stats.SecondsBehindMaster)
	if oldRepLag <= lowRepLag && newRepLag <= lowRepLag {
		return true
	}

	// Skip replag filter when replag remains in the high rep lag range,
	// and did not change beyond +/- 10%.
	// when there is a high rep lag, it takes a long time for it to reduce,
	// so it is not necessary to re-calculate every time.
	// In that case, we won't save the new record, so we still
	// remember the original replication lag.
	if oldRepLag > lowRepLag && newRepLag > lowRepLag && newRepLag < oldRepLag*1.1 && newRepLag > oldRepLag*0.9 {
		return true
	}

	return false
}

func (th *tabletHealth) deleteConnLocked() {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.Up = false
	th.conn = nil
	th.cancelFunc()
}

func (th *tabletHealth) isHealthy() bool {
	return th.Serving && th.LastError == nil && th.Stats != nil && !IsReplicationLagVeryHigh(th)
}
