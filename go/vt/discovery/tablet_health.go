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
type TabletHealth struct {
	// cancelFunc must be called before discarding TabletHealth.
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
}

// String is defined because we want to print a []*TabletHealth array nicely.
func (th *TabletHealth) String() string {
	th.mu.Lock()
	defer th.mu.Unlock()
	return fmt.Sprintf("TabletHealth{Tablet: %v,Target: %v,Serving: %v, MasterTermStartTime: %v, Stats: %v, LastError: %v",
		th.Tablet, th.Target, th.Serving, th.MasterTermStartTime, *th.Stats, th.LastError)
}

// Copy returns a copy of TabletHealth. Note that this is not really a deep copy
// because we point to the same underlying RealtimeStats.
// That is fine because the RealtimeStats object is never changed after creation.
func (th *TabletHealth) Copy() *TabletHealth {
	th.mu.Lock()
	defer th.mu.Unlock()
	// we have to explicitly create a new object rather than relying on assignment to make a copy for us
	// the following doesn't work for synchronized objects
	// t := *th
	// return &t
	return &TabletHealth{
		Conn:                th.Conn,
		Tablet:              th.Tablet,
		Target:              th.Target,
		Serving:             th.Serving,
		MasterTermStartTime: th.MasterTermStartTime,
		Stats:               th.Stats,
		LastError:           th.LastError,
	}
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
	th.mu.Lock()
	hostname := th.Tablet.Hostname
	vtPort := th.Tablet.PortMap["vt"]
	th.mu.Unlock()
	return netutil.JoinHostPort(hostname, vtPort)
}

// GetHostNameLevel returns the specified hostname level. If the level does not exist it will pick the closest level.
// This seems unused but can be utilized by certain url formatting templates. See getTabletDebugURL for more details.
func (th *TabletHealth) GetHostNameLevel(level int) string {
	th.mu.Lock()
	hostname := th.Tablet.Hostname
	th.mu.Unlock()
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
// the html/template string defined to a TabletHealth object. The
// format string can refer to members and functions of TabletHealth
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

func (th *TabletHealth) deleteConnLocked() {
	th.mu.Lock()
	th.Conn = nil
	th.mu.Unlock()
	th.cancelFunc()
}

func (th *TabletHealth) isHealthy() bool {
	return th.Serving && th.LastError == nil && th.Stats != nil && !IsReplicationLagVeryHigh(th)
}
