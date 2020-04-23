package discovery

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
)

// tabletStats is returned when getting the set of tablets.
type tabletStats struct {
	// Key uniquely identifies that serving tablet. It is computed
	// from the Tablet's record Hostname and PortMap. If a tablet
	// is restarted on different ports, its Key will be different.
	// Key is computed using the TabletToMapKey method below.
	// key can be used in GetConnection().
	Key string
	// Tablet is the tablet object that was sent to HealthCheck.AddTablet.
	Tablet *topodata.Tablet
	// Name is an optional tag (e.g. alternative address) for the
	// tablet.  It is supposed to represent the tablet as a task,
	// not as a process.  For instance, it can be a
	// cell+keyspace+shard+tabletType+taskIndex value.
	Name string
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

// String is defined because we want to print a []*tabletStats array nicely.
func (e *tabletStats) String() string {
	return fmt.Sprint(*e)
}

// DeepEqual compares two tabletStats. Since we include protos, we
// need to use proto.Equal on these.
func (e *tabletStats) DeepEqual(f *tabletStats) bool {
	return e.Key == f.Key &&
		proto.Equal(e.Tablet, f.Tablet) &&
		e.Name == f.Name &&
		proto.Equal(e.Target, f.Target) &&
		e.Up == f.Up &&
		e.Serving == f.Serving &&
		e.MasterTermStartTime == f.MasterTermStartTime &&
		proto.Equal(e.Stats, f.Stats) &&
		((e.LastError == nil && f.LastError == nil) ||
			(e.LastError != nil && f.LastError != nil && e.LastError.Error() == f.LastError.Error()))
}

// Copy produces a copy of tabletStats.
func (e *tabletStats) Copy() *tabletStats {
	ts := *e
	return &ts
}

// GetTabletHostPort formats a tablet host port address.
func (e tabletStats) GetTabletHostPort() string {
	vtPort := e.Tablet.PortMap["vt"]
	return netutil.JoinHostPort(e.Tablet.Hostname, vtPort)
}

// GetHostNameLevel returns the specified hostname level. If the level does not exist it will pick the closest level.
// This seems unused but can be utilized by certain url formatting templates. See getTabletDebugURL for more details.
func (e tabletStats) GetHostNameLevel(level int) string {
	chunkedHostname := strings.Split(e.Tablet.Hostname, ".")

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
// the html/template string defined to a tabletStats object. The
// format string can refer to members and functions of tabletStats
// like a regular html/template string.
//
// For instance given a tablet with hostname:port of host.dc.domain:22
// could be configured as follows:
// http://{{.GetTabletHostPort}} -> http://host.dc.domain:22
// https://{{.Tablet.Hostname}} -> https://host.dc.domain
// https://{{.GetHostNameLevel 0}}.bastion.corp -> https://host.bastion.corp
func (e tabletStats) getTabletDebugURL() string {
	var buffer bytes.Buffer
	tabletURLTemplate.Execute(&buffer, e)
	return buffer.String()
}

// TrivialStatsUpdate returns true iff the old and new tabletStats
// haven't changed enough to warrant re-calling FilterLegacyStatsByReplicationLag.
func (e *tabletStats) TrivialStatsUpdate(n *tabletStats) bool {
	// Skip replag filter when replag remains in the low rep lag range,
	// which should be the case majority of the time.
	lowRepLag := lowReplicationLag.Seconds()
	oldRepLag := float64(e.Stats.SecondsBehindMaster)
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
