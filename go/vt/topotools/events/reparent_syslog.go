package events

import (
	"fmt"
	"log/syslog"

	"vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/event/syslogger"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// Syslog writes a Reparent event to syslog.
func (r *Reparent) Syslog() (syslog.Priority, string) {
	var oldAlias *topodata.TabletAlias
	var newAlias *topodata.TabletAlias
	if r.OldPrimary != nil {
		oldAlias = r.OldPrimary.Alias
	}
	if r.NewPrimary != nil {
		newAlias = r.NewPrimary.Alias
	}

	return syslog.LOG_INFO, fmt.Sprintf("%s/%s [reparent %v -> %v] %s (%s)",
		r.ShardInfo.Keyspace(), r.ShardInfo.ShardName(),
		topoproto.TabletAliasString(oldAlias),
		topoproto.TabletAliasString(newAlias),
		r.Status, r.ExternalID)
}

var _ syslogger.Syslogger = (*Reparent)(nil) // compile-time interface check
