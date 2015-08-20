package events

import (
	"fmt"
	"log/syslog"

	"github.com/youtube/vitess/go/event/syslogger"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
)

// Syslog writes the event to syslog.
func (tc *TabletChange) Syslog() (syslog.Priority, string) {
	return syslog.LOG_INFO, fmt.Sprintf("%s/%s/%s [tablet] %s",
		tc.Tablet.Keyspace, tc.Tablet.Shard, topoproto.TabletAliasString(tc.Tablet.Alias), tc.Status)
}

var _ syslogger.Syslogger = (*TabletChange)(nil) // compile-time interface check
