package events

import (
	"fmt"
	"log/syslog"

	"github.com/henryanand/vitess/go/event/syslogger"
)

// Syslog writes the event to syslog.
func (tc *TabletChange) Syslog() (syslog.Priority, string) {
	return syslog.LOG_INFO, fmt.Sprintf("%s/%s/%s [tablet] %s",
		tc.Tablet.Keyspace, tc.Tablet.Shard, tc.Tablet.Alias, tc.Status)
}

var _ syslogger.Syslogger = (*TabletChange)(nil) // compile-time interface check
