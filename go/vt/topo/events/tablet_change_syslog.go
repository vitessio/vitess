package events

import (
	"fmt"
	"github.com/youtube/vitess/go/event/syslogger"
	"log/syslog"
)

// Syslog writes the event to syslog.
func (tc *TabletChange) Syslog(w *syslog.Writer) {
	w.Info(fmt.Sprintf("%s/%s/%s [tablet] %s",
		tc.Tablet.Keyspace, tc.Tablet.Shard, tc.Tablet.Alias, tc.Status))
}

var _ syslogger.Syslogger = (*TabletChange)(nil) // compile-time interface check
