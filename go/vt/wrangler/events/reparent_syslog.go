package events

import (
	"fmt"
	"log/syslog"

	"github.com/youtube/vitess/go/event/syslogger"
)

// Syslog writes a Reparent event to syslog.
func (r *Reparent) Syslog() (syslog.Priority, string) {
	return syslog.LOG_INFO, fmt.Sprintf("%s/%s [reparent %v -> %v] %s",
		r.ShardInfo.Keyspace(), r.ShardInfo.ShardName(),
		r.OldMaster.Alias, r.NewMaster.Alias, r.Status)
}

var _ syslogger.Syslogger = (*Reparent)(nil) // compile-time interface check
