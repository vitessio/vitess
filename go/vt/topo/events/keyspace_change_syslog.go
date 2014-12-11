package events

import (
	"fmt"
	"log/syslog"

	"github.com/henryanand/vitess/go/event/syslogger"
)

// Syslog writes the event to syslog.
func (kc *KeyspaceChange) Syslog() (syslog.Priority, string) {
	return syslog.LOG_INFO, fmt.Sprintf("%s [keyspace] %s",
		kc.KeyspaceInfo.KeyspaceName(), kc.Status)
}

var _ syslogger.Syslogger = (*KeyspaceChange)(nil) // compile-time interface check
