package events

import (
	"fmt"
	"github.com/youtube/vitess/go/event/syslogger"
	"log/syslog"
)

// Syslog writes the event to syslog.
func (kc *KeyspaceChange) Syslog(w *syslog.Writer) {
	w.Info(fmt.Sprintf("%s [keyspace] %s", kc.KeyspaceInfo.KeyspaceName(), kc.Status))
}

var _ syslogger.Syslogger = (*KeyspaceChange)(nil) // compile-time interface check
