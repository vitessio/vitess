package events

import (
	"fmt"
	"github.com/youtube/vitess/go/event/syslogger"
	"log/syslog"
)

// Syslog writes the event to syslog.
func (sc *ShardChange) Syslog(w *syslog.Writer) {
	w.Info(fmt.Sprintf("%s/%s [shard] %s",
		sc.ShardInfo.Keyspace(), sc.ShardInfo.ShardName(), sc.Status))
}

var _ syslogger.Syslogger = (*ShardChange)(nil) // compile-time interface check
