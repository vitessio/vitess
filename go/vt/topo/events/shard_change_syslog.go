package events

import (
	"fmt"
	"log/syslog"

	"github.com/youtube/vitess/go/event/syslogger"
)

// Syslog writes the event to syslog.
func (sc *ShardChange) Syslog() (syslog.Priority, string) {
	return syslog.LOG_INFO, fmt.Sprintf("%s/%s [shard] %s",
		sc.ShardInfo.Keyspace(), sc.ShardInfo.ShardName(), sc.Status)
}

var _ syslogger.Syslogger = (*ShardChange)(nil) // compile-time interface check
