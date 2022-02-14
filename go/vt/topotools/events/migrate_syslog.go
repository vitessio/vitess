package events

import (
	"fmt"
	"log/syslog"
	"strings"

	"vitess.io/vitess/go/event/syslogger"
)

// Syslog writes a MigrateServedFrom event to syslog.
func (ev *MigrateServedFrom) Syslog() (syslog.Priority, string) {
	var format string
	if ev.Reverse {
		format = "%s [migrate served-from %s/%s <- %s/%s] %s"
	} else {
		format = "%s [migrate served-from %s/%s -> %s/%s] %s"
	}
	return syslog.LOG_INFO, fmt.Sprintf(format,
		ev.KeyspaceName,
		ev.SourceShard.Keyspace(), ev.SourceShard.ShardName(),
		ev.DestinationShard.Keyspace(), ev.DestinationShard.ShardName(),
		ev.Status)
}

var _ syslogger.Syslogger = (*MigrateServedFrom)(nil) // compile-time interface check

// Syslog writes a MigrateServedTypes event to syslog.
func (ev *MigrateServedTypes) Syslog() (syslog.Priority, string) {
	var format string
	if ev.Reverse {
		format = "%s [migrate served-types {%v} <- {%v}] %s"
	} else {
		format = "%s [migrate served-types {%v} -> {%v}] %s"
	}

	sourceShards := make([]string, len(ev.SourceShards))
	for i, shard := range ev.SourceShards {
		if shard == nil {
			continue
		}
		sourceShards[i] = shard.ShardName()
	}
	destShards := make([]string, len(ev.DestinationShards))
	for i, shard := range ev.DestinationShards {
		if shard == nil {
			continue
		}
		destShards[i] = shard.ShardName()
	}

	return syslog.LOG_INFO, fmt.Sprintf(format,
		ev.KeyspaceName, strings.Join(sourceShards, ", "),
		strings.Join(destShards, ", "), ev.Status)
}

var _ syslogger.Syslogger = (*MigrateServedTypes)(nil) // compile-time interface check
