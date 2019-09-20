/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
