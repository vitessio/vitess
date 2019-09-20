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

	"vitess.io/vitess/go/event/syslogger"
)

// Syslog writes a SplitClone event to syslog.
func (ev *SplitClone) Syslog() (syslog.Priority, string) {
	return syslog.LOG_INFO, fmt.Sprintf("%s/%s/%s [split clone] %s",
		ev.Keyspace, ev.Shard, ev.Cell, ev.Status)
}

// Syslog writes a VerticalSplitClone event to syslog.
func (ev *VerticalSplitClone) Syslog() (syslog.Priority, string) {
	return syslog.LOG_INFO, fmt.Sprintf("%s/%s/%s [vertical split clone] %s",
		ev.Keyspace, ev.Shard, ev.Cell, ev.Status)
}

var _ syslogger.Syslogger = (*SplitClone)(nil)         // compile-time interface check
var _ syslogger.Syslogger = (*VerticalSplitClone)(nil) // compile-time interface check
