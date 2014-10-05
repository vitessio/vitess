// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package events

import (
	"fmt"
	"log/syslog"

	"github.com/youtube/vitess/go/event/syslogger"
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
