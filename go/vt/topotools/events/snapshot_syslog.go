// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package events

import (
	"fmt"
	"log/syslog"

	"github.com/youtube/vitess/go/event/syslogger"
)

// Syslog writes a MultiSnapshot event to syslog.
func (ev *MultiSnapshot) Syslog() (syslog.Priority, string) {
	return syslog.LOG_INFO, fmt.Sprintf("%s/%s/%s [multisnapshot] %s",
		ev.Tablet.Keyspace, ev.Tablet.Shard, ev.Tablet.Alias, ev.Status)
}

var _ syslogger.Syslogger = (*MultiSnapshot)(nil) // compile-time interface check

// Syslog writes a MultiRestore event to syslog.
func (ev *MultiRestore) Syslog() (syslog.Priority, string) {
	return syslog.LOG_INFO, fmt.Sprintf("%s/%s/%s [multirestore] %s",
		ev.Tablet.Keyspace, ev.Tablet.Shard, ev.Tablet.Alias, ev.Status)
}

var _ syslogger.Syslogger = (*MultiRestore)(nil) // compile-time interface check
