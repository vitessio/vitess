// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package events

import (
	"fmt"
	"log/syslog"

	"github.com/youtube/vitess/go/event/syslogger"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
)

// Syslog writes a Reparent event to syslog.
func (r *Reparent) Syslog() (syslog.Priority, string) {
	return syslog.LOG_INFO, fmt.Sprintf("%s/%s [reparent %v -> %v] %s (%s)",
		r.ShardInfo.Keyspace(), r.ShardInfo.ShardName(),
		topoproto.TabletAliasString(r.OldMaster.Alias),
		topoproto.TabletAliasString(r.NewMaster.Alias),
		r.Status, r.ExternalID)
}

var _ syslogger.Syslogger = (*Reparent)(nil) // compile-time interface check
