package events

import (
	"fmt"
	"github.com/youtube/vitess/go/event/syslogger"
	"log/syslog"
)

func (r *Reparent) Syslog(w *syslog.Writer) {
	w.Info(fmt.Sprintf("%s/%s [reparent %v -> %v] %s",
		r.Keyspace, r.Shard, r.OldMaster.Alias, r.NewMaster.Alias, r.Status))
}

var _ syslogger.Syslogger = (*Reparent)(nil) // compile-time interface check
