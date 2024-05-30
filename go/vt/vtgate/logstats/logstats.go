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

package logstats

import (
	"context"
	"io"
	"net/url"
	"time"

	"github.com/google/safehtml"

	"vitess.io/vitess/go/logstats"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// LogStats records the stats for a single vtgate query
type LogStats struct {
	Ctx            context.Context
	Method         string
	TabletType     string
	StmtType       string
	SQL            string
	BindVariables  map[string]*querypb.BindVariable
	StartTime      time.Time
	EndTime        time.Time
	ShardQueries   uint64
	RowsAffected   uint64
	RowsReturned   uint64
	PlanTime       time.Duration
	ExecuteTime    time.Duration
	CommitTime     time.Duration
	Error          error
	TablesUsed     []string
	SessionUUID    string
	CachedPlan     bool
	ActiveKeyspace string // ActiveKeyspace is the selected keyspace `use ks`
}

// NewLogStats constructs a new LogStats with supplied Method and ctx
// field values, and the StartTime field set to the present time.
func NewLogStats(ctx context.Context, methodName, sql, sessionUUID string, bindVars map[string]*querypb.BindVariable) *LogStats {
	return &LogStats{
		Ctx:           ctx,
		Method:        methodName,
		SQL:           sql,
		SessionUUID:   sessionUUID,
		BindVariables: bindVars,
		StartTime:     time.Now(),
	}
}

// SaveEndTime sets the end time of this request to now
func (stats *LogStats) SaveEndTime() {
	stats.EndTime = time.Now()
}

// ImmediateCaller returns the immediate caller stored in LogStats.Ctx
func (stats *LogStats) ImmediateCaller() string {
	return callerid.GetUsername(callerid.ImmediateCallerIDFromContext(stats.Ctx))
}

// EffectiveCaller returns the effective caller stored in LogStats.Ctx
func (stats *LogStats) EffectiveCaller() string {
	return callerid.GetPrincipal(callerid.EffectiveCallerIDFromContext(stats.Ctx))
}

// EventTime returns the time the event was created.
func (stats *LogStats) EventTime() time.Time {
	return stats.EndTime
}

// TotalTime returns how long this query has been running
func (stats *LogStats) TotalTime() time.Duration {
	return stats.EndTime.Sub(stats.StartTime)
}

// ContextHTML returns the HTML version of the context that was used, or "".
// This is a method on LogStats instead of a field so that it doesn't need
// to be passed by value everywhere.
func (stats *LogStats) ContextHTML() safehtml.HTML {
	return callinfo.HTMLFromContext(stats.Ctx)
}

// ErrorStr returns the error string or ""
func (stats *LogStats) ErrorStr() string {
	if stats.Error != nil {
		return stats.Error.Error()
	}
	return ""
}

// RemoteAddrUsername returns some parts of CallInfo if set
func (stats *LogStats) RemoteAddrUsername() (string, string) {
	ci, ok := callinfo.FromContext(stats.Ctx)
	if !ok {
		return "", ""
	}
	return ci.RemoteAddr(), ci.Username()
}

// Logf formats the log record to the given writer, either as
// tab-separated list of logged fields or as JSON.
func (stats *LogStats) Logf(w io.Writer, params url.Values) error {
	if !streamlog.ShouldEmitLog(stats.SQL, stats.RowsAffected, stats.RowsReturned) {
		return nil
	}

	redacted := streamlog.GetRedactDebugUIQueries()
	_, fullBindParams := params["full"]
	remoteAddr, username := stats.RemoteAddrUsername()

	log := logstats.NewLogger()
	log.Init(streamlog.GetQueryLogFormat() == streamlog.QueryLogFormatJSON)
	log.Key("Method")
	log.StringUnquoted(stats.Method)
	log.Key("RemoteAddr")
	log.StringUnquoted(remoteAddr)
	log.Key("Username")
	log.StringUnquoted(username)
	log.Key("ImmediateCaller")
	log.StringSingleQuoted(stats.ImmediateCaller())
	log.Key("Effective Caller")
	log.StringSingleQuoted(stats.EffectiveCaller())
	log.Key("Start")
	log.Time(stats.StartTime)
	log.Key("End")
	log.Time(stats.EndTime)
	log.Key("TotalTime")
	log.Duration(stats.TotalTime())
	log.Key("PlanTime")
	log.Duration(stats.PlanTime)
	log.Key("ExecuteTime")
	log.Duration(stats.ExecuteTime)
	log.Key("CommitTime")
	log.Duration(stats.CommitTime)
	log.Key("StmtType")
	log.StringUnquoted(stats.StmtType)
	log.Key("SQL")
	log.String(stats.SQL)
	log.Key("BindVars")
	if redacted {
		log.Redacted()
	} else {
		log.BindVariables(stats.BindVariables, fullBindParams)
	}
	log.Key("ShardQueries")
	log.Uint(stats.ShardQueries)
	log.Key("RowsAffected")
	log.Uint(stats.RowsAffected)
	log.Key("Error")
	log.String(stats.ErrorStr())
	log.Key("TabletType")
	log.String(stats.TabletType)
	log.Key("SessionUUID")
	log.String(stats.SessionUUID)
	log.Key("Cached Plan")
	log.Bool(stats.CachedPlan)
	log.Key("TablesUsed")
	log.Strings(stats.TablesUsed)
	log.Key("ActiveKeyspace")
	log.String(stats.ActiveKeyspace)

	return log.Flush(w)
}
