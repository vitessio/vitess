/*
Copyright 2017 Google Inc.

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

package vtgate

import (
	"fmt"
	"html/template"
	"io"
	"net/url"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// LogStats records the stats for a single vtgate query
type LogStats struct {
	Ctx           context.Context
	Method        string
	Target        *querypb.Target
	StmtType      string
	SQL           string
	BindVariables map[string]*querypb.BindVariable
	StartTime     time.Time
	EndTime       time.Time
	ShardQueries  uint32
	RowsAffected  uint64
	PlanTime      time.Duration
	ExecuteTime   time.Duration
	CommitTime    time.Duration
	Error         error
}

// NewLogStats constructs a new LogStats with supplied Method and ctx
// field values, and the StartTime field set to the present time.
func NewLogStats(ctx context.Context, methodName, sql string, bindVars map[string]*querypb.BindVariable) *LogStats {
	return &LogStats{
		Ctx:           ctx,
		Method:        methodName,
		SQL:           sql,
		BindVariables: bindVars,
		StartTime:     time.Now(),
	}
}

// Send finalizes a record and sends it
func (stats *LogStats) Send() {
	stats.EndTime = time.Now()
	QueryLogger.Send(stats)
	TimingStatistics.recordStats(stats)
}

// Context returns the context used by LogStats.
func (stats *LogStats) Context() context.Context {
	return stats.Ctx
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
func (stats *LogStats) ContextHTML() template.HTML {
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
	formattedBindVars := "\"[REDACTED]\""
	if !*streamlog.RedactDebugUIQueries {
		_, fullBindParams := params["full"]
		formattedBindVars = sqltypes.FormatBindVariables(
			stats.BindVariables,
			fullBindParams,
			*streamlog.QueryLogFormat == streamlog.QueryLogFormatJSON,
		)
	}

	// TODO: remove username here we fully enforce immediate caller id
	remoteAddr, username := stats.RemoteAddrUsername()

	var fmtString string
	switch *streamlog.QueryLogFormat {
	case streamlog.QueryLogFormatText:
		fmtString = "%v\t%v\t%v\t'%v'\t'%v'\t%v\t%v\t%.6f\t%.6f\t%.6f\t%.6f\t%v\t%q\t%v\t%v\t%v\t%q\t\n"
	case streamlog.QueryLogFormatJSON:
		fmtString = "{\"Method\": %q, \"RemoteAddr\": %q, \"Username\": %q, \"ImmediateCaller\": %q, \"Effective Caller\": %q, \"Start\": \"%v\", \"End\": \"%v\", \"TotalTime\": %.6f, \"PlanTime\": %v, \"ExecuteTime\": %v, \"CommitTime\": %v, \"StmtType\": %q, \"SQL\": %q, \"BindVars\": %v, \"ShardQueries\": %v, \"RowsAffected\": %v, \"Error\": %q}\n"
	}

	_, err := fmt.Fprintf(
		w,
		fmtString,
		stats.Method,
		remoteAddr,
		username,
		stats.ImmediateCaller(),
		stats.EffectiveCaller(),
		stats.StartTime.Format("2006-01-02 15:04:05.000000"),
		stats.EndTime.Format("2006-01-02 15:04:05.000000"),
		stats.TotalTime().Seconds(),
		stats.PlanTime.Seconds(),
		stats.ExecuteTime.Seconds(),
		stats.CommitTime.Seconds(),
		stats.StmtType,
		stats.SQL,
		formattedBindVars,
		stats.ShardQueries,
		stats.RowsAffected,
		stats.ErrorStr(),
	)
	return err
}
