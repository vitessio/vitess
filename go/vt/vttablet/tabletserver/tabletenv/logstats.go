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

package tabletenv

import (
	"bytes"
	"fmt"
	"html/template"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/streamlog"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

const (
	// QuerySourceConsolidator means query result is found in consolidator.
	QuerySourceConsolidator = 1 << iota
	// QuerySourceMySQL means query result is returned from MySQL.
	QuerySourceMySQL
)

// LogStats records the stats for a single query
type LogStats struct {
	Ctx                  context.Context
	Method               string
	Target               *querypb.Target
	PlanType             string
	OriginalSQL          string
	BindVariables        map[string]*querypb.BindVariable
	rewrittenSqls        []string
	RowsAffected         int
	NumberOfQueries      int
	StartTime            time.Time
	EndTime              time.Time
	MysqlResponseTime    time.Duration
	WaitingForConnection time.Duration
	QuerySources         byte
	Rows                 [][]sqltypes.Value
	TransactionID        int64
	Error                error
}

// NewLogStats constructs a new LogStats with supplied Method and ctx
// field values, and the StartTime field set to the present time.
func NewLogStats(ctx context.Context, methodName string) *LogStats {
	return &LogStats{
		Ctx:       ctx,
		Method:    methodName,
		StartTime: time.Now(),
	}
}

// Send finalizes a record and sends it
func (stats *LogStats) Send() {
	stats.EndTime = time.Now()
	StatsLogger.Send(stats)
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

// AddRewrittenSQL adds a single sql statement to the rewritten list
func (stats *LogStats) AddRewrittenSQL(sql string, start time.Time) {
	stats.QuerySources |= QuerySourceMySQL
	stats.NumberOfQueries++
	stats.rewrittenSqls = append(stats.rewrittenSqls, sql)
	stats.MysqlResponseTime += time.Now().Sub(start)
}

// TotalTime returns how long this query has been running
func (stats *LogStats) TotalTime() time.Duration {
	return stats.EndTime.Sub(stats.StartTime)
}

// RewrittenSQL returns a semicolon separated list of SQL statements
// that were executed.
func (stats *LogStats) RewrittenSQL() string {
	return strings.Join(stats.rewrittenSqls, "; ")
}

// SizeOfResponse returns the approximate size of the response in
// bytes (this does not take in account protocol encoding). It will return
// 0 for streaming requests.
func (stats *LogStats) SizeOfResponse() int {
	if stats.Rows == nil {
		return 0
	}
	size := 0
	for _, row := range stats.Rows {
		for _, field := range row {
			size += field.Len()
		}
	}
	return size
}

// FmtBindVariables returns the map of bind variables as a string or a json
// string depending on the streamlog.QueryLogFormat value. If RedactDebugUIQueries
// is true then this returns the string "[REDACTED]"
//
// For values that are strings or byte slices it only reports their type
// and length unless full is true.
func (stats *LogStats) FmtBindVariables(full bool) string {
	if *streamlog.RedactDebugUIQueries {
		return "\"[REDACTED]\""
	}

	var out map[string]*querypb.BindVariable
	if full {
		out = stats.BindVariables
	} else {
		// NOTE(szopa): I am getting rid of potentially large bind
		// variables.
		out = make(map[string]*querypb.BindVariable)
		for k, v := range stats.BindVariables {
			if sqltypes.IsIntegral(v.Type) || sqltypes.IsFloat(v.Type) {
				out[k] = v
			} else {
				out[k] = sqltypes.StringBindVariable(fmt.Sprintf("%v bytes", len(v.Value)))
			}
		}
	}

	if *streamlog.QueryLogFormat == streamlog.QueryLogFormatJSON {
		var buf bytes.Buffer
		buf.WriteString("{")
		first := true
		for k, v := range out {
			if !first {
				buf.WriteString(", ")
			} else {
				first = false
			}
			if sqltypes.IsIntegral(v.Type) || sqltypes.IsFloat(v.Type) {
				fmt.Fprintf(&buf, "%q: {\"type\": %q, \"value\": %v}", k, v.Type, string(v.Value))
			} else {
				fmt.Fprintf(&buf, "%q: {\"type\": %q, \"value\": %q}", k, v.Type, string(v.Value))
			}
		}
		buf.WriteString("}")
		return buf.String()
	}

	return fmt.Sprintf("%v", out)
}

// FmtQuerySources returns a comma separated list of query
// sources. If there were no query sources, it returns the string
// "none".
func (stats *LogStats) FmtQuerySources() string {
	if stats.QuerySources == 0 {
		return "none"
	}
	sources := make([]string, 2)
	n := 0
	if stats.QuerySources&QuerySourceMySQL != 0 {
		sources[n] = "mysql"
		n++
	}
	if stats.QuerySources&QuerySourceConsolidator != 0 {
		sources[n] = "consolidator"
		n++
	}
	return strings.Join(sources[:n], ",")
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

// Format returns a tab separated list of logged fields.
func (stats *LogStats) Format(params url.Values) string {
	rewrittenSQL := "[REDACTED]"
	if !*streamlog.RedactDebugUIQueries {
		rewrittenSQL = stats.RewrittenSQL()
	}

	_, fullBindParams := params["full"]
	formattedBindVars := stats.FmtBindVariables(fullBindParams)

	// TODO: remove username here we fully enforce immediate caller id
	remoteAddr, username := stats.RemoteAddrUsername()

	// Valid options for the QueryLogFormat are text or json
	var fmtString string
	switch *streamlog.QueryLogFormat {
	case streamlog.QueryLogFormatText:
		fmtString = "%v\t%v\t%v\t'%v'\t'%v'\t%v\t%v\t%.6f\t%v\t%q\t%v\t%v\t%q\t%v\t%.6f\t%.6f\t%v\t%v\t%q\t\n"
	case streamlog.QueryLogFormatJSON:
		fmtString = "{\"Method\": %q, \"RemoteAddr\": %q, \"Username\": %q, \"ImmediateCaller\": %q, \"Effective Caller\": %q, \"Start\": \"%v\", \"End\": \"%v\", \"TotalTime\": %.6f, \"PlanType\": %q, \"OriginalSQL\": %q, \"BindVars\": %v, \"Queries\": %v, \"RewrittenSQL\": %q, \"QuerySources\": %q, \"MysqlTime\": %.6f, \"ConnWaitTime\": %.6f, \"RowsAffected\": %v, \"ResponseSize\": %v, \"Error\": %q}\n"
	}

	return fmt.Sprintf(
		fmtString,
		stats.Method,
		remoteAddr,
		username,
		stats.ImmediateCaller(),
		stats.EffectiveCaller(),
		stats.StartTime.Format(time.StampMicro),
		stats.EndTime.Format(time.StampMicro),
		stats.TotalTime().Seconds(),
		stats.PlanType,
		stats.OriginalSQL,
		formattedBindVars,
		stats.NumberOfQueries,
		rewrittenSQL,
		stats.FmtQuerySources(),
		stats.MysqlResponseTime.Seconds(),
		stats.WaitingForConnection.Seconds(),
		stats.RowsAffected,
		stats.SizeOfResponse(),
		stats.ErrorStr(),
	)
}
