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
	"bytes"
	"fmt"
	"html/template"
	"net/url"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/streamlog"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
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

// FmtBindVariables returns the map of bind variables as JSON. For
// values that are strings or byte slices it only reports their type
// and length.
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
	_, fullBindParams := params["full"]
	formattedBindVars := stats.FmtBindVariables(fullBindParams)

	// TODO: remove username here we fully enforce immediate caller id
	remoteAddr, username := stats.RemoteAddrUsername()

	var fmtString string
	switch *streamlog.QueryLogFormat {
	case streamlog.QueryLogFormatText:
		fmtString = "%v\t%v\t%v\t'%v'\t'%v'\t%v\t%v\t%.6f\t%.6f\t%.6f\t%.6f\t%v\t%q\t%v\t%v\t%v\t%q\t\n"
	case streamlog.QueryLogFormatJSON:
		fmtString = "{\"Method\": %q, \"RemoteAddr\": %q, \"Username\": %q, \"ImmediateCaller\": %q, \"Effective Caller\": %q, \"Start\": \"%v\", \"End\": \"%v\", \"TotalTime\": %.6f, \"PlanTime\": %v, \"ExecuteTime\": %v, \"CommitTime\": %v, \"StmtType\": %q, \"SQL\": %q, \"BindVars\": %v, \"ShardQueries\": %v, \"RowsAffected\": %v, \"Error\": %q}\n"
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
}
