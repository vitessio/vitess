// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/rpcwrap/proto"
	"code.google.com/p/vitess/go/sqltypes"
	"code.google.com/p/vitess/go/streamlog"
)

var SqlQueryLogger = streamlog.New("SqlQuery", 50)

const (
	QUERY_SOURCE_ROWCACHE = 1 << iota
	QUERY_SOURCE_CONSOLIDATOR
	QUERY_SOURCE_MYSQL
)

type sqlQueryStats struct {
	Method               string
	PlanType             string
	OriginalSql          string
	BindVariables        map[string]interface{}
	rewrittenSqls        []string
	RowsAffected         int
	NumberOfQueries      int
	StartTime            time.Time
	EndTime              time.Time
	MysqlResponseTime    time.Duration
	WaitingForConnection time.Duration
	CacheHits            int64
	CacheAbsent          int64
	CacheMisses          int64
	CacheInvalidations   int64
	QuerySources         byte
	Rows                 [][]sqltypes.Value
	context              *proto.Context
}

func newSqlQueryStats(methodName string, context *proto.Context) *sqlQueryStats {
	s := &sqlQueryStats{Method: methodName, StartTime: time.Now(), context: context}
	return s
}

func (stats *sqlQueryStats) Send() {
	stats.EndTime = time.Now()
	SqlQueryLogger.Send(stats)
}

func (stats *sqlQueryStats) AddRewrittenSql(sql string) {
	stats.rewrittenSqls = append(stats.rewrittenSqls, sql)
}

func (stats *sqlQueryStats) TotalTime() time.Duration {
	return stats.EndTime.Sub(stats.StartTime)
}

// RewrittenSql returns a semicolon separated list of SQL statements
// that were executed.
func (stats *sqlQueryStats) RewrittenSql() string {
	return strings.Join(stats.rewrittenSqls, "; ")
}

// SizeOfResponse returns the approximate size of the response in
// bytes (this does not take in account BSON encoding). It will return
// 0 for streaming requests.
func (stats *sqlQueryStats) SizeOfResponse() int {
	if stats.Rows == nil {
		return 0
	}
	size := 0
	for _, row := range stats.Rows {
		for _, field := range row {
			size += len(field.Raw())
		}
	}
	return size
}

// FmtBindVariables returns the map of bind variables as JSON. For
// values that are strings or byte slices it only reports their type
// and length.
func (stats *sqlQueryStats) FmtBindVariables(full bool) string {
	var out map[string]interface{}
	if full {
		out = stats.BindVariables
	} else {
		// NOTE(szopa): I am getting rid of potentially large bind
		// variables.
		out := make(map[string]interface{})
		for k, v := range stats.BindVariables {
			switch val := v.(type) {
			case string:
				out[k] = fmt.Sprintf("string %v", len(val))
			case []byte:
				out[k] = fmt.Sprintf("bytes %v", len(val))
			default:
				out[k] = v
			}
		}
	}
	b, err := json.Marshal(out)
	if err != nil {
		relog.Warning("could not marshal %q", stats.BindVariables)
		return ""
	}
	return string(b)
}

// FmtQuerySources returns a comma separated list of query
// sources. If there were no query sources, it returns the string
// "none".
func (stats *sqlQueryStats) FmtQuerySources() string {
	if stats.QuerySources == 0 {
		return "none"
	}
	sources := make([]string, 3)
	n := 0
	if stats.QuerySources&QUERY_SOURCE_MYSQL != 0 {
		sources[n] = "mysql"
		n++
	}
	if stats.QuerySources&QUERY_SOURCE_ROWCACHE != 0 {
		sources[n] = "rowcache"
		n++
	}
	if stats.QuerySources&QUERY_SOURCE_CONSOLIDATOR != 0 {
		sources[n] = "consolidator"
		n++
	}
	return strings.Join(sources[:n], ",")
}

func (log *sqlQueryStats) RemoteAddr() string {
	return log.context.RemoteAddr
}

func (log *sqlQueryStats) Username() string {
	return log.context.Username
}

// String returns a tab separated list of logged fields.
func (log *sqlQueryStats) Format(params url.Values) string {
	_, fullBindParams := params["full"]
	return fmt.Sprintf(
		"%v\t%v\t%v\t%v\t%v\t%v\t%v\t%q\t%v\t%v\t%q\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t\n",
		log.Method,
		log.RemoteAddr(),
		log.Username(),
		log.StartTime,
		log.EndTime,
		log.TotalTime().Seconds(),
		log.PlanType,
		log.OriginalSql,
		log.FmtBindVariables(fullBindParams),
		log.NumberOfQueries,
		log.RewrittenSql(),
		log.FmtQuerySources(),
		log.MysqlResponseTime.Seconds(),
		log.WaitingForConnection.Seconds(),
		log.SizeOfResponse(),
		log.CacheHits,
		log.CacheMisses,
		log.CacheAbsent,
		log.CacheInvalidations)
}
