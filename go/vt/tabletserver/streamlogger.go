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

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/streamlog"
)

var SqlQueryLogger = streamlog.New("SqlQuery", 50)

const (
	QUERY_SOURCE_ROWCACHE = 1 << iota
	QUERY_SOURCE_CONSOLIDATOR
	QUERY_SOURCE_MYSQL
)

type SQLQueryStats struct {
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
	context              Context
	TransactionID        int64
}

func newSqlQueryStats(methodName string, context Context) *SQLQueryStats {
	return &SQLQueryStats{
		Method:    methodName,
		StartTime: time.Now(),
		context:   context,
	}
}

func (stats *SQLQueryStats) Send() {
	stats.EndTime = time.Now()
	SqlQueryLogger.Send(stats)
}

func (stats *SQLQueryStats) AddRewrittenSql(sql string) {
	stats.rewrittenSqls = append(stats.rewrittenSqls, sql)
}

func (stats *SQLQueryStats) TotalTime() time.Duration {
	return stats.EndTime.Sub(stats.StartTime)
}

// RewrittenSql returns a semicolon separated list of SQL statements
// that were executed.
func (stats *SQLQueryStats) RewrittenSql() string {
	return strings.Join(stats.rewrittenSqls, "; ")
}

// SizeOfResponse returns the approximate size of the response in
// bytes (this does not take in account BSON encoding). It will return
// 0 for streaming requests.
func (stats *SQLQueryStats) SizeOfResponse() int {
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
func (stats *SQLQueryStats) FmtBindVariables(full bool) string {
	var out map[string]interface{}
	if full {
		out = stats.BindVariables
	} else {
		// NOTE(szopa): I am getting rid of potentially large bind
		// variables.
		out = make(map[string]interface{})
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
		log.Warningf("could not marshal %q", stats.BindVariables)
		return ""
	}
	return string(b)
}

// FmtQuerySources returns a comma separated list of query
// sources. If there were no query sources, it returns the string
// "none".
func (stats *SQLQueryStats) FmtQuerySources() string {
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

func (log *SQLQueryStats) RemoteAddr() string {
	return log.context.GetRemoteAddr()
}

func (log *SQLQueryStats) Username() string {
	return log.context.GetUsername()
}

// String returns a tab separated list of logged fields.
func (log *SQLQueryStats) Format(params url.Values) string {
	_, fullBindParams := params["full"]
	return fmt.Sprintf(
		"%v\t%v\t%v\t%v\t%v\t%.6f\t%v\t%q\t%v\t%v\t%q\t%v\t%.6f\t%.6f\t%v\t%v\t%v\t%v\t%v\t\n",
		log.Method,
		log.RemoteAddr(),
		log.Username(),
		log.StartTime.Format(time.StampMicro),
		log.EndTime.Format(time.StampMicro),
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
