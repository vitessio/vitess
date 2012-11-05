package tabletserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/rpcwrap/proto"
	"code.google.com/p/vitess/go/sqltypes"
	"code.google.com/p/vitess/go/streamlog"
)

var sqlQueryLogger = streamlog.New("/debug/vt/querylog", 50)

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
	rewrittenSqls        [][]byte
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

func newSqlQueryStats(methodName string) *sqlQueryStats {
	s := &sqlQueryStats{Method: methodName}
	return s
}

func (stats *sqlQueryStats) AddRewrittenSql(sql []byte) {
	stats.rewrittenSqls = append(stats.rewrittenSqls, sql)
}

func (stats *sqlQueryStats) TotalTime() time.Duration {
	return stats.EndTime.Sub(stats.StartTime)
}

// RewrittenSql returns a semicolon separated list of SQL statements
// that were executed.
func (stats *sqlQueryStats) RewrittenSql() string {
	return string(bytes.Join(stats.rewrittenSqls, []byte("; ")))
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
func (stats *sqlQueryStats) FmtBindVariables() string {
	// NOTE(szopa): I am getting rid of potentially large bind
	// variables.
	scrubbed := make(map[string]interface{})
	for k, v := range stats.BindVariables {
		switch val := v.(type) {
		case string:
			scrubbed[k] = fmt.Sprintf("string %v", len(val))
		case []byte:
			scrubbed[k] = fmt.Sprintf("bytes %v", len(val))
		default:
			scrubbed[k] = v
		}
	}
	b, err := json.Marshal(scrubbed)
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

func (log sqlQueryStats) RemoteAddr() string {
	return log.context.RemoteAddr
}

func (log sqlQueryStats) Username() string {
	return log.context.Username
}

//String returns a tab separated list of logged fields.
func (log sqlQueryStats) String() string {
	return fmt.Sprintf(
		"%v\t%v\t%v\t%v\t%v\t%v\t%v\t%q\t%v\t%v\t%q\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t",
		log.Method,
		log.RemoteAddr(),
		log.Username(),
		log.StartTime,
		log.EndTime,
		log.TotalTime(),
		log.PlanType,
		log.OriginalSql,
		log.FmtBindVariables(),
		log.NumberOfQueries,
		log.RewrittenSql(),
		log.FmtQuerySources(),
		log.MysqlResponseTime,
		log.SizeOfResponse(),
		log.CacheHits,
		log.CacheMisses,
		log.CacheAbsent,
		log.CacheInvalidations)
}
