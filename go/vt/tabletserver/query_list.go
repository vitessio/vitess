package tabletserver

import (
	"sort"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

// QueryDetail is a simple wrapper for Query, Context and PoolConnection
type QueryDetail struct {
	query   *proto.Query
	context *Context
	connID  int64
	start   time.Time
}

// NewQueryDetail creates a new QueryDetail
func NewQueryDetail(query *proto.Query, context *Context, connID int64) *QueryDetail {
	return &QueryDetail{query: query, context: context, connID: connID, start: time.Now()}
}

// QueryList holds a thread safe list of QueryDetails
type QueryList struct {
	mu           sync.Mutex
	queryDetails map[int64]*QueryDetail
}

// NewQueryList creates a new QueryList
func NewQueryList() *QueryList {
	return &QueryList{queryDetails: make(map[int64]*QueryDetail)}
}

// Add adds a QueryDetail to QueryList
func (ql *QueryList) Add(qd *QueryDetail) {
	ql.mu.Lock()
	defer ql.mu.Unlock()
	ql.queryDetails[qd.connID] = qd
}

// Remove removes a QueryDetail from QueryList
func (ql *QueryList) Remove(qd *QueryDetail) {
	ql.mu.Lock()
	defer ql.mu.Unlock()
	delete(ql.queryDetails, qd.connID)
}

// QueryDetailzRow is used for rendering QueryDetail in a template
type QueryDetailzRow struct {
	Query         string
	RemoteAddr    string
	Username      string
	Start         time.Time
	Duration      time.Duration
	SessionID     int64
	TransactionID int64
	ConnID        int64
}

type byStartTime []QueryDetailzRow

func (a byStartTime) Len() int           { return len(a) }
func (a byStartTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byStartTime) Less(i, j int) bool { return a[i].Start.Before(a[j].Start) }

// GetQueryzRows returns a list of QueryDetailzRow sorted by start time
func (ql *QueryList) GetQueryzRows() []QueryDetailzRow {
	ql.mu.Lock()
	rows := []QueryDetailzRow{}
	for _, qd := range ql.queryDetails {
		row := QueryDetailzRow{
			Query:         qd.query.Sql,
			RemoteAddr:    qd.context.RemoteAddr,
			Username:      qd.context.Username,
			Start:         qd.start,
			Duration:      time.Now().Sub(qd.start),
			SessionID:     qd.query.SessionId,
			TransactionID: qd.query.TransactionId,
			ConnID:        qd.connID,
		}
		rows = append(rows, row)
	}
	ql.mu.Unlock()
	sort.Sort(byStartTime(rows))
	return rows
}
