package tabletserver

import (
	"fmt"
	"html/template"
	"sort"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/context"
)

// QueryDetail is a simple wrapper for Query, Context and PoolConnection
type QueryDetail struct {
	query   string
	context context.Context
	connID  int64
	start   time.Time
}

// NewQueryDetail creates a new QueryDetail
func NewQueryDetail(query string, context context.Context, connID int64) *QueryDetail {
	return &QueryDetail{query: query, context: context, connID: connID, start: time.Now()}
}

// QueryList holds a thread safe list of QueryDetails
type QueryList struct {
	mu           sync.Mutex
	queryDetails map[int64]*QueryDetail
	connKiller   *ConnectionKiller
}

// NewQueryList creates a new QueryList
func NewQueryList(connKiller *ConnectionKiller) *QueryList {
	return &QueryList{queryDetails: make(map[int64]*QueryDetail), connKiller: connKiller}
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

// Terminate updates the query status and kills the connection
func (ql *QueryList) Terminate(connID int64) error {
	ql.mu.Lock()
	defer ql.mu.Unlock()
	qd := ql.queryDetails[connID]
	if qd == nil {
		return fmt.Errorf("query %v not found", connID)
	}
	if err := ql.connKiller.Kill(connID); err != nil {
		return err
	}
	return nil
}

// TerminateAll terminates all queries and kills the MySQL connections
func (ql *QueryList) TerminateAll() {
	ql.mu.Lock()
	defer ql.mu.Unlock()
	for _, qd := range ql.queryDetails {
		ql.connKiller.Kill(qd.connID)
	}
}

// QueryDetailzRow is used for rendering QueryDetail in a template
type QueryDetailzRow struct {
	Query             string
	ContextHTML       template.HTML
	Start             time.Time
	Duration          time.Duration
	ConnID            int64
	State             string
	ShowTerminateLink bool
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
			Query:       qd.query,
			ContextHTML: callinfo.FromContext(qd.context).HTML(),
			Start:       qd.start,
			Duration:    time.Now().Sub(qd.start),
			ConnID:      qd.connID,
		}
		rows = append(rows, row)
	}
	ql.mu.Unlock()
	sort.Sort(byStartTime(rows))
	return rows
}
