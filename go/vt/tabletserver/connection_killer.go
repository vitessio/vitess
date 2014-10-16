package tabletserver

import (
	"fmt"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/dbconnpool"
)

// ConnectionKiller is used for killing MySQL connections
type ConnectionKiller struct {
	connPool *dbconnpool.ConnectionPool
}

// NewConnectionKiller creates a new ConnectionKiller
func NewConnectionKiller(poolSize int, idleTimeout time.Duration) *ConnectionKiller {
	return &ConnectionKiller{
		connPool: dbconnpool.NewConnectionPool("ConnKiller", poolSize, idleTimeout),
	}
}

// Open opens the connection pool used to kill other connections
func (ck *ConnectionKiller) Open(ConnFactory dbconnpool.CreateConnectionFunc) {
	ck.connPool.Open(ConnFactory)
}

// Close closes the underlying connection pool
func (ck *ConnectionKiller) Close() {
	ck.connPool.Close()
}

// Kill kills the specified connection
func (ck *ConnectionKiller) Kill(connID int64) error {
	killStats.Add("Queries", 1)
	log.Infof("killing query %d", connID)
	killConn := getOrPanic(ck.connPool)
	defer killConn.Recycle()
	sql := fmt.Sprintf("kill %d", connID)
	var err error
	if _, err = killConn.ExecuteFetch(sql, 10000, false); err != nil {
		log.Errorf("Could not kill query %d: %v", connID, err)
	}
	return err
}

// SetIdleTimeout sets the idle timeout for the underlying connection pool
func (ck *ConnectionKiller) SetIdleTimeout(idleTimeout time.Duration) {
	ck.connPool.SetIdleTimeout(idleTimeout)
}

// QueryDeadliner is meant to kill a query if it doesn't
// complete by the specified deadline.
type QueryDeadliner chan bool

// SetDeadline sets a deadline for the specifed connID. It also returns a QueryDeadliner.
// If Done is not called on QueryDeadliner before the deadline is reached, the connection
// is killed.
func (ck *ConnectionKiller) SetDeadline(connID int64, deadline Deadline) QueryDeadliner {
	timeout, err := deadline.Timeout()
	if err != nil {
		panic(NewTabletError(FAIL, "SetDeadline: %v", err))
	}
	if timeout == 0 {
		return nil
	}
	qd := make(QueryDeadliner)
	tmr := time.NewTimer(timeout)
	go func() {
		defer func() {
			tmr.Stop()
			if x := recover(); x != nil {
				internalErrors.Add("ConnKiller", 1)
				log.Errorf("ConnectionKiller error: %v", x)
			}
		}()
		select {
		case <-tmr.C:
			_ = ck.Kill(connID)
		case <-qd:
		}
	}()
	return qd
}

// Done informs the ConnectionKiller that the query completed successfully.
// If this happens before the deadline, the query will not be killed.
// Done should not be called more than once.
func (qd QueryDeadliner) Done() {
	close(qd)
}
