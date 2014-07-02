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
