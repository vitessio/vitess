package dbconnpool

import "context"

// PooledDBConnection re-exposes DBConnection to be used by ConnectionPool.
type PooledDBConnection struct {
	*DBConnection
	pool *ConnectionPool
}

// Recycle should be called to return the PooledDBConnection to the pool.
func (pc *PooledDBConnection) Recycle() {
	if pc.IsClosed() {
		pc.pool.Put(nil)
	} else {
		pc.pool.Put(pc)
	}
}

// Reconnect replaces the existing underlying connection with a new one,
// if possible. Recycle should still be called afterwards.
func (pc *PooledDBConnection) Reconnect(ctx context.Context) error {
	pc.DBConnection.Close()
	newConn, err := NewDBConnection(ctx, pc.pool.info)
	if err != nil {
		return err
	}
	pc.DBConnection = newConn
	return nil
}
