package messages

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sync"

	"github.com/youtube/vitess/go/vt/vitessdriver"
)

// subscription allows users to interact with the queue
type subscription struct {
	mu sync.RWMutex

	// this is true while a Subscription is live
	isOpen bool

	cancelFunc context.CancelFunc

	// GetMessage sends through scan targets for rows.Scan
	destChan chan []interface{}
	// this returns the rows.Scan error back to GetMessage
	errChan chan error

	// Subscribe requires unique connection string properties,
	// so we will manage that on behalf of the user
	db *sql.DB

	// store db connection details
	dbConfig vitessdriver.Configuration

	streamSQL string
	ackSQL    string
	failSQL   string
}

// Open connects to an underlying Vitess cluster and streams messages. The queue will
// buffer the defined max concurrent number of messages in memory and will block until
// one of the messages is acknowledged.
//  using the vitessdriver for database/sql.
// Only a single connection is opened and it remains open until Close is called.
// Context cancellation is respected
func (q *Queue) Open(ctx context.Context, address, target string) error {
	q.s.mu.Lock()
	defer q.s.mu.Unlock()

	if q.s.isOpen {
		return nil
	}

	// generate the raw subscription
	q.s = q.newSubscription(q.maxConcurrent)

	// create sub-context for subscribe goroutine with cancelFunc for cleanup
	var newCtx context.Context
	newCtx, q.s.cancelFunc = context.WithCancel(ctx)

	// open a direct database connection if needed
	if err := q.s.openDB(newCtx, address, target); err != nil {
		return err
	}

	// start a streaming query that will run indefinitely
	rows, err := q.s.db.QueryContext(newCtx, q.s.streamSQL)
	if err != nil {
		return err
	}

	// this goroutine will be waiting for rows and is the only writer to the channel
	go func() {
		defer rows.Close()

		// we don't need to check for context cancellation, because that is already
		// happening behind the scenes in the Vitess database/sql driver
		for rows.Next() {
			// get a pointer to the scanData struct provided by Get
			dest := <-q.s.destChan
			// scan into the provided destination fields and also set the error
			// this is a synchronous call, so isn't a data race, while getting all the data back to Get
			q.s.errChan <- rows.Scan(dest...)
		}

		// close the channel before exiting
		close(q.s.destChan)
		close(q.s.errChan)
	}()

	return nil
}

func (q *Queue) newSubscription(maxConcurrent int) *subscription {
	s := &subscription{
		isOpen:    false,
		mu:        sync.RWMutex{},
		destChan:  make(chan []interface{}),
		errChan:   make(chan error),
		streamSQL: fmt.Sprintf("stream * from `%s`", q.name),
		ackSQL:    fmt.Sprintf("UPDATE `%s` SET time_acked=?, time_next=null WHERE id=? AND time_acked is null", q.name),
		failSQL:   fmt.Sprintf("UPDATE `%s` SET time_next=%d WHERE id=? AND time_acked is null", q.name, math.MaxInt64),
	}

	return s
}

func (s *subscription) openDB(ctx context.Context, address, target string) error {
	s.dbConfig = vitessdriver.Configuration{
		Address:   address,
		Target:    target,
		Streaming: true,
	}

	var err error
	s.db, err = vitessdriver.OpenWithConfiguration(s.dbConfig)
	if err != nil {
		return err
	}
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)

	return nil
}

// Close drains the processing channel and closes the connection to the database
// TODO: Nack all remaining messages
func (q *Queue) Close() error {
	q.s.mu.Lock()
	defer q.s.mu.Unlock()

	// if the connection isn't open, no further work required
	if !q.s.isOpen {
		return nil
	}

	q.s.isOpen = false

	// cancel context inside rows.Next()
	q.s.cancelFunc()

	return q.s.db.Close()
}
